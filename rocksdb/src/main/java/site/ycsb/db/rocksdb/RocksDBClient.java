/*
 * Copyright (c) 2018 - 2019 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package site.ycsb.db.rocksdb;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import site.ycsb.*;
import site.ycsb.Status;
import net.jcip.annotations.GuardedBy;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import site.ycsb.measurements.Measurements;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * RocksDB binding for <a href="http://rocksdb.org/">RocksDB</a>.
 *
 * See {@code rocksdb/README.md} for details.
 */
public class RocksDBClient extends DB {

  static final String PROPERTY_ROCKSDB_DIR = "rocksdb.dir";
  static final String PROPERTY_ROCKSDB_OPTIONS_FILE = "rocksdb.optionsfile";
  private static final String COLUMN_FAMILY_NAMES_FILENAME = "CF_NAMES";
  private static final String CDC_LOG_FILE_NAME = "/tmp/cdc_log";
  private static final String STATISTIC_FILE_NAME = "/tmp/statistic";
  private static final int BATCH_SIZE = 512;

  private static final Logger LOGGER = LoggerFactory.getLogger(RocksDBClient.class);

  @GuardedBy("RocksDBClient.class") private static Path rocksDbDir = null;
  @GuardedBy("RocksDBClient.class") private static Path optionsFile = null;
  @GuardedBy("RocksDBClient.class") private static RocksObject dbOptions = null;
  @GuardedBy("RocksDBClient.class") private static WriteOptions writeOptions = null;
  @GuardedBy("RocksDBClient.class") private static ReadOptions readOptions = null;
  @GuardedBy("RocksDBClient.class") private static RocksDB rocksDb = null;
  @GuardedBy("RocksDBClient.class") private static int references = 0;
  @GuardedBy("RocksDBClient.class") private static FileOutputStream outputStream;
  @GuardedBy("RocksDBClient.class") private static ObjectMapper objectMapper;
  @GuardedBy("RocksDBClient.class") private static Statistics statistic;

  private static final ConcurrentMap<String, ColumnFamily> COLUMN_FAMILIES = new ConcurrentHashMap<>();
  private static final ConcurrentMap<String, Lock> COLUMN_FAMILY_LOCKS = new ConcurrentHashMap<>();
  private final List<byte[]> bufferKeys = new ArrayList<>();
  private final List<byte[]> bufferUpdateAfter = new ArrayList<>();
  private final Set<ByteBuffer> inBufferKeySet = new HashSet<>();
  private final List<Long> bufferUpdateTimeStamps = new ArrayList<>();

  @Override
  public void init() throws DBException {
    synchronized(RocksDBClient.class) {
      if(rocksDb == null) {
        rocksDbDir = Paths.get(getProperties().getProperty(PROPERTY_ROCKSDB_DIR));
        LOGGER.info("RocksDB data dir: " + rocksDbDir);

        String optionsFileString = getProperties().getProperty(PROPERTY_ROCKSDB_OPTIONS_FILE);
        if (optionsFileString != null) {
          optionsFile = Paths.get(optionsFileString);
          LOGGER.info("RocksDB options file: " + optionsFile);
        }

        try {
          if (optionsFile != null) {
            rocksDb = initRocksDBWithOptionsFile();
          } else {
            rocksDb = initRocksDB();
          }
        } catch (final IOException | RocksDBException e) {
          throw new DBException(e);
        }
        try {
          if (outputStream == null) {
            outputStream = new FileOutputStream(CDC_LOG_FILE_NAME);
          }
        } catch (final  IOException e) {
          throw new DBException(e);
        }

        writeOptions = new WriteOptions().setDisableWAL(true);
        readOptions = new ReadOptions();

        // set object mapper
        objectMapper = new ObjectMapper();
        // disable auto close
        objectMapper.getFactory().disable(JsonGenerator.Feature.AUTO_CLOSE_TARGET);
      }

      references++;
    }
  }

  /**
   * Initializes and opens the RocksDB database.
   *
   * Should only be called with a {@code synchronized(RocksDBClient.class)` block}.
   *
   * @return The initialized and open RocksDB instance.
   */
  private RocksDB initRocksDBWithOptionsFile() throws IOException, RocksDBException {
    if(!Files.exists(rocksDbDir)) {
      Files.createDirectories(rocksDbDir);
    }

    final DBOptions options = new DBOptions();
    final List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<>();
    final List<ColumnFamilyHandle> cfHandles = new ArrayList<>();

    final ConfigOptions configOptions =
        new ConfigOptions().setEnv(Env.getDefault());
    OptionsUtil.loadOptionsFromFile(
        configOptions, optionsFile.toAbsolutePath().toString(), options, cfDescriptors);
    // OptionsUtil.loadOptionsFromFile(
    // optionsFile.toAbsolutePath().toString(), Env.getDefault(), options, cfDescriptors);
    dbOptions = options;

    final RocksDB db = RocksDB.open(options, rocksDbDir.toAbsolutePath().toString(), cfDescriptors, cfHandles);

    for(int i = 0; i < cfDescriptors.size(); i++) {
      String cfName = new String(cfDescriptors.get(i).getName());
      final ColumnFamilyHandle cfHandle = cfHandles.get(i);
      final ColumnFamilyOptions cfOptions = cfDescriptors.get(i).getOptions();
      COLUMN_FAMILIES.put(cfName, new ColumnFamily(cfHandle, cfOptions));
    }

    return db;
  }

  /**
   * Initializes and opens the RocksDB database.
   *
   * Should only be called with a {@code synchronized(RocksDBClient.class)` block}.
   *
   * @return The initialized and open RocksDB instance.
   */
  private RocksDB initRocksDB() throws IOException, RocksDBException {
    if(!Files.exists(rocksDbDir)) {
      Files.createDirectories(rocksDbDir);
    }

    final List<String> cfNames = loadColumnFamilyNames();
    final List<ColumnFamilyOptions> cfOptionss = new ArrayList<>();
    final List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<>();

    for(final String cfName : cfNames) {
      final ColumnFamilyOptions cfOptions = new ColumnFamilyOptions()
          .optimizeLevelStyleCompaction();
      final ColumnFamilyDescriptor cfDescriptor = new ColumnFamilyDescriptor(
          cfName.getBytes(UTF_8),
          cfOptions
      );
      cfOptionss.add(cfOptions);
      cfDescriptors.add(cfDescriptor);
    }

    final int rocksThreads = Runtime.getRuntime().availableProcessors() * 2;

    if(cfDescriptors.isEmpty()) {
      final Options options = new Options()
          .optimizeLevelStyleCompaction()
          .setCreateIfMissing(true)
          .setCreateMissingColumnFamilies(true)
          .setIncreaseParallelism(rocksThreads)
          .setMaxBackgroundCompactions(rocksThreads)
          .setInfoLogLevel(InfoLogLevel.INFO_LEVEL);
      statistic = new Statistics();
      options.setStatistics(statistic);
      dbOptions = options;
      return RocksDB.open(options, rocksDbDir.toAbsolutePath().toString());
    } else {
      final DBOptions options = new DBOptions()
          .setCreateIfMissing(true)
          .setCreateMissingColumnFamilies(true)
          .setIncreaseParallelism(rocksThreads)
          .setMaxBackgroundCompactions(rocksThreads)
          .setInfoLogLevel(InfoLogLevel.INFO_LEVEL);
      statistic = new Statistics();
      options.setStatistics(statistic);
      dbOptions = options;

      final List<ColumnFamilyHandle> cfHandles = new ArrayList<>();
      final RocksDB db = RocksDB.open(options, rocksDbDir.toAbsolutePath().toString(), cfDescriptors, cfHandles);
      for(int i = 0; i < cfNames.size(); i++) {
        COLUMN_FAMILIES.put(cfNames.get(i), new ColumnFamily(cfHandles.get(i), cfOptionss.get(i)));
      }
      return db;
    }
  }

  @Override
  public void cleanup() throws DBException {
    mayFlush(true);
    super.cleanup();

    synchronized (RocksDBClient.class) {
      try {
        if (references == 1) {
          outputStatistic();
          for (final ColumnFamily cf : COLUMN_FAMILIES.values()) {
            cf.getHandle().close();
          }

          rocksDb.close();
          rocksDb = null;

          dbOptions.close();
          dbOptions = null;

          writeOptions.close();
          writeOptions = null;

          readOptions.close();
          readOptions = null;

          outputStream.close();
          outputStream = null;
          statistic.close();
          statistic = null;

          for (final ColumnFamily cf : COLUMN_FAMILIES.values()) {
            cf.getOptions().close();
          }
          saveColumnFamilyNames();
          COLUMN_FAMILIES.clear();

          rocksDbDir = null;
        }

      } catch (final IOException e) {
        throw new DBException(e);
      } finally {
        references--;
      }
    }
  }

  private void outputStatistic() throws IOException{
    try(FileWriter fileWriter = new FileWriter(STATISTIC_FILE_NAME)) {
      try {
        fileWriter.write(rocksDb.getProperty("rocksdb.stats"));
      } catch (RocksDBException e) {
        throw new IOException("Fail to output statistic.", e);
      }
    }
  }

  @Override
  public Status read(final String table, final String key, final Set<String> fields,
      final Map<String, ByteIterator> result) {
    try {
      if (!COLUMN_FAMILIES.containsKey(table)) {
        createColumnFamily(table);
      }

      ColumnFamilyHandle cf = COLUMN_FAMILIES.get(table).getHandle();
      // note: current we use default cf for only in default cf, we can get the
      // file read latency
      cf = rocksDb.getDefaultColumnFamily();
      final byte[] values = rocksDb.get(cf, key.getBytes(UTF_8));
      if(values == null) {
        return Status.NOT_FOUND;
      }
      deserializeValues(values, fields, result);
      return Status.OK;
    } catch(final RocksDBException e) {
      LOGGER.error(e.getMessage(), e);
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(final String table, final String startkey, final int recordcount, final Set<String> fields,
        final Vector<HashMap<String, ByteIterator>> result) {
    try {
      if (!COLUMN_FAMILIES.containsKey(table)) {
        createColumnFamily(table);
      }

      final ColumnFamilyHandle cf = COLUMN_FAMILIES.get(table).getHandle();
      try(final RocksIterator iterator = rocksDb.newIterator(cf)) {
        int iterations = 0;
        iterator.seek(startkey.getBytes(UTF_8));
        while (iterator.isValid() && iterations < recordcount) {
          iterator.next();
          final HashMap<String, ByteIterator> values = new HashMap<>();
          deserializeValues(iterator.value(), fields, values);
          result.add(values);
          iterations++;
        }
      }

      return Status.OK;
    } catch(final RocksDBException e) {
      LOGGER.error(e.getMessage(), e);
      return Status.ERROR;
    }
  }

  @Override
  public Status update(final String table, final String key, final Map<String, ByteIterator> values) {
    //TODO(AR) consider if this would be faster with merge operator

    try {
      if (!COLUMN_FAMILIES.containsKey(table)) {
        createColumnFamily(table);
      }

      final byte[] keyBytes = key.getBytes(UTF_8);

      ByteBuffer keyByteBuffer = ByteBuffer.wrap(keyBytes);
      // we have to flush if we seen the key again, otherwise we will get wrong result
      boolean forceFlush = inBufferKeySet.contains(keyByteBuffer);
      if (forceFlush) {
        mayFlush(true);
      }

      inBufferKeySet.add(keyByteBuffer);
      bufferKeys.add(keyBytes);
      byte[] updateAfterValues = serializeValues(values);
      bufferUpdateAfter.add(updateAfterValues);
      bufferUpdateTimeStamps.add(System.nanoTime());
      mayFlush(false);
      return Status.OK;
    } catch(final RocksDBException | IOException e) {
      LOGGER.error(e.getMessage(), e);
      return Status.ERROR;
    }
  }

  private void mayFlush(boolean force) {
    // if force, but no thing to flush, return directly
    if (force && bufferKeys.isEmpty()) {
      return;
    }
    // if not force, and the size is less than batch size, return directly
    if (!force && (bufferKeys.size() < BATCH_SIZE)) {
      return;
    }

    try {
      long startTs = System.nanoTime();
      List<byte[]> updateBefore = rocksDb.multiGetAsList(readOptions, bufferKeys);
      long endTs = System.nanoTime();
      Measurements.getMeasurements().measure("batch_get_key_size", bufferKeys.size());
      Measurements.getMeasurements().measure("batch_get_as_list",
          (int) ((endTs - startTs) / (1000 * bufferKeys.size())));

      for (int i = 0; i < updateBefore.size(); i++) {
        startTs = System.nanoTime();
        writeUpdateToCDCLog(bufferKeys.get(i),
            updateBefore.get(i),
            bufferUpdateAfter.get(i));
        endTs = System.nanoTime();
        Measurements.getMeasurements().measure("write_cdc_log",
            (int) ((endTs - startTs) / 1000));

        startTs = System.nanoTime();
        // note: current we use default cf for only in default cf, we can get the
        // file read latency
        ColumnFamilyHandle cf = rocksDb.getDefaultColumnFamily();
        rocksDb.put(cf, bufferKeys.get(i),  bufferUpdateAfter.get(i));
        endTs = System.nanoTime();
        Measurements.getMeasurements().measure("write_update_to_cdc",
            (int) ((endTs - startTs) / 1000));

        Measurements.getMeasurements().measure("update_latency",
            (int) ((System.nanoTime() - bufferUpdateTimeStamps.get(i)) / 1000));
      }
      bufferKeys.clear();
      inBufferKeySet.clear();
      bufferUpdateAfter.clear();
      bufferUpdateTimeStamps.clear();
    } catch (Exception e) {
      throw new RuntimeException("Fail to multi get keys", e);
    }
  }

  @Override
  public Status insert(final String table, final String key, final Map<String, ByteIterator> values) {
    try {
      if (!COLUMN_FAMILIES.containsKey(table)) {
        createColumnFamily(table);
      }

      ColumnFamilyHandle cf = COLUMN_FAMILIES.get(table).getHandle();
      // note: current we use default cf for only in default cf, we can get the
      // file read latency
      cf = rocksDb.getDefaultColumnFamily();
      rocksDb.put(cf, key.getBytes(UTF_8), serializeValues(values));

      return Status.OK;
    } catch(final RocksDBException | IOException e) {
      LOGGER.error(e.getMessage(), e);
      return Status.ERROR;
    }
  }

  @Override
  public Status delete(final String table, final String key) {
    try {
      if (!COLUMN_FAMILIES.containsKey(table)) {
        createColumnFamily(table);
      }

      final ColumnFamilyHandle cf = COLUMN_FAMILIES.get(table).getHandle();
      rocksDb.delete(cf, key.getBytes(UTF_8));

      return Status.OK;
    } catch(final RocksDBException e) {
      LOGGER.error(e.getMessage(), e);
      return Status.ERROR;
    }
  }

  private void writeUpdateToCDCLog(
      byte[] key,
      byte[] updateBeforeValues, byte[] updateAfterValues) throws IOException{
    Map<String, Object> map = new HashMap<>();
    map.put("key", key);
    map.put("before", updateBeforeValues);
    map.put("after", updateAfterValues);
    objectMapper.writeValue(outputStream, map);
    // we only flush instead of sync to keep it same as rocksdb default
    // strategy
    outputStream.flush();
   // outputStream.getFD().sync();
  }

  private void saveColumnFamilyNames() throws IOException {
    final Path file = rocksDbDir.resolve(COLUMN_FAMILY_NAMES_FILENAME);
    try(final PrintWriter writer = new PrintWriter(Files.newBufferedWriter(file, UTF_8))) {
      writer.println(new String(RocksDB.DEFAULT_COLUMN_FAMILY, UTF_8));
      for(final String cfName : COLUMN_FAMILIES.keySet()) {
        writer.println(cfName);
      }
    }
  }

  private List<String> loadColumnFamilyNames() throws IOException {
    final List<String> cfNames = new ArrayList<>();
    final Path file = rocksDbDir.resolve(COLUMN_FAMILY_NAMES_FILENAME);
    if(Files.exists(file)) {
      try (final LineNumberReader reader =
               new LineNumberReader(Files.newBufferedReader(file, UTF_8))) {
        String line = null;
        while ((line = reader.readLine()) != null) {
          cfNames.add(line);
        }
      }
    }
    return cfNames;
  }

  private Map<String, ByteIterator> deserializeValues(final byte[] values, final Set<String> fields,
      final Map<String, ByteIterator> result) {
    final ByteBuffer buf = ByteBuffer.allocate(4);

    int offset = 0;
    while(offset < values.length) {
      buf.put(values, offset, 4);
      buf.flip();
      final int keyLen = buf.getInt();
      buf.clear();
      offset += 4;

      final String key = new String(values, offset, keyLen);
      offset += keyLen;

      buf.put(values, offset, 4);
      buf.flip();
      final int valueLen = buf.getInt();
      buf.clear();
      offset += 4;

      if(fields == null || fields.contains(key)) {
        result.put(key, new ByteArrayByteIterator(values, offset, valueLen));
      }

      offset += valueLen;
    }

    return result;
  }

  private byte[] serializeValues(final Map<String, ByteIterator> values) throws IOException {
    try(final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      final ByteBuffer buf = ByteBuffer.allocate(4);

      for(final Map.Entry<String, ByteIterator> value : values.entrySet()) {
        final byte[] keyBytes = value.getKey().getBytes(UTF_8);
        final byte[] valueBytes = value.getValue().toArray();

        buf.putInt(keyBytes.length);
        baos.write(buf.array());
        baos.write(keyBytes);

        buf.clear();

        buf.putInt(valueBytes.length);
        baos.write(buf.array());
        baos.write(valueBytes);

        buf.clear();
      }
      return baos.toByteArray();
    }
  }

  private ColumnFamilyOptions getDefaultColumnFamilyOptions(final String destinationCfName) {
    final ColumnFamilyOptions cfOptions;

    if (COLUMN_FAMILIES.containsKey("default")) {
      LOGGER.warn("no column family options for \"" + destinationCfName + "\" " +
                  "in options file - using options from \"default\"");
      cfOptions = COLUMN_FAMILIES.get("default").getOptions();
    } else {
      LOGGER.warn("no column family options for either \"" + destinationCfName + "\" or " +
                  "\"default\" in options file - initializing with empty configuration");
      cfOptions = new ColumnFamilyOptions();
    }
    LOGGER.warn("Add a CFOptions section for \"" + destinationCfName + "\" to the options file, " +
                "or subsequent runs on this DB will fail.");

    return cfOptions;
  }

  private void createColumnFamily(final String name) throws RocksDBException {
    COLUMN_FAMILY_LOCKS.putIfAbsent(name, new ReentrantLock());

    final Lock l = COLUMN_FAMILY_LOCKS.get(name);
    l.lock();
    try {
      if(!COLUMN_FAMILIES.containsKey(name)) {
        final ColumnFamilyOptions cfOptions;

        if (optionsFile != null) {
          // RocksDB requires all options files to include options for the "default" column family;
          // apply those options to this column family
          cfOptions = getDefaultColumnFamilyOptions(name);
        } else {
          cfOptions = new ColumnFamilyOptions().optimizeLevelStyleCompaction();
        }
        final ColumnFamilyHandle cfHandle = rocksDb.createColumnFamily(
            new ColumnFamilyDescriptor(name.getBytes(UTF_8), cfOptions)
        );
        COLUMN_FAMILIES.put(name, new ColumnFamily(cfHandle, cfOptions));
      }
    } finally {
      l.unlock();
    }
  }

  private static final class ColumnFamily {
    private final ColumnFamilyHandle handle;
    private final ColumnFamilyOptions options;

    private ColumnFamily(final ColumnFamilyHandle handle, final ColumnFamilyOptions options) {
      this.handle = handle;
      this.options = options;
    }

    public ColumnFamilyHandle getHandle() {
      return handle;
    }

    public ColumnFamilyOptions getOptions() {
      return options;
    }
  }
}
