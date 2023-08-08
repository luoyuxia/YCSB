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
import java.nio.ByteOrder;
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
  private static final long WAIT_LOG_AGENT_FINISH_INTERVAL = 100;

  private static final String WAL_LOG = "/tmp/wal_log";
  private static final String CDC_LOG = "/tmp/cdc_log";

  private static final Logger LOGGER = LoggerFactory.getLogger(RocksDBClient.class);

  @GuardedBy("RocksDBClient.class") private static Path rocksDbDir = null;
  @GuardedBy("RocksDBClient.class") private static Path optionsFile = null;
  @GuardedBy("RocksDBClient.class") private static RocksObject dbOptions = null;
  @GuardedBy("RocksDBClient.class") private static WriteOptions writeOptions = null;
  @GuardedBy("RocksDBClient.class") private static RocksDB rocksDb = null;
  @GuardedBy("RocksDBClient.class") private static int references = 0;
  @GuardedBy("RocksDBClient.class") private static FileOutputStream outputStream;
  @GuardedBy("RocksDBClient.class") private static ObjectMapper objectMapper;
  @GuardedBy("RocksDBClient.class") private static Options enableTimestampOptions = new Options()
      .setComparator(BuiltinComparator.BYTEWISE_COMPARATOR_WITHU64Ts);
  @GuardedBy("RocksDBClient.class") private static Statistics statistic;

  private static final ConcurrentMap<String, ColumnFamily> COLUMN_FAMILIES = new ConcurrentHashMap<>();
  private static final ConcurrentMap<String, Lock> COLUMN_FAMILY_LOCKS = new ConcurrentHashMap<>();

  private final ByteBuffer buffer = ByteBuffer.allocate(8).order(ByteOrder.nativeOrder());
  private static  LogAgent logAgent;
  private final int maxCountPerSecond = 1_000_000;
  private  int currentCountInSameSecond = 0;
  private long currentUseTimestamp = -1;

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
          outputStream = new FileOutputStream(WAL_LOG);
        } catch (final IOException | RocksDBException e) {
          throw new DBException(e);
        }
        writeOptions = new WriteOptions().setDisableWAL(true);
        objectMapper = new ObjectMapper();
        objectMapper.getFactory().disable(JsonGenerator.Feature.AUTO_CLOSE_TARGET);
        logAgent = new LogAgent(WAL_LOG, CDC_LOG, rocksDb, COLUMN_FAMILIES);
        references++;
      }
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
      final ColumnFamilyOptions cfOptions = new ColumnFamilyOptions(enableTimestampOptions)
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
          .setInfoLogLevel(InfoLogLevel.DEBUG_LEVEL)
          .setComparator(BuiltinComparator.BYTEWISE_COMPARATOR_WITHU64Ts);
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
          .setInfoLogLevel(InfoLogLevel.DEBUG_LEVEL);
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
    super.cleanup();
    synchronized (RocksDBClient.class) {
      try {
        if (references == 1) {
          // it means the ops finish, but we may still need to wait the log agent finish
          Measurements.getMeasurements().setOpsFinishTime(System.currentTimeMillis());
          waitLogAgent();
          for (final ColumnFamily cf : COLUMN_FAMILIES.values()) {
            cf.getHandle().close();
          }

          dbOptions.close();
          dbOptions = null;

          writeOptions.close();
          writeOptions = null;

          enableTimestampOptions.close();
          enableTimestampOptions = null;

          outputStream.close();
          outputStream = null;

          for (final ColumnFamily cf : COLUMN_FAMILIES.values()) {
            cf.getOptions().close();
          }
          saveColumnFamilyNames();
          COLUMN_FAMILIES.clear();

          rocksDbDir = null;
          if (logAgent != null) {
            logAgent.close();
          }

          rocksDb.close();
          rocksDb = null;
          statistic.close();
        }
      } catch (final Exception e) {
        throw new DBException(e);
      } finally {
        references--;
      }
    }
  }

  private void waitLogAgent() throws Exception {
    // write eof to WAL
    writeEOFToWAL();
    // while loop to see the log agent has modify this value
    while (true) {
      if (logAgent.isFinished()) {
        return;
      }
      Thread.sleep(WAIT_LOG_AGENT_FINISH_INTERVAL);
    }
  }

  private void writeEOFToWAL() throws Exception {
    outputStream.write(System.lineSeparator().getBytes());
  }

  @Override
  public Status read(final String table, final String key, final Set<String> fields,
      final Map<String, ByteIterator> result) {
    try {
      if (!COLUMN_FAMILIES.containsKey(table)) {
        createColumnFamily(table);
      }

      ColumnFamilyHandle cf = COLUMN_FAMILIES.get(table).getHandle();
      cf = rocksDb.getDefaultColumnFamily();
      byte[] values;
      byte[] timestamp = generateTimestamp();
      try(ReadOptions readOptions = new ReadOptions()) {
        readOptions.setTimestamp(new Slice(timestamp));
        values = rocksDb.get(cf, readOptions, key.getBytes(UTF_8));
      }
      if(values == null) {
        return Status.NOT_FOUND;
      }
      deserializeValues(values, fields, result);
      return Status.OK;
    } catch(Exception e) {
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
      if (logAgent.needWait()) {
        try{
          Thread.sleep(1);
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
      if (!COLUMN_FAMILIES.containsKey(table)) {
        createColumnFamily(table);
      }

      ColumnFamilyHandle cf = COLUMN_FAMILIES.get(table).getHandle();
      final Map<String, ByteIterator> result = new HashMap<>();
      cf = rocksDb.getDefaultColumnFamily();

      //update
      result.putAll(values);
      byte[] updateAfterValues = serializeValues(result);
      byte[] keyBytes =  key.getBytes(UTF_8);

      byte[] timestamp = generateTimestamp();

      //store
      rocksDb.put(cf, keyBytes, timestamp, updateAfterValues);
//      rocksDb.flushWal(true);
//      rocksDb.flush(new FlushOptions().setWaitForFlush(true));

      long startTs = System.nanoTime();
      // now write update_before and update_after
      writeToWAL(timestamp, keyBytes, updateAfterValues);
      long endTs = System.nanoTime();
      Measurements.getMeasurements().measure("write_to_wal",
          (int) ((endTs - startTs) / 1000));
      return Status.OK;

    } catch(final RocksDBException | IOException e) {
      LOGGER.error(e.getMessage(), e);
      return Status.ERROR;
    }
  }

  @Override
  public Status insert(final String table, final String key, final Map<String, ByteIterator> values) {
    try {
      if (!COLUMN_FAMILIES.containsKey(table)) {
        createColumnFamily(table);
      }

      ColumnFamilyHandle cf = COLUMN_FAMILIES.get(table).getHandle();
      cf = rocksDb.getDefaultColumnFamily();
      rocksDb.put(cf, key.getBytes(UTF_8), generateTimestamp(), serializeValues(values));

      return Status.OK;
    } catch(final RocksDBException | IOException e) {
      LOGGER.error(e.getMessage(), e);
      return Status.ERROR;
    }
  }

  private byte[] generateTimestamp() {
    long value = System.currentTimeMillis();
    if (value == currentUseTimestamp) {
      // it means we have meet conflict
      currentCountInSameSecond++;
    } else {
      // new timestamp
      currentUseTimestamp = value;
      currentCountInSameSecond = 0;
    }
    value = value * maxCountPerSecond + currentCountInSameSecond;
    buffer.clear();
    buffer.putLong(value);
    return buffer.array();
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

  private void writeToWAL(byte[] timestamp,
                          byte[] key, byte[] updateAfterValues) throws IOException {
    Map<String, Object> map = new HashMap<>();
    map.put("timestamp", timestamp);
    map.put("key", key);
    map.put("updateAfter", updateAfterValues);
    objectMapper.writeValue(outputStream, map);
    outputStream.write(System.lineSeparator().getBytes());
    // only flush to keep it similar to rocksdb default strategy
    outputStream.flush();
//    outputStream.getFD().sync();
  }

  private void writeUpdateToCDCLog(byte[] timestamp, byte[] updateBeforeValues,
                                   byte[] updateAfterValues) throws IOException{
    Map<String, Object> map = new HashMap<>();
    map.put("timestamp", timestamp);
    map.put("before", updateBeforeValues);
    map.put("after", updateAfterValues);
    objectMapper.writeValue(outputStream, map);
    outputStream.getFD().sync();
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
          cfOptions = new ColumnFamilyOptions(enableTimestampOptions).optimizeLevelStyleCompaction();
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

  /** Column Family.*/
  public static final class ColumnFamily {
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
