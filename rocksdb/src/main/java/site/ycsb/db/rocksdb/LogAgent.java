package site.ycsb.db.rocksdb;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.input.Tailer;
import org.apache.commons.io.input.TailerListenerAdapter;
import org.rocksdb.*;
import site.ycsb.measurements.Measurements;
import site.ycsb.workloads.CoreWorkload;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;
import java.util.concurrent.ConcurrentMap;

/** Agent for WAL LOG to produce cdc log.*/
public class LogAgent {

  private static final String STATISTIC = "/tmp/statistic";
  private static final int SLEEP = 10;
  private final WALLogListener walLogListener;
  private final Tailer tailer;
  private final RocksDB rocksDB;

  public LogAgent(String walLog, String cdcLog, RocksDB rocksDB,
                  ConcurrentMap<String, RocksDBClient.ColumnFamily> columnFamilies) {
    walLogListener = new WALLogListener(cdcLog, rocksDB, columnFamilies);
    tailer = Tailer.create(new File(walLog), walLogListener, SLEEP);
    this.rocksDB = rocksDB;
  }

  public void close() throws Exception {
    if (tailer != null) {
      tailer.stop();
    }
    if (walLogListener != null) {
      walLogListener.close();
    }

    try(FileWriter fileWriter = new FileWriter(STATISTIC)) {
      fileWriter.write(rocksDB.getProperty("rocksdb.stats"));
    }
  }

  public boolean needWait() {
    return walLogListener.needWait();
  }

  public boolean isFinished() {
    return walLogListener.isFinished();
  }

  private static final class WALLogListener extends TailerListenerAdapter {

    private static final int MAX_BATCH_SIZE = 512;
    private static final int MAX_ALLOW_LATENCY_MILLS = 1_000;
    private final FileOutputStream cdcOutputStream;
    private final ObjectMapper objectMapper;
    private final RocksDB rocksDB;
    private final ReadOptions readOptions = new ReadOptions();
    private  ColumnFamilyHandle targetColumnFamilyHandle;
    private RocksDBClient.ColumnFamily columnFamily;
    private final ConcurrentMap<String, RocksDBClient.ColumnFamily> columnFamilies;
    private final ByteBuffer buffer = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);
    private  boolean logAgentIsFinish;

    private final List<byte[]> bufferKeys = new ArrayList<>();
    private final List<byte[]> bufferUpdateAfter = new ArrayList<>();
    private final Set<ByteBuffer> inBufferKeySet = new HashSet<>();
    private final List<Long> updateTimestamp= new ArrayList<>();
    private long searchTimeStamp = 0;


    private boolean cdcIsLate;
    private WALLogListener(String cdcLog, RocksDB rocksDB,
                           ConcurrentMap<String, RocksDBClient.ColumnFamily> columnFamilies) {
      try{
        cdcOutputStream = new FileOutputStream(cdcLog);
        objectMapper = new ObjectMapper();
        objectMapper.getFactory().disable(JsonGenerator.Feature.AUTO_CLOSE_TARGET);
        this.columnFamilies = columnFamilies;
        this.rocksDB = rocksDB;
      } catch (Exception e) {
        throw new RuntimeException("Fail to create WAL log listener.", e);
      }
    }

    @Override
    public void handle(String line) {
      if (line.isEmpty()) {
        try{
          mayFlush(true);
        } catch (Exception e) {
          throw new RuntimeException("Fail to flush while ending lines.", e);
        }
        logAgentIsFinish = true;
        return;
      }
      if (targetColumnFamilyHandle == null) {
        columnFamily = columnFamilies.get(CoreWorkload.TABLENAME_PROPERTY_DEFAULT);
        targetColumnFamilyHandle = columnFamily.getHandle();
        // use default column since we needs to get the file level statistic
//        targetColumnFamilyHandle = rocksDB.getDefaultColumnFamily();
      }
      try{
        JsonNode jsonNode =  objectMapper.readTree(line);
        // get the timestamp while writing
        byte[] timestamp = jsonNode.get("timestamp").binaryValue();
        long timestampLong = getTimeStamp(timestamp);
        // get the update after value
        byte[] updateAfter = jsonNode.get("updateAfter").binaryValue();

        // get the key
        byte[] key = jsonNode.get("key").binaryValue();
        ByteBuffer keyByteBuffer = ByteBuffer.wrap(key);

        // we have to flush if we seen the key again, otherwise we will get wrong result
        boolean forceFlush = inBufferKeySet.contains(keyByteBuffer);
        if (forceFlush) {
          mayFlush(true);
        }

        // add the key to buffer keys
        inBufferKeySet.add(keyByteBuffer);
        bufferKeys.add(key);
        // add update after
        bufferUpdateAfter.add(updateAfter);
        updateTimestamp.add(timestampLong / 1_000_000);
        searchTimeStamp = timestampLong - 1;
        mayFlush(false);
      } catch (Exception e) {
        throw new RuntimeException("Fail to handle the line.", e);
      }
    }

    private void mayFlush(boolean force) throws Exception {
      // if force flush, but nothing to flush, return directly
      if (force && bufferKeys.size() == 0) {
        return;
      }
      // if not force flush, and nothing to flush or the buffer size < max batch size
      if (!force && (bufferKeys.size() == 0 || bufferKeys.size() < MAX_BATCH_SIZE)) {
        return;
      }
      long startTs = System.nanoTime();
      readOptions.setTimestamp(new Slice(toTimestamp(searchTimeStamp)));
      List<byte[]> updateBefore = rocksDB.multiGetAsList(readOptions, bufferKeys);
      long endTs = System.nanoTime();
      Measurements.getMeasurements().measure("batch_get_as_list",
          (int) ((endTs - startTs) / (1000 * bufferKeys.size())));
      Measurements.getMeasurements().measure("batch_get_size",
          bufferKeys.size());

      for (int i = 0; i < updateBefore.size(); i++) {
        startTs = System.nanoTime();
        writeUpdateToCDCLog(bufferKeys.get(i),
            updateBefore.get(i), bufferUpdateAfter.get(i));
        endTs = System.nanoTime();
        Measurements.getMeasurements().measure("write_cdc_log",
            (int) ((endTs - startTs) / 1000));

        long latency = System.currentTimeMillis() - updateTimestamp.get(i);
        Measurements.getMeasurements().measure("cdc_generate", (int) (latency * 1000));
        cdcIsLate = latency > MAX_ALLOW_LATENCY_MILLS;
      }
      bufferKeys.clear();
      inBufferKeySet.clear();
      bufferUpdateAfter.clear();
      updateTimestamp.clear();
    }

    private void writeUpdateToCDCLog(byte[] key,
                                     byte[] updateBeforeValues,
                                     byte[] updateAfterValues) throws IOException {
      Map<String, Object> map = new HashMap<>();
      map.put("key", key);
      map.put("updateBefore", updateBeforeValues);
      map.put("updateAfter", updateAfterValues);
      objectMapper.writeValue(cdcOutputStream, map);
      // flush instead of sync
      cdcOutputStream.flush();
     // cdcOutputStream.getFD().sync();
    }

    public boolean isFinished() {
      return logAgentIsFinish;
    }

    public boolean needWait() {
      return cdcIsLate;
    }


    private void close() throws Exception {
      if (cdcOutputStream != null) {
        cdcOutputStream.close();
      }
    }

    private long getTimeStamp(byte[] bytes) {
      buffer.clear();
      buffer.put(bytes);
      buffer.flip();
      return buffer.getLong();
    }

    private byte[] toTimestamp(long value) {
      buffer.clear();
      buffer.putLong(value);
      return buffer.array();
    }
  }
}
