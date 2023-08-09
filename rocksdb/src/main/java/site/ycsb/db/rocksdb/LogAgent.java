package site.ycsb.db.rocksdb;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.input.Tailer;
import org.apache.commons.io.input.TailerListenerAdapter;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.Slice;
import site.ycsb.measurements.Measurements;
import site.ycsb.workloads.CoreWorkload;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

/** Agent for WAL LOG to produce cdc log.*/
public class LogAgent {
  private static final int SLEEP = 100;
  private final WALLogListener walLogListener;
  private final Tailer tailer;

  public LogAgent(String walLog, String cdcLog, RocksDB rocksDB,
                  ConcurrentMap<String, RocksDBClient.ColumnFamily> columnFamilies,
                  AtomicBoolean logAgentIsFinish) {
    walLogListener = new WALLogListener(cdcLog, rocksDB, columnFamilies, logAgentIsFinish);
    tailer = Tailer.create(new File(walLog), walLogListener, SLEEP);
  }

  public void close() throws Exception {
    if (tailer != null) {
      tailer.stop();
    }
    if (walLogListener != null) {
      walLogListener.close();
    }
  }

  private static final class WALLogListener extends TailerListenerAdapter {
    private final FileOutputStream cdcOutputStream;
    private final ObjectMapper objectMapper;
    private final RocksDB rocksDB;
    private final ReadOptions readOptions = new ReadOptions();
    private  ColumnFamilyHandle targetColumnFamilyHandle;
    private RocksDBClient.ColumnFamily columnFamily;
    private final ConcurrentMap<String, RocksDBClient.ColumnFamily> columnFamilies;
    private final ByteBuffer buffer = ByteBuffer.allocate(64).order(ByteOrder.LITTLE_ENDIAN);
    private final AtomicBoolean logAgentIsFinish;
    private WALLogListener(String cdcLog, RocksDB rocksDB,
                           ConcurrentMap<String, RocksDBClient.ColumnFamily> columnFamilies,
                           AtomicBoolean logAgentIsFinish) {
      try{
        cdcOutputStream = new FileOutputStream(cdcLog);
        objectMapper = new ObjectMapper();
        objectMapper.getFactory().disable(JsonGenerator.Feature.AUTO_CLOSE_TARGET);
        this.columnFamilies = columnFamilies;
        this.rocksDB = rocksDB;
        this.logAgentIsFinish = logAgentIsFinish;
      } catch (Exception e) {
        throw new RuntimeException("Fail to create WAL log listener.", e);
      }
    }

    @Override
    public void handle(String line) {
      if (line.isEmpty()) {
        logAgentIsFinish.set(true);
        return;
      }
      if (targetColumnFamilyHandle == null) {
        columnFamily = columnFamilies.get(CoreWorkload.TABLENAME_PROPERTY_DEFAULT);
        targetColumnFamilyHandle = columnFamily.getHandle();
      }
      try{
        JsonNode jsonNode =  objectMapper.readTree(line);
        byte[] timestamp = jsonNode.get("timestamp").binaryValue();
        byte[] key = jsonNode.get("key").binaryValue();
        byte[] updateAfter = jsonNode.get("updateAfter").binaryValue();
        // try to get update before
        readOptions.setTimestamp(new Slice(timestamp));
        long startTs = System.nanoTime();
//        if (!targetColumnFamilyHandle.isOwningHandle()) {
//          return;
//        }
        final byte[] updateBefore = rocksDB.get(targetColumnFamilyHandle, readOptions, key);
        long endTs = System.nanoTime();
        Measurements.getMeasurements().measure("reading_for_updating",
            (int) ((endTs - startTs) / 1000));
        startTs = System.nanoTime();
        writeUpdateToCDCLog(updateBefore, updateAfter);
        endTs = System.nanoTime();
        Measurements.getMeasurements().measure("writing_to_cdc",
            (int) ((endTs - startTs) / 1000));
        long writeTimestamp = getTimeStamp(timestamp);
        Measurements.getMeasurements().measure("cdc_generate",
            (int) ((System.currentTimeMillis() - writeTimestamp) * 1000));
      } catch (Exception e) {
        throw new RuntimeException("Fail to handle the line.", e);
      }
    }

    private void writeUpdateToCDCLog(byte[] updateBeforeValues, byte[] updateAfterValues) throws IOException {
      Map<String, Object> map = new HashMap<>();
      map.put("updateBefore", updateBeforeValues);
      map.put("updateAfter", updateAfterValues);
      objectMapper.writeValue(cdcOutputStream, map);
      // flush instead of sync
      cdcOutputStream.flush();
     // cdcOutputStream.getFD().sync();
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
  }
}
