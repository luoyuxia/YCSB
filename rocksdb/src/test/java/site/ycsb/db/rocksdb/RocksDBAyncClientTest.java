package site.ycsb.db.rocksdb;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import site.ycsb.ByteIterator;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;
import site.ycsb.measurements.Measurements;
import site.ycsb.workloads.CoreWorkload;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static site.ycsb.workloads.CoreWorkload.TABLENAME_PROPERTY_DEFAULT;

public class RocksDBAyncClientTest {
  @Rule
  public TemporaryFolder tmpFolder = new TemporaryFolder();

  private static final String MOCK_TABLE = TABLENAME_PROPERTY_DEFAULT;
  private static final String MOCK_KEY2 = "2";
  private static final int NUM_RECORDS = 10;
  private static final String FIELD_PREFIX = CoreWorkload.FIELD_NAME_PREFIX_DEFAULT;

  private RocksDBClient instance;

  private static final Map<String, ByteIterator> MOCK_DATA;
  static {
    MOCK_DATA = new HashMap<>(NUM_RECORDS);
    for (int i = 0; i < NUM_RECORDS; i++) {
      MOCK_DATA.put(FIELD_PREFIX + i, new StringByteIterator("value" + i));
    }
  }

  @Before
  public void setup() throws Exception {
    Measurements.setProperties(new Properties());
    instance = new RocksDBClient();

    final Properties properties = new Properties();
    properties.setProperty(RocksDBClient.PROPERTY_ROCKSDB_DIR, tmpFolder.getRoot().getAbsolutePath());
    instance.setProperties(properties);

    instance.init();
  }

  @After
  public void tearDown() throws Exception {
    instance.cleanup();
  }

  @Test
  public void t1() throws Exception {
    instance.insert(MOCK_TABLE, MOCK_KEY2, MOCK_DATA);

    final Map<String, ByteIterator> newValues = new HashMap<>(NUM_RECORDS);
    for (int i = 0; i < NUM_RECORDS; i++) {
      newValues.put(FIELD_PREFIX + i, new StringByteIterator("newvalue" + i));
    }

    Status result = instance.update(MOCK_TABLE, MOCK_KEY2, newValues);
    System.out.println(result);
     result = instance.update(MOCK_TABLE, MOCK_KEY2, newValues);
    final Map<String, ByteIterator> resultParam = new HashMap<>(NUM_RECORDS);
    result = instance.read(MOCK_TABLE, MOCK_KEY2, MOCK_DATA.keySet(),resultParam);
//    System.out.println(result);
//    while (true) {
//      Thread.sleep(1000L);
//    }
  }
}
