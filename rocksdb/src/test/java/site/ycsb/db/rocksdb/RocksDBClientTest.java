/*
 * Copyright (c) 2018 YCSB contributors. All rights reserved.
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

import org.rocksdb.*;
import site.ycsb.ByteIterator;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;
import site.ycsb.measurements.Measurements;
import site.ycsb.workloads.CoreWorkload;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.util.*;

import static org.junit.Assert.assertEquals;

public class RocksDBClientTest {

  @Rule
  public TemporaryFolder tmpFolder = new TemporaryFolder();

  private static final String MOCK_TABLE = "ycsb";
  private static final String MOCK_KEY0 = "0";
  private static final String MOCK_KEY1 = "1";
  private static final String MOCK_KEY2 = "2";
  private static final String MOCK_KEY3 = "3";
  private static final int NUM_RECORDS = 10;
  private static final String FIELD_PREFIX = CoreWorkload.FIELD_NAME_PREFIX_DEFAULT;

  private static final Map<String, ByteIterator> MOCK_DATA;
  static {
    MOCK_DATA = new HashMap<>(NUM_RECORDS);
    for (int i = 0; i < NUM_RECORDS; i++) {
      MOCK_DATA.put(FIELD_PREFIX + i, new StringByteIterator("value" + i));
    }
  }

  private RocksDBClient instance;

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
  public void insertAndRead() throws Exception {
    final Status insertResult = instance.insert(MOCK_TABLE, MOCK_KEY0, MOCK_DATA);
    assertEquals(Status.OK, insertResult);

    final Set<String> fields = MOCK_DATA.keySet();
    final Map<String, ByteIterator> resultParam = new HashMap<>(NUM_RECORDS);
    final Status readResult = instance.read(MOCK_TABLE, MOCK_KEY0, fields, resultParam);
    assertEquals(Status.OK, readResult);
  }


  public void myTest() throws Exception {
    List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
    final Options options = new Options()
        .setCreateIfMissing(true)
        .setCreateMissingColumnFamilies(true)
        .setComparator(BuiltinComparator.BYTEWISE_COMPARATOR_WITHU64Ts);
    DBOptions dbOptions = new DBOptions(options);
    ColumnFamilyOptions cfOptions = new ColumnFamilyOptions(options);
    List<ColumnFamilyDescriptor> cfs = new ArrayList<>();
    cfs.add(new ColumnFamilyDescriptor("default".getBytes(), cfOptions));
    cfs.add(new ColumnFamilyDescriptor("t".getBytes(), cfOptions));

    final RocksDB rocksDb = RocksDB.open(dbOptions,
        tmpFolder.getRoot().getAbsolutePath(), cfs, columnFamilyHandles);
    ReadOptions readOptions  = new ReadOptions();
    readOptions.setTimestamp(new Slice("seeeeeee"));
    System.out.println(rocksDb.get(columnFamilyHandles.get(0),
        readOptions, "abc1".getBytes()));

    rocksDb.put(columnFamilyHandles.get(0),
        "abc1".getBytes(), "seeeeeee".getBytes(),  "abc1".getBytes());

    System.out.println(rocksDb.get(columnFamilyHandles.get(0),
        readOptions, "abc1".getBytes()));

    readOptions.setTimestamp(new Slice("aeeeeeee"));
    System.out.println(rocksDb.get(columnFamilyHandles.get(0),
        readOptions, "abc1".getBytes()));
  }


  public void t2() throws Exception {
    final Options options = new Options()
        .setCreateIfMissing(true)
        .setCreateMissingColumnFamilies(true)
        .setComparator(BuiltinComparator.BYTEWISE_COMPARATOR_WITHU64Ts);
    DBOptions dbOptions = new DBOptions(options);

    final RocksDB rocksDb = RocksDB.open(options, tmpFolder.getRoot().getAbsolutePath());

    ColumnFamilyOptions cfOptions = new ColumnFamilyOptions(options);
    ColumnFamilyHandle columnFamilyHandle =
        rocksDb.createColumnFamily(new ColumnFamilyDescriptor("t".getBytes(), cfOptions));
    ReadOptions readOptions  = new ReadOptions();
    readOptions.setTimestamp(new Slice("11111111"));


    System.out.println(Arrays.toString(rocksDb.get(columnFamilyHandle, readOptions, "abc1".getBytes())));

    rocksDb.put(columnFamilyHandle,
        "abc1".getBytes(), "11111111".getBytes(),  "abc1".getBytes());

    System.out.println(Arrays.toString(rocksDb.get(columnFamilyHandle, readOptions, "abc1".getBytes())));

    readOptions  = new ReadOptions();
    readOptions.setTimestamp(new Slice("00000000"));
    System.out.println(Arrays.toString(rocksDb.get(columnFamilyHandle, readOptions, "abc1".getBytes())));
  }



  @Test
  public void insertAndDelete() throws Exception {
    final Status insertResult = instance.insert(MOCK_TABLE, MOCK_KEY1, MOCK_DATA);
    assertEquals(Status.OK, insertResult);

    final Status result = instance.delete(MOCK_TABLE, MOCK_KEY1);
    assertEquals(Status.OK, result);
  }

  @Test
  public void insertUpdateAndRead() throws Exception {
    final Map<String, ByteIterator> newValues = new HashMap<>(NUM_RECORDS);

    final Status insertResult = instance.insert(MOCK_TABLE, MOCK_KEY2, MOCK_DATA);
    assertEquals(Status.OK, insertResult);

    for (int i = 0; i < NUM_RECORDS; i++) {
      newValues.put(FIELD_PREFIX + i, new StringByteIterator("newvalue" + i));
    }

    final Status result = instance.update(MOCK_TABLE, MOCK_KEY2, newValues);
    assertEquals(Status.OK, result);

    //validate that the values changed
    final Map<String, ByteIterator> resultParam = new HashMap<>(NUM_RECORDS);
    instance.read(MOCK_TABLE, MOCK_KEY2, MOCK_DATA.keySet(), resultParam);

    for (int i = 0; i < NUM_RECORDS; i++) {
      assertEquals("newvalue" + i, resultParam.get(FIELD_PREFIX + i).toString());
    }
  }

  @Test
  public void insertAndScan() throws Exception {
    final Status insertResult = instance.insert(MOCK_TABLE, MOCK_KEY3, MOCK_DATA);

    final Set<String> fields = MOCK_DATA.keySet();
    final Vector<HashMap<String, ByteIterator>> resultParam = new Vector<>(NUM_RECORDS);
//    final Status result = instance.scan(MOCK_TABLE, MOCK_KEY3, NUM_RECORDS, fields, resultParam);
//    System.out.println(result);
  }
}
