package com._4paradigm.rtidb.client.ut.ha;

import com._4paradigm.rtidb.client.*;
import com._4paradigm.rtidb.client.base.TestCaseBase;
import com._4paradigm.rtidb.common.Common;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com._4paradigm.rtidb.common.Common.ColumnDesc;
import com._4paradigm.rtidb.common.Common.ColumnKey;
import com._4paradigm.rtidb.ns.NS.TableInfo;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;


public class ColumnKeyTest extends TestCaseBase {
    private final static Logger logger = LoggerFactory.getLogger(ColumnKeyTest.class);
    private static AtomicInteger id = new AtomicInteger(50000);

    @BeforeClass
    public void setUp() {
        super.setUp();
    }

    @AfterClass
    public void tearDown() {
        super.tearDown();
    }

    @DataProvider(name = "StorageMode")
    public Object[][] StorageMode() {
        return new Object[][] {
                new Object[] { Common.StorageMode.kMemory },
                new Object[] { Common.StorageMode.kSSD },
                new Object[] { Common.StorageMode.kHDD },
        };
    }

    @Test(dataProvider = "StorageMode")
    public void testPutNoTsCol(Common.StorageMode sm) {
        String name = String.valueOf(id.incrementAndGet());
        nsc.dropTable(name);
        ColumnDesc col0 = ColumnDesc.newBuilder().setName("card").setAddTsIdx(true).setType("string").build();
        ColumnDesc col1 = ColumnDesc.newBuilder().setName("mcc").setAddTsIdx(false).setType("string").build();
        ColumnDesc col2 = ColumnDesc.newBuilder().setName("amt").setAddTsIdx(false).setType("double").build();
        TableInfo table = TableInfo.newBuilder()
                .setName(name).setTtl(0)
                .addColumnDescV1(col0).addColumnDescV1(col1).addColumnDescV1(col2)
                .setStorageMode(sm)
                .build();
        boolean ok = nsc.createTable(table);
        Assert.assertTrue(ok);
        client.refreshRouteTable();
        try {
            Map<String, Object> data = new HashMap<String, Object>();
            data.put("card", "card0");
            data.put("mcc", "mcc0");
            data.put("amt", 1.5);
            tableSyncClient.put(name, 1122, data);
            data.clear();
            data.put("card", "card0");
            data.put("mcc", "mcc1");
            data.put("amt", 1.6);
            tableSyncClient.put(name, 1234, data);
            KvIterator it = tableSyncClient.scan(name, "card0", "card", 1235, 0);
            Assert.assertTrue(it.valid());
            Assert.assertTrue(it.getCount() == 2);
            Object[] row = tableSyncClient.getRow(name, "card0", "card", 0);
            Assert.assertEquals(row.length, 3);
            Assert.assertEquals(row[0], "card0");
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        }
        nsc.dropTable(name);
    }

    @Test(dataProvider = "StorageMode")
    public void testPutNoTs(Common.StorageMode sm) {
            String name = String.valueOf(id.incrementAndGet());
            nsc.dropTable(name);
            ColumnDesc col0 = ColumnDesc.newBuilder().setName("card").setAddTsIdx(true).setType("string").build();
            ColumnDesc col1 = ColumnDesc.newBuilder().setName("mcc").setAddTsIdx(false).setType("string").build();
            ColumnDesc col2 = ColumnDesc.newBuilder().setName("amt").setAddTsIdx(false).setType("double").build();
            ColumnDesc col3 = ColumnDesc.newBuilder().setName("ts").setAddTsIdx(false).setType("int64").setIsTsCol(true).build();
            TableInfo table = TableInfo.newBuilder()
                    .setName(name).setTtl(0)
                    .addColumnDescV1(col0).addColumnDescV1(col1).addColumnDescV1(col2).addColumnDescV1(col3)
                    .setStorageMode(sm)
                    .build();
            boolean ok = nsc.createTable(table);
            Assert.assertTrue(ok);
            client.refreshRouteTable();
            try {
                tableSyncClient.put(name, 1122, new Object[]{"card1", "mcc1", 9.2d, 1234});
                Assert.assertTrue(false);
            } catch (Exception e) {
                Assert.assertTrue(true);
            }
            try {
                Map<String, Object> data = new HashMap<String, Object>();
                data.put("card", "card0");
                data.put("mcc", "mcc0");
                data.put("amt", 1.5);
                data.put("ts", 1234l);
                tableSyncClient.put(name, data);
                data.clear();
                data.put("card", "card0");
                data.put("mcc", "mcc1");
                data.put("amt", 1.6);
                data.put("ts", 1235l);
                tableSyncClient.put(name, data);
                KvIterator it = tableSyncClient.scan(name, "card0", "card", 1235, 0);
                Assert.assertTrue(it.valid());
                Assert.assertTrue(it.getCount() == 2);
            } catch (Exception e) {
                Assert.assertTrue(false);
            }

            Map<String, Object> data = new HashMap<>();
            try {
                data.put("card", "card0");
                data.put("mcc", "mcc0");
                data.put("amt", 1.2d);
                data.put("ts", null);
                tableSyncClient.put(name, data);
                Assert.assertTrue(false);
            } catch (Exception e) {
                Assert.assertTrue(true);
            }
            nsc.dropTable(name);
    }

    @Test(dataProvider = "StorageMode")
    public void testPutTwoIndex(Common.StorageMode sm) {
        String name = String.valueOf(id.incrementAndGet());
        nsc.dropTable(name);
        ColumnDesc col0 = ColumnDesc.newBuilder().setName("card").setAddTsIdx(true).setType("string").build();
        ColumnDesc col1 = ColumnDesc.newBuilder().setName("mcc").setAddTsIdx(true).setType("string").build();
        ColumnDesc col2 = ColumnDesc.newBuilder().setName("amt").setAddTsIdx(false).setType("double").build();
        ColumnDesc col3 = ColumnDesc.newBuilder().setName("ts").setAddTsIdx(false).setType("int64").setIsTsCol(true).build();
        TableInfo table = TableInfo.newBuilder()
                .setName(name).setTtl(0)
                .addColumnDescV1(col0).addColumnDescV1(col1).addColumnDescV1(col2).addColumnDescV1(col3)
                .setStorageMode(sm)
                .build();
        boolean ok = nsc.createTable(table);
        Assert.assertTrue(ok);
        client.refreshRouteTable();
        try {
            tableSyncClient.put(name, 1122, new Object[]{"card1", "mcc1", 9.2d, 1234});
            Assert.assertTrue(false);
        } catch (Exception e) {
            Assert.assertTrue(true);
        }
        try {
            Map<String, Object> data = new HashMap<String, Object>();
            data.put("card", "card0");
            data.put("mcc", "mcc0");
            data.put("amt", 1.5);
            data.put("ts", 1234l);
            tableSyncClient.put(name, data);
            data.clear();
            data.put("card", "card0");
            data.put("mcc", "mcc1");
            data.put("amt", 1.6);
            data.put("ts", 1235l);
            tableSyncClient.put(name, data);
            KvIterator it = tableSyncClient.scan(name, "card0", "card", 1235, 0);
            Assert.assertTrue(it.valid());
            Assert.assertTrue(it.getCount() == 2);
            it = tableSyncClient.scan(name, "mcc0", "mcc", 1235, 0);
            Assert.assertTrue(it.valid());
            Assert.assertTrue(it.getCount() == 1);
        } catch (Exception e) {
            Assert.assertTrue(false);
        }
        nsc.dropTable(name);
    }

    @Test(dataProvider = "StorageMode")
    public void testPutCombinedKey(Common.StorageMode sm) {
        String name = String.valueOf(id.incrementAndGet());
        nsc.dropTable(name);
        ColumnDesc col0 = ColumnDesc.newBuilder().setName("card").setAddTsIdx(false).setType("string").build();
        ColumnDesc col1 = ColumnDesc.newBuilder().setName("mcc").setAddTsIdx(false).setType("string").build();
        ColumnDesc col2 = ColumnDesc.newBuilder().setName("amt").setAddTsIdx(false).setType("double").build();
        ColumnDesc col3 = ColumnDesc.newBuilder().setName("ts").setAddTsIdx(false).setType("int64").setIsTsCol(true).build();
        ColumnKey colKey1 = ColumnKey.newBuilder().setIndexName("card_mcc").addColName("card").addColName("mcc").addTsName("ts").build();
        TableInfo table = TableInfo.newBuilder()
                .setName(name).setTtl(0)
                .addColumnDescV1(col0).addColumnDescV1(col1).addColumnDescV1(col2).addColumnDescV1(col3)
                .addColumnKey(colKey1)
                .setStorageMode(sm)
                .build();
        boolean ok = nsc.createTable(table);
        Assert.assertTrue(ok);
        client.refreshRouteTable();
        try {
            tableSyncClient.put(name, 1122, new Object[]{"card1", "mcc1", 9.2d, 1234});
            Assert.assertTrue(false);
        } catch (Exception e) {
            Assert.assertTrue(true);
        }
        try {
            Map<String, Object> data = new HashMap<String, Object>();
            data.put("card", "card0");
            data.put("mcc", "mcc0");
            data.put("amt", 1.5);
            data.put("ts", 1234l);
            tableSyncClient.put(name, data);
            data.clear();
            data.put("card", "card0");
            data.put("mcc", "mcc1");
            data.put("amt", 1.6);
            data.put("ts", 1235l);
            tableSyncClient.put(name, data);
            Map<String, Object> scan_key = new HashMap<String, Object>();
            scan_key.put("card", "card0");
            scan_key.put("mcc", "mcc0");
            KvIterator it = tableSyncClient.scan(name, scan_key, "card_mcc", 1235l, 0l, "ts", 0);
            Assert.assertTrue(it.valid());
            Assert.assertTrue(it.getCount() == 1);
            Object[] row = it.getDecodedValue();
            Assert.assertEquals(it.getKey(), 1234);
            Assert.assertEquals(row.length, 4);
            Assert.assertEquals(row[0], "card0");
            Assert.assertEquals(row[1], "mcc0");
            Assert.assertEquals(row[2], 1.5d);
            Assert.assertEquals(((Long) row[3]).longValue(), 1234l);
            scan_key.put("mcc", "mcc1");
            it = tableSyncClient.scan(name, scan_key, "card_mcc", 1235l, 0l, "ts", 0);
            Assert.assertTrue(it.valid());
            Assert.assertTrue(it.getCount() == 1);
            scan_key.put("mcc", "mcc2");
            it = tableSyncClient.scan(name, scan_key, "card_mcc", 1235l, 0l, "ts", 0);
            Assert.assertFalse(it.valid());
            row = tableSyncClient.getRow(name, new Object[]{"card0", "mcc0"}, "card_mcc", 1234, "ts", null);
            Assert.assertEquals(row.length, 4);
            Assert.assertEquals(row[0], "card0");
            Assert.assertEquals(row[1], "mcc0");
            Assert.assertEquals(row[2], 1.5d);

            Map<String, Object> key_map = new HashMap<String, Object>();
            key_map.put("card", "card0");
            key_map.put("mcc", "mcc1");
            row = tableSyncClient.getRow(name, key_map, "card_mcc", 1235, "ts", null);
            Assert.assertEquals(row[0], "card0");
            Assert.assertEquals(row[1], "mcc1");
            Assert.assertEquals(row[2], 1.6d);

            Assert.assertEquals(tableSyncClient.count(name, key_map, "card_mcc", "ts", false), 1);
            data.clear();
            data.put("card", "card0");
            data.put("mcc", "mcc1");
            data.put("amt", 1.6);
            data.put("ts", 1240l);
            tableSyncClient.put(name, data);
            Assert.assertEquals(tableSyncClient.count(name, key_map, "card_mcc", "ts", false), 2);

            data.clear();
            data.put("card", "card0");
            data.put("mcc", "mcc1");
            data.put("amt", 1.7);
            data.put("ts", 1245l);
            PutFuture pf = tableAsyncClient.put(name, data);
            Assert.assertTrue(pf.get());

            ScanFuture sf = tableAsyncClient.scan(name, key_map, "card_mcc", 1245, 0, "ts", 0);
            it = sf.get();
            Assert.assertTrue(it.valid());
            Assert.assertTrue(it.getCount() == 3);
            row = it.getDecodedValue();
            Assert.assertEquals(it.getKey(), 1245);
            Assert.assertEquals(row[0], "card0");
            Assert.assertEquals(row[1], "mcc1");
            Assert.assertEquals(row[2], 1.7d);

            GetFuture gf = tableAsyncClient.get(name, key_map, "card_mcc", 1235, "ts", null);
            row = gf.getRow();
            Assert.assertEquals(row[0], "card0");
            Assert.assertEquals(row[1], "mcc1");
            Assert.assertEquals(row[2], 1.6d);

            data.clear();
            data.put("card", "card0");
            data.put("mcc", "");
            data.put("amt", 1.8);
            data.put("ts", 1250l);
            tableSyncClient.put(name, data);
            row = tableSyncClient.getRow(name, new Object[]{"card0", ""}, "card_mcc", 0, "ts", null);
            Assert.assertEquals(row[0], "card0");
            Assert.assertEquals(row[1], "");
            Assert.assertEquals(row[2], 1.8);
        } catch (Exception e) {
            Assert.assertTrue(false);
        }
        nsc.dropTable(name);
    }

    @Test(dataProvider = "StorageMode")
    public void testPutMultiCombinedKey(Common.StorageMode sm) {
        String name = String.valueOf(id.incrementAndGet());
        nsc.dropTable(name);
        ColumnDesc col0 = ColumnDesc.newBuilder().setName("card").setAddTsIdx(false).setType("string").build();
        ColumnDesc col1 = ColumnDesc.newBuilder().setName("mcc").setAddTsIdx(false).setType("string").build();
        ColumnDesc col2 = ColumnDesc.newBuilder().setName("amt").setAddTsIdx(false).setType("double").build();
        ColumnDesc col3 = ColumnDesc.newBuilder().setName("ts").setAddTsIdx(false).setType("int64").setIsTsCol(true).build();
        ColumnDesc col4 = ColumnDesc.newBuilder().setName("ts_1").setAddTsIdx(false).setType("int64").setIsTsCol(true).build();
        ColumnDesc col5 = ColumnDesc.newBuilder().setName("col1").setAddTsIdx(false).setType("string").build();
        ColumnDesc col6 = ColumnDesc.newBuilder().setName("ts_2").setAddTsIdx(false).setType("int64").setIsTsCol(true).build();
        ColumnKey colKey1 = ColumnKey.newBuilder().setIndexName("card").addTsName("ts").addTsName("ts_1").build();
        ColumnKey colKey2 = ColumnKey.newBuilder().setIndexName("mcc").addTsName("ts").build();
        ColumnKey colKey3 = ColumnKey.newBuilder().setIndexName("col1").addTsName("ts_1").build();
        ColumnKey colKey4 = ColumnKey.newBuilder().setIndexName("card2").addColName("card").addColName("mcc").addTsName("ts_1").addTsName("ts_2").build();
        TableInfo table = TableInfo.newBuilder()
                .setName(name).setTtl(0)
                .addColumnDescV1(col0).addColumnDescV1(col1).addColumnDescV1(col2).addColumnDescV1(col3).addColumnDescV1(col4).addColumnDescV1(col5).addColumnDescV1(col6)
                .addColumnKey(colKey1).addColumnKey(colKey2).addColumnKey(colKey3).addColumnKey(colKey4)
                .setStorageMode(sm)
                .setPartitionNum(1).setReplicaNum(1)
                .build();
        boolean ok = nsc.createTable(table);
        Assert.assertTrue(ok);
        client.refreshRouteTable();
        try {
            Map<String, Object> data = new HashMap<String, Object>();
            data.put("card", "card0");
            data.put("mcc", "mcc0");
            data.put("amt", 1.5);
            data.put("ts", 1234l);
            data.put("ts_1", 222l);
            data.put("col1", "col_key0");
            data.put("ts_2", 2222l);
            tableSyncClient.put(name, data);
            data.clear();
            data.put("card", "card0");
            data.put("mcc", "mcc1");
            data.put("amt", 1.6);
            data.put("ts", 1235l);
            data.put("ts_1", 333l);
            data.put("col1", "col_key1");
            data.put("ts_2", 3333l);
            tableSyncClient.put(name, data);
            KvIterator it = tableSyncClient.scan(name, "card0", "card", 1235l, 0l, "ts", 0);
            Assert.assertTrue(it.valid());
            Assert.assertTrue(it.getCount() == 2);
            Object[] row = it.getDecodedValue();
            Assert.assertEquals(it.getKey(), 1235);
            Assert.assertEquals(row.length, 7);
            Assert.assertEquals(row[0], "card0");
            Assert.assertEquals(row[1], "mcc1");
            Assert.assertEquals(row[2], 1.6d);
            Assert.assertEquals(((Long) row[3]).longValue(), 1235l);
            Assert.assertEquals(((Long) row[4]).longValue(), 333l);
            Assert.assertEquals(row[5], "col_key1");
            Assert.assertEquals(((Long) row[6]).longValue(), 3333l);
            it = tableSyncClient.scan(name, "card0", "card", 1235l, 0l, "ts_1", 0);
            Assert.assertTrue(it.getCount() == 2);
            row = it.getDecodedValue();
            Assert.assertEquals(it.getKey(), 333);
            Assert.assertEquals(row.length, 7);
            Assert.assertEquals(row[0], "card0");
            Assert.assertEquals(row[1], "mcc1");
            Assert.assertEquals(row[2], 1.6d);
            Assert.assertEquals(((Long) row[3]).longValue(), 1235l);
            Assert.assertEquals(((Long) row[4]).longValue(), 333l);
            Assert.assertEquals(row[5], "col_key1");
            Assert.assertEquals(((Long) row[6]).longValue(), 3333l);
            it = tableSyncClient.scan(name, "mcc1", "mcc", 1235l, 0l, "ts", 0);
            Assert.assertTrue(it.valid());
            Assert.assertTrue(it.getCount() == 1);

            GetFuture gf = tableAsyncClient.get(name, "mcc1", "mcc", 1235l, "ts", null);
            row = gf.getRow();
            Assert.assertEquals(row[0], "card0");
            Assert.assertEquals(row[1], "mcc1");
            Assert.assertEquals(row[2], 1.6d);
            Assert.assertEquals(((Long) row[3]).longValue(), 1235l);
            Assert.assertEquals(((Long) row[4]).longValue(), 333l);
            Assert.assertEquals(row[5], "col_key1");
            Assert.assertEquals(((Long) row[6]).longValue(), 3333l);

            gf = tableAsyncClient.get(name, "mcc1", "mcc", 1235l, "ts_0", null);
            try {
                gf.getRow();
                Assert.assertTrue(false);
            } catch (Exception e) {
                Assert.assertTrue(true);
            }

            try {
                tableSyncClient.getRow(name, "mcc1", "mcc", 1235l, "ts_1", null);
                Assert.assertTrue(false);
            } catch (Exception e) {
                Assert.assertTrue(true);
            }

            Map<String, Object> query = new HashMap<String, Object>();
            query.put("card", "card0");
            query.put("mcc", "mcc0");
            it = tableSyncClient.scan(name, query, "card2", 3333l, 0l, "ts_2", 0);
            Assert.assertTrue(it.valid());
            Assert.assertTrue(it.getCount() == 1);
            try {
                it = tableSyncClient.scan(name, "col_key1", "col1", 1235l, 0l, "ts_2", 0);
                Assert.assertFalse(it.valid());
                Assert.assertTrue(false);
            } catch (Exception e) {
                Assert.assertTrue(true);
            }
            it = tableSyncClient.scan(name, "col_key1", "col1", 1235l, 0l, "ts_1", 0);
            Assert.assertTrue(it.valid());
            Assert.assertTrue(it.getCount() == 1);

            Assert.assertEquals(1, tableSyncClient.count(name, "mcc0", "mcc", "ts", false));
            try {
                Assert.assertEquals(0, tableSyncClient.count(name, "mcc0", "mcc", "ts_1", false));
                Assert.assertTrue(false);
            } catch (Exception e){
                Assert.assertTrue(true);
            }

            data.clear();
            data.put("card", "card0");
            data.put("mcc", "mcc0");
            data.put("amt", 1.7);
            data.put("ts", 1236l);
            data.put("ts_1", 334l);
            data.put("col1", "col_key2");
            data.put("ts_2", 3334l);
            tableSyncClient.put(name, data);
            Assert.assertEquals(2, tableSyncClient.count(name, query, "card2", "ts_1", false));
            Assert.assertEquals(2, tableSyncClient.count(name, query, "card2", "ts_2", false));
            try {
                tableSyncClient.count(name, query, "card2", "ts_0", false);
                Assert.assertTrue(false);
            } catch (Exception e) {
                Assert.assertTrue(true);
            }
            try {
                tableSyncClient.count(name, query, "card2", "ts", false);
            } catch (Exception e) {
                Assert.assertTrue(true);
            }

            it = tableSyncClient.traverse(name, "card2", "ts_1");
            int count = 0;
            for (; it.valid(); it.next(), count++) {
            }
            Assert.assertEquals(3, count);

            it = tableSyncClient.traverse(name, "card2", "ts_2");
            count = 0;
            for (; it.valid(); it.next(), count++) {
            }
            Assert.assertEquals(3, count);

            it = tableSyncClient.traverse(name, "card2");
            count = 0;
            for (; it.valid(); it.next(), count++) {
            }
            Assert.assertEquals(3, count);

            try {
                tableSyncClient.traverse(name, "card2", "ts_0");
                Assert.assertTrue(false);
            } catch (Exception e) {
                Assert.assertTrue(true);
            }
            try {
                tableSyncClient.traverse(name, "card2", "ts");
                Assert.assertTrue(false);
            } catch (Exception e) {
                Assert.assertTrue(true);
            }


            Assert.assertTrue(tableSyncClient.delete(name, "card0|mcc0", "card2"));
            it = tableSyncClient.scan(name, query, "card2", 3333, 0l, "ts_1", 0);
            Assert.assertTrue(it.getCount() == 0);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        }
        nsc.dropTable(name);
    }

    public void putData(String name) {
        long ts = 10000;
        for (int i = 0; i < 20; i++) {
            Map<String, Object> dataMap = new HashMap<String, Object>();
            dataMap.put("card", "card" + String.valueOf(i));
            dataMap.put("mcc", "mcc" + String.valueOf(i % 10));
            for (int j = 0; j < 20; j++) {
                dataMap.put("amt", 1.2d + j);
                dataMap.put("ts", Long.valueOf(ts));
                ts++;
                dataMap.put("ts1", Long.valueOf(j + 1));
                try {
                    boolean ret = tableSyncClient.put(name, dataMap);
                    Assert.assertTrue(ret);
                } catch (Exception e) {
                    Assert.assertTrue(false);
                }
            }
        }
    }

    public void putDataWithTS(String name) {
        long ts = 10000;
        for (int i = 0; i < 20; i++) {
            Map<String, Object> dataMap = new HashMap<String, Object>();
            dataMap.put("card", "card" + String.valueOf(i));
            dataMap.put("mcc", "mcc" + String.valueOf(i % 10));
            for (int j = 0; j < 20; j++) {
                dataMap.put("amt", 1.2d + j);
                dataMap.put("ts", Long.valueOf(ts));
                dataMap.put("ts1", Long.valueOf(j));
                try {
                    boolean ret = tableSyncClient.put(name, ts, dataMap);
                    Assert.assertTrue(ret);
                } catch (Exception e) {
                    Assert.assertTrue(false);
                }
                ts++;
            }
        }
    }

    @Test(dataProvider = "StorageMode")
    public void testTs(Common.StorageMode sm) {
        String name = String.valueOf(id.incrementAndGet());
        nsc.dropTable(name);
        ColumnDesc col0 = ColumnDesc.newBuilder().setName("card").setAddTsIdx(false).setType("string").build();
        ColumnDesc col1 = ColumnDesc.newBuilder().setName("mcc").setAddTsIdx(false).setType("string").build();
        ColumnDesc col2 = ColumnDesc.newBuilder().setName("amt").setAddTsIdx(false).setType("double").build();
        ColumnDesc col3 = ColumnDesc.newBuilder().setName("ts").setAddTsIdx(false).setType("int64").setIsTsCol(true).build();
        ColumnDesc col4 = ColumnDesc.newBuilder().setName("ts1").setAddTsIdx(false).setType("int64").build();
        ColumnKey colKey1 = ColumnKey.newBuilder().setIndexName("card").addTsName("ts").build();
        ColumnKey colKey2 = ColumnKey.newBuilder().setIndexName("mcc").addTsName("ts").build();
        TableInfo table = TableInfo.newBuilder()
                .setName(name).setTtl(0)
                .addColumnDescV1(col0).addColumnDescV1(col1).addColumnDescV1(col2).addColumnDescV1(col3).addColumnDescV1(col4)
                .addColumnKey(colKey1).addColumnKey(colKey2)
                .setStorageMode(sm)
                .build();
        boolean ok = nsc.createTable(table);
        Assert.assertTrue(ok);
        client.refreshRouteTable();
        putData(name);
        try {
            KvIterator it = tableSyncClient.scan(name, "card0", "card", 20000, 0, "xxx", 0);
            Assert.assertTrue(false);
        } catch (Exception e) {
            Assert.assertTrue(true);
        }
        try {
            Object[] row = tableSyncClient.getRow(name, "card0", "card", 0, "xxx", null);

            Assert.assertTrue(false);
        } catch (Exception e) {
            Assert.assertTrue(true);
        }
        try {
            KvIterator it = tableSyncClient.scan(name, "card0", "card", 20000, 0, null, 0);
            Assert.assertEquals(20, it.getCount());
            it = tableSyncClient.scan(name, "card0", "card", 20000, 0, "ts", 0);
            Assert.assertEquals(20, it.getCount());
            Object[] row = tableSyncClient.getRow(name, "card0", "card", 10008, "ts", null);
            Assert.assertEquals("card0", row[0]);
            Assert.assertEquals(9.2d, row[2]);
            Assert.assertEquals(10008, (long) row[3]);
            GetFuture gf = tableAsyncClient.get(name, "card0", "card", 10008, "ts", null);
            row = gf.getRow();
            Assert.assertEquals("card0", row[0]);
            Assert.assertEquals(9.2d, row[2]);
            Assert.assertEquals(10008, (long) row[3]);
            Assert.assertEquals(20, tableSyncClient.count(name, "card0", "card"));

            it = tableSyncClient.traverse(name, "card");
            int count = 0;
            while (it.valid()) {
                count++;
                it.next();
            }
            Assert.assertEquals(400, count);

        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        }
        nsc.dropTable(name);
    }

    @Test(dataProvider = "StorageMode")
    public void testTwoTs(Common.StorageMode sm) {
        String name = String.valueOf(id.incrementAndGet());
        nsc.dropTable(name);
        ColumnDesc col0 = ColumnDesc.newBuilder().setName("card").setAddTsIdx(false).setType("string").build();
        ColumnDesc col1 = ColumnDesc.newBuilder().setName("mcc").setAddTsIdx(false).setType("string").build();
        ColumnDesc col2 = ColumnDesc.newBuilder().setName("amt").setAddTsIdx(false).setType("double").build();
        ColumnDesc col3 = ColumnDesc.newBuilder().setName("ts").setAddTsIdx(false).setType("int64").setIsTsCol(true).build();
        ColumnDesc col4 = ColumnDesc.newBuilder().setName("ts1").setAddTsIdx(false).setType("int64").setIsTsCol(true).build();
        ColumnKey colKey1 = ColumnKey.newBuilder().setIndexName("card").addTsName("ts").addTsName("ts1").build();
        ColumnKey colKey2 = ColumnKey.newBuilder().setIndexName("mcc").addTsName("ts").build();
        TableInfo table = TableInfo.newBuilder()
                .setName(name).setTtl(0)
                .addColumnDescV1(col0).addColumnDescV1(col1).addColumnDescV1(col2).addColumnDescV1(col3).addColumnDescV1(col4)
                .addColumnKey(colKey1).addColumnKey(colKey2)
                .setStorageMode(sm)
                .build();
        boolean ok = nsc.createTable(table);
        Assert.assertTrue(ok);
        client.refreshRouteTable();
        putData(name);
        try {
            KvIterator it = tableSyncClient.scan(name, "card0", "card", 20000, 0, "xxx", 0);
            Assert.assertTrue(false);
        } catch (Exception e) {
            Assert.assertTrue(true);
        }
        try {
            Object[] row = tableSyncClient.getRow(name, "card0", "card", 0, "xxx", null);

            Assert.assertTrue(false);
        } catch (Exception e) {
            Assert.assertTrue(true);
        }
        try {
            KvIterator it = tableSyncClient.scan(name, "card0", "card", 20000, 0, null, 0);
            Assert.assertEquals(20, it.getCount());
            it = tableSyncClient.scan(name, "card0", "card", 20000, 0, "ts", 0);
            Assert.assertEquals(20, it.getCount());
            it = tableSyncClient.scan(name, "card0", "card", 20000, 0);
            Assert.assertEquals(20, it.getCount());
            it = tableSyncClient.scan(name, "card0", "card", 20000, 0, "ts1", 0);
            Assert.assertEquals(20, it.getCount());
            Object[] row = tableSyncClient.getRow(name, "card0", "card", 10008, "ts", null);
            Assert.assertEquals("card0", row[0]);
            Assert.assertEquals(9.2d, row[2]);
            Assert.assertEquals(10008, (long) row[3]);
            GetFuture gf = tableAsyncClient.get(name, "card0", "card", 10008, "ts", null);
            row = gf.getRow();
            Assert.assertEquals("card0", row[0]);
            Assert.assertEquals(9.2d, row[2]);
            Assert.assertEquals(10008, (long) row[3]);
            Assert.assertEquals(20, tableSyncClient.count(name, "card0", "card"));

            Assert.assertEquals(20, tableSyncClient.count(name, "card0", "card"));
            it = tableSyncClient.traverse(name, "card");
            int count = 0;
            while (it.valid()) {
                count++;
                it.next();
            }
            Assert.assertEquals(400, count);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        }

        Map<String, Object> data = new HashMap<>();
        try {
            data.put("card", "cardxxx");
            data.put("mcc", "mccx");
            data.put("amt", 1.2d);
            data.put("ts", 1122l);
            data.put("ts1", 2122l);
            Assert.assertTrue(tableSyncClient.put(name, data));
            data.put("amt", 1.3d);
            data.put("ts", null);
            data.put("ts1", 3122l);
            Assert.assertTrue(tableSyncClient.put(name, data));
            data.put("amt", 1.4d);
            data.put("ts", 1234l);
            data.put("ts1", null);
            Assert.assertTrue(tableSyncClient.put(name, data));
            data.put("amt", 1.5d);
            data.put("ts", 1334l);
            data.put("ts1", null);
            Assert.assertTrue(tableSyncClient.put(name, data));

            KvIterator it = tableSyncClient.scan(name, "cardxxx", "card", 20000, 0, null, 0);
            Assert.assertEquals(3, it.getCount());
            it = tableSyncClient.scan(name, "cardxxx", "card", 20000, 0, "ts", 0);
            Assert.assertEquals(3, it.getCount());
            it = tableSyncClient.scan(name, "cardxxx", "card", 20000, 0, "ts1", 0);
            Assert.assertEquals(2, it.getCount());
            Object[]row = tableSyncClient.getRow(name, "cardxxx", "card",  0, "ts", null);
            Assert.assertEquals("cardxxx", row[0]);
            Assert.assertEquals(1.5d, row[2]);
            Assert.assertEquals(1334, (long)row[3]);
            Assert.assertEquals(null, row[4]);
            GetFuture gf = tableAsyncClient.get(name, "cardxxx", "card",  0, "ts1", null);
            row = gf.getRow();
            Assert.assertEquals("cardxxx", row[0]);
            Assert.assertEquals(1.3d, row[2]);
            Assert.assertEquals(null, row[3]);
            Assert.assertEquals(3122, (long)row[4]);
            if (sm == Common.StorageMode.kMemory) {
                Assert.assertEquals(3, tableSyncClient.count(name, "cardxxx", "card", "ts", false));
                Assert.assertEquals(2, tableSyncClient.count(name, "cardxxx", "card", "ts1", false));
            }
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        }
        try {
            data.put("ts", null);
            data.put("ts1", null);
            tableSyncClient.put(name, data);
            Assert.assertTrue(false);
        } catch (Exception e) {
            Assert.assertTrue(true);
        }
        nsc.dropTable(name);
    }

    @Test
    public void testNullOneTs() {
        String name = String.valueOf(id.incrementAndGet());
        nsc.dropTable(name);
        ColumnDesc col0 = ColumnDesc.newBuilder().setName("card").setAddTsIdx(false).setType("string").build();
        ColumnDesc col1 = ColumnDesc.newBuilder().setName("mcc").setAddTsIdx(false).setType("string").build();
        ColumnDesc col2 = ColumnDesc.newBuilder().setName("amt").setAddTsIdx(false).setType("double").build();
        ColumnDesc col3 = ColumnDesc.newBuilder().setName("ts").setAddTsIdx(false).setType("int64").setIsTsCol(true).build();
        ColumnKey colKey1 = ColumnKey.newBuilder().setIndexName("card").build();
        ColumnKey colKey2 = ColumnKey.newBuilder().setIndexName("mcc").build();
        TableInfo table = TableInfo.newBuilder()
                .setName(name).setTtl(0)
                .addColumnDescV1(col0).addColumnDescV1(col1).addColumnDescV1(col2).addColumnDescV1(col3)
                .addColumnKey(colKey1).addColumnKey(colKey2)
                .build();
        boolean ok = nsc.createTable(table);
        Assert.assertTrue(ok);
        client.refreshRouteTable();
        Map<String, Object> data = new HashMap<>();
        try {
            data.put("card", "card0");
            data.put("mcc", "mcc0");
            data.put("amt", 1.2d);
            data.put("ts", null);
            tableSyncClient.put(name, data);
            Assert.assertTrue(false);
        } catch (Exception e) {
            Assert.assertTrue(true);
        }
        nsc.dropTable(name);
    }

    @Test(dataProvider = "StorageMode")
    public void testCombinedKey(Common.StorageMode sm) {
        String name = String.valueOf(id.incrementAndGet());
        nsc.dropTable(name);
        ColumnDesc col0 = ColumnDesc.newBuilder().setName("card").setAddTsIdx(false).setType("string").build();
        ColumnDesc col1 = ColumnDesc.newBuilder().setName("mcc").setAddTsIdx(false).setType("string").build();
        ColumnDesc col2 = ColumnDesc.newBuilder().setName("amt").setAddTsIdx(false).setType("double").build();
        ColumnDesc col3 = ColumnDesc.newBuilder().setName("ts").setAddTsIdx(false).setType("int64").build();
        ColumnDesc col4 = ColumnDesc.newBuilder().setName("ts1").setAddTsIdx(false).setType("int64").build();
        ColumnKey colKey1 = ColumnKey.newBuilder().setIndexName("card_mcc").addColName("card").addColName("mcc").build();
        TableInfo table = TableInfo.newBuilder()
                .setName(name).setTtl(0)
                .addColumnDescV1(col0).addColumnDescV1(col1).addColumnDescV1(col2).addColumnDescV1(col3).addColumnDescV1(col4)
                .addColumnKey(colKey1)
                .setStorageMode(sm)
                .build();
        boolean ok = nsc.createTable(table);
        Assert.assertTrue(ok);
        client.refreshRouteTable();
        putDataWithTS(name);
        try {
            KvIterator it = tableSyncClient.scan(name, new Object[]{"card0", "mcc0"}, "card_mcc", 20000, 0, "xxx", 0);
            Assert.assertTrue(false);
        } catch (Exception e) {
            Assert.assertTrue(true);
        }
        try {
            Object[] row = tableSyncClient.getRow(name, new Object[]{"card0", "mcc0"}, "card_mcc", 0, "xxx", null);

            Assert.assertTrue(false);
        } catch (Exception e) {
            Assert.assertTrue(true);
        }
        try {
            KvIterator it = tableSyncClient.scan(name, new Object[]{"card0", "mcc0"}, "card_mcc", 20000, 0, null, 0);
            Assert.assertEquals(20, it.getCount());
            ScanFuture sf = tableAsyncClient.scan(name, new Object[]{"card0", "mcc0"}, "card_mcc", 20000, 0, null, 0);
            it = sf.get();
            Assert.assertEquals(20, it.getCount());
            Object[] row = tableSyncClient.getRow(name, new Object[]{"card0", "mcc0"}, "card_mcc", 10008, null, null);
            Assert.assertEquals("card0", row[0]);
            Assert.assertEquals(9.2d, row[2]);
            Assert.assertEquals(10008, (long) row[3]);
            GetFuture gf = tableAsyncClient.get(name, new Object[]{"card0", "mcc0"}, "card_mcc", 10008, null, null);
            row = gf.getRow();
            Assert.assertEquals("card0", row[0]);
            Assert.assertEquals(9.2d, row[2]);
            Assert.assertEquals(10008, (long) row[3]);
            Assert.assertEquals(20, tableSyncClient.count(name, new Object[]{"card0", "mcc0"}, "card_mcc", null, true));

            it = tableSyncClient.traverse(name, "card_mcc");
            int count = 0;
            while (it.valid()) {
                count++;
                it.next();
            }
            Assert.assertEquals(400, count);

        } catch (Exception e) {
            Assert.assertTrue(false);
        }
        nsc.dropTable(name);
    }

    @Test(dataProvider = "StorageMode")
    public void testCombinedKeyTS(Common.StorageMode sm) {
        String name = String.valueOf(id.incrementAndGet());
        nsc.dropTable(name);
        ColumnDesc col0 = ColumnDesc.newBuilder().setName("card").setAddTsIdx(false).setType("string").build();
        ColumnDesc col1 = ColumnDesc.newBuilder().setName("mcc").setAddTsIdx(false).setType("string").build();
        ColumnDesc col2 = ColumnDesc.newBuilder().setName("amt").setAddTsIdx(false).setType("double").build();
        ColumnDesc col3 = ColumnDesc.newBuilder().setName("ts").setAddTsIdx(false).setType("int64").setIsTsCol(true).build();
        ColumnDesc col4 = ColumnDesc.newBuilder().setName("ts1").setAddTsIdx(false).setType("int64").build();
        ColumnKey colKey1 = ColumnKey.newBuilder().setIndexName("card_mcc").addColName("card").addColName("mcc").addTsName("ts").build();
        TableInfo table = TableInfo.newBuilder()
                .setName(name).setTtl(0)
                .addColumnDescV1(col0).addColumnDescV1(col1).addColumnDescV1(col2).addColumnDescV1(col3).addColumnDescV1(col4)
                .addColumnKey(colKey1)
                .setStorageMode(sm)
                .build();
        boolean ok = nsc.createTable(table);
        Assert.assertTrue(ok);
        client.refreshRouteTable();
        putData(name);
        try {
            KvIterator it = tableSyncClient.scan(name, new Object[]{"card0", "mcc0"}, "card_mcc", 20000, 0, "xxx", 0);
            Assert.assertTrue(false);
        } catch (Exception e) {
            Assert.assertTrue(true);
        }
        try {
            Object[] row = tableSyncClient.getRow(name, new Object[]{"card0", "mcc0"}, "card_mcc", 0, "xxx", null);

            Assert.assertTrue(false);
        } catch (Exception e) {
            Assert.assertTrue(true);
        }
        try {
            KvIterator it = tableSyncClient.scan(name, new Object[]{"card0", "mcc0"}, "card_mcc", 20000, 0, "ts", 0);
            Assert.assertEquals(20, it.getCount());
            ScanFuture sf = tableAsyncClient.scan(name, new Object[]{"card0", "mcc0"}, "card_mcc", 20000, 0, "ts", 0);
            it = sf.get();
            Assert.assertEquals(20, it.getCount());
            Object[] row = tableSyncClient.getRow(name, new Object[]{"card0", "mcc0"}, "card_mcc", 10008, "ts", null);
            Assert.assertEquals("card0", row[0]);
            Assert.assertEquals(9.2d, row[2]);
            Assert.assertEquals(10008, (long) row[3]);
            GetFuture gf = tableAsyncClient.get(name, new Object[]{"card0", "mcc0"}, "card_mcc", 10008, "ts", null);
            row = gf.getRow();
            Assert.assertEquals("card0", row[0]);
            Assert.assertEquals(9.2d, row[2]);
            Assert.assertEquals(10008, (long) row[3]);
            Assert.assertEquals(20, tableSyncClient.count(name, new Object[]{"card0", "mcc0"}, "card_mcc", "ts", true));

            it = tableSyncClient.traverse(name, "card_mcc");
            int count = 0;
            while (it.valid()) {
                count++;
                it.next();
            }
            Assert.assertEquals(400, count);

        } catch (Exception e) {
            Assert.assertTrue(false);
        }
        nsc.dropTable(name);
    }

    @Test(dataProvider = "StorageMode")
    public void testCombinedKeyTwoTS(Common.StorageMode sm) {
        String name = String.valueOf(id.incrementAndGet());
        nsc.dropTable(name);
        ColumnDesc col0 = ColumnDesc.newBuilder().setName("card").setAddTsIdx(false).setType("string").build();
        ColumnDesc col1 = ColumnDesc.newBuilder().setName("mcc").setAddTsIdx(false).setType("string").build();
        ColumnDesc col2 = ColumnDesc.newBuilder().setName("amt").setAddTsIdx(false).setType("double").build();
        ColumnDesc col3 = ColumnDesc.newBuilder().setName("ts").setAddTsIdx(false).setType("int64").setIsTsCol(true).build();
        ColumnDesc col4 = ColumnDesc.newBuilder().setName("ts1").setAddTsIdx(false).setType("int64").setIsTsCol(true).build();
        ColumnKey colKey1 = ColumnKey.newBuilder().setIndexName("card_mcc").addColName("card").addColName("mcc").addTsName("ts").addTsName("ts1").build();
        TableInfo table = TableInfo.newBuilder()
                .setName(name).setTtl(0)
                .addColumnDescV1(col0).addColumnDescV1(col1).addColumnDescV1(col2).addColumnDescV1(col3).addColumnDescV1(col4)
                .addColumnKey(colKey1)
                .setStorageMode(sm)
                .build();
        boolean ok = nsc.createTable(table);
        Assert.assertTrue(ok);
        client.refreshRouteTable();
        putData(name);
        try {
            KvIterator it = tableSyncClient.scan(name, new Object[]{"card0", "mcc0"}, "card_mcc", 20000, 0, "xxx", 0);
            Assert.assertTrue(false);
        } catch (Exception e) {
            Assert.assertTrue(true);
        }
        try {
            Object[] row = tableSyncClient.getRow(name, new Object[]{"card0", "mcc0"}, "card_mcc", 0, "xxx", null);

            Assert.assertTrue(false);
        } catch (Exception e) {
            Assert.assertTrue(true);
        }
        try {
            KvIterator it = tableSyncClient.scan(name, new Object[]{"card0", "mcc0"}, "card_mcc", 20000, 0, "ts", 0);
            Assert.assertEquals(20, it.getCount());
            it = tableSyncClient.scan(name, new Object[]{"card0", "mcc0"}, "card_mcc", 20000, 0, "ts1", 0);
            Assert.assertEquals(20, it.getCount());
            ScanFuture sf = tableAsyncClient.scan(name, new Object[]{"card0", "mcc0"}, "card_mcc", 20000, 0, "ts", 0);
            it = sf.get();
            Assert.assertEquals(20, it.getCount());
            Object[] row = tableSyncClient.getRow(name, new Object[]{"card0", "mcc0"}, "card_mcc", 10008, "ts", null);
            Assert.assertEquals("card0", row[0]);
            Assert.assertEquals(9.2d, row[2]);
            Assert.assertEquals(10008, (long) row[3]);
            GetFuture gf = tableAsyncClient.get(name, new Object[]{"card0", "mcc0"}, "card_mcc", 10008, "ts", null);
            row = gf.getRow();
            Assert.assertEquals("card0", row[0]);
            Assert.assertEquals(9.2d, row[2]);
            Assert.assertEquals(10008, (long) row[3]);
            Assert.assertEquals(20, tableSyncClient.count(name, new Object[]{"card0", "mcc0"}, "card_mcc", "ts", true));

            it = tableSyncClient.traverse(name, "card_mcc", "ts1");
            int count = 0;
            while (it.valid()) {
                count++;
                it.next();
            }
            Assert.assertEquals(400, count);

        } catch (Exception e) {
            Assert.assertTrue(false);
        }
        nsc.dropTable(name);
    }

    @Test(dataProvider = "StorageMode")
    public void testTwoColIndex(Common.StorageMode sm) {
        String name = String.valueOf(id.incrementAndGet());
        nsc.dropTable(name);
        ColumnDesc col0 = ColumnDesc.newBuilder().setName("card").setAddTsIdx(false).setType("string").build();
        ColumnDesc col1 = ColumnDesc.newBuilder().setName("mcc").setAddTsIdx(false).setType("string").build();
        ColumnDesc col2 = ColumnDesc.newBuilder().setName("amt").setAddTsIdx(false).setType("double").build();
        ColumnDesc col3 = ColumnDesc.newBuilder().setName("ts").setAddTsIdx(false).setType("int64").setIsTsCol(true).build();
        ColumnDesc col4 = ColumnDesc.newBuilder().setName("ts1").setAddTsIdx(false).setType("int64").setIsTsCol(true).build();
        ColumnKey colKey1 = ColumnKey.newBuilder().setIndexName("card").addColName("card").addTsName("ts").build();
        ColumnKey colKey2 = ColumnKey.newBuilder().setIndexName("card_mcc").addColName("card").addColName("mcc").addTsName("ts").addTsName("ts1").build();
        TableInfo table = TableInfo.newBuilder()
                .setName(name).setTtl(0)
                .addColumnDescV1(col0).addColumnDescV1(col1).addColumnDescV1(col2).addColumnDescV1(col3).addColumnDescV1(col4)
                .addColumnKey(colKey1).addColumnKey(colKey2)
                .setStorageMode(sm)
                .build();
        boolean ok = nsc.createTable(table);
        Assert.assertTrue(ok);
        client.refreshRouteTable();
        putData(name);
        try {
            KvIterator it = tableSyncClient.scan(name, new Object[]{"card0", "mcc0"}, "card_mcc", 20000, 0, "xxx", 0);
            Assert.assertTrue(false);
        } catch (Exception e) {
            Assert.assertTrue(true);
        }
        try {
            Object[] row = tableSyncClient.getRow(name, new Object[]{"card0", "mcc0"}, "card_mcc", 0, "xxx", null);

            Assert.assertTrue(false);
        } catch (Exception e) {
            Assert.assertTrue(true);
        }
        try {
            KvIterator it = tableSyncClient.scan(name, new Object[]{"card0", "mcc0"}, "card_mcc", 20000, 0, "ts", 0);
            Assert.assertEquals(20, it.getCount());
            it = tableSyncClient.scan(name, new Object[]{"card0", "mcc0"}, "card_mcc", 20000, 0, "ts1", 0);
            Assert.assertEquals(20, it.getCount());
            it = tableSyncClient.scan(name, "card11", "card", 20000, 0, "ts", 0);
            Assert.assertEquals(20, it.getCount());
            ScanFuture sf = tableAsyncClient.scan(name, new Object[]{"card0", "mcc0"}, "card_mcc", 20000, 0, "ts", 0);
            it = sf.get();
            Assert.assertEquals(20, it.getCount());
            Object[] row = tableSyncClient.getRow(name, new Object[]{"card0", "mcc0"}, "card_mcc", 10008, "ts", null);
            Assert.assertEquals("card0", row[0]);
            Assert.assertEquals(9.2d, row[2]);
            Assert.assertEquals(10008, (long) row[3]);
            GetFuture gf = tableAsyncClient.get(name, new Object[]{"card0", "mcc0"}, "card_mcc", 10008, "ts", null);
            row = gf.getRow();
            Assert.assertEquals("card0", row[0]);
            Assert.assertEquals(9.2d, row[2]);
            Assert.assertEquals(10008, (long) row[3]);
            Assert.assertEquals(20, tableSyncClient.count(name, new Object[]{"card0", "mcc0"}, "card_mcc", "ts", true));

            it = tableSyncClient.traverse(name, "card_mcc", "ts1");
            int count = 0;
            while (it.valid()) {
                count++;
                it.next();
            }
            Assert.assertEquals(400, count);

        } catch (Exception e) {
            Assert.assertTrue(false);
        }
        nsc.dropTable(name);
    }

    @Test(dataProvider = "StorageMode")
    public void testMutlColAndTS(Common.StorageMode sm) {
        String name = String.valueOf(id.incrementAndGet());
        nsc.dropTable(name);
        List<ColumnDesc> colList = new ArrayList<>();
        for (int i = 0; i < 11; i++) {
            colList.add(ColumnDesc.newBuilder().setName("col" + i).setAddTsIdx(false).setType("string").build());
        }
        for (int i = 1; i < 6; i++) {
            colList.add(ColumnDesc.newBuilder().setName("ts" + i).setAddTsIdx(false).setType("int64").setIsTsCol(true).build());
        }
        List<ColumnKey> keyList = new ArrayList<>();
        keyList.add(ColumnKey.newBuilder().setIndexName("col0").addTsName("ts1").build());
        keyList.add(ColumnKey.newBuilder().setIndexName("col1").addTsName("ts3").build());
        keyList.add(ColumnKey.newBuilder().setIndexName("col2").addTsName("ts5").build());
        keyList.add(ColumnKey.newBuilder().setIndexName("col3").addTsName("ts1").addTsName("ts2").build());
        keyList.add(ColumnKey.newBuilder().setIndexName("col4").addTsName("ts3").addTsName("ts5").build());
        keyList.add(ColumnKey.newBuilder().setIndexName("col5").addTsName("ts1").addTsName("ts3").addTsName("ts4").build());
        keyList.add(ColumnKey.newBuilder().setIndexName("col6").addTsName("ts1").addTsName("ts2").addTsName("ts3").build());
        keyList.add(ColumnKey.newBuilder().setIndexName("col7").addTsName("ts1").addTsName("ts2").addTsName("ts3").addTsName("ts5").build());
        keyList.add(ColumnKey.newBuilder().setIndexName("col8").addTsName("ts1").addTsName("ts2").addTsName("ts3")
                .addTsName("ts4").addTsName("ts5").build());
        keyList.add(ColumnKey.newBuilder().setIndexName("combine1").addColName("col1").addColName("col2").addTsName("ts1").addTsName("ts5").build());
        keyList.add(ColumnKey.newBuilder().setIndexName("combine2").addColName("col8").addColName("col9").addColName("col10")
                .addTsName("ts1").addTsName("ts2").addTsName("ts3").addTsName("ts5").build());
        TableInfo.Builder builder = TableInfo.newBuilder();
        builder.setName(name).setTtl(0);
        for (ColumnDesc col : colList) {
            builder.addColumnDescV1(col);
        }
        for (ColumnKey key : keyList) {
            builder.addColumnKey(key);
        }
        builder.setStorageMode(sm);
        TableInfo table = builder.build();
        boolean ok = nsc.createTable(table);
        Assert.assertTrue(ok);
        client.refreshRouteTable();
        Map<String, Object> keyMap = new HashMap<>();
        for (int i = 0; i < 11; i++) {
            keyMap.put("col" + i, "col_value" + i);
        }
        for (int i = 1; i < 6; i++) {
            keyMap.put("ts" + i, 100l + i);
        }
        try {
            tableSyncClient.put(name, keyMap);
        } catch (Exception e) {
            Assert.assertTrue(false);
        }
        try {
            for (int i = 1; i < 6; i++) {
                KvIterator it = tableSyncClient.scan(name, "col_value0", "col0", 200, 0, "ts" + i, 0);
                if (i == 1) {
                    Assert.assertTrue(it.valid());
                    Assert.assertEquals(it.getCount(), 1);
                } else {
                    Assert.assertFalse(it.valid());
                }

            }
            for (int i = 1; i < 6; i++) {
                KvIterator it = tableSyncClient.scan(name, "col_value1", "col1", 200, 0, "ts" + i, 0);
                if (i == 3) {
                    Assert.assertTrue(it.valid());
                    Assert.assertEquals(it.getCount(), 1);
                } else {
                    Assert.assertFalse(it.valid());
                }

            }
            for (int i = 1; i < 6; i++) {
                KvIterator it = tableSyncClient.scan(name, "col_value2", "col2", 200, 0, "ts" + i, 0);
                if (i == 5) {
                    Assert.assertTrue(it.valid());
                    Assert.assertEquals(it.getCount(), 1);
                } else {
                    Assert.assertFalse(it.valid());
                }

            }
            for (int i = 1; i < 6; i++) {
                KvIterator it = tableSyncClient.scan(name, "col_value3", "col3", 200, 0, "ts" + i, 0);
                if (i == 1 || i == 2) {
                    Assert.assertTrue(it.valid());
                    Assert.assertEquals(it.getCount(), 1);
                } else {
                    Assert.assertFalse(it.valid());
                }

            }
            for (int i = 1; i < 6; i++) {
                KvIterator it = tableSyncClient.scan(name, "col_value4", "col4", 200, 0, "ts" + i, 0);
                if (i == 3 || i == 5) {
                    Assert.assertTrue(it.valid());
                    Assert.assertEquals(it.getCount(), 1);
                } else {
                    Assert.assertFalse(it.valid());
                }

            }
            for (int i = 1; i < 6; i++) {
                KvIterator it = tableSyncClient.scan(name, "col_value5", "col5", 200, 0, "ts" + i, 0);
                if (i == 1 || i == 3 || i == 4) {
                    Assert.assertTrue(it.valid());
                    Assert.assertEquals(it.getCount(), 1);
                } else {
                    Assert.assertFalse(it.valid());
                }

            }
            for (int i = 1; i < 6; i++) {
                KvIterator it = tableSyncClient.scan(name, "col_value6", "col6", 200, 0, "ts" + i, 0);
                if (i == 1 || i == 2 || i == 3) {
                    Assert.assertTrue(it.valid());
                    Assert.assertEquals(it.getCount(), 1);
                } else {
                    Assert.assertFalse(it.valid());
                }

            }
            for (int i = 1; i < 6; i++) {
                KvIterator it = tableSyncClient.scan(name, "col_value7", "col7", 200, 0, "ts" + i, 0);
                if (i == 1 || i == 2 || i == 3 || i == 5) {
                    Assert.assertTrue(it.valid());
                    Assert.assertEquals(it.getCount(), 1);
                } else {
                    Assert.assertFalse(it.valid());
                }
            }
            for (int i = 1; i < 6; i++) {
                KvIterator it = tableSyncClient.scan(name, "col_value8", "col8", 200, 0, "ts" + i, 0);
                Assert.assertTrue(it.valid());
                Assert.assertEquals(it.getCount(), 1);
            }
            for (int i = 1; i < 6; i++) {
                Map<String, Object> scanMap = new HashMap<>();
                scanMap.put("col1", "col_value1");
                scanMap.put("col2", "col_value2");

                KvIterator it = tableSyncClient.scan(name, scanMap, "combine1", 200, 0, "ts" + i, 0);
                if (i == 1 || i == 5) {
                    Assert.assertTrue(it.valid());
                    Assert.assertEquals(it.getCount(), 1);
                } else {
                    Assert.assertFalse(it.valid());
                }
            }
            for (int i = 1; i < 6; i++) {
                Map<String, Object> scanMap = new HashMap<>();
                scanMap.put("col8", "col_value8");
                scanMap.put("col9", "col_value9");
                scanMap.put("col10", "col_value10");

                KvIterator it = tableSyncClient.scan(name, scanMap, "combine2", 200, 0, "ts" + i, 0);
                if (i == 1 || i == 2 || i == 3 || i == 5) {
                    Assert.assertTrue(it.valid());
                    Assert.assertEquals(it.getCount(), 1);
                } else {
                    Assert.assertFalse(it.valid());
                }
            }
        } catch (Exception e) {
            Assert.assertTrue(true);
        }

        try {
            KvIterator it = tableSyncClient.scan(name, "col_value0", "col0", 200, 0, "tsNo", 0);
            Assert.assertTrue(false);
        } catch (Exception e) {
            Assert.assertTrue(true);
        }
        try {
            KvIterator it = tableSyncClient.scan(name, "col_value9", "col9", 200, 0, "ts1", 0);
            Assert.assertFalse(true);
        } catch (Exception e) {
            Assert.assertTrue(true);
        }
        nsc.dropTable(name);
    }

    @Test(dataProvider = "StorageMode")
    public void testExpiredCount(Common.StorageMode sm) {
        String name = String.valueOf(id.incrementAndGet());
        nsc.dropTable(name);
        ColumnDesc col0 = ColumnDesc.newBuilder().setName("card").setAddTsIdx(false).setType("string").build();
        ColumnDesc col1 = ColumnDesc.newBuilder().setName("mcc").setAddTsIdx(false).setType("string").build();
        ColumnDesc col2 = ColumnDesc.newBuilder().setName("amt").setAddTsIdx(false).setType("double").build();
        ColumnDesc col3 = ColumnDesc.newBuilder().setName("ts").setAddTsIdx(false).setType("int64").setIsTsCol(true).build();
        ColumnDesc col4 = ColumnDesc.newBuilder().setName("ts_1").setAddTsIdx(false).setType("int64").setIsTsCol(true).build();
        ColumnDesc col5 = ColumnDesc.newBuilder().setName("col1").setAddTsIdx(false).setType("string").build();
        ColumnDesc col6 = ColumnDesc.newBuilder().setName("ts_2").setAddTsIdx(false).setType("int64").setIsTsCol(true).build();
        ColumnKey colKey1 = ColumnKey.newBuilder().setIndexName("card").addTsName("ts").addTsName("ts_1").build();
        ColumnKey colKey2 = ColumnKey.newBuilder().setIndexName("mcc").addTsName("ts").build();
        ColumnKey colKey3 = ColumnKey.newBuilder().setIndexName("col1").addTsName("ts_1").build();
        ColumnKey colKey4 = ColumnKey.newBuilder().setIndexName("card2").addColName("card").addColName("mcc").addTsName("ts_1").addTsName("ts_2").build();
        TableInfo table = TableInfo.newBuilder()
                .setName(name).setTtl(10)
                .addColumnDescV1(col0).addColumnDescV1(col1).addColumnDescV1(col2).addColumnDescV1(col3).addColumnDescV1(col4).addColumnDescV1(col5).addColumnDescV1(col6)
                .addColumnKey(colKey1).addColumnKey(colKey2).addColumnKey(colKey3).addColumnKey(colKey4)
                .setStorageMode(sm)
                .setPartitionNum(1).setReplicaNum(1)
                .build();
        boolean ok = nsc.createTable(table);
        Assert.assertTrue(ok);
        client.refreshRouteTable();
        long ts = System.currentTimeMillis();
        try {
            Map<String, Object> data = new HashMap<String, Object>();
            data.put("card", "card0");
            data.put("mcc", "mcc0");
            data.put("amt", 1.5);
            data.put("ts", ts + 1234l);
            data.put("ts_1", ts + 222l);
            data.put("col1", "col_key0");
            data.put("ts_2", ts + 2222l);
            tableSyncClient.put(name, data);
            data.clear();
            data.put("card", "card0");
            data.put("mcc", "mcc1");
            data.put("amt", 1.6);
            data.put("ts", ts + 1235l);
            data.put("ts_1", ts + 333l);
            data.put("col1", "col_key1");
            data.put("ts_2", ts + 3333l);
            tableSyncClient.put(name, data);
            data.clear();
            data.put("card", "card0");
            data.put("mcc", "mcc0");
            data.put("amt", 1.7);
            data.put("ts", ts + 1236l);
            data.put("ts_1", ts - 333l);
            data.put("col1", "col_key1");
            data.put("ts_2", ts - 20 * 60 * 1000);
            tableSyncClient.put(name, data);
            KvIterator it = tableSyncClient.scan(name, "card0", "card", ts + 1237l, 0l, "ts", 0);
            Assert.assertTrue(it.valid());
            Assert.assertTrue(it.getCount() == 3);

            Map<String, Object> query = new HashMap<String, Object>();
            query.put("card", "card0");
            query.put("mcc", "mcc0");
            Assert.assertEquals(2, tableSyncClient.count(name, query, "card2", "ts_1", false));
            Assert.assertEquals(2, tableSyncClient.count(name, query, "card2", "ts_1", true));
            Assert.assertEquals(1, tableSyncClient.count(name, query, "card2", "ts_2", true));
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        }
        nsc.dropTable(name);
    }
}
