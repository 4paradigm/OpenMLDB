package com._4paradigm.rtidb.client.ut.ha;

import com._4paradigm.rtidb.client.*;
import com._4paradigm.rtidb.client.impl.TableAsyncClientImpl;
import com._4paradigm.rtidb.client.ut.Config;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com._4paradigm.rtidb.client.ha.RTIDBClientConfig;
import com._4paradigm.rtidb.client.ha.impl.NameServerClientImpl;
import com._4paradigm.rtidb.client.ha.impl.RTIDBClusterClient;
import com._4paradigm.rtidb.client.impl.TableSyncClientImpl;
import com._4paradigm.rtidb.common.Common.ColumnDesc;
import com._4paradigm.rtidb.common.Common.ColumnKey;
import com._4paradigm.rtidb.ns.NS.TableInfo;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;


public class ColumnKeyTest {
    private final static Logger logger = LoggerFactory.getLogger(TableSchemaTest.class);
    private static String zkEndpoints = Config.ZK_ENDPOINTS;
    private static String zkRootPath = Config.ZK_ROOT_PATH;
    private static String leaderPath  = zkRootPath + "/leader";
    private static AtomicInteger id = new AtomicInteger(50000);
    private static NameServerClientImpl nsc = new NameServerClientImpl(zkEndpoints, leaderPath);
    private static RTIDBClientConfig config = new RTIDBClientConfig();
    private static RTIDBClusterClient client = null;
    private static TableSyncClient tableSyncClient = null;
    private static TableAsyncClient tableAsyncClient = null;

    @BeforeClass
    public static void setUp() {
        try {
            nsc.init();
            config.setZkEndpoints(zkEndpoints);
            config.setZkRootPath(zkRootPath);
            client = new RTIDBClusterClient(config);
            client.init();
            tableSyncClient = new TableSyncClientImpl(client);
            tableAsyncClient = new TableAsyncClientImpl(client);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    @AfterClass
    public static void tearDown() {
        nsc.close();
    }

    @Test
    public void testPutNoTs() {
        String name = String.valueOf(id.incrementAndGet());
        nsc.dropTable(name);
        ColumnDesc col0 = ColumnDesc.newBuilder().setName("card").setAddTsIdx(true).setType("string").build();
        ColumnDesc col1 = ColumnDesc.newBuilder().setName("mcc").setAddTsIdx(false).setType("string").build();
        ColumnDesc col2 = ColumnDesc.newBuilder().setName("amt").setAddTsIdx(false).setType("double").build();
        ColumnDesc col3 = ColumnDesc.newBuilder().setName("ts").setAddTsIdx(false).setType("int64").setIsTsCol(true).build();
        TableInfo table = TableInfo.newBuilder()
                .setName(name).setTtl(0)
                .addColumnDescV1(col0).addColumnDescV1(col1).addColumnDescV1(col2).addColumnDescV1(col3)
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
        nsc.dropTable(name);
    }

    @Test
    public void testPutTwoIndex() {
        String name = String.valueOf(id.incrementAndGet());
        nsc.dropTable(name);
        ColumnDesc col0 = ColumnDesc.newBuilder().setName("card").setAddTsIdx(true).setType("string").build();
        ColumnDesc col1 = ColumnDesc.newBuilder().setName("mcc").setAddTsIdx(true).setType("string").build();
        ColumnDesc col2 = ColumnDesc.newBuilder().setName("amt").setAddTsIdx(false).setType("double").build();
        ColumnDesc col3 = ColumnDesc.newBuilder().setName("ts").setAddTsIdx(false).setType("int64").setIsTsCol(true).build();
        TableInfo table = TableInfo.newBuilder()
                .setName(name).setTtl(0)
                .addColumnDescV1(col0).addColumnDescV1(col1).addColumnDescV1(col2).addColumnDescV1(col3)
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

    @Test
    public void testPutCombinedKey() {
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
            Assert.assertEquals(row[0],"card0");
            Assert.assertEquals(row[1],"mcc0");
            Assert.assertEquals(row[2],1.5d);
            Assert.assertEquals(((Long)row[3]).longValue(),1234l);
            scan_key.put("mcc", "mcc1");
            it = tableSyncClient.scan(name, scan_key, "card_mcc", 1235l, 0l, "ts", 0);
            Assert.assertTrue(it.valid());
            Assert.assertTrue(it.getCount() == 1);
            scan_key.put("mcc", "mcc2");
            it = tableSyncClient.scan(name, scan_key, "card_mcc", 1235l, 0l, "ts", 0);
            Assert.assertFalse(it.valid());
            row = tableSyncClient.getRow(name, new Object[] {"card0", "mcc0"}, "card_mcc", 1234, "ts", null);
            Assert.assertEquals(row.length, 4);
            Assert.assertEquals(row[0],"card0");
            Assert.assertEquals(row[1],"mcc0");
            Assert.assertEquals(row[2],1.5d);

            Map<String, Object> key_map = new HashMap<String, Object>();
            key_map.put("card", "card0");
            key_map.put("mcc", "mcc1");
            row = tableSyncClient.getRow(name, key_map, "card_mcc", 1235, "ts", null);
            Assert.assertEquals(row[0],"card0");
            Assert.assertEquals(row[1],"mcc1");
            Assert.assertEquals(row[2],1.6d);

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
            Assert.assertEquals(row[0],"card0");
            Assert.assertEquals(row[1],"mcc1");
            Assert.assertEquals(row[2],1.7d);

            GetFuture gf = tableAsyncClient.get(name, key_map, "card_mcc", 1235, "ts", null);
            row = gf.getRow();
            Assert.assertEquals(row[0],"card0");
            Assert.assertEquals(row[1],"mcc1");
            Assert.assertEquals(row[2],1.6d);

            data.clear();
            data.put("card", "card0");
            data.put("mcc", "");
            data.put("amt", 1.8);
            data.put("ts", 1250l);
            tableSyncClient.put(name, data);
            row = tableSyncClient.getRow(name, new Object[] {"card0", ""}, "card_mcc", 0, "ts", null);
            Assert.assertEquals(row[0], "card0");
            Assert.assertEquals(row[1], "");
            Assert.assertEquals(row[2], 1.8);
        } catch (Exception e) {
            Assert.assertTrue(false);
        }
        nsc.dropTable(name);
    }

    @Test
    public void testPutMultiCombinedKey() {
        String name = String.valueOf(id.incrementAndGet());
        nsc.dropTable(name);
        ColumnDesc col0 = ColumnDesc.newBuilder().setName("card").setAddTsIdx(false).setType("string").build();
        ColumnDesc col1 = ColumnDesc.newBuilder().setName("mcc").setAddTsIdx(false).setType("string").build();
        ColumnDesc col2 = ColumnDesc.newBuilder().setName("amt").setAddTsIdx(false).setType("double").build();
        ColumnDesc col3 = ColumnDesc.newBuilder().setName("ts").setAddTsIdx(false).setType("int64").setIsTsCol(true).build();
        ColumnDesc col4 = ColumnDesc.newBuilder().setName("ts_1").setAddTsIdx(false).setType("int64").setIsTsCol(true).build();
        ColumnKey colKey1 = ColumnKey.newBuilder().setIndexName("card").addTsName("ts").addTsName("ts_1").build();
        ColumnKey colKey2 = ColumnKey.newBuilder().setIndexName("mcc").addTsName("ts").build();
        TableInfo table = TableInfo.newBuilder()
                .setName(name).setTtl(0)
                .addColumnDescV1(col0).addColumnDescV1(col1).addColumnDescV1(col2).addColumnDescV1(col3).addColumnDescV1(col4)
                .addColumnKey(colKey1).addColumnKey(colKey2)
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
            tableSyncClient.put(name, data);
            data.clear();
            data.put("card", "card0");
            data.put("mcc", "mcc1");
            data.put("amt", 1.6);
            data.put("ts", 1235l);
            data.put("ts_1", 333l);
            tableSyncClient.put(name, data);
            KvIterator it = tableSyncClient.scan(name, "card0", "card", 1235l, 0l, "ts", 0);
            Assert.assertTrue(it.valid());
            Assert.assertTrue(it.getCount() == 2);
            Object[] row = it.getDecodedValue();
            Assert.assertEquals(it.getKey(), 1235);
            Assert.assertEquals(row.length, 5);
            Assert.assertEquals(row[0],"card0");
            Assert.assertEquals(row[1],"mcc1");
            Assert.assertEquals(row[2],1.6d);
            Assert.assertEquals(((Long)row[3]).longValue(),1235l);
            Assert.assertEquals(((Long)row[4]).longValue(),333l);
            it = tableSyncClient.scan(name, "card0", "card", 1235l, 0l, "ts_1", 0);
            Assert.assertTrue(it.getCount() == 2);
            row = it.getDecodedValue();
            Assert.assertEquals(it.getKey(), 333);
            Assert.assertEquals(row.length, 5);
            Assert.assertEquals(row[0],"card0");
            Assert.assertEquals(row[1],"mcc1");
            Assert.assertEquals(row[2],1.6d);
            Assert.assertEquals(((Long)row[3]).longValue(),1235l);
            Assert.assertEquals(((Long)row[4]).longValue(),333l);
            it = tableSyncClient.scan(name, "mcc1", "mcc", 1235l, 0l, "ts", 0);
            Assert.assertTrue(it.valid());
            Assert.assertTrue(it.getCount() == 1);
        } catch (Exception e) {
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

    @Test
    public void testTs() {
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
            Object[]row = tableSyncClient.getRow(name, "card0", "card",  0, "xxx", null);

            Assert.assertTrue(false);
        } catch (Exception e) {
            Assert.assertTrue(true);
        }
        try {
            KvIterator it = tableSyncClient.scan(name, "card0", "card", 20000, 0, null, 0);
            Assert.assertEquals(20, it.getCount());
            it = tableSyncClient.scan(name, "card0", "card", 20000, 0, "ts", 0);
            Assert.assertEquals(20, it.getCount());
            Object[]row = tableSyncClient.getRow(name, "card0", "card",  10008, "ts", null);
            Assert.assertEquals("card0", row[0]);
            Assert.assertEquals(9.2d, row[2]);
            Assert.assertEquals(10008, (long)row[3]);
            GetFuture gf = tableAsyncClient.get(name, "card0", "card",  10008, "ts", null);
            row = gf.getRow();
            Assert.assertEquals("card0", row[0]);
            Assert.assertEquals(9.2d, row[2]);
            Assert.assertEquals(10008, (long)row[3]);
            Assert.assertEquals(20, tableSyncClient.count(name, "card0", "card"));

            it = tableSyncClient.traverse(name, "card");
            int count = 0;
            while(it.valid()) {
                count++;
                it.next();
            }
            Assert.assertEquals(400, count);

        } catch (Exception e) {
            Assert.assertTrue(false);
        }
        nsc.dropTable(name);
    }

    @Test
    public void testTwoTs() {
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
            Object[]row = tableSyncClient.getRow(name, "card0", "card",  0, "xxx", null);

            Assert.assertTrue(false);
        } catch (Exception e) {
            Assert.assertTrue(true);
        }
        try {
            KvIterator it = tableSyncClient.scan(name, "card0", "card", 20000, 0, null, 0);
            Assert.assertEquals(20, it.getCount());
            it = tableSyncClient.scan(name, "card0", "card", 20000, 0, "ts", 0);
            Assert.assertEquals(20, it.getCount());
            it = tableSyncClient.scan(name, "card0", "card", 20000, 0, "ts1", 0);
            Assert.assertEquals(20, it.getCount());
            Object[]row = tableSyncClient.getRow(name, "card0", "card",  10008, "ts", null);
            Assert.assertEquals("card0", row[0]);
            Assert.assertEquals(9.2d, row[2]);
            Assert.assertEquals(10008, (long)row[3]);
            GetFuture gf = tableAsyncClient.get(name, "card0", "card",  10008, "ts", null);
            row = gf.getRow();
            Assert.assertEquals("card0", row[0]);
            Assert.assertEquals(9.2d, row[2]);
            Assert.assertEquals(10008, (long)row[3]);
            Assert.assertEquals(20, tableSyncClient.count(name, "card0", "card"));

            it = tableSyncClient.traverse(name, "card");
            int count = 0;
            while(it.valid()) {
                count++;
                it.next();
            }
            Assert.assertEquals(400, count);

        } catch (Exception e) {
            Assert.assertTrue(false);
        }
        nsc.dropTable(name);
    }

    @Test
    public void testCombinedKey() {
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
                .build();
        boolean ok = nsc.createTable(table);
        Assert.assertTrue(ok);
        client.refreshRouteTable();
        putDataWithTS(name);
        try {
            KvIterator it = tableSyncClient.scan(name, new Object[] {"card0", "mcc0"}, "card_mcc", 20000, 0, "xxx", 0);
            Assert.assertTrue(false);
        } catch (Exception e) {
            Assert.assertTrue(true);
        }
        try {
            Object[]row = tableSyncClient.getRow(name, new Object[] {"card0", "mcc0"}, "card_mcc",  0, "xxx", null);

            Assert.assertTrue(false);
        } catch (Exception e) {
            Assert.assertTrue(true);
        }
        try {
            KvIterator it = tableSyncClient.scan(name, new Object[] {"card0", "mcc0"}, "card_mcc", 20000, 0, null, 0);
            Assert.assertEquals(20, it.getCount());
            ScanFuture sf = tableAsyncClient.scan(name, new Object[] {"card0", "mcc0"}, "card_mcc", 20000, 0, null, 0);
            it = sf.get();
            Assert.assertEquals(20, it.getCount());
            Object[]row = tableSyncClient.getRow(name, new Object[] {"card0", "mcc0"}, "card_mcc",  10008, null, null);
            Assert.assertEquals("card0", row[0]);
            Assert.assertEquals(9.2d, row[2]);
            Assert.assertEquals(10008, (long)row[3]);
            GetFuture gf = tableAsyncClient.get(name, new Object[] {"card0", "mcc0"}, "card_mcc",  10008, null, null);
            row = gf.getRow();
            Assert.assertEquals("card0", row[0]);
            Assert.assertEquals(9.2d, row[2]);
            Assert.assertEquals(10008, (long)row[3]);
            Assert.assertEquals(20, tableSyncClient.count(name, new Object[] {"card0", "mcc0"}, "card_mcc", null, true));

            it = tableSyncClient.traverse(name, "card_mcc");
            int count = 0;
            while(it.valid()) {
                count++;
                it.next();
            }
            Assert.assertEquals(400, count);

        } catch (Exception e) {
            Assert.assertTrue(false);
        }
        nsc.dropTable(name);
    }

    @Test
    public void testCombinedKeyTS() {
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
                .build();
        boolean ok = nsc.createTable(table);
        Assert.assertTrue(ok);
        client.refreshRouteTable();
        putData(name);
        try {
            KvIterator it = tableSyncClient.scan(name, new Object[] {"card0", "mcc0"}, "card_mcc", 20000, 0, "xxx", 0);
            Assert.assertTrue(false);
        } catch (Exception e) {
            Assert.assertTrue(true);
        }
        try {
            Object[]row = tableSyncClient.getRow(name, new Object[] {"card0", "mcc0"}, "card_mcc",  0, "xxx", null);

            Assert.assertTrue(false);
        } catch (Exception e) {
            Assert.assertTrue(true);
        }
        try {
            KvIterator it = tableSyncClient.scan(name, new Object[] {"card0", "mcc0"}, "card_mcc", 20000, 0, "ts", 0);
            Assert.assertEquals(20, it.getCount());
            ScanFuture sf = tableAsyncClient.scan(name, new Object[] {"card0", "mcc0"}, "card_mcc", 20000, 0, "ts", 0);
            it = sf.get();
            Assert.assertEquals(20, it.getCount());
            Object[]row = tableSyncClient.getRow(name, new Object[] {"card0", "mcc0"}, "card_mcc",  10008, "ts", null);
            Assert.assertEquals("card0", row[0]);
            Assert.assertEquals(9.2d, row[2]);
            Assert.assertEquals(10008, (long)row[3]);
            GetFuture gf = tableAsyncClient.get(name, new Object[] {"card0", "mcc0"}, "card_mcc",  10008, "ts", null);
            row = gf.getRow();
            Assert.assertEquals("card0", row[0]);
            Assert.assertEquals(9.2d, row[2]);
            Assert.assertEquals(10008, (long)row[3]);
            Assert.assertEquals(20, tableSyncClient.count(name, new Object[] {"card0", "mcc0"}, "card_mcc", "ts", true));

            it = tableSyncClient.traverse(name, "card_mcc");
            int count = 0;
            while(it.valid()) {
                count++;
                it.next();
            }
            Assert.assertEquals(400, count);

        } catch (Exception e) {
            Assert.assertTrue(false);
        }
        nsc.dropTable(name);
    }

    @Test
    public void testCombinedKeyTwoTS() {
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
                .build();
        boolean ok = nsc.createTable(table);
        Assert.assertTrue(ok);
        client.refreshRouteTable();
        putData(name);
        try {
            KvIterator it = tableSyncClient.scan(name, new Object[] {"card0", "mcc0"}, "card_mcc", 20000, 0, "xxx", 0);
            Assert.assertTrue(false);
        } catch (Exception e) {
            Assert.assertTrue(true);
        }
        try {
            Object[]row = tableSyncClient.getRow(name, new Object[] {"card0", "mcc0"}, "card_mcc",  0, "xxx", null);

            Assert.assertTrue(false);
        } catch (Exception e) {
            Assert.assertTrue(true);
        }
        try {
            KvIterator it = tableSyncClient.scan(name, new Object[] {"card0", "mcc0"}, "card_mcc", 20000, 0, "ts", 0);
            Assert.assertEquals(20, it.getCount());
            it = tableSyncClient.scan(name, new Object[] {"card0", "mcc0"}, "card_mcc", 20000, 0, "ts1", 0);
            Assert.assertEquals(20, it.getCount());
            ScanFuture sf = tableAsyncClient.scan(name, new Object[] {"card0", "mcc0"}, "card_mcc", 20000, 0, "ts", 0);
            it = sf.get();
            Assert.assertEquals(20, it.getCount());
            Object[]row = tableSyncClient.getRow(name, new Object[] {"card0", "mcc0"}, "card_mcc",  10008, "ts", null);
            Assert.assertEquals("card0", row[0]);
            Assert.assertEquals(9.2d, row[2]);
            Assert.assertEquals(10008, (long)row[3]);
            GetFuture gf = tableAsyncClient.get(name, new Object[] {"card0", "mcc0"}, "card_mcc",  10008, "ts", null);
            row = gf.getRow();
            Assert.assertEquals("card0", row[0]);
            Assert.assertEquals(9.2d, row[2]);
            Assert.assertEquals(10008, (long)row[3]);
            Assert.assertEquals(20, tableSyncClient.count(name, new Object[] {"card0", "mcc0"}, "card_mcc", "ts", true));

            it = tableSyncClient.traverse(name, "card_mcc", "ts1");
            int count = 0;
            while(it.valid()) {
                count++;
                it.next();
            }
            Assert.assertEquals(400, count);

        } catch (Exception e) {
            Assert.assertTrue(false);
        }
        nsc.dropTable(name);
    }

    @Test
    public void testTwoColIndex() {
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
                .build();
        boolean ok = nsc.createTable(table);
        Assert.assertTrue(ok);
        client.refreshRouteTable();
        putData(name);
        try {
            KvIterator it = tableSyncClient.scan(name, new Object[] {"card0", "mcc0"}, "card_mcc", 20000, 0, "xxx", 0);
            Assert.assertTrue(false);
        } catch (Exception e) {
            Assert.assertTrue(true);
        }
        try {
            Object[]row = tableSyncClient.getRow(name, new Object[] {"card0", "mcc0"}, "card_mcc",  0, "xxx", null);

            Assert.assertTrue(false);
        } catch (Exception e) {
            Assert.assertTrue(true);
        }
        try {
            KvIterator it = tableSyncClient.scan(name, new Object[] {"card0", "mcc0"}, "card_mcc", 20000, 0, "ts", 0);
            Assert.assertEquals(20, it.getCount());
            it = tableSyncClient.scan(name, new Object[] {"card0", "mcc0"}, "card_mcc", 20000, 0, "ts1", 0);
            Assert.assertEquals(20, it.getCount());
            it = tableSyncClient.scan(name, "card11", "card", 20000, 0, "ts", 0);
            Assert.assertEquals(20, it.getCount());
            ScanFuture sf = tableAsyncClient.scan(name, new Object[] {"card0", "mcc0"}, "card_mcc", 20000, 0, "ts", 0);
            it = sf.get();
            Assert.assertEquals(20, it.getCount());
            Object[]row = tableSyncClient.getRow(name, new Object[] {"card0", "mcc0"}, "card_mcc",  10008, "ts", null);
            Assert.assertEquals("card0", row[0]);
            Assert.assertEquals(9.2d, row[2]);
            Assert.assertEquals(10008, (long)row[3]);
            GetFuture gf = tableAsyncClient.get(name, new Object[] {"card0", "mcc0"}, "card_mcc",  10008, "ts", null);
            row = gf.getRow();
            Assert.assertEquals("card0", row[0]);
            Assert.assertEquals(9.2d, row[2]);
            Assert.assertEquals(10008, (long)row[3]);
            Assert.assertEquals(20, tableSyncClient.count(name, new Object[] {"card0", "mcc0"}, "card_mcc", "ts", true));

            it = tableSyncClient.traverse(name, "card_mcc", "ts1");
            int count = 0;
            while(it.valid()) {
                count++;
                it.next();
            }
            Assert.assertEquals(400, count);

        } catch (Exception e) {
            Assert.assertTrue(false);
        }
        nsc.dropTable(name);
    }

}
