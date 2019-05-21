package com._4paradigm.rtidb.client.ut.ha;

import com._4paradigm.rtidb.client.KvIterator;
import com._4paradigm.rtidb.client.TabletException;
import com._4paradigm.rtidb.client.ut.Config;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com._4paradigm.rtidb.client.TableSyncClient;
import com._4paradigm.rtidb.client.ha.RTIDBClientConfig;
import com._4paradigm.rtidb.client.ha.impl.NameServerClientImpl;
import com._4paradigm.rtidb.client.ha.impl.RTIDBClusterClient;
import com._4paradigm.rtidb.client.impl.TableSyncClientImpl;
import com._4paradigm.rtidb.common.Common.ColumnDesc;
import com._4paradigm.rtidb.common.Common.ColumnKey;
import com._4paradigm.rtidb.ns.NS.TableInfo;
import com._4paradigm.rtidb.ns.NS.TablePartition;

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

    @BeforeClass
    public static void setUp() {
        try {
            nsc.init();
            config.setZkEndpoints(zkEndpoints);
            config.setZkRootPath(zkRootPath);
            client = new RTIDBClusterClient(config);
            client.init();
            tableSyncClient = new TableSyncClientImpl(client);
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
        ColumnKey colKey1 = ColumnKey.newBuilder().setKeyName("card_mcc").addColName("card").addColName("mcc").setTsName("ts").build();
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
        ColumnKey colKey1 = ColumnKey.newBuilder().setKeyName("card").setTsName("ts").build();
        ColumnKey colKey2 = ColumnKey.newBuilder().setKeyName("card").setTsName("ts_1").build();
        ColumnKey colKey3 = ColumnKey.newBuilder().setKeyName("mcc").setTsName("ts").build();
        TableInfo table = TableInfo.newBuilder()
                .setName(name).setTtl(0)
                .addColumnDescV1(col0).addColumnDescV1(col1).addColumnDescV1(col2).addColumnDescV1(col3).addColumnDescV1(col4)
                .addColumnKey(colKey1).addColumnKey(colKey2).addColumnKey(colKey3)
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
        //nsc.dropTable(name);
    }

}
