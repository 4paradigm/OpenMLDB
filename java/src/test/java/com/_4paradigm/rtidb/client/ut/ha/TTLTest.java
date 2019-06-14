package com._4paradigm.rtidb.client.ut.ha;

import com._4paradigm.rtidb.client.KvIterator;
import com._4paradigm.rtidb.client.TableSyncClient;
import com._4paradigm.rtidb.client.TabletException;
import com._4paradigm.rtidb.client.ha.RTIDBClientConfig;
import com._4paradigm.rtidb.client.ha.TableHandler;
import com._4paradigm.rtidb.client.ha.impl.NameServerClientImpl;
import com._4paradigm.rtidb.client.ha.impl.RTIDBClusterClient;
import com._4paradigm.rtidb.client.impl.TableSyncClientImpl;
import com._4paradigm.rtidb.client.ut.Config;
import com._4paradigm.rtidb.common.Common;
import com._4paradigm.rtidb.ns.NS;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.concurrent.atomic.AtomicInteger;

public class TTLTest {
    private static String zkEndpoints = Config.ZK_ENDPOINTS;
    private static String zkRootPath = Config.ZK_ROOT_PATH;
    private static String leaderPath  = zkRootPath + "/leader";
    private static AtomicInteger id = new AtomicInteger(10000);
    private static NameServerClientImpl nsc = new NameServerClientImpl(zkEndpoints, leaderPath);
    private static RTIDBClientConfig config = new RTIDBClientConfig();
    private static RTIDBClusterClient client = null;
    private static TableSyncClient tableSyncClient = null;
    private static String[] nodes = Config.NODES;

    @BeforeClass
    public static void setUp() {
        try {
            nsc.init();
            config.setZkEndpoints(zkEndpoints);
            config.setZkRootPath(zkRootPath);
            config.setGlobalReadStrategies(TableHandler.ReadStrategy.kReadLeader);
            client = new RTIDBClusterClient(config);
            client.init();
            tableSyncClient = new TableSyncClientImpl(client);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    @AfterClass
    public static void closeResource() {
        nsc.close();
        client.close();
    }

    @Test
    public void testOneTS() {
        String name = String.valueOf(id.incrementAndGet());
        nsc.dropTable(name);
        Common.ColumnDesc col0 = Common.ColumnDesc.newBuilder().setName("card").setAddTsIdx(false).setType("string").build();
        Common.ColumnDesc col1 = Common.ColumnDesc.newBuilder().setName("mcc").setAddTsIdx(false).setType("string").build();
        Common.ColumnDesc col2 = Common.ColumnDesc.newBuilder().setName("amt").setAddTsIdx(false).setType("double").build();
        Common.ColumnDesc col3 = Common.ColumnDesc.newBuilder().setName("ts").setAddTsIdx(false).setType("int64").setIsTsCol(true).build();
        Common.ColumnKey colKey1 = Common.ColumnKey.newBuilder().setIndexName("card").addColName("card").addTsName("ts").build();
        NS.TableInfo table = NS.TableInfo.newBuilder()
                .setName(name).setTtl(10)
                .addColumnDescV1(col0).addColumnDescV1(col1).addColumnDescV1(col2).addColumnDescV1(col3)
                .addColumnKey(colKey1)
                .build();
        boolean ok = nsc.createTable(table);
        Assert.assertTrue(ok);
        client.refreshRouteTable();
        long curTime = System.currentTimeMillis();
        try {
            tableSyncClient.put(name, new Object[]{"card0", "mcc0", 1.1d, curTime - 10 * 60 * 1000 - 1});
            tableSyncClient.put(name, new Object[]{"card0", "mcc0", 9.2d, curTime - 10});
            tableSyncClient.put(name, new Object[]{"card0", "mcc1", 15.6d, curTime});
            KvIterator it = tableSyncClient.scan(name, "card0", "card", curTime, 0l, "ts", 0);
            Assert.assertTrue(it.valid());
            Assert.assertTrue(it.getCount() == 2);
            Object[] row = it.getDecodedValue();
            Assert.assertEquals(it.getKey(), curTime);
            Assert.assertEquals(row[0],"card0");
            Assert.assertEquals(row[1],"mcc1");
            Assert.assertEquals(row[2],15.6d);
            it.next();
            row = it.getDecodedValue();
            Assert.assertEquals(it.getKey(), curTime - 10);
            Assert.assertEquals(row[0],"card0");
            Assert.assertEquals(row[1],"mcc0");
            Assert.assertEquals(row[2],9.2d);
            it.next();
            Assert.assertFalse(it.valid());
        } catch (Exception e) {
            Assert.assertTrue(false);
        } finally {
            nsc.dropTable(name);
        }
    }

    @Test
    public void testTwoTS() {
        String name = String.valueOf(id.incrementAndGet());
        nsc.dropTable(name);
        Common.ColumnDesc col0 = Common.ColumnDesc.newBuilder().setName("card").setAddTsIdx(false).setType("string").build();
        Common.ColumnDesc col1 = Common.ColumnDesc.newBuilder().setName("mcc").setAddTsIdx(false).setType("string").build();
        Common.ColumnDesc col2 = Common.ColumnDesc.newBuilder().setName("amt").setAddTsIdx(false).setType("double").build();
        Common.ColumnDesc col3 = Common.ColumnDesc.newBuilder().setName("ts1").setAddTsIdx(false).setType("int64").setIsTsCol(true).build();
        Common.ColumnDesc col4 = Common.ColumnDesc.newBuilder().setName("ts2").setAddTsIdx(false).setType("int64").setIsTsCol(true).build();
        Common.ColumnKey colKey1 = Common.ColumnKey.newBuilder().setIndexName("card").addColName("card")
                .addTsName("ts1").addTsName("ts2").build();
        NS.TableInfo table = NS.TableInfo.newBuilder()
                .setName(name).setTtl(10)
                .addColumnDescV1(col0).addColumnDescV1(col1).addColumnDescV1(col2).addColumnDescV1(col3).addColumnDescV1(col4)
                .addColumnKey(colKey1)
                .build();
        boolean ok = nsc.createTable(table);
        Assert.assertTrue(ok);
        client.refreshRouteTable();
        long curTime = System.currentTimeMillis();
        try {
            tableSyncClient.put(name, new Object[]{"card0", "mcc0", 1.1d, curTime - 10 * 60 * 1000 - 1, curTime});
            tableSyncClient.put(name, new Object[]{"card0", "mcc0", 9.2d, curTime - 10, curTime - 10 * 60 * 1000 - 1});
            tableSyncClient.put(name, new Object[]{"card0", "mcc1", 15.6d, curTime, curTime - 10});
            KvIterator it = tableSyncClient.scan(name, "card0", "card", curTime, 0l, "ts1", 0);
            Assert.assertTrue(it.valid());
            Assert.assertTrue(it.getCount() == 2);
            Object[] row = it.getDecodedValue();
            Assert.assertEquals(it.getKey(), curTime);
            Assert.assertEquals(row[0],"card0");
            Assert.assertEquals(row[1],"mcc1");
            Assert.assertEquals(row[2],15.6d);
            it.next();
            row = it.getDecodedValue();
            Assert.assertEquals(it.getKey(), curTime - 10);
            Assert.assertEquals(row[0],"card0");
            Assert.assertEquals(row[1],"mcc0");
            Assert.assertEquals(row[2],9.2d);
            it.next();
            Assert.assertFalse(it.valid());

            it = tableSyncClient.scan(name, "card0", "card", curTime, 0, "ts2", 0);
            Assert.assertTrue(it.valid());
            Assert.assertTrue(it.getCount() == 2);
            row = it.getDecodedValue();
            Assert.assertEquals(it.getKey(), curTime);
            Assert.assertEquals(row[0],"card0");
            Assert.assertEquals(row[1],"mcc0");
            Assert.assertEquals(row[2],1.1d);
            it.next();
            row = it.getDecodedValue();
            Assert.assertEquals(it.getKey(), curTime - 10);
            Assert.assertEquals(row[0],"card0");
            Assert.assertEquals(row[1],"mcc1");
            Assert.assertEquals(row[2],15.6d);
            it.next();
            Assert.assertFalse(it.valid());
        } catch (Exception e) {
            Assert.assertTrue(false);
        } finally {
            nsc.dropTable(name);
        }
    }

    @Test
    public void testTSTTL() {
        String name = String.valueOf(id.incrementAndGet());
        nsc.dropTable(name);
        Common.ColumnDesc col0 = Common.ColumnDesc.newBuilder().setName("card").setAddTsIdx(false).setType("string").build();
        Common.ColumnDesc col1 = Common.ColumnDesc.newBuilder().setName("mcc").setAddTsIdx(false).setType("string").build();
        Common.ColumnDesc col2 = Common.ColumnDesc.newBuilder().setName("amt").setAddTsIdx(false).setType("double").build();
        Common.ColumnDesc col3 = Common.ColumnDesc.newBuilder().setName("ts1").setAddTsIdx(false).setType("int64")
                .setIsTsCol(true).setTtl(20).build();
        Common.ColumnDesc col4 = Common.ColumnDesc.newBuilder().setName("ts2").setAddTsIdx(false).setType("int64")
                .setIsTsCol(true).setTtl(30).build();
        Common.ColumnDesc col5 = Common.ColumnDesc.newBuilder().setName("ts3").setAddTsIdx(false).setType("int64")
                .setIsTsCol(true).build();
        Common.ColumnKey colKey1 = Common.ColumnKey.newBuilder().setIndexName("card").addColName("card")
                .addTsName("ts1").addTsName("ts2").addTsName("ts3").build();
        NS.TableInfo table = NS.TableInfo.newBuilder()
                .setName(name).setTtl(10)
                .addColumnDescV1(col0).addColumnDescV1(col1).addColumnDescV1(col2).addColumnDescV1(col3)
                .addColumnDescV1(col4).addColumnDescV1(col5)
                .addColumnKey(colKey1)
                .build();
        boolean ok = nsc.createTable(table);
        Assert.assertTrue(ok);
        client.refreshRouteTable();
        long curTime = System.currentTimeMillis();
        try {
            tableSyncClient.put(name, new Object[]{"card0", "mcc0", 1.1d,
                    curTime - 30 * 60 * 1000 - 1, curTime - 30 * 60 * 1000 - 1, curTime - 30 * 60 * 1000 - 1});
            tableSyncClient.put(name, new Object[]{"card0", "mcc0", 9.2d,
                    curTime - 20 * 60 * 1000 - 1, curTime - 20 * 60 * 1000 - 1, curTime - 20 * 60 * 1000 - 1});
            tableSyncClient.put(name, new Object[]{"card0", "mcc1", 10.6d,
                    curTime - 10 * 60 * 1000 - 1, curTime - 10 * 60 * 1000 - 1, curTime - 10 * 60 * 1000 - 1});
            tableSyncClient.put(name, new Object[]{"card0", "mcc1", 15.8d,
                    curTime, curTime, curTime});
            KvIterator it = tableSyncClient.scan(name, "card0", "card", curTime, 0l, "ts1", 0);
            Assert.assertTrue(it.valid());
            Assert.assertEquals(it.getCount(), 2);
            it = tableSyncClient.scan(name, "card0", "card", curTime, 0, "ts2", 0);
            Assert.assertTrue(it.valid());
            Assert.assertEquals(it.getCount(), 3);
            it = tableSyncClient.scan(name, "card0", "card", curTime, 0, "ts3", 0);
            Assert.assertTrue(it.valid());
            Assert.assertEquals(it.getCount(), 1);
        } catch (Exception e) {
            Assert.assertTrue(false);
        } finally {
            nsc.dropTable(name);
        }
    }

    @Test
    public void testTSTTLCombinedKey() {
        String name = String.valueOf(id.incrementAndGet());
        nsc.dropTable(name);
        Common.ColumnDesc col0 = Common.ColumnDesc.newBuilder().setName("card").setAddTsIdx(false).setType("string").build();
        Common.ColumnDesc col1 = Common.ColumnDesc.newBuilder().setName("mcc").setAddTsIdx(false).setType("string").build();
        Common.ColumnDesc col2 = Common.ColumnDesc.newBuilder().setName("amt").setAddTsIdx(false).setType("double").build();
        Common.ColumnDesc col3 = Common.ColumnDesc.newBuilder().setName("ts1").setAddTsIdx(false).setType("int64")
                .setIsTsCol(true).setTtl(20).build();
        Common.ColumnDesc col4 = Common.ColumnDesc.newBuilder().setName("ts2").setAddTsIdx(false).setType("int64")
                .setIsTsCol(true).setTtl(30).build();
        Common.ColumnDesc col5 = Common.ColumnDesc.newBuilder().setName("ts3").setAddTsIdx(false).setType("int64")
                .setIsTsCol(true).build();
        Common.ColumnKey colKey1 = Common.ColumnKey.newBuilder().setIndexName("card_mcc").addColName("card").addColName("mcc")
                .addTsName("ts1").addTsName("ts2").addTsName("ts3").build();
        NS.TableInfo table = NS.TableInfo.newBuilder()
                .setName(name).setTtl(10)
                .addColumnDescV1(col0).addColumnDescV1(col1).addColumnDescV1(col2).addColumnDescV1(col3)
                .addColumnDescV1(col4).addColumnDescV1(col5)
                .addColumnKey(colKey1)
                .build();
        boolean ok = nsc.createTable(table);
        Assert.assertTrue(ok);
        client.refreshRouteTable();
        long curTime = System.currentTimeMillis();
        try {
            tableSyncClient.put(name, new Object[]{"card0", "mcc0", 1.1d,
                    curTime - 30 * 60 * 1000 - 1, curTime - 30 * 60 * 1000 - 1, curTime - 30 * 60 * 1000 - 1});
            tableSyncClient.put(name, new Object[]{"card0", "mcc0", 9.2d,
                    curTime - 20 * 60 * 1000 - 1, curTime - 20 * 60 * 1000 - 1, curTime - 20 * 60 * 1000 - 1});
            tableSyncClient.put(name, new Object[]{"card0", "mcc0", 10.6d,
                    curTime - 10 * 60 * 1000 - 1, curTime - 10 * 60 * 1000 - 1, curTime - 10 * 60 * 1000 - 1});
            tableSyncClient.put(name, new Object[]{"card0", "mcc0", 15.8d,
                    curTime, curTime, curTime});
            KvIterator it = tableSyncClient.scan(name, new Object[]{"card0", "mcc0"}, "card_mcc", curTime, 0l, "ts1", 0);
            Assert.assertTrue(it.valid());
            Assert.assertEquals(it.getCount(), 2);
            it = tableSyncClient.scan(name, new Object[]{"card0", "mcc0"}, "card_mcc", curTime, 0, "ts2", 0);
            Assert.assertTrue(it.valid());
            Assert.assertEquals(it.getCount(), 3);
            it = tableSyncClient.scan(name, new Object[]{"card0", "mcc0"}, "card_mcc", curTime, 0, "ts3", 0);
            Assert.assertTrue(it.valid());
            Assert.assertEquals(it.getCount(), 1);
            Object[] row = tableSyncClient.getRow(name, new Object[]{"card0", "mcc0"}, "card_mcc", curTime - 10 * 60 * 1000 - 1, "ts1", null);
            Assert.assertEquals(row[0], "card0");
        } catch (Exception e) {
            Assert.assertTrue(false);
        }
        try {
            Object[] row = tableSyncClient.getRow(name, new Object[]{"card0", "mcc0"}, "card_mcc", curTime - 20 * 60 * 1000 - 1, "ts1", null);
            // Assert.assertEquals(row[0], null);
            Assert.assertNull(row);
        } catch (TabletException e) {
            Assert.assertEquals(e.getCode(), 109);
        } catch (Exception e) {
            Assert.assertTrue(false);
        }
        nsc.dropTable(name);
    }

}
