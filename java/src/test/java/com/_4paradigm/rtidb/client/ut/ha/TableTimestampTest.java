package com._4paradigm.rtidb.client.ut.ha;

import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;

import org.joda.time.DateTime;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com._4paradigm.rtidb.client.TableSyncClient;
import com._4paradigm.rtidb.client.ha.RTIDBClientConfig;
import com._4paradigm.rtidb.client.ha.impl.NameServerClientImpl;
import com._4paradigm.rtidb.client.ha.impl.RTIDBClusterClient;
import com._4paradigm.rtidb.client.impl.TableSyncClientImpl;
import com._4paradigm.rtidb.ns.NS.ColumnDesc;
import com._4paradigm.rtidb.ns.NS.PartitionMeta;
import com._4paradigm.rtidb.ns.NS.TableInfo;
import com._4paradigm.rtidb.ns.NS.TablePartition;

public class TableTimestampTest {
    private static String zkEndpoints = Const.ZK_ENDPOINTS;
    private static String leaderPath = Const.ZK_ROOT_PATH + "/leader";
    private static AtomicInteger id = new AtomicInteger(50000);
    private static NameServerClientImpl nsc = new NameServerClientImpl(zkEndpoints, leaderPath);
    private static RTIDBClientConfig config = new RTIDBClientConfig();
    private static RTIDBClusterClient client = null;
    private static TableSyncClient tableSyncClient = null;
    private static String[] nodes = new String[]{"127.0.0.1:9522", "127.0.0.1:9521", "127.0.0.1:9520"};

    @BeforeClass
    public static void setUp() {
        try {
            nsc.init();
            config.setZkEndpoints(zkEndpoints);
            config.setZkNodeRootPath(Const.ZK_ROOT_PATH + "/nodes");
            config.setZkTableRootPath(Const.ZK_ROOT_PATH + "/table/table_data");
            config.setZkTableNotifyPath(Const.ZK_ROOT_PATH + "/table/notify");
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
        client.close();
    }

    private String createSchemaTable() {
        String name = String.valueOf(id.incrementAndGet());
        nsc.dropTable(name);
        PartitionMeta pm0_0 = PartitionMeta.newBuilder().setEndpoint(nodes[0]).setIsLeader(true).build();
        PartitionMeta pm0_1 = PartitionMeta.newBuilder().setEndpoint(nodes[1]).setIsLeader(false).build();
        ColumnDesc col0 = ColumnDesc.newBuilder().setName("card").setAddTsIdx(true).setType("string").build();
        ColumnDesc col1 = ColumnDesc.newBuilder().setName("mcc").setAddTsIdx(true).setType("string").build();
        ColumnDesc col2 = ColumnDesc.newBuilder().setName("amt").setAddTsIdx(false).setType("double").build();
        ColumnDesc col3 = ColumnDesc.newBuilder().setName("ts").setAddTsIdx(false).setType("timestamp").build();
        TablePartition tp0 = TablePartition.newBuilder().addPartitionMeta(pm0_0).addPartitionMeta(pm0_1).setPid(0).build();
        TablePartition tp1 = TablePartition.newBuilder().addPartitionMeta(pm0_0).addPartitionMeta(pm0_1).setPid(1).build();
        TableInfo table = TableInfo.newBuilder().addTablePartition(tp0).addTablePartition(tp1)
                .setSegCnt(8).setName(name).setTtl(0)
                .addColumnDesc(col0).addColumnDesc(col1).addColumnDesc(col2).addColumnDesc(col3)
                .build();
        boolean ok = nsc.createTable(table);
        Assert.assertTrue(ok);
        client.refreshRouteTable();
        return name;
    }

    @Test
    public void testTimestampPut() {
        String name = createSchemaTable();
        long time = System.currentTimeMillis();
        try {
            boolean ok = tableSyncClient.put(name, time, new Object[]{"card0", "mcc0", 1.1d, new DateTime(time)});
            Assert.assertTrue(ok);
            Object[] row = tableSyncClient.getRow(name, "card0", 0);
            Assert.assertNotNull(row);
            Assert.assertEquals(4, row.length);
            Assert.assertEquals("card0", row[0]);
            Assert.assertEquals("mcc0", row[1]);
            Assert.assertEquals(1.1d, row[2]);
            Assert.assertEquals(time, ((DateTime) row[3]).getMillis());
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        }
    }
}
