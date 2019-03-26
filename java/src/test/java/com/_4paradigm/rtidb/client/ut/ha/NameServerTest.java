package com._4paradigm.rtidb.client.ut.ha;

import java.util.List;
import java.util.Map;

import com._4paradigm.rtidb.client.ha.RTIDBClientConfig;
import com._4paradigm.rtidb.ns.NS;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com._4paradigm.rtidb.client.ha.impl.NameServerClientImpl;
import com._4paradigm.rtidb.ns.NS.PartitionMeta;
import com._4paradigm.rtidb.ns.NS.TableInfo;
import com._4paradigm.rtidb.ns.NS.TablePartition;


/**
 * 需要外部启动ns 环境
 * @author wangtaize
 *
 */
public class NameServerTest {

    private static String zkEndpoints = "127.0.0.1:6181";
    private static String zkRootPath = "/onebox";
    private static String leaderPath  = zkRootPath + "/leader";
    private static String[] nodes = new String[] {"127.0.0.1:9522", "127.0.0.1:9521", "127.0.0.1:9520"};
    private static RTIDBClientConfig config = new RTIDBClientConfig();
    static {
        String envZkEndpoints = System.getenv("zkEndpoints");
        if (envZkEndpoints != null) {
            zkEndpoints = envZkEndpoints;
        }
        String envleaderPath = System.getenv("leaderPath");
        if (envleaderPath != null) {
            leaderPath = envleaderPath;
        }
        config.setZkEndpoints(zkEndpoints);
        config.setZkRootPath(zkRootPath);
        config.setWriteTimeout(100000);
        config.setReadTimeout(100000);
    }
    
    @Test
    public void testInvalidZkInit() {
        try {
            NameServerClientImpl nsc = new NameServerClientImpl("xxxxx", "xxxx");
            nsc.init();
            Assert.assertTrue(false);
            nsc.close();
        } catch(Exception e) {
            Assert.assertTrue(true);
        }
    }
    
    @Test
    public void testInvalidEndpointInit() {
        try {
            NameServerClientImpl nsc = new NameServerClientImpl(zkEndpoints, "xxxx");
            nsc.init();
            Assert.assertTrue(false);
            nsc.close();
        } catch(Exception e) {
            Assert.assertTrue(true);
        }
    }
    
    @Test
    public void testNsInit() {
        try {
            NameServerClientImpl nsc = new NameServerClientImpl(zkEndpoints, leaderPath);
            nsc.init();
            Assert.assertTrue(true);
            nsc.close();
        } catch(Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        }
    }

    @Test
    public void testNsInitByConfig() {
        RTIDBClientConfig config = new RTIDBClientConfig();
        config.setZkEndpoints(zkEndpoints);
        config.setZkRootPath(zkRootPath);
        config.setReadTimeout(3000);
        config.setWriteTimeout(3000);
        try {
            NameServerClientImpl nsc = new NameServerClientImpl(config);
            nsc.init();
            Assert.assertTrue(true);
            TableInfo tableInfo = TableInfo.newBuilder().setName("t1").setSegCnt(8).build();
            Assert.assertTrue(nsc.createTable(tableInfo));
            List<TableInfo> tables = nsc.showTable("t1");
            Assert.assertTrue(tables.size() == 1);
            Assert.assertEquals(tables.get(0).getStorageMode(), NS.StorageMode.kMemory);
            Assert.assertTrue( nsc.dropTable("t1"));
            nsc.close();
        } catch(Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        }
    }
    
    @Test
    public void testAllFlow() {
        PartitionMeta pm = PartitionMeta.newBuilder().setEndpoint(nodes[0]).setIsLeader(true).build();
        TablePartition tp = TablePartition.newBuilder().addPartitionMeta(pm).setPid(0).build();
        TableInfo tableInfo = TableInfo.newBuilder().setName("t1").setSegCnt(8).addTablePartition(tp).build();
        try {
            NameServerClientImpl nsc = new NameServerClientImpl(zkEndpoints, leaderPath);
            nsc.init();
            Assert.assertTrue(true);
            Assert.assertTrue(nsc.createTable(tableInfo));
            List<TableInfo> tables = nsc.showTable("t1");
            Map<String,String> nscMap = nsc.showNs();
            Assert.assertTrue(nscMap.size() == 3);
            TableInfo e = tables.get(0);
            Assert.assertTrue(e.getTablePartitionList().size() == 1);
            Assert.assertTrue(e.getTablePartition(0).getRecordCnt() == 0);
            Assert.assertTrue(e.getTablePartition(0).getRecordCnt() == 0);
            Assert.assertTrue(tables.size() == 1);
            Assert.assertTrue(tables.get(0).getName().equals("t1"));
            Assert.assertTrue( nsc.dropTable("t1"));
            tables = nsc.showTable("t1");
            Assert.assertTrue(tables.size() == 0);
            nsc.close();
        } catch(Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        }
    }

    @Test
    public void testCreateTableTTL() {
        NameServerClientImpl nsc = new NameServerClientImpl(zkEndpoints, leaderPath);
        try {
            nsc.init();
            int max_ttl = 60*24*365*30;
            TableInfo tableInfo1 = TableInfo.newBuilder().setName("t1").setSegCnt(8).setTtl(max_ttl + 1).build();
            Assert.assertFalse(nsc.createTable(tableInfo1));
            TableInfo tableInfo2 = TableInfo.newBuilder().setName("t2").setSegCnt(8).setTtl(max_ttl).build();
            Assert.assertTrue(nsc.createTable(tableInfo2));
            Assert.assertTrue(nsc.dropTable("t2"));
            TableInfo tableInfo3 = TableInfo.newBuilder().setName("t3").setSegCnt(8).setTtlType("kLatestTime").setTtl(1001).build();
            Assert.assertFalse(nsc.createTable(tableInfo3));
            TableInfo tableInfo4 = TableInfo.newBuilder().setName("t4").setSegCnt(8).setTtlType("kLatestTime").setTtl(1000).build();
            Assert.assertTrue(nsc.createTable(tableInfo4));
            Assert.assertTrue(nsc.dropTable("t4"));
        } catch(Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } finally {
            nsc.close();
        }
    }

    @Test
    public void testCreateTableHDD() {
        NameServerClientImpl nsc = new NameServerClientImpl(config);
        try {
            nsc.init();
            String name = "t1";
            TableInfo tableInfo1 = TableInfo.newBuilder().setName(name).setSegCnt(8).setTtl(0).setReplicaNum(1)
                    .setStorageMode(NS.StorageMode.kHDD).build();
            Assert.assertTrue(nsc.createTable(tableInfo1));
            List<TableInfo> tables = nsc.showTable(name);
            Assert.assertTrue(tables.size() == 1);
            Assert.assertEquals(tables.get(0).getStorageMode(), NS.StorageMode.kHDD);
            Assert.assertTrue(nsc.dropTable(name));
        } catch(Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } finally {
            nsc.close();
        }
    }

    @Test
    public void testCreateTableSSD() {
        NameServerClientImpl nsc = new NameServerClientImpl(config);
        try {
            nsc.init();
            String name = "t1";
            TableInfo tableInfo1 = TableInfo.newBuilder().setName(name).setSegCnt(8).setTtl(0).setReplicaNum(1)
                    .setStorageMode(NS.StorageMode.kSSD).build();
            Assert.assertTrue(nsc.createTable(tableInfo1));
            List<TableInfo> tables = nsc.showTable(name);
            Assert.assertTrue(tables.size() == 1);
            Assert.assertEquals(tables.get(0).getStorageMode(), NS.StorageMode.kSSD);
            Assert.assertTrue(nsc.dropTable(name));
        } catch(Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } finally {
            nsc.close();
        }
    }
}
