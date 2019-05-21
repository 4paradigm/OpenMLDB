package com._4paradigm.rtidb.client.ut.ha;

import java.util.List;
import java.util.Map;

import com._4paradigm.rtidb.client.ha.RTIDBClientConfig;
import com._4paradigm.rtidb.client.ha.TableHandler;
import com._4paradigm.rtidb.client.schema.ColumnDesc;
import com._4paradigm.rtidb.client.ut.Config;
import com._4paradigm.rtidb.ns.NS;
import org.testng.Assert;
import org.testng.annotations.Test;

import com._4paradigm.rtidb.client.ha.impl.NameServerClientImpl;
import com._4paradigm.rtidb.ns.NS.PartitionMeta;
import com._4paradigm.rtidb.ns.NS.TableInfo;
import com._4paradigm.rtidb.ns.NS.TablePartition;


/**
 * 需要外部启动ns 环境
 *
 * @author wangtaize
 */
public class NameServerTest {

    private static String zkEndpoints = Config.ZK_ENDPOINTS;
    private static String zkRootPath = Config.ZK_ROOT_PATH;
    private static String leaderPath = zkRootPath + "/leader";
    private static String[] nodes = Config.NODES;

    static {
        String envZkEndpoints = System.getenv("zkEndpoints");
        if (envZkEndpoints != null) {
            zkEndpoints = envZkEndpoints;
        }
        String envleaderPath = System.getenv("leaderPath");
        if (envleaderPath != null) {
            leaderPath = envleaderPath;
        }
    }

    @Test
    public void testInvalidZkInit() {
        try {
            NameServerClientImpl nsc = new NameServerClientImpl("xxxxx", "xxxx");
            nsc.init();
            Assert.assertTrue(false);
            nsc.close();
        } catch (Exception e) {
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
        } catch (Exception e) {
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
        } catch (Exception e) {
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
            Assert.assertTrue(nsc.dropTable("t1"));
            nsc.close();
        } catch (Exception e) {
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
            Map<String, String> nscMap = nsc.showNs();
            Assert.assertTrue(nscMap.size() == 3);
            TableInfo e = tables.get(0);
            Assert.assertTrue(e.getTablePartitionList().size() == 1);
            Assert.assertTrue(e.getTablePartition(0).getRecordCnt() == 0);
            Assert.assertTrue(e.getTablePartition(0).getRecordCnt() == 0);
            Assert.assertTrue(tables.size() == 1);
            Assert.assertTrue(tables.get(0).getName().equals("t1"));
            Assert.assertTrue(nsc.dropTable("t1"));
            tables = nsc.showTable("t1");
            Assert.assertTrue(tables.size() == 0);
            nsc.close();
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        }
    }

    @Test
    public void testCreateTableTTL() {
        NameServerClientImpl nsc = new NameServerClientImpl(zkEndpoints, leaderPath);
        try {
            nsc.init();
            int max_ttl = 60 * 24 * 365 * 30;
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
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } finally {
            nsc.close();
        }
    }

    @Test
    public void testTableHandler() {
        NS.TableInfo.Builder builder = NS.TableInfo.newBuilder()
                .setName("test")  // 设置表名
                .setTtl(144000);      // 设置ttl
        NS.ColumnDesc col0 = NS.ColumnDesc.newBuilder().setName("col_0").setAddTsIdx(true).setType("string").build();
        NS.ColumnDesc col1 = NS.ColumnDesc.newBuilder().setName("col_1").setAddTsIdx(true).setType("int64").build();
        NS.ColumnDesc col2 = NS.ColumnDesc.newBuilder().setName("col_2").setAddTsIdx(false).setType("double").build();
        NS.ColumnDesc col3 = NS.ColumnDesc.newBuilder().setName("col_3").setAddTsIdx(false).setType("float").build();
        builder.addColumnDesc(col0).addColumnDesc(col1).addColumnDesc(col2).addColumnDesc(col3);
        NS.TableInfo tableInfo = builder.build();
        TableHandler tableHandler = new TableHandler(tableInfo);
        List<ColumnDesc> schema = tableHandler.getSchema();

        Assert.assertTrue(schema.size() == 4, "schema size mistook");
        Assert.assertTrue((schema.get(0).getName().equals("col_0"))
                && (schema.get(0).isAddTsIndex() == true)
                && (schema.get(0).getType().toString().equals("kString")), "col_0 mistook");
        Assert.assertTrue(schema.get(1).getName().equals("col_1")
                && schema.get(1).isAddTsIndex() == true
                && schema.get(1).getType().toString().equals("kInt64"), "col_1 mistook");
        Assert.assertTrue(schema.get(2).getName().equals("col_2")
                && schema.get(2).isAddTsIndex() == false
                && schema.get(2).getType().toString().equals("kDouble"), "col_2 mistook");
        Assert.assertTrue(schema.get(3).getName().equals("col_3")
                && schema.get(3).isAddTsIndex() == false
                && schema.get(3).getType().toString().equals("kFloat"), "col_3 mistook");

        Map<Integer, List<Integer>> indexes = tableHandler.getIndexes();
        Assert.assertTrue(indexes.size() == 2, "indexes size mistook");
        Assert.assertTrue(indexes.get(0).size() == 1);
        Assert.assertTrue(indexes.get(1).size() == 1);
    }
}
