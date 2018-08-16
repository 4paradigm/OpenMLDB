package com._4paradigm.rtidb.client.ut.ha;

import com._4paradigm.rtidb.client.*;
import com._4paradigm.rtidb.client.ha.RTIDBClientConfig;
import com._4paradigm.rtidb.client.ha.impl.NameServerClientImpl;
import com._4paradigm.rtidb.client.ha.impl.RTIDBClusterClient;
import com._4paradigm.rtidb.client.impl.TableAsyncClientImpl;
import com._4paradigm.rtidb.client.impl.TableSyncClientImpl;
import com._4paradigm.rtidb.ns.NS;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.HashMap;
import java.util.Map;


public class TableFieldCompressTest {
    private static String zkEndpoints = "127.0.0.1:6181";
    private static String leaderPath  = "/onebox/leader";
    private static AtomicInteger id = new AtomicInteger(10000);
    private static NameServerClientImpl nsc = new NameServerClientImpl(zkEndpoints, leaderPath);
    private static RTIDBClientConfig config = new RTIDBClientConfig();
    private static RTIDBClusterClient client = null;
    private static TableSyncClient tableSyncClient = null;
    private static TableAsyncClient tableAsyncClient = null;
    private static String[] nodes = new String[] {"127.0.0.1:9522", "127.0.0.1:9521", "127.0.0.1:9520"};
    static {
        try {
            nsc.init();
            config.setZkEndpoints(zkEndpoints);
            config.setZkRootPath("/onebox");
            client = new RTIDBClusterClient(config);
            client.init();
            tableSyncClient = new TableSyncClientImpl(client);
            tableAsyncClient = new TableAsyncClientImpl(client);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    private String createSchemaTable(int schema_num) {
        String name = String.valueOf(id.incrementAndGet());
        nsc.dropTable(name);
        NS.PartitionMeta pm0_0 = NS.PartitionMeta.newBuilder().setEndpoint(nodes[0]).setIsLeader(true).build();
        NS.PartitionMeta pm0_1 = NS.PartitionMeta.newBuilder().setEndpoint(nodes[1]).setIsLeader(false).build();
        List<NS.ColumnDesc> list = new ArrayList<NS.ColumnDesc>();
        list.add(NS.ColumnDesc.newBuilder().setName("card").setAddTsIdx(true).setType("string").build());
        for (int idx = 0; idx < schema_num - 1; idx++) {
             list.add(NS.ColumnDesc.newBuilder().setName("field" + idx).setAddTsIdx(false).setType("string").build());

        }
        NS.TablePartition tp0 = NS.TablePartition.newBuilder().addPartitionMeta(pm0_0).addPartitionMeta(pm0_1).setPid(0).build();
        NS.TablePartition tp1 = NS.TablePartition.newBuilder().addPartitionMeta(pm0_0).addPartitionMeta(pm0_1).setPid(1).build();
        NS.TableInfo.Builder builder = NS.TableInfo.newBuilder();
        builder = NS.TableInfo.newBuilder().addTablePartition(tp0).addTablePartition(tp1)
                .setSegCnt(8).setName(name).setTtl(0).setCompressType(NS.CompressType.kSnappy);
        for (NS.ColumnDesc desc: list) {
            builder.addColumnDesc(desc);
        }
        NS.TableInfo table = builder.build();
        boolean ok = nsc.createTable(table);
        Assert.assertTrue(ok);
        client.refreshRouteTable();
        return name;
    }

    @Test
    public void testCompressFieldSync() {
        int schema_num = 3;
        String name = createSchemaTable(schema_num);
        Map<String, Object> row = new HashMap<String, Object>();
        row.put("card", "card0");
        for (int i = 0; i < schema_num -1; i++) {
            row.put("field" + i, "valueabcad123ajcr3456" + i);
        }
        try {
            boolean ok = tableSyncClient.put(name, 9527, row);
            Assert.assertTrue(ok);
            KvIterator it = tableSyncClient.scan(name, "card0", "card", 9529, 1000);
            Assert.assertTrue(it.valid());
            Object[] result = it.getDecodedValue();
            Assert.assertEquals(result[0], "card0");
            Assert.assertEquals(result[1], "valueabcad123ajcr34560");
            Object[] result1 = tableSyncClient.getRow(name, "card0", "card", 9527);
            Assert.assertEquals(result1[0], "card0");
            Assert.assertEquals(result1[1], "valueabcad123ajcr34560");
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } finally {
             nsc.dropTable(name);
        }
    }

    @Test
    public void testCompressFieldASync() {
        int schema_num = 3;
        String name = createSchemaTable(schema_num);
        Map<String, Object> row = new HashMap<String, Object>();
        row.put("card", "card1");
        for (int i = 0; i < schema_num -1; i++) {
            row.put("field" + i, "Xvalueabcad123ajcr3456" + i);
        }
        try {
            PutFuture pf =  tableAsyncClient.put(name, 9527, row);
            Assert.assertTrue(pf.get());
            ScanFuture sf = tableAsyncClient.scan(name, "card1", "card", 9529, 0);
            KvIterator it = sf.get();
            Assert.assertTrue(it.valid());
            Object[] result = it.getDecodedValue();
            Assert.assertEquals(result[0], "card1");
            Assert.assertEquals(result[1], "Xvalueabcad123ajcr34560");
            GetFuture gf = tableAsyncClient.get(name, "card1", "card", 9527);
            Object[] result1 = gf.getRow();
            Assert.assertEquals(result1[0], "card1");
            Assert.assertEquals(result1[1], "Xvalueabcad123ajcr34560");
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } finally {
            nsc.dropTable(name);
        }
    }
}
