package com._4paradigm.rtidb.client.ha;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Test;

import com._4paradigm.rtidb.client.TableSyncClient;
import com._4paradigm.rtidb.client.ha.impl.NameServerClientImpl;
import com._4paradigm.rtidb.client.ha.impl.RTIDBClusterClient;
import com._4paradigm.rtidb.client.impl.TableSyncClientImpl;
import com._4paradigm.rtidb.ns.NS.TableInfo;
import com._4paradigm.rtidb.ns.NS.TablePartition;
import com.google.protobuf.ByteString;

public class TableSyncClientTest {

    private static String zkEndpoints = "127.0.0.1:12181";
    private static String leaderPath  = "/onebox/leader";
    private static AtomicInteger id = new AtomicInteger(1000);
    private static NameServerClientImpl nsc = new NameServerClientImpl(zkEndpoints, leaderPath);
    private static RTIDBClientConfig config = new RTIDBClientConfig();
    private static RTIDBClusterClient client = null;
    private static TableSyncClient tableSyncClient = null;
    private static String[] nodes = new String[] {"127.0.0.1:9522", "127.0.0.1:9521", "127.0.0.1:9520"};
    static {
        try {
            nsc.init();
            config.setZkEndpoints(zkEndpoints);
            config.setZkNodeRootPath("/onebox/nodes");
            config.setZkTableRootPath("/onebox/table/table_data");
            client = new RTIDBClusterClient(config);
            client.init();
            tableSyncClient = new TableSyncClientImpl(client);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    
    @Test
    public void testPut() {
        String name = String.valueOf(id.incrementAndGet());
        TablePartition tp0_0 = TablePartition.newBuilder().setEndpoint(nodes[0]).setIsLeader(true).setPid(0).build();
        TablePartition tp0_1 = TablePartition.newBuilder().setEndpoint(nodes[1]).setIsLeader(false).setPid(0).build();
        TablePartition tp1_0 = TablePartition.newBuilder().setEndpoint(nodes[0]).setIsLeader(true).setPid(1).build();
        TablePartition tp1_1 = TablePartition.newBuilder().setEndpoint(nodes[1]).setIsLeader(true).setPid(1).build();
        TableInfo table = TableInfo.newBuilder().addTablePartition(tp0_0).addTablePartition(tp0_1).addTablePartition(tp1_0).addTablePartition(tp1_1)
                .setSegCnt(8).setName(name).setTtl(0).build();
        boolean ok = nsc.createTable(table);
        Assert.assertTrue(ok);
        client.refreshRouteTable();
        try {
            ok = tableSyncClient.put(name, "test1", 9527, "value0");
            Assert.assertTrue(ok);
            ByteString bs = tableSyncClient.get(name, "test1");
            String value = new String(bs.toByteArray());
            Assert.assertEquals(value, "value0");
            nsc.dropTable(name);
        } catch (Exception e) {
            Assert.assertTrue(false);
        }
        
    }
    
}
