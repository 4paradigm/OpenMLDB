package com._4paradigm.rtidb.client.ut.ha;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;

import com._4paradigm.rtidb.client.KvIterator;
import com._4paradigm.rtidb.client.TableSyncClient;
import com._4paradigm.rtidb.client.TabletClient;
import com._4paradigm.rtidb.client.ha.RTIDBClientConfig;
import com._4paradigm.rtidb.client.ha.TableHandler;
import com._4paradigm.rtidb.client.ha.impl.NameServerClientImpl;
import com._4paradigm.rtidb.client.ha.impl.RTIDBClusterClient;
import com._4paradigm.rtidb.client.ha.impl.RTIDBSingleNodeClient;
import com._4paradigm.rtidb.client.impl.TableSyncClientImpl;
import com._4paradigm.rtidb.client.impl.TabletClientImpl;
import com._4paradigm.rtidb.ns.NS.ColumnDesc;
import com._4paradigm.rtidb.ns.NS.PartitionMeta;
import com._4paradigm.rtidb.ns.NS.TableInfo;
import com._4paradigm.rtidb.ns.NS.TablePartition;

import io.brpc.client.EndPoint;

public class LeaderChangedTest {
    private static String zkEndpoints = "127.0.0.1:6181";
    private static String leaderPath  = "/onebox/leader";
    private static AtomicInteger id = new AtomicInteger(30000);
    private static NameServerClientImpl nsc = new NameServerClientImpl(zkEndpoints, leaderPath);
    private static RTIDBClientConfig config = new RTIDBClientConfig();
    private static RTIDBClusterClient client = null;
    private static TableSyncClient tableSyncClient = null;
    private static String[] nodes = new String[] {"127.0.0.1:9522", "127.0.0.1:9521", "127.0.0.1:9520"};
    private static TabletClientImpl tc = null;
    private static EndPoint endpoint = new EndPoint("127.0.0.1:9522");
    private static RTIDBSingleNodeClient snc = new RTIDBSingleNodeClient(config, endpoint);
    static {
        try {
            nsc.init();
            snc.init();
            config.setZkEndpoints(zkEndpoints);
            config.setZkNodeRootPath("/onebox/nodes");
            config.setZkTableRootPath("/onebox/table/table_data");
            config.setZkTableNotifyPath("/onebox/table/notify");
            client = new RTIDBClusterClient(config);
            client.init();
            tableSyncClient = new TableSyncClientImpl(client);
            tc = new TabletClientImpl(snc);
            
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    
    @AfterClass
    public void closeResource() {
        nsc.close();
        snc.close();
    }
    
    private String createSchemaTable() {
        String name = String.valueOf(id.incrementAndGet());
        PartitionMeta pm0_0 = PartitionMeta.newBuilder().setEndpoint(nodes[0]).setIsLeader(true).build();
        PartitionMeta pm0_1 = PartitionMeta.newBuilder().setEndpoint(nodes[1]).setIsLeader(false).build();
        PartitionMeta pm0_2 = PartitionMeta.newBuilder().setEndpoint(nodes[2]).setIsLeader(false).build();
        ColumnDesc col0 = ColumnDesc.newBuilder().setName("card").setAddTsIdx(true).setType("string").build();
        ColumnDesc col1 = ColumnDesc.newBuilder().setName("mcc").setAddTsIdx(true).setType("string").build();
        ColumnDesc col2 = ColumnDesc.newBuilder().setName("amt").setAddTsIdx(false).setType("double").build();
        TablePartition tp0 = TablePartition.newBuilder().addPartitionMeta(pm0_0).addPartitionMeta(pm0_1).addPartitionMeta(pm0_2).setPid(0).build();
        TableInfo table = TableInfo.newBuilder().addTablePartition(tp0)
                .setSegCnt(8).setName(name).setTtl(0)
                .addColumnDesc(col0).addColumnDesc(col1).addColumnDesc(col2)
                .build();
        boolean ok = nsc.createTable(table);
        Assert.assertTrue(ok);
        return name;
    }
    
    @Test
    public void testChangeLeader() {
        String tname = createSchemaTable();
        try {
            Thread.sleep(1000 * 5);
        } catch (InterruptedException e) {
            Assert.fail();
        }
        
        TableHandler th = client.getHandler(tname);
        Assert.assertNotNull(th);
        try {
            boolean ok = tableSyncClient.put(tname, 9527l, new Object[] {"card0", "mcc0", 1.1d});
            Assert.assertTrue(ok);
        } catch (Exception e) {
            Assert.fail();
        }
        boolean ok = tc.disConnectZK();
        Assert.assertTrue(ok);
        try {
            Thread.sleep(1000 * 20);
        }catch(Exception e) {
            Assert.fail();
        } 
        ok = nsc.changeLeader(tname, 0);
        Assert.assertTrue(ok);
        try {
            Thread.sleep(1000 * 10);
        }catch(Exception e) {
            Assert.fail();
        }
        th = client.getHandler(tname);
        TablePartition tp = th.getTableInfo().getTablePartitionList().get(0);
        boolean hasLeader = false;
        for (PartitionMeta pm : tp.getPartitionMetaList()) {
            if (pm.getEndpoint().equals(nodes[0])) {
                Assert.assertFalse(pm.getIsAlive());
            } else {
                if (pm.getIsLeader() && pm.getIsAlive()) {
                    hasLeader = true;
                }
            }
        }
        Assert.assertTrue(hasLeader);
        try {
            ok = tableSyncClient.put(tname, 9528l, new Object[] {"card0", "mcc1", 1.2d});
            Assert.assertTrue(ok);
        } catch (Exception e) {
            Assert.fail();
        }
        try {
            KvIterator it = tableSyncClient.scan(tname, "card0", "card", 9528l, 9520l);
            Assert.assertNotNull(it);
            Assert.assertTrue(it.valid());
            Assert.assertTrue(it.getCount() == 2);
            Object[] row = it.getDecodedValue();
            Assert.assertEquals("card0", row[0]);
            Assert.assertEquals("mcc1", row[1]);
            Assert.assertEquals(1.2d, row[2]);
            it.next();
            Assert.assertTrue(it.valid());
            row = it.getDecodedValue();
            Assert.assertEquals("card0", row[0]);
            Assert.assertEquals("mcc0", row[1]);
            Assert.assertEquals(1.1d, row[2]);
            it.next();
            Assert.assertFalse(it.valid());
        } catch (Exception e) {
            Assert.fail();
        }
        ok = tc.connectZK();
        Assert.assertTrue(ok);
        try {
            Thread.sleep(1000 * 20);
        }catch(Exception e) {
            Assert.fail();
        }
        ok = nsc.recoverEndpoint(nodes[0]);
        Assert.assertTrue(ok);
        try {
            Thread.sleep(1000 * 20);
        }catch(Exception e) {
            Assert.fail();
        }
        th = client.getHandler(tname);
        int aliveCnt = 0;
        for (PartitionMeta pm : th.getTableInfo().getTablePartition(0).getPartitionMetaList()) {
            if (pm.getIsAlive()) {
                aliveCnt ++;
            }
        }
        Assert.assertEquals(3, aliveCnt);
    }
    
}
