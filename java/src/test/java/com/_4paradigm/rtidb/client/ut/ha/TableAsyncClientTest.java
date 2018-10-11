package com._4paradigm.rtidb.client.ut.ha;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com._4paradigm.rtidb.client.GetFuture;
import com._4paradigm.rtidb.client.KvIterator;
import com._4paradigm.rtidb.client.PutFuture;
import com._4paradigm.rtidb.client.ScanFuture;
import com._4paradigm.rtidb.client.TableAsyncClient;
import com._4paradigm.rtidb.client.ha.RTIDBClientConfig;
import com._4paradigm.rtidb.client.ha.impl.NameServerClientImpl;
import com._4paradigm.rtidb.client.ha.impl.RTIDBClusterClient;
import com._4paradigm.rtidb.client.impl.TableAsyncClientImpl;
import com._4paradigm.rtidb.ns.NS.ColumnDesc;
import com._4paradigm.rtidb.ns.NS.PartitionMeta;
import com._4paradigm.rtidb.ns.NS.TableInfo;
import com._4paradigm.rtidb.ns.NS.TablePartition;
import com._4paradigm.rtidb.tablet.Tablet;

public class TableAsyncClientTest {

    private static String zkEndpoints = "127.0.0.1:6181";
    private static String leaderPath  = "/onebox/leader";
    private static AtomicInteger id = new AtomicInteger(20000);
    private static NameServerClientImpl nsc = new NameServerClientImpl(zkEndpoints, leaderPath);
    private static RTIDBClientConfig config = new RTIDBClientConfig();
    private static RTIDBClusterClient client = null;
    private static TableAsyncClient tableAsyncClient = null;
    private static String[] nodes = new String[] {"127.0.0.1:9522", "127.0.0.1:9521", "127.0.0.1:9520"};
    @BeforeClass
    public static void setUp() {
        try {
            nsc.init();
            config.setZkEndpoints(zkEndpoints);
            config.setZkNodeRootPath("/onebox/nodes");
            config.setZkTableRootPath("/onebox/table/table_data");
            config.setZkTableNotifyPath("/onebox/table/notify");
            client = new RTIDBClusterClient(config);
            client.init();
            tableAsyncClient = new TableAsyncClientImpl(client);
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
    
    private String createKvTable() {
        String name = String.valueOf(id.incrementAndGet());
        nsc.dropTable(name);
        PartitionMeta pm0_0 = PartitionMeta.newBuilder().setEndpoint(nodes[0]).setIsLeader(true).build();
        PartitionMeta pm0_1 = PartitionMeta.newBuilder().setEndpoint(nodes[1]).setIsLeader(false).build();
        TablePartition tp0 = TablePartition.newBuilder().addPartitionMeta(pm0_0).addPartitionMeta(pm0_1).setPid(0).build();
        TablePartition tp1 = TablePartition.newBuilder().addPartitionMeta(pm0_0).addPartitionMeta(pm0_1).setPid(1).build();
        
        TableInfo table = TableInfo.newBuilder().addTablePartition(tp0).addTablePartition(tp1)
                .setSegCnt(8).setName(name).setTtl(0).build();
        
        boolean ok = nsc.createTable(table);
        Assert.assertTrue(ok);
        client.refreshRouteTable();
        return name;
    }
    private String createSchemaTable(String ttlType) {
        String name = String.valueOf(id.incrementAndGet());
        nsc.dropTable(name);
        PartitionMeta pm0_0 = PartitionMeta.newBuilder().setEndpoint(nodes[0]).setIsLeader(true).build();
        PartitionMeta pm0_1 = PartitionMeta.newBuilder().setEndpoint(nodes[1]).setIsLeader(false).build();
        ColumnDesc col0 = ColumnDesc.newBuilder().setName("card").setAddTsIdx(true).setType("string").build();
        ColumnDesc col1 = ColumnDesc.newBuilder().setName("mcc").setAddTsIdx(true).setType("string").build();
        ColumnDesc col2 = ColumnDesc.newBuilder().setName("amt").setAddTsIdx(false).setType("double").build();
        TablePartition tp0 = TablePartition.newBuilder().addPartitionMeta(pm0_0).addPartitionMeta(pm0_1).setPid(0).build();
        TablePartition tp1 = TablePartition.newBuilder().addPartitionMeta(pm0_0).addPartitionMeta(pm0_1).setPid(1).build();
        TableInfo table = TableInfo.newBuilder().setTtlType(ttlType).addTablePartition(tp0).addTablePartition(tp1)
                .setSegCnt(8).setName(name).setTtl(10)
                .addColumnDesc(col0).addColumnDesc(col1).addColumnDesc(col2)
                .build();
        boolean ok = nsc.createTable(table);
        Assert.assertTrue(ok);
        client.refreshRouteTable();
        return name;
    }

    private String createSchemaTable() {
        String name = String.valueOf(id.incrementAndGet());
        nsc.dropTable(name);
        PartitionMeta pm0_0 = PartitionMeta.newBuilder().setEndpoint(nodes[0]).setIsLeader(true).build();
        PartitionMeta pm0_1 = PartitionMeta.newBuilder().setEndpoint(nodes[1]).setIsLeader(false).build();
        ColumnDesc col0 = ColumnDesc.newBuilder().setName("card").setAddTsIdx(true).setType("string").build();
        ColumnDesc col1 = ColumnDesc.newBuilder().setName("mcc").setAddTsIdx(true).setType("string").build();
        ColumnDesc col2 = ColumnDesc.newBuilder().setName("amt").setAddTsIdx(false).setType("double").build();
        TablePartition tp0 = TablePartition.newBuilder().addPartitionMeta(pm0_0).addPartitionMeta(pm0_1).setPid(0).build();
        TablePartition tp1 = TablePartition.newBuilder().addPartitionMeta(pm0_0).addPartitionMeta(pm0_1).setPid(1).build();
        TableInfo table = TableInfo.newBuilder().addTablePartition(tp0).addTablePartition(tp1)
                .setSegCnt(8).setName(name).setTtl(0)
                .addColumnDesc(col0).addColumnDesc(col1).addColumnDesc(col2)
                .build();
        boolean ok = nsc.createTable(table);
        Assert.assertTrue(ok);
        client.refreshRouteTable();
        return name;
    }
    
    @Test
    public void testPut() {
        String name = createKvTable();
        try {
            PutFuture pf = tableAsyncClient.put(name, "test1", 9527, "value0");
            Assert.assertTrue(pf.get());
            pf = tableAsyncClient.put(name, "test2", 9527, "value1");
            Assert.assertTrue(pf.get());
            GetFuture gf = tableAsyncClient.get(name, "test1");
            String value = new String(gf.get().toByteArray());
            Assert.assertEquals(value, "value0");
            gf = tableAsyncClient.get(name, "test2");
            value = new String(gf.get().toByteArray());
            Assert.assertEquals(value, "value1");
        } catch (Exception e) {
            Assert.assertTrue(false);
        }finally {
            nsc.dropTable(name);
        }
        
    }
    
    @Test
    public void testSchemaPut() {
        
        String name = createSchemaTable();
        try {
            PutFuture pf = tableAsyncClient.put(name, 9527, new Object[]{"card0", "mcc0", 9.15d});
            Assert.assertTrue(pf.get());
            pf = tableAsyncClient.put(name, 9528, new Object[] {"card1", "mcc1", 9.2d});
            Assert.assertTrue(pf.get());
            GetFuture gf = tableAsyncClient.get(name, "card0", 9527);
            Object[] row = gf.getRow();
            Assert.assertEquals(row[0], "card0");
            Assert.assertEquals(row[1], "mcc0");
            Assert.assertEquals(row[2], 9.15d);
            gf = tableAsyncClient.get(name, "card1", 9528);
            row = gf.getRow();
            Assert.assertEquals(row[0], "card1");
            Assert.assertEquals(row[1], "mcc1");
            Assert.assertEquals(row[2], 9.2d);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } finally {
            nsc.dropTable(name);
        }
    }
    
    @Test
    public void testScan() {
        String name = createKvTable();
        try {
            PutFuture pf = tableAsyncClient.put(name, "test1", 9527, "value0");
            Assert.assertTrue(pf.get());
            pf = tableAsyncClient.put(name, "test1", 9528, "value1");
            Assert.assertTrue(pf.get());
            pf = tableAsyncClient.put(name, "test1", 9529, "value2");
            Assert.assertTrue(pf.get());
            ScanFuture sf = tableAsyncClient.scan(name, "test1", 9529, 1000);
            KvIterator it = sf.get();
            Assert.assertTrue(it.getCount() == 3);
            Assert.assertTrue(it.valid());
            byte[] buffer = new byte[6];
            it.getValue().get(buffer);
            String value = new String(buffer);
            Assert.assertEquals(value, "value2");
            it.next();
            
            Assert.assertTrue(it.valid());
            it.getValue().get(buffer);
            value = new String(buffer);
            Assert.assertEquals(value, "value1");
            it.next();
            
            Assert.assertTrue(it.valid());
            it.getValue().get(buffer);
            value = new String(buffer);
            Assert.assertEquals(value, "value0");
            it.next();
            
            Assert.assertFalse(it.valid());
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        }finally {
            nsc.dropTable(name);
        }
    }
    
    @Test
    public void testSchemaPutForMap() {
        
        String name = createSchemaTable();
        try {
            Map<String, Object> rowMap = new HashMap<String, Object>();
            rowMap.put("card", "card0");
            rowMap.put("mcc", "mcc0");
            rowMap.put("amt", 9.15d);
            PutFuture pf = tableAsyncClient.put(name, 9527, rowMap);
            Assert.assertTrue(pf.get());
            rowMap = new HashMap<String, Object>();
            rowMap.put("card", "card1");
            rowMap.put("mcc", "mcc1");
            rowMap.put("amt", 9.2d);
            pf = tableAsyncClient.put(name, 9528, rowMap);
            Assert.assertTrue(pf.get());
            GetFuture gf = tableAsyncClient.get(name, "card0", 9527);
            Object[] row = gf.getRow();
            Assert.assertEquals(row[0], "card0");
            Assert.assertEquals(row[1], "mcc0");
            Assert.assertEquals(row[2], 9.15d);
            gf = tableAsyncClient.get(name, "card1", 9528);
            row = gf.getRow();
            Assert.assertEquals(row[0], "card1");
            Assert.assertEquals(row[1], "mcc1");
            Assert.assertEquals(row[2], 9.2d);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } finally {
            nsc.dropTable(name);
        }
    }

    @Test
    public void testNullDimension() {
        String name = createSchemaTable();
        try {
            PutFuture pf = tableAsyncClient.put(name, 10, new Object[] { null, "1222", 1.0 });
            Assert.assertTrue(pf.get());
            ScanFuture sf = tableAsyncClient.scan(name, "1222", "mcc", 12, 9);
            KvIterator it = sf.get();
            Assert.assertEquals(it.getCount(), 1);
            Assert.assertTrue(it.valid());
            Object[] row = it.getDecodedValue();
            Assert.assertEquals(null, row[0]);
            Assert.assertEquals("1222", row[1]);
            Assert.assertEquals(1.0, row[2]);
        } catch (Exception e) {
            Assert.fail();
        }
        
        try {
            PutFuture pf = tableAsyncClient.put(name, 10, new Object[] { "9527", null, 1.0 });
            Assert.assertTrue(pf.get());
            ScanFuture sf = tableAsyncClient.scan(name, "9527", "card", 12, 9);
            KvIterator it = sf.get();
            Assert.assertEquals(it.getCount(), 1);
            Assert.assertTrue(it.valid());
            Object[] row = it.getDecodedValue();
            Assert.assertEquals("9527", row[0]);
            Assert.assertEquals(null, row[1]);
            Assert.assertEquals(1.0, row[2]);
        } catch (Exception e) {
            Assert.fail();
        }
        
        try {
            tableAsyncClient.put(name, 10, new Object[] { null, null, 1.0 });
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(true);
        }
        
        try {
            tableAsyncClient.put(name, 10, new Object[] { "", "", 1.0 });
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(true);
        }
    }

    @Test
    public void testScanDuplicateRecord() {
        config.setRemoveDuplicateByTime(true);
        String name = createSchemaTable();
        try {
            PutFuture pf = tableAsyncClient.put(name, 10, new Object[] { "card0", "1222", 1.0 });
            Assert.assertTrue(pf.get());
            pf = tableAsyncClient.put(name, 10, new Object[] { "card0", "1223", 2.0 });
            Assert.assertTrue(pf.get());
            ScanFuture sf = tableAsyncClient.scan(name, "card0", "card", 12, 9);
            KvIterator it = sf.get();
            Assert.assertEquals(it.getCount(), 1);
            Assert.assertTrue(it.valid());
            Object[] row = it.getDecodedValue();
            Assert.assertEquals("card0", row[0]);
            Assert.assertEquals("1223", row[1]);
            Assert.assertEquals(2.0, row[2]);
        } catch (Exception e) {
            Assert.fail();
        } finally {
            config.setRemoveDuplicateByTime(false);
        }
       
    }


    @Test
    public void testGetWithOperator() {
        String name = createSchemaTable("kLatestTime");
        try {
            PutFuture pf = tableAsyncClient.put(name, 10, new Object[] { "card0", "1222", 1.0 });
            Assert.assertTrue(pf.get());
            pf = tableAsyncClient.put(name, 11, new Object[] { "card0", "1224", 2.0 });
            Assert.assertTrue(pf.get());
            pf = tableAsyncClient.put(name, 13, new Object[] { "card0", "1224", 3.0 });
            Assert.assertTrue(pf.get());
            // equal
            {
                GetFuture gf = tableAsyncClient.get(name, "card0", 13, Tablet.GetType.kSubKeyEq);
                Object[] row = gf.getRow();
                Assert.assertArrayEquals(new Object[] { "card0", "1224", 3.0 }, row);
            }

            // le
            {
                GetFuture gf = tableAsyncClient.get(name, "card0", 11, Tablet.GetType.kSubKeyLe);
                Object[] row = gf.getRow();
                Assert.assertArrayEquals(new Object[] { "card0", "1224", 2.0 }, row);
            }

            // ge
            {
                GetFuture gf = tableAsyncClient.get(name, "card0", 12, Tablet.GetType.kSubKeyGe);
                Object[] row = gf.getRow();
                Assert.assertArrayEquals(new Object[] { "card0", "1224", 3.0 }, row);
            }

            // ge
            {
                GetFuture gf = tableAsyncClient.get(name, "card0", 13, Tablet.GetType.kSubKeyGe);
                Object[] row = gf.getRow();
                Assert.assertArrayEquals(new Object[] { "card0", "1224", 3.0 }, row);
            }

            // gt
            {
                GetFuture gf = tableAsyncClient.get(name, "card0", 12, Tablet.GetType.kSubKeyGt);
                Object[] row = gf.getRow();
                Assert.assertArrayEquals(new Object[] { "card0", "1224", 3.0 }, row);
            }

            // gt
            {
                GetFuture gf = tableAsyncClient.get(name, "card0", 11, Tablet.GetType.kSubKeyGt);
                Object[] row = gf.getRow();
                Assert.assertArrayEquals(new Object[] { "card0", "1224", 3.0 }, row);
            }
             // le
            {
                GetFuture gf = tableAsyncClient.get(name, "card0", 12, Tablet.GetType.kSubKeyLe);
                Object[] row = gf.getRow();
                Assert.assertArrayEquals(new Object[] { "card0", "1224", 2.0 }, row);
            }
        } catch (Exception e) {
            Assert.fail();
        } finally {
            config.setRemoveDuplicateByTime(false);
        }
    }
    
}
