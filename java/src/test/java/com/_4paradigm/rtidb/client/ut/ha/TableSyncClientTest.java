package com._4paradigm.rtidb.client.ut.ha;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com._4paradigm.rtidb.client.TableSyncClient;
import com._4paradigm.rtidb.client.base.ClientBuilder;
import com._4paradigm.rtidb.client.base.TestCaseBase;
import com._4paradigm.rtidb.client.base.Config;
import com._4paradigm.rtidb.client.ha.RTIDBClientConfig;
import com._4paradigm.rtidb.client.ha.impl.RTIDBClusterClient;
import com._4paradigm.rtidb.client.impl.TableSyncClientImpl;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com._4paradigm.rtidb.client.KvIterator;
import com._4paradigm.rtidb.ns.NS.ColumnDesc;
import com._4paradigm.rtidb.ns.NS.PartitionMeta;
import com._4paradigm.rtidb.ns.NS.TableInfo;
import com._4paradigm.rtidb.ns.NS.TablePartition;
import com._4paradigm.rtidb.tablet.Tablet;
import com.google.protobuf.ByteString;

public class TableSyncClientTest extends TestCaseBase {
    private static AtomicInteger id = new AtomicInteger(10000);
    private static String[] nodes = Config.NODES;
    @BeforeClass
    public  void setUp() {
        super.setUp();
    }
    @AfterClass
    public  void tearDown() {
        super.tearDown();
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
        PartitionMeta pm0_2 = PartitionMeta.newBuilder().setEndpoint(nodes[0]).setIsLeader(false).build();
        PartitionMeta pm0_3 = PartitionMeta.newBuilder().setEndpoint(nodes[1]).setIsLeader(true).build();
        ColumnDesc col0 = ColumnDesc.newBuilder().setName("card").setAddTsIdx(true).setType("string").build();
        ColumnDesc col1 = ColumnDesc.newBuilder().setName("mcc").setAddTsIdx(true).setType("string").build();
        ColumnDesc col2 = ColumnDesc.newBuilder().setName("amt").setAddTsIdx(false).setType("double").build();
        TablePartition tp0 = TablePartition.newBuilder().addPartitionMeta(pm0_0).addPartitionMeta(pm0_1).setPid(0).build();
        TablePartition tp1 = TablePartition.newBuilder().addPartitionMeta(pm0_3).addPartitionMeta(pm0_2).setPid(1).build();
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
            boolean ok = tableSyncClient.put(name, "test1", 9527, "value0");
            Assert.assertTrue(ok);
            ok = tableSyncClient.put(name, "test2", 9527, "value1");
            Assert.assertTrue(ok);
            ByteString bs = tableSyncClient.get(name, "test1");
            String value = new String(bs.toByteArray());
            Assert.assertEquals(value, "value0");
            bs = tableSyncClient.get(name, "test2");
            value = new String(bs.toByteArray());
            Assert.assertEquals(value, "value1");
            Thread.sleep(1000 * 5);
            List<TableInfo> tables = nsc.showTable(name);
            Assert.assertTrue(tables.get(0).getTablePartition(0).getRecordCnt() == 1);
            Assert.assertEquals(tables.get(0).getTablePartition(0).getRecordByteSize(), 235);

        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        }finally {
            nsc.dropTable(name);
        }
        
    }
    
    @Test
    public void testSchemaPut() {
        
        String name = createSchemaTable();
        try {
            boolean ok = tableSyncClient.put(name, 9527, new Object[]{"card0", "mcc0", 9.15d});
            Assert.assertTrue(ok);
            ok = tableSyncClient.put(name, 9528, new Object[] {"card1", "mcc1", 9.2d});
            Assert.assertTrue(ok);
            Object[] row = tableSyncClient.getRow(name, "card0", 9527);
            Assert.assertEquals(row[0], "card0");
            Assert.assertEquals(row[1], "mcc0");
            Assert.assertEquals(row[2], 9.15d);
            row = tableSyncClient.getRow(name, "card1", 9528);
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
            boolean ok = tableSyncClient.put(name, "test1", 9527, "value0");
            Assert.assertTrue(ok);
            ok = tableSyncClient.put(name, "test1", 9528, "value1");
            Assert.assertTrue(ok);
            ok = tableSyncClient.put(name, "test1", 9529, "value2");
            Assert.assertTrue(ok);
            KvIterator it = tableSyncClient.scan(name, "test1", 9529, 1000);
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
    public void testDelete() {
        String name = createKvTable();
        try {
            boolean ok = tableSyncClient.put(name, "test1", 9527, "value0");
            Assert.assertTrue(ok);
            ok = tableSyncClient.put(name, "test1", 9528, "value1");
            Assert.assertTrue(ok);
            ok = tableSyncClient.put(name, "test1", 9529, "value2");
            Assert.assertTrue(ok);
            KvIterator it = tableSyncClient.scan(name, "test1", 9529, 1000);
            Assert.assertTrue(it.getCount() == 3);
            Assert.assertTrue(tableSyncClient.delete(name, "test1"));
            it = tableSyncClient.scan(name, "test1", 9529, 1000);
            Assert.assertTrue(it.getCount() == 0);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        }finally {
            nsc.dropTable(name);
        }
    }

    @Test
    public void testDeleteSchema() {
        String name = createSchemaTable();
        try {
            Map<String, Object> rowMap = new HashMap<String, Object>();
            rowMap.put("card", "card0");
            rowMap.put("mcc", "mcc0");
            rowMap.put("amt", 9.15d);
            boolean ok = tableSyncClient.put(name, 9527, rowMap);
            Assert.assertTrue(ok);
            rowMap = new HashMap<String, Object>();
            rowMap.put("card", "card0");
            rowMap.put("mcc", "mcc1");
            rowMap.put("amt", 9.2d);
            ok = tableSyncClient.put(name, 9528, rowMap);
            Assert.assertTrue(ok);
            KvIterator it = tableSyncClient.scan(name, "card0", "card", 9529, 1000);
            Assert.assertTrue(it.getCount() == 2);
            Assert.assertTrue(tableSyncClient.delete(name, "card0", "card"));
            it = tableSyncClient.scan(name, "card0", "card", 9529, 1000);
            Assert.assertTrue(it.getCount() == 0);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        }finally {
            nsc.dropTable(name);
        }
    }

    @Test
    public void testCount() {
        String name = createKvTable();
        try {
            boolean ok = tableSyncClient.put(name, "test1", 9527, "value0");
            Assert.assertTrue(ok);
            ok = tableSyncClient.put(name, "test1", 9528, "value1");
            Assert.assertTrue(ok);
            ok = tableSyncClient.put(name, "test1", 9529, "value2");
            Assert.assertTrue(ok);
            ok = tableSyncClient.put(name, "test2", 9529, "value3");
            Assert.assertTrue(ok);
            ok = tableSyncClient.put(name, "test3", 9530, "value1");
            Assert.assertTrue(ok);
            ok = tableSyncClient.put(name, "test3", 9530, "value2");
            Assert.assertTrue(ok);
            Assert.assertEquals(3, tableSyncClient.count(name, "test1"));
            Assert.assertEquals(3, tableSyncClient.count(name, "test1", true));
            Assert.assertEquals(1, tableSyncClient.count(name, "test2"));
            Assert.assertEquals(1, tableSyncClient.count(name, "test2", true));
            Assert.assertEquals(2, tableSyncClient.count(name, "test3"));
            RTIDBClientConfig configA = client.getConfig();
            RTIDBClientConfig configB = new RTIDBClientConfig();
            configB.setZkEndpoints(configA.getZkEndpoints());
            configB.setZkRootPath(configA.getZkRootPath());
            configB.setNsEndpoint(configA.getNsEndpoint());
            configB.setReadTimeout(configA.getReadTimeout());
            configB.setWriteTimeout(configA.getWriteTimeout());
            configB.setRemoveDuplicateByTime(true);
            RTIDBClusterClient testNSc = new RTIDBClusterClient(configB);
            testNSc.init();
            TableSyncClient tableSyncClientB = new TableSyncClientImpl(testNSc);
            Assert.assertEquals(1, tableSyncClientB.count(name, "test3", true));
            testNSc.close();
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        }finally {
            nsc.dropTable(name);
        }
    }

    @Test
    public void testCountSchema() {
        String name = createSchemaTable();
        try {
            Map<String, Object> rowMap = new HashMap<String, Object>();
            rowMap.put("card", "card0");
            rowMap.put("mcc", "mcc0");
            rowMap.put("amt", 9.15d);
            boolean ok = tableSyncClient.put(name, 9527, rowMap);
            Assert.assertTrue(ok);
            rowMap = new HashMap<String, Object>();
            rowMap.put("card", "card0");
            rowMap.put("mcc", "mcc1");
            rowMap.put("amt", 9.2d);
            ok = tableSyncClient.put(name, 9528, rowMap);
            Assert.assertTrue(ok);
            rowMap = new HashMap<String, Object>();
            rowMap.put("card", "card1");
            rowMap.put("mcc", "mcc2");
            rowMap.put("amt", 9.25d);
            ok = tableSyncClient.put(name, 9529, rowMap);
            Assert.assertTrue(ok);
            rowMap.put("card", "card1");
            rowMap.put("mcc", "mcc2");
            rowMap.put("amt", 9.3d);
            ok = tableSyncClient.put(name, 9529, rowMap);
            Assert.assertTrue(ok);
            Assert.assertEquals(2, tableSyncClient.count(name, "card0", "card"));
            Assert.assertEquals(2, tableSyncClient.count(name, "card0", "card", true));
            Assert.assertEquals(1, tableSyncClient.count(name, "mcc1", "mcc"));
            Assert.assertEquals(1, tableSyncClient.count(name, "mcc1", "mcc", true));
            Assert.assertEquals(2, tableSyncClient.count(name, "mcc2", "mcc"));
            Assert.assertEquals(2, tableSyncClient.count(name, "card1", "card"));
            RTIDBClientConfig configA = client.getConfig();
            RTIDBClientConfig configB = new RTIDBClientConfig();
            configB.setZkEndpoints(configA.getZkEndpoints());
            configB.setZkRootPath(configA.getZkRootPath());
            configB.setNsEndpoint(configA.getNsEndpoint());
            configB.setReadTimeout(configA.getReadTimeout());
            configB.setWriteTimeout(configA.getWriteTimeout());
            configB.setRemoveDuplicateByTime(true);
            RTIDBClusterClient testNSc = new RTIDBClusterClient(configB);
            testNSc.init();
            TableSyncClient tableSyncClientB = new TableSyncClientImpl(testNSc);
            Assert.assertEquals(1, tableSyncClientB.count(name, "mcc2", "mcc", true));
            Assert.assertEquals(1, tableSyncClientB.count(name, "card1", "card", true));
            testNSc.close();
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        }finally {
            nsc.dropTable(name);
        }
    }

    @Test
    public void testTraverse() {
        String name = createSchemaTable();
        try {
            Map<String, Object> rowMap = new HashMap<String, Object>();
            rowMap.put("card", "card0");
            rowMap.put("mcc", "mcc0");
            rowMap.put("amt", 9.15d);
            boolean ok = tableSyncClient.put(name, 9527, rowMap);
            Assert.assertTrue(ok);
            rowMap = new HashMap<String, Object>();
            rowMap.put("card", "card0");
            rowMap.put("mcc", "mcc1");
            rowMap.put("amt", 9.2d);
            ok = tableSyncClient.put(name, 9528, rowMap);
            Assert.assertTrue(ok);
            Thread.sleep(200);
            KvIterator it = tableSyncClient.traverse(name, "card");
            Assert.assertTrue(it.valid());
            Object[] row = it.getDecodedValue();
            Assert.assertEquals(it.getKey(), 9528);
            Assert.assertEquals(it.getPK(), "card0");
            Assert.assertEquals(row.length, 3);
            Assert.assertEquals(row[0], "card0");
            Assert.assertEquals(row[1], "mcc1");
            Assert.assertEquals(row[2], 9.2d);
            it.next();
            Assert.assertTrue(it.valid());
            row = it.getDecodedValue();
            Assert.assertEquals(it.getKey(), 9527);
            Assert.assertEquals(it.getPK(), "card0");
            Assert.assertEquals(row[0], "card0");
            Assert.assertEquals(row[1], "mcc0");
            Assert.assertEquals(row[2], 9.15d);
            it.next();
            Assert.assertFalse(it.valid());
            for (int i = 0; i < 200; i++) {
                rowMap = new HashMap<String, Object>();
                rowMap.put("card", "card" + (i + 9529));
                rowMap.put("mcc", "mcc" + i);
                rowMap.put("amt", 9.2d);
                ok = tableSyncClient.put(name, i + 9529, rowMap);
                Assert.assertTrue(ok);
            }
            it = tableSyncClient.traverse(name, "card");
            for (int j = 0; j < 202; j++) {
                Assert.assertTrue(it.valid());
                it.next();
            }
            Assert.assertFalse(it.valid());
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        }finally {
            nsc.dropTable(name);
        }
    }

    @Test
    public void testScanLimit() {
        String name = createKvTable();
        try {
            boolean ok = tableSyncClient.put(name, "test1", 9527, "value0");
            Assert.assertTrue(ok);
            ok = tableSyncClient.put(name, "test1", 9528, "value1");
            Assert.assertTrue(ok);
            ok = tableSyncClient.put(name, "test1", 9529, "value2");
            Assert.assertTrue(ok);
            KvIterator it = tableSyncClient.scan(name, "test1", 9529, 1000, 2);
            Assert.assertTrue(it.getCount() == 2);
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
            Assert.assertFalse(it.valid());
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } finally {
            nsc.dropTable(name);
        }
    }

    @Test
    public void testScanLatestN() {
        String name = createKvTable();
        try {
            boolean ok = tableSyncClient.put(name, "test1", 9527, "value0");
            Assert.assertTrue(ok);
            ok = tableSyncClient.put(name, "test1", 9528, "value1");
            Assert.assertTrue(ok);
            ok = tableSyncClient.put(name, "test1", 9529, "value2");
            Assert.assertTrue(ok);
            KvIterator it = tableSyncClient.scan(name, "test1",2);
            Assert.assertTrue(it.getCount() == 2);
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
            Assert.assertFalse(it.valid());
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } finally {
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
            boolean ok = tableSyncClient.put(name, 9527, rowMap);
            Assert.assertTrue(ok);
            rowMap = new HashMap<String, Object>();
            rowMap.put("card", "card1");
            rowMap.put("mcc", "mcc1");
            rowMap.put("amt", 9.2d);
            ok = tableSyncClient.put(name, 9528, rowMap);
            Assert.assertTrue(ok);
            Thread.sleep(200);
            Object[] row = tableSyncClient.getRow(name, "card0", 9527);
            Assert.assertEquals(row[0], "card0");
            Assert.assertEquals(row[1], "mcc0");
            Assert.assertEquals(row[2], 9.15d);
            row = tableSyncClient.getRow(name, "card1", 9528);
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
            boolean ok = tableSyncClient.put(name, 10, new Object[] { null, "1222", 1.0 });
            Assert.assertTrue(ok);
            KvIterator it = tableSyncClient.scan(name, "1222", "mcc", 12, 9);
            Assert.assertNotNull(it);
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
            boolean ok = tableSyncClient.put(name, 10, new Object[] { "9527", null, 1.0 });
            Assert.assertTrue(ok);
            KvIterator it = tableSyncClient.scan(name, "9527", "card", 12, 9);
            Assert.assertNotNull(it);
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
            tableSyncClient.put(name, 10, new Object[] { null, null, 1.0 });
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(true);
        }
        
        try {
            tableSyncClient.put(name, 10, new Object[] { "", "", 1.0 });
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(true);
        }
    }
    
    @Test
    public void testScanDuplicateRecord() {
        ClientBuilder.config.setRemoveDuplicateByTime(true);
        String name = createSchemaTable();
        try {
            boolean ok = tableSyncClient.put(name, 10, new Object[] { "card0", "1222", 1.0 });
            Assert.assertTrue(ok);
            ok = tableSyncClient.put(name, 10, new Object[] { "card0", "1223", 2.0 });
            Assert.assertTrue(ok);
            KvIterator it = tableSyncClient.scan(name, "card0", "card", 12, 9);
            Assert.assertEquals(it.getCount(), 1);
            Assert.assertTrue(it.valid());
            Object[] row = it.getDecodedValue();
            Assert.assertEquals("card0", row[0]);
            Assert.assertEquals("1223", row[1]);
            Assert.assertEquals(2.0, row[2]);
        } catch (Exception e) {
            Assert.fail();
        } finally {
            ClientBuilder.config.setRemoveDuplicateByTime(false);
        }
       
    }
      @Test
    public void testGetWithOperator() {
        String name = createSchemaTable("kLatestTime");
        try {
            boolean ok = tableSyncClient.put(name, 10, new Object[] { "card0", "1222", 1.0 });
            Assert.assertTrue(ok);
            ok = tableSyncClient.put(name, 11, new Object[] { "card0", "1224", 2.0 });
            Assert.assertTrue(ok);
            ok = tableSyncClient.put(name, 13, new Object[] { "card0", "1224", 3.0 });
            Assert.assertTrue(ok);
            // equal
            {
                Object[] row = tableSyncClient.getRow(name, "card0", 13, Tablet.GetType.kSubKeyEq);
                Assert.assertEquals(new Object[] { "card0", "1224", 3.0 }, row);
            }

            // le
            {
                Object[] row = tableSyncClient.getRow(name, "card0", 11, Tablet.GetType.kSubKeyLe);
                Assert.assertEquals(new Object[] { "card0", "1224", 2.0 }, row);
            }

            // ge
            {
                Object[] row = tableSyncClient.getRow(name, "card0", 12, Tablet.GetType.kSubKeyGe);
                Assert.assertEquals(new Object[] { "card0", "1224", 3.0 }, row);
            }

            // ge
            {
                Object[] row = tableSyncClient.getRow(name, "card0", 13, Tablet.GetType.kSubKeyGe);
                Assert.assertEquals(new Object[] { "card0", "1224", 3.0 }, row);
            }

            // gt
            {
                Object[] row = tableSyncClient.getRow(name, "card0", 12, Tablet.GetType.kSubKeyGt);
                Assert.assertEquals(new Object[] { "card0", "1224", 3.0 }, row);
            }

            // gt
            {
                Object[] row = tableSyncClient.getRow(name, "card0", 11, Tablet.GetType.kSubKeyGt);
                Assert.assertEquals(new Object[] { "card0", "1224", 3.0 }, row);
            }
             // le
            {
                Object[] row = tableSyncClient.getRow(name, "card0", 12, Tablet.GetType.kSubKeyLe);
                Assert.assertEquals(new Object[] { "card0", "1224", 2.0 }, row);
            }
        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void testIsRunning(){
        String name = String.valueOf(id.incrementAndGet());
        nsc.dropTable(name);
        PartitionMeta pm0_0 = PartitionMeta.newBuilder().setEndpoint(nodes[0]).setIsLeader(true).build();
        PartitionMeta pm0_1 = PartitionMeta.newBuilder().setEndpoint(nodes[1]).setIsLeader(false).build();
        ColumnDesc col0 = ColumnDesc.newBuilder().setName("card").setAddTsIdx(true).setType("string").build();
        ColumnDesc col1 = ColumnDesc.newBuilder().setName("mcc").setAddTsIdx(true).setType("string").build();
        ColumnDesc col2 = ColumnDesc.newBuilder().setName("amt").setAddTsIdx(false).setType("double").build();
        TablePartition tp0 = TablePartition.newBuilder().addPartitionMeta(pm0_0).addPartitionMeta(pm0_1).setPid(0).build();
        TablePartition tp1 = TablePartition.newBuilder().addPartitionMeta(pm0_0).addPartitionMeta(pm0_1).setPid(1).build();
        TableInfo table = TableInfo.newBuilder().setTtlType("kLatestTime").addTablePartition(tp0).addTablePartition(tp1)
                .setSegCnt(8).setName(name).setTtl(10)
                .addColumnDesc(col0).addColumnDesc(col1).addColumnDesc(col2)
                .build();
        System.out.println(table);
        boolean ok = nsc.createTable(table);
        Assert.assertTrue(ok);
        client.refreshRouteTable();
        System.out.println(name);
        try {
            ok = tableSyncClient.put(name, 9527, new Object[]{"card0", "mcc0", 9.15d});
            Assert.assertTrue(ok);
            ok = tableSyncClient.put(name, 9528, new Object[] {"card1", "mcc1", 9.2d});
            Assert.assertTrue(ok);
            Object[] row = tableSyncClient.getRow(name, "card0", 9527);
            Assert.assertEquals(row[0], "card0");
            Assert.assertEquals(row[1], "mcc0");
            Assert.assertEquals(row[2], 9.15d);
            row = tableSyncClient.getRow(name, "card1", 9528);
            Assert.assertEquals(row[0], "card1");
            Assert.assertEquals(row[1], "mcc1");
            Assert.assertEquals(row[2], 9.2d);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } finally {
            nsc.dropTable(name);
        }
//        return name;
    }


    @Test
    public void testGetWithOpDefault() {
        String name = createSchemaTable("kLatestTime");
        try {
            boolean ok  = tableSyncClient.put(name, 10, new Object[]{"card0", "1222", 1.0});
            Assert.assertTrue(ok);
            ok = tableSyncClient.put(name, 11, new Object[]{"card0", "1224", 2.0});
            Assert.assertTrue(ok);
            ok = tableSyncClient.put(name, 13, new Object[]{"card0", "1224", 3.0});
            Assert.assertTrue(ok);
            // range
            {
                Object[] row = tableSyncClient.getRow(name, "card0", "card", 14, null, Tablet.GetType.kSubKeyLe,
                        9, Tablet.GetType.kSubKeyGe);
                Assert.assertEquals(new Object[]{"card0", "1224", 3.0}, row);
            }

            //
            {
                Object[] row = tableSyncClient.getRow(name, "card0","card", 14, null, Tablet.GetType.kSubKeyLe,
                        14, Tablet.GetType.kSubKeyGe);
                Assert.assertEquals(null, row);
            }

            //
            {
                Object[] row = tableSyncClient.getRow(name, "card0","card", 13, null, Tablet.GetType.kSubKeyEq,
                        13, Tablet.GetType.kSubKeyEq);
                Assert.assertEquals(new Object[]{"card0", "1224", 3.0}, row);
            }

            {
                Object[] row = tableSyncClient.getRow(name, "card0","card", 11, null, Tablet.GetType.kSubKeyEq,
                        11, Tablet.GetType.kSubKeyEq);
                Assert.assertEquals(new Object[]{"card0", "1224", 2.0}, row);
            }

        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void testSchemaPutByKvWay() {

        String name = createSchemaTable();
        try {
            tableSyncClient.put(name, "11", 1535371622000l, "11");
            Assert.assertTrue(false);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(true);
        } finally {
            nsc.dropTable(name);
        }
    }

    @Test
    public void testTraverSeEmptyKvTest() {
        String name = String.valueOf(id.incrementAndGet());
        nsc.dropTable(name);
        TableInfo tableinfo = TableInfo.newBuilder().setName(name).setSegCnt(8).setReplicaNum(1).setTtl(0).build();
        boolean ok = nsc.createTable(tableinfo);
        Assert.assertTrue(ok);
        client.refreshRouteTable();
        try {
            long basets = 1564992840;
            String[] value = {"test", "", "test1", ""};

            for (int i = 0; i < value.length; i++) {
                ok = tableSyncClient.put(name, "key1", basets + i, value[i]);
                Assert.assertTrue(ok);
                Assert.assertEquals(i + 1, tableSyncClient.count(name, "key1"));
            }

            KvIterator it = tableSyncClient.traverse(name);
            Assert.assertTrue(it.valid());
            for (int i = value.length -1; i > 0; i--) {
                byte[] buffer = new byte[it.getValue().remaining()];
                it.getValue().get(buffer);
                String v = new String(buffer);
                Assert.assertEquals(value[i], v);
                Assert.assertEquals(it.getKey(), basets + i);
                Assert.assertEquals(it.getPK(), "key1");
                it.next();
                Assert.assertTrue(it.valid());
            }
            it = tableSyncClient.traverse(name);
            for (int i = 0; i < value.length; i++) {
                Assert.assertTrue(it.valid());
                it.next();
            }
            Assert.assertFalse(it.valid());
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } finally {
            nsc.dropTable(name);
        }

    }
}
