package com._4paradigm.rtidb.client.ut.ha;

import com._4paradigm.rtidb.client.GetFuture;
import com._4paradigm.rtidb.client.KvIterator;
import com._4paradigm.rtidb.client.PutFuture;
import com._4paradigm.rtidb.client.ScanFuture;
import com._4paradigm.rtidb.client.base.Config;
import com._4paradigm.rtidb.client.base.TestCaseBase;
import com._4paradigm.rtidb.ns.NS;
import com.google.protobuf.ByteString;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class TableFieldCompressTest extends TestCaseBase {
    private static AtomicInteger id = new AtomicInteger(10000);
    private static String[] nodes = Config.NODES;

    @BeforeClass
    public void setUp() {
        super.setUp();
    }

    @AfterClass
    public  void closeResource() {
        super.tearDown();
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

    private String createKVTable() {
        String name = String.valueOf(id.incrementAndGet());
        nsc.dropTable(name);
        NS.PartitionMeta pm0_0 = NS.PartitionMeta.newBuilder().setEndpoint(nodes[0]).setIsLeader(true).build();
        NS.PartitionMeta pm0_1 = NS.PartitionMeta.newBuilder().setEndpoint(nodes[1]).setIsLeader(false).build();
        NS.TablePartition tp0 = NS.TablePartition.newBuilder().addPartitionMeta(pm0_0).addPartitionMeta(pm0_1).setPid(0).build();
        NS.TablePartition tp1 = NS.TablePartition.newBuilder().addPartitionMeta(pm0_0).addPartitionMeta(pm0_1).setPid(1).build();
        NS.TableInfo.Builder builder = NS.TableInfo.newBuilder();
        builder = NS.TableInfo.newBuilder().addTablePartition(tp0).addTablePartition(tp1)
                .setSegCnt(8).setName(name).setTtl(0).setCompressType(NS.CompressType.kSnappy);
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
        String kvTable = createKVTable();
        Map<String, Object> row = new HashMap<String, Object>();
        row.put("card", "card0");
        for (int i = 0; i < schema_num -1; i++) {
            row.put("field" + i, "valueabcad123ajcr3456" + i);
        }
        Map<String, Object> row1 = new HashMap<String, Object>();
        row1.put("card", "card0");
        for (int i = 0; i < schema_num -1; i++) {
            row1.put("field" + i, "XXvalueabcad123ajcr3456" + i);
        }
        try {
            boolean ok = tableSyncClient.put(name, 9527, row);
            Assert.assertTrue(ok);
            ok = tableSyncClient.put(name, 9528, row1);
            Assert.assertTrue(ok);
            KvIterator it = tableSyncClient.scan(name, "card0", "card", 9529, 1000);
            Assert.assertTrue(it.valid());
            Object[] result = it.getDecodedValue();
            Assert.assertEquals(result[0], "card0");
            Assert.assertEquals(result[1], "XXvalueabcad123ajcr34560");
            it.next();
            result = it.getDecodedValue();
            Assert.assertEquals(result[0], "card0");
            Assert.assertEquals(result[1], "valueabcad123ajcr34560");
            it.next();
            Assert.assertFalse(it.valid());
            Object[] result1 = tableSyncClient.getRow(name, "card0", "card", 9527);
            Assert.assertEquals(result1[0], "card0");
            Assert.assertEquals(result1[1], "valueabcad123ajcr34560");

            it = tableSyncClient.traverse(name, "card", null);
            Assert.assertTrue(it.valid());
            result = it.getDecodedValue();
            Assert.assertEquals(result[0], "card0");
            Assert.assertEquals(result[1], "XXvalueabcad123ajcr34560");

            ok = tableSyncClient.put(kvTable, "test1", 9527, "value1");
            Assert.assertTrue(ok);
            ok = tableSyncClient.put(kvTable, "test1", 9528, "value2");
            Assert.assertTrue(ok);
            it = tableSyncClient.scan(kvTable, "test1", 9529, 1000);
            Assert.assertTrue(it.valid());
            Assert.assertEquals(9528l, it.getKey());
            ByteBuffer bb = it.getValue();
            Assert.assertEquals(6, bb.limit() - bb.position());
            byte[] buf = new byte[6];
            bb.get(buf);
            Assert.assertEquals("value2", new String(buf));
            it.next();
            Assert.assertTrue(it.valid());
            Assert.assertEquals(9527l, it.getKey());
            bb = it.getValue();
            Assert.assertEquals(6, bb.limit() - bb.position());
            buf = new byte[6];
            bb.get(buf);
            Assert.assertEquals("value1", new String(buf));
            it.next();
            Assert.assertFalse(it.valid());

            ByteString bs = tableSyncClient.get(kvTable, "test1", 9527);
            Assert.assertEquals(bs.toStringUtf8(), "value1");

        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } finally {
             nsc.dropTable(name);
             nsc.dropTable(kvTable);
        }
    }

    @Test
    public void testCompressFieldASync() {
        int schema_num = 3;
        String name = createSchemaTable(schema_num);
        String kvTable = createKVTable();
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

            pf = tableAsyncClient.put(kvTable, "test1", 9527, "value1");
            Assert.assertTrue(pf.get());
            sf = tableAsyncClient.scan(kvTable, "test1", 9529, 1000);
            it = sf.get();
            Assert.assertTrue(it.valid());
            Assert.assertEquals(9527l, it.getKey());
            ByteBuffer bb = it.getValue();
            Assert.assertEquals(6, bb.limit() - bb.position());
            byte[] buf = new byte[6];
            bb.get(buf);
            Assert.assertEquals("value1", new String(buf));

            gf = tableAsyncClient.get(kvTable, "test1", 0);
            Assert.assertEquals(gf.get().toStringUtf8(), "value1");
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } finally {
            nsc.dropTable(name);
            nsc.dropTable(kvTable);
        }
    }
}
