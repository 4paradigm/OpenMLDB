package com._4paradigm.rtidb.client.ut.ha;

import com._4paradigm.rtidb.client.*;
import com._4paradigm.rtidb.client.base.ClientBuilder;
import com._4paradigm.rtidb.client.base.TestCaseBase;
import com._4paradigm.rtidb.common.Common;
import com._4paradigm.rtidb.ns.NS.ColumnDesc;
import com._4paradigm.rtidb.ns.NS.PartitionMeta;
import com._4paradigm.rtidb.ns.NS.TableInfo;
import com._4paradigm.rtidb.ns.NS.TablePartition;
import com._4paradigm.rtidb.tablet.Tablet;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class TableAsyncClientTest extends TestCaseBase {

    private static AtomicInteger id = new AtomicInteger(20000);
    private static String[] nodes = com._4paradigm.rtidb.client.base.Config.NODES;

    @BeforeClass
    public void setUp() {
        super.setUp();
    }

    @AfterClass
    public void tearDown() {
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

    private String createSchemaTable(Tablet.TTLDesc ttl) {
        String name = String.valueOf(id.incrementAndGet());
        nsc.dropTable(name);
        Common.ColumnDesc col0 = Common.ColumnDesc.newBuilder().setName("card").setAddTsIdx(false).setType("string").build();
        Common.ColumnDesc col1 = Common.ColumnDesc.newBuilder().setName("mcc").setAddTsIdx(false).setType("string").build();
        Common.ColumnDesc col2 = Common.ColumnDesc.newBuilder().setName("amt").setAddTsIdx(false).setType("double").build();
        Common.ColumnDesc col3 = Common.ColumnDesc.newBuilder().setName("ts").setAddTsIdx(false).setType("int64").setIsTsCol(true).build();
        Common.ColumnKey colKey1 = Common.ColumnKey.newBuilder().setIndexName("card").addTsName("ts").build();
        Common.ColumnKey colKey2 = Common.ColumnKey.newBuilder().setIndexName("mcc").addTsName("ts").build();
        TableInfo table = TableInfo.newBuilder()
                .setName(name).setTtlDesc(ttl)
                .addColumnDescV1(col0).addColumnDescV1(col1).addColumnDescV1(col2).addColumnDescV1(col3)
                .addColumnKey(colKey1).addColumnKey(colKey2)
                .setPartitionNum(1).setReplicaNum(1)
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
        } finally {
            nsc.dropTable(name);
        }

    }

    @Test
    public void testSchemaPut() {

        String name = createSchemaTable();
        try {
            PutFuture pf = tableAsyncClient.put(name, 9527, new Object[]{"card0", "mcc0", 9.15d});
            Assert.assertTrue(pf.get());
            pf = tableAsyncClient.put(name, 9528, new Object[]{"card1", "mcc1", 9.2d});
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
    public void testAddTableFieldWithColumnKey() {
        String name = String.valueOf(id.incrementAndGet());
        nsc.dropTable(name);
        Common.ColumnDesc col0 = Common.ColumnDesc.newBuilder().setName("card").setAddTsIdx(false).setType("string").build();
        Common.ColumnDesc col1 = Common.ColumnDesc.newBuilder().setName("mcc").setAddTsIdx(false).setType("string").build();
        Common.ColumnDesc col2 = Common.ColumnDesc.newBuilder().setName("amt").setAddTsIdx(false).setType("double").build();
        Common.ColumnDesc col3 = Common.ColumnDesc.newBuilder().setName("ts").setAddTsIdx(false).setType("int64").setIsTsCol(true).build();
        Common.ColumnDesc col4 = Common.ColumnDesc.newBuilder().setName("ts_1").setAddTsIdx(false).setType("int64").setIsTsCol(true).build();
        Common.ColumnKey colKey1 = Common.ColumnKey.newBuilder().setIndexName("card").addTsName("ts").addTsName("ts_1").build();
        Common.ColumnKey colKey2 = Common.ColumnKey.newBuilder().setIndexName("mcc").addTsName("ts").build();
        TableInfo table = TableInfo.newBuilder()
                .setName(name).setTtl(0)
                .addColumnDescV1(col0).addColumnDescV1(col1).addColumnDescV1(col2).addColumnDescV1(col3).addColumnDescV1(col4)
                .addColumnKey(colKey1).addColumnKey(colKey2)
                .setPartitionNum(1).setReplicaNum(1)
                .build();
        boolean ok = nsc.createTable(table);
        Assert.assertTrue(ok);
        client.refreshRouteTable();
        try {
            Map<String, Object> data = new HashMap<String, Object>();
            data.put("card", "card0");
            data.put("mcc", "mcc0");
            data.put("amt", 1.5);
            data.put("ts", 1234l);
            data.put("ts_1", 222l);
            PutFuture pf = tableAsyncClient.put(name, data);
            Assert.assertTrue(pf.get());

            //for col_size > schema_size
            data.clear();
            data.put("card", "card01");
            data.put("mcc", "mcc01");
            data.put("amt", 1.5);
            data.put("ts", 1111l);
            data.put("ts_1", 111l);
            data.put("aa", "aa0");
            pf = tableAsyncClient.put(name, data);
            Assert.assertTrue(pf.get());
            GetFuture gf = tableAsyncClient.get(name, "card01", "card", 1111l, "ts", null);
            Object[] row = gf.getRow();
            Assert.assertEquals(row[0], "card01");
            Assert.assertEquals(row[1], "mcc01");
            Assert.assertEquals(row[2], 1.5);
            Assert.assertEquals(row[3], 1111l);
            Assert.assertEquals(row[4], 111l);
            pf = tableAsyncClient.put(name, new Object[]{"card01", "mcc01", 1.5, 1111l, 111l});
            Assert.assertTrue(pf.get());
            gf = tableAsyncClient.get(name, "card01", "card", 1111l, "ts", null);
            row = gf.getRow();
            Assert.assertEquals(row[0], "card01");
            Assert.assertEquals(row[1], "mcc01");
            Assert.assertEquals(row[2], 1.5);
            Assert.assertEquals(row[3], 1111l);
            Assert.assertEquals(row[4], 111l);
            pf = tableAsyncClient.put(name, new Object[]{"card01", "mcc01", 1.5, 1111l, 111l, 111});
            Assert.assertTrue(pf.get());
            gf = tableAsyncClient.get(name, "card01", "card", 1111l, "ts", null);
            row = gf.getRow();
            Assert.assertEquals(row[0], "card01");
            Assert.assertEquals(row[1], "mcc01");
            Assert.assertEquals(row[2], 1.5);
            Assert.assertEquals(row[3], 1111l);
            Assert.assertEquals(row[4], 111l);
            Assert.assertTrue(pf.get());

            ScanFuture sf = tableAsyncClient.scan(name, "card0", "card", 1235l, 0l, "ts", 0);
            KvIterator it = sf.get();
            Assert.assertTrue(it.valid());
            Assert.assertEquals(it.getSchema().size(), 5);

            ok = nsc.addTableField(name, "aa", "string");
//            Thread.currentThread().sleep(15);
            Assert.assertTrue(ok);
            client.refreshRouteTable();

            data.clear();
            data.put("card", "card0");
            data.put("mcc", "mcc1");
            data.put("amt", 1.6);
            data.put("ts", 1235l);
            data.put("ts_1", 333l);
            data.put("aa", "aa1");
            pf = tableAsyncClient.put(name, data);
            Assert.assertTrue(pf.get());

            data.clear();
            data.put("card", "card0");
            data.put("mcc", "mcc1");
            data.put("amt", 1.7);
            data.put("ts", 1236l);
            data.put("ts_1", 444l);
            pf = tableAsyncClient.put(name, data);
            Assert.assertTrue(pf.get());

            gf = tableAsyncClient.get(name, "card0", "card", 1236l, "ts", null);
            row = gf.getRow();
            Assert.assertEquals(row[0], "card0");
            Assert.assertEquals(row[1], "mcc1");
            Assert.assertEquals(row[2], 1.7);
            Assert.assertEquals(row[3], 1236l);
            Assert.assertEquals(row[4], 444l);
            Assert.assertEquals(row[5], null);

            //for col_size > schema_size
            data.clear();
            data.put("card", "card01");
            data.put("mcc", "mcc01");
            data.put("amt", 1.5);
            data.put("ts", 1111l);
            data.put("ts_1", 111l);
            data.put("aa", "aa0");
            data.put("bb", "bb0");
            pf = tableAsyncClient.put(name, data);
            Assert.assertTrue(pf.get());
            gf = tableAsyncClient.get(name, "card01", "card", 1111l, "ts", null);
            row = gf.getRow();
            Assert.assertEquals(row[0], "card01");
            Assert.assertEquals(row[1], "mcc01");
            Assert.assertEquals(row[2], 1.5);
            Assert.assertEquals(row[3], 1111l);
            Assert.assertEquals(row[4], 111l);
            pf = tableAsyncClient.put(name, new Object[]{"card02", "mcc02", 1.5, 1111l, 111l, "aa", 111});
            Assert.assertTrue(pf.get());
            gf = tableAsyncClient.get(name, "card02", "card", 1111l, "ts", null);
            row = gf.getRow();
            Assert.assertEquals(row[0], "card02");
            Assert.assertEquals(row[1], "mcc02");
            Assert.assertEquals(row[2], 1.5);
            Assert.assertEquals(row[3], 1111l);
            Assert.assertEquals(row[4], 111l);
            pf = tableAsyncClient.put(name, new Object[]{"card01", "mcc01", 1.5, 1111l, 111l, "aa", 111});
            Assert.assertTrue(pf.get());
            gf = tableAsyncClient.get(name, "card01", "card", 1111l, "ts", null);
            row = gf.getRow();
            Assert.assertEquals(row[0], "card01");
            Assert.assertEquals(row[1], "mcc01");
            Assert.assertEquals(row[2], 1.5);
            Assert.assertEquals(row[3], 1111l);
            Assert.assertEquals(row[4], 111l);

            sf = tableAsyncClient.scan(name, "card0", "card", 1235l, 0l, "ts", 0);
            it = sf.get();
            Assert.assertTrue(it.valid());
            Assert.assertEquals(it.getCount(), 2);
            Assert.assertEquals(it.getSchema().size(), 6);
            row = it.getDecodedValue();
            Assert.assertEquals(it.getKey(), 1235);
            Assert.assertEquals(row.length, 6);
            Assert.assertEquals(row[0], "card0");
            Assert.assertEquals(row[1], "mcc1");
            Assert.assertEquals(row[2], 1.6d);
            Assert.assertEquals(((Long) row[3]).longValue(), 1235l);
            Assert.assertEquals(((Long) row[4]).longValue(), 333l);
            Assert.assertEquals(row[5], "aa1");
            sf = tableAsyncClient.scan(name, "card0", "card", 1235l, 0l, "ts_1", 0);
            it = sf.get();
            Assert.assertEquals(it.getCount(), 3);
            row = it.getDecodedValue();
            Assert.assertEquals(it.getKey(), 444);
            Assert.assertEquals(row.length, 6);
            Assert.assertEquals(row[0], "card0");
            Assert.assertEquals(row[1], "mcc1");
            Assert.assertEquals(row[2], 1.7d);
            Assert.assertEquals(((Long) row[3]).longValue(), 1236l);
            Assert.assertEquals(((Long) row[4]).longValue(), 444l);
            Assert.assertEquals(row[5], null);
            sf = tableAsyncClient.scan(name, "mcc1", "mcc", 1235l, 0l, "ts", 0);
            it = sf.get();
            Assert.assertTrue(it.valid());
            Assert.assertTrue(it.getCount() == 1);

            ok = nsc.addTableField(name, "bb", "string");
//            Thread.currentThread().sleep(15);
            Assert.assertTrue(ok);
            client.refreshRouteTable();
            data.clear();
            data.put("card", "card0");
            data.put("mcc", "mcc1");
            data.put("amt", 1.6);
            data.put("ts", 1235l);
            data.put("ts_1", 333l);
            data.put("aa", "aa1");
            pf = tableAsyncClient.put(name, data);
            Assert.assertTrue(pf.get());

            pf = tableAsyncClient.put(name, new Object[]{"card02", "mcc02", 1.5, 1111l, 111l, "aa"});
            Assert.assertTrue(pf.get());

            sf = tableAsyncClient.scan(name, "card0", "card", 1235l, 0l, "ts", 0);
            it = sf.get();
            Assert.assertEquals(it.getSchema().size(), 7);

            try {
                data.clear();
                data.put("card", "card0");
                data.put("mcc", "mcc1");
                data.put("amt", 1.6);
                data.put("ts", 1235l);
                pf = tableAsyncClient.put(name, data);
                Assert.assertTrue(pf.get());
            } catch (Exception e) {
                Assert.assertTrue(true);
            }
            try {
                pf = tableAsyncClient.put(name, new Object[]{"card02", "mcc02", 1.5, 1111l});
                Assert.assertFalse(pf.get());
            } catch (Exception e) {
                Assert.assertTrue(true);
            }

        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        }
        nsc.dropTable(name);
    }

    @Test
    public void testAddTableFieldWithoutColumnKey() {
        String name = createSchemaTable();
        try {
            PutFuture pf = tableAsyncClient.put(name, 9527, new Object[]{"card0", "mcc0", 9.15d});
            Assert.assertTrue(pf.get());

            boolean ok = nsc.addTableField(name, "aa", "string");
            Thread.currentThread().sleep(1000);
            Assert.assertTrue(ok);
//            client.refreshRouteTable();

            pf = tableAsyncClient.put(name, 9528, new Object[]{"card1", "mcc1", 9.2d, "aa1"});
            Assert.assertTrue(pf.get());
            pf = tableAsyncClient.put(name, 9529, new Object[]{"card2", "mcc2", 9.3d});
            Assert.assertTrue(pf.get());

            GetFuture gf = tableAsyncClient.get(name, "card0", 9527);
            Object[] row = gf.getRow();
            Assert.assertEquals(row[0], "card0");
            Assert.assertEquals(row[1], "mcc0");
            Assert.assertEquals(row[2], 9.15d);
            Assert.assertEquals(row[3], null);
            row = gf.getRow(1, TimeUnit.SECONDS);
            Assert.assertEquals(row[0], "card0");
            Assert.assertEquals(row[1], "mcc0");
            Assert.assertEquals(row[2], 9.15d);
            Assert.assertEquals(row[3], null);

            gf = tableAsyncClient.get(name, "card1", 9528);
            row = gf.getRow();
            Assert.assertEquals(row[0], "card1");
            Assert.assertEquals(row[1], "mcc1");
            Assert.assertEquals(row[2], 9.2d);
            Assert.assertEquals(row[3], "aa1");
            row = gf.getRow(1, TimeUnit.SECONDS);
            Assert.assertEquals(row[0], "card1");
            Assert.assertEquals(row[1], "mcc1");
            Assert.assertEquals(row[2], 9.2d);
            Assert.assertEquals(row[3], "aa1");

            gf = tableAsyncClient.get(name, "card2", 9529);
            row = gf.getRow();
            Assert.assertEquals(row[0], "card2");
            Assert.assertEquals(row[1], "mcc2");
            Assert.assertEquals(row[2], 9.3d);
            Assert.assertEquals(row[3], null);
            row = gf.getRow(1, TimeUnit.SECONDS);
            Assert.assertEquals(row[0], "card2");
            Assert.assertEquals(row[1], "mcc2");
            Assert.assertEquals(row[2], 9.3d);
            Assert.assertEquals(row[3], null);

            pf = tableAsyncClient.put(name, 9528, new Object[]{"card0", "mcc1", 9.2d, "aa1"});
            Assert.assertTrue(pf.get());
            pf = tableAsyncClient.put(name, 9529, new Object[]{"card0", "mcc2", 9.3d});
            Assert.assertTrue(pf.get());

            ScanFuture sf = tableAsyncClient.scan(name, "card0", "card", 9530, 0);
            KvIterator it = sf.get();
            Assert.assertEquals(it.getCount(), 3);
            Assert.assertTrue(it.valid());
            row = it.getDecodedValue();
            Assert.assertEquals("card0", row[0]);
            Assert.assertEquals("mcc2", row[1]);
            Assert.assertEquals(9.3d, row[2]);
            Assert.assertEquals(null, row[3]);

            it.next();
            Assert.assertTrue(it.valid());
            row = it.getDecodedValue();
            Assert.assertEquals("card0", row[0]);
            Assert.assertEquals("mcc1", row[1]);
            Assert.assertEquals(9.2d, row[2]);
            Assert.assertEquals("aa1", row[3]);

            it.next();
            Assert.assertTrue(it.valid());
            row = it.getDecodedValue();
            Assert.assertEquals("card0", row[0]);
            Assert.assertEquals("mcc0", row[1]);
            Assert.assertEquals(9.15d, row[2]);
            Assert.assertEquals(null, row[3]);
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
            PutFuture pf = tableAsyncClient.put(name, 10, new Object[]{null, "1222", 1.0});
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
            PutFuture pf = tableAsyncClient.put(name, 10, new Object[]{"9527", null, 1.0});
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
            tableAsyncClient.put(name, 10, new Object[]{null, null, 1.0});
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(true);
        }

        try {
            tableAsyncClient.put(name, 10, new Object[]{"", "", 1.0});
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
            PutFuture pf = tableAsyncClient.put(name, 10, new Object[]{"card0", "1222", 1.0});
            Assert.assertTrue(pf.get());
            pf = tableAsyncClient.put(name, 10, new Object[]{"card0", "1223", 2.0});
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
            ClientBuilder.config.setRemoveDuplicateByTime(false);
        }

    }

    @Test
    public void testGetWithOpDefault() {
        String name = createSchemaTable("kLatestTime");
        try {
            PutFuture pf = tableAsyncClient.put(name, 10, new Object[]{"card0", "1222", 1.0});
            Assert.assertTrue(pf.get());
            pf = tableAsyncClient.put(name, 11, new Object[]{"card0", "1224", 2.0});
            Assert.assertTrue(pf.get());
            pf = tableAsyncClient.put(name, 13, new Object[]{"card0", "1224", 3.0});
            Assert.assertTrue(pf.get());
            // range
            {
                GetFuture gf = tableAsyncClient.get(name, "card0", "card", 14, null, Tablet.GetType.kSubKeyLe,
                        9, Tablet.GetType.kSubKeyGe);
                Object[] row = gf.getRow();
                Assert.assertEquals(new Object[]{"card0", "1224", 3.0}, row);
            }

            //
            {
                GetFuture gf = tableAsyncClient.get(name, "card0", "card", 14, null, Tablet.GetType.kSubKeyLe,
                        14, Tablet.GetType.kSubKeyGe);
                Object[] row = gf.getRow();
                Assert.assertEquals(null, row);
            }

            //
            {
                GetFuture gf = tableAsyncClient.get(name, "card0", "card", 13, null, Tablet.GetType.kSubKeyEq,
                        13, Tablet.GetType.kSubKeyEq);
                Object[] row = gf.getRow();
                Assert.assertEquals(new Object[]{"card0", "1224", 3.0}, row);
            }

            {
                GetFuture gf = tableAsyncClient.get(name, "card0", "card", 11, null, Tablet.GetType.kSubKeyEq,
                        11, Tablet.GetType.kSubKeyEq);
                Object[] row = gf.getRow();
                Assert.assertEquals(new Object[]{"card0", "1224", 2.0}, row);
            }

        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void testGetWithOperator() {
        String name = createSchemaTable("kLatestTime");
        try {
            PutFuture pf = tableAsyncClient.put(name, 10, new Object[]{"card0", "1222", 1.0});
            Assert.assertTrue(pf.get());
            pf = tableAsyncClient.put(name, 11, new Object[]{"card0", "1224", 2.0});
            Assert.assertTrue(pf.get());
            pf = tableAsyncClient.put(name, 13, new Object[]{"card0", "1224", 3.0});
            Assert.assertTrue(pf.get());
            // equal
            {
                GetFuture gf = tableAsyncClient.get(name, "card0", 13, Tablet.GetType.kSubKeyEq);
                Object[] row = gf.getRow();
                Assert.assertEquals(new Object[]{"card0", "1224", 3.0}, row);
            }

            // le
            {
                GetFuture gf = tableAsyncClient.get(name, "card0", 11, Tablet.GetType.kSubKeyLe);
                Object[] row = gf.getRow();
                Assert.assertEquals(new Object[]{"card0", "1224", 2.0}, row);
            }

            // ge
            {
                GetFuture gf = tableAsyncClient.get(name, "card0", 12, Tablet.GetType.kSubKeyGe);
                Object[] row = gf.getRow();
                Assert.assertEquals(new Object[]{"card0", "1224", 3.0}, row);
            }

            // ge
            {
                GetFuture gf = tableAsyncClient.get(name, "card0", 13, Tablet.GetType.kSubKeyGe);
                Object[] row = gf.getRow();
                Assert.assertEquals(new Object[]{"card0", "1224", 3.0}, row);
            }

            // gt
            {
                GetFuture gf = tableAsyncClient.get(name, "card0", 12, Tablet.GetType.kSubKeyGt);
                Object[] row = gf.getRow();
                Assert.assertEquals(new Object[]{"card0", "1224", 3.0}, row);
            }

            // gt
            {
                GetFuture gf = tableAsyncClient.get(name, "card0", 11, Tablet.GetType.kSubKeyGt);
                Object[] row = gf.getRow();
                Assert.assertEquals(new Object[]{"card0", "1224", 3.0}, row);
            }
            // le
            {
                GetFuture gf = tableAsyncClient.get(name, "card0", 12, Tablet.GetType.kSubKeyLe);
                Object[] row = gf.getRow();
                Assert.assertEquals(new Object[]{"card0", "1224", 2.0}, row);
            }
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testSchemaPutByKvWay() {

        String name = createSchemaTable();
        try {
            tableAsyncClient.put(name, "11", 1535371622000l, "11");
            Assert.assertTrue(false);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(true);
        } finally {
            nsc.dropTable(name);
        }
    }
    @Test
    public void testMultiTTLAnd() {
        Tablet.TTLDesc.Builder builder = Tablet.TTLDesc.newBuilder();
        builder.setAbsTtl(1);
        builder.setLatTtl(2);
        builder.setTtlType(Tablet.TTLType.kAbsAndLat);
        // string, string, amt, ts
        String name = createSchemaTable(builder.build());
        long now = System.currentTimeMillis();
        long expired = now - 1 * 60  * 1000;
        try {
            Assert.assertTrue(tableSyncClient.put(name, new Object[] {"card1", "mcc1", 1.0d, now}));
            Assert.assertTrue(tableSyncClient.put(name, new Object[] {"card1", "mcc3", 1.0d, now - 1000}));
            Assert.assertTrue(tableSyncClient.put(name, new Object[] {"card1", "mcc3", 1.0d, now - 1000}));
            Assert.assertTrue(tableSyncClient.put(name, new Object[] {"card1", "mcc4", 1.0d, expired - 2000}));
            ScanOption option = new ScanOption();
            option.setIdxName("card");
            ScanFuture sf = tableAsyncClient.scan(name, "card1", now, 0);
            KvIterator it = sf.get();
            Assert.assertEquals(it.getCount(), 3);
            Assert.assertTrue(it.valid());
            Assert.assertEquals(new Object[] {"card1", "mcc1", 1.0d, now}, it.getDecodedValue());
            it.next();
            Assert.assertTrue(it.valid());
            Assert.assertEquals(new Object[] {"card1", "mcc3", 1.0d, now - 1000}, it.getDecodedValue());
            it.next();
            Assert.assertTrue(it.valid());
            Assert.assertEquals(new Object[] {"card1", "mcc3", 1.0d, now - 1000}, it.getDecodedValue());
            option = new ScanOption();
            option.setIdxName("mcc");
            sf = tableAsyncClient.scan(name, "mcc4", now, 0, option);
            it = sf.get();
            Assert.assertEquals(it.getCount(), 1);
            Assert.assertTrue(it.valid());
            Assert.assertEquals(new Object[] {"card1", "mcc4", 1.0d, expired - 2000}, it.getDecodedValue());
        }catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }


    @Test
    public void testMultiTTLOr() {
        Tablet.TTLDesc.Builder builder = Tablet.TTLDesc.newBuilder();
        builder.setAbsTtl(1);
        builder.setLatTtl(2);
        builder.setTtlType(Tablet.TTLType.kAbsOrLat);
        // string, string, amt, ts
        String name = createSchemaTable(builder.build());
        long now = System.currentTimeMillis();
        long expired = now - 1 * 60  * 1000;
        try {
            Assert.assertTrue(tableSyncClient.put(name, new Object[] {"card1", "mcc1", 1.0d, now}));
            Assert.assertTrue(tableSyncClient.put(name, new Object[] {"card1", "mcc3", 1.0d, now - 1000}));
            Assert.assertTrue(tableSyncClient.put(name, new Object[] {"card1", "mcc3", 1.0d, now - 1000}));
            Assert.assertTrue(tableSyncClient.put(name, new Object[] {"card1", "mcc4", 1.0d, expired - 1000}));
            ScanFuture sf = tableAsyncClient.scan(name, "card1", now, 0);
            KvIterator it = sf.get();
            Assert.assertEquals(it.getCount(), 2);
            Assert.assertTrue(it.valid());
            Assert.assertEquals(new Object[] {"card1", "mcc1", 1.0d, now}, it.getDecodedValue());
            it.next();
            Assert.assertTrue(it.valid());
            Assert.assertEquals(new Object[] {"card1", "mcc3", 1.0d, now - 1000}, it.getDecodedValue());
            it.next();
            Assert.assertFalse(it.valid());

            ScanOption option = new ScanOption();
            option.setIdxName("mcc");
            sf = tableAsyncClient.scan(name, "mcc4", now, 0, option);
            it = sf.get();
            Assert.assertEquals(it.getCount(), 0);
        }catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }


}
