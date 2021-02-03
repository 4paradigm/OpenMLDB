package com._4paradigm.rtidb.client.ut.ha;

import com._4paradigm.rtidb.client.*;
import com._4paradigm.rtidb.client.base.TestCaseBase;
import com._4paradigm.rtidb.client.impl.TableClientCommon;
import com._4paradigm.rtidb.common.Common;
import com._4paradigm.rtidb.ns.NS;
import com._4paradigm.rtidb.tablet.Tablet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class PartitionKeyTest extends TestCaseBase {
    private final static Logger logger = LoggerFactory.getLogger(ColumnKeyTest.class);
    private static AtomicInteger id = new AtomicInteger(50000);

    @BeforeClass
    public void setUp() {
        super.setUp();
    }

    @AfterClass
    public void tearDown() {
        super.tearDown();
    }

    @DataProvider(name = "FormatVersion")
    public Object[][] FormatVersion() {
        return new Object[][] {
                new Object[] { 1, new String[] {"card"}},
                new Object[] { 0, new String[] {"card"}},
                new Object[] { 1, new String[] {"mcc"}},
                new Object[] { 0, new String[] {"mcc"}},
                new Object[] { 1, new String[] {"amt"}},
                new Object[] { 0, new String[] {"amt"}},
                new Object[] { 1, new String[] {"card", "mcc"}},
                new Object[] { 0, new String[] {"card", "mcc"}},
        };
    }

    @Test (dataProvider = "FormatVersion")
    public void testPartitionKey(int formatVersion, String[] partitionKey) {
        String name = String.valueOf(id.incrementAndGet());
        nsc.dropTable(name);
        Common.ColumnDesc col0 = Common.ColumnDesc.newBuilder().setName("card").setAddTsIdx(true).setType("string").build();
        Common.ColumnDesc col1 = Common.ColumnDesc.newBuilder().setName("mcc").setAddTsIdx(false).setType("string").build();
        Common.ColumnDesc col2 = Common.ColumnDesc.newBuilder().setName("amt").setAddTsIdx(false).setType("double").build();
        NS.TableInfo.Builder builder = NS.TableInfo.newBuilder()
                .setName(name).setTtl(0)
                .addColumnDescV1(col0).addColumnDescV1(col1).addColumnDescV1(col2)
                .setFormatVersion(formatVersion);
        for (String key : partitionKey) {
            builder.addPartitionKey(key);
        }
        NS.TableInfo table = builder.build();
        boolean ok = nsc.createTable(table);
        Assert.assertTrue(ok);
        client.refreshRouteTable();
        try {
            Map<String, Object> data = new HashMap<String, Object>();
            data.put("card", "card0");
            data.put("mcc", "mcc0");
            data.put("amt", 1.5);
            tableSyncClient.put(name, 1122, data);
            data.clear();
            data.put("card", "card0");
            data.put("mcc", "mcc1");
            data.put("amt", 1.6);
            tableSyncClient.put(name, 1234, data);
            KvIterator it = tableSyncClient.scan(name, "card0", "card", 1235, 0);
            Assert.assertTrue(it.valid());
            Assert.assertTrue(it.getCount() == 2);
            Object[] value = it.getDecodedValue();
            Assert.assertEquals(value[1], "mcc1");
            Object[] row = tableSyncClient.getRow(name, "card0", "card", 0);
            Assert.assertEquals(row.length, 3);
            Assert.assertEquals(row[0], "card0");
            Assert.assertEquals(row[1], "mcc1");
            it = tableSyncClient.scan(name, "cardxxx", "card", 0, 0);
            Assert.assertFalse(it.valid());
            row = tableSyncClient.getRow(name, "cardxxx", "card", 0);
            Assert.assertEquals(row, null);
            Assert.assertEquals(0, tableSyncClient.count(name, "cardxxx", "card"));
            Assert.assertTrue(tableSyncClient.delete(name, "card0", "card"));
            Assert.assertFalse(tableSyncClient.delete(name, "card0", "card"));
            Assert.assertFalse(tableSyncClient.delete(name, "cardxxx", "card"));
            Assert.assertFalse(tableSyncClient.delete(name, "cardxxx", "mcc"));
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
        try {
            PutFuture pf = tableAsyncClient.put(name, 1133, new Object[]{"card0", "mcc1", 1.2});
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(true);
        }
        try {
            ScanFuture sf = tableAsyncClient.scan(name, "card0", "card", 0, 0);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(true);
        }
        try {
            GetFuture gf = tableAsyncClient.get(name, "card0", "card", 0);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(true);
        }
        try {
            KvIterator it = tableSyncClient.scan(name, "mccxxx", "mcc", 0, 0);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(true);
        }
        try {
            Object[] row = tableSyncClient.getRow(name, "mccxxx", "mcc", 0);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(true);
        }
        try {
            Assert.assertEquals(0, tableSyncClient.count(name, "mccxxx", "mcc"));
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(true);
        }

        nsc.dropTable(name);
    }

    @Test (dataProvider = "FormatVersion")
    public void testPartitionKeyTTL(int formatVersion, String[] partitionKey) {
        String name = String.valueOf(id.incrementAndGet());
        nsc.dropTable(name);
        Common.ColumnDesc col0 = Common.ColumnDesc.newBuilder().setName("card").setAddTsIdx(true).setType("string").build();
        Common.ColumnDesc col1 = Common.ColumnDesc.newBuilder().setName("mcc").setAddTsIdx(false).setType("string").build();
        Common.ColumnDesc col2 = Common.ColumnDesc.newBuilder().setName("amt").setAddTsIdx(false).setType("double").build();
        NS.TableInfo.Builder builder = NS.TableInfo.newBuilder()
                .setName(name).setTtl(10)
                .addColumnDescV1(col0).addColumnDescV1(col1).addColumnDescV1(col2)
                .setFormatVersion(formatVersion);
        for (String key : partitionKey) {
            builder.addPartitionKey(key);
        }
        NS.TableInfo table = builder.build();
        boolean ok = nsc.createTable(table);
        Assert.assertTrue(ok);
        client.refreshRouteTable();
        long ts = System.currentTimeMillis();
        try {
            Map<String, Object> data = new HashMap<String, Object>();
            data.put("card", "card0");
            data.put("mcc", "mcc0");
            data.put("amt", 1.5);
            tableSyncClient.put(name, ts - 100, data);
            data.clear();
            data.put("card", "card0");
            data.put("mcc", "mcc1");
            data.put("amt", 1.6);
            tableSyncClient.put(name, ts, data);
            data.clear();
            data.put("card", "card0");
            data.put("mcc", "mcc2");
            data.put("amt", 1.7);
            tableSyncClient.put(name, ts - 11 * 60 * 1000, data);
            KvIterator it = tableSyncClient.scan(name, "card0", "card", ts, 0);
            Assert.assertTrue(it.valid());
            Assert.assertEquals(it.getCount(), 2);
            Object[] value = it.getDecodedValue();
            Assert.assertEquals(value[1], "mcc1");
            Object[] row = tableSyncClient.getRow(name, "card0", "card", 0);
            Assert.assertEquals(row.length, 3);
            Assert.assertEquals(row[0], "card0");
            Assert.assertEquals(row[1], "mcc1");
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
        try {
            KvIterator it = tableSyncClient.scan(name, "card0", "card", ts - 11 * 60 * 1000, 0);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(true);
        }
        try {
            Object[] row = tableSyncClient.getRow(name, "card0", "card", ts - 11 * 60 * 1000);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(true);
        }
        nsc.dropTable(name);
    }

    @Test (dataProvider = "FormatVersion")
    public void testMerge(int formatVersion, String[] partitionKey) {
        String name = String.valueOf(id.incrementAndGet());
        nsc.dropTable(name);
        Common.ColumnDesc col0 = Common.ColumnDesc.newBuilder().setName("card").setAddTsIdx(true).setType("string").build();
        Common.ColumnDesc col1 = Common.ColumnDesc.newBuilder().setName("mcc").setAddTsIdx(true).setType("string").build();
        Common.ColumnDesc col2 = Common.ColumnDesc.newBuilder().setName("amt").setAddTsIdx(false).setType("double").build();
        NS.TableInfo.Builder builder = NS.TableInfo.newBuilder()
                .setName(name).setTtl(0)
                .addColumnDescV1(col0).addColumnDescV1(col1).addColumnDescV1(col2)
                .setFormatVersion(formatVersion);
        for (String key : partitionKey) {
            builder.addPartitionKey(key);
        }
        NS.TableInfo table = builder.build();
        Assert.assertTrue(nsc.createTable(table));
        client.refreshRouteTable();
        try {
            long ts = System.currentTimeMillis();
            for (int i = 0; i < 3; i++) {
                Map<String, Object> data = new HashMap<String, Object>();
                data.put("mcc", "mcc" + String.valueOf(i));
                data.put("amt", 1.5);
                for (int j = 0; j < 1000; j++) {
                    data.put("card", "card" + String.valueOf(i) + "_" + String.valueOf(j));
                    Assert.assertTrue(tableSyncClient.put(name, ts + j, data));
                }
            }
            Assert.assertEquals(1000, tableSyncClient.count(name, "mcc2", "mcc"));
            KvIterator it = tableSyncClient.scan(name, "card1_1", "card", 0, 0);
            Assert.assertEquals(1, it.getCount());
            it = tableSyncClient.scan(name, "mcc2", "mcc", 0, 0);
            Assert.assertEquals(1000, it.getCount());
            long cur_ts = ts + 999;
            for (int i = 0; i < 1000; i++) {
                Assert.assertEquals(cur_ts, it.getKey());
                cur_ts--;
                it.next();
            }
            it = tableSyncClient.traverse(name);
            int count = 0;
            while(it.valid()) {
                it.next();
                count++;
            }
            Assert.assertEquals(3000, count);
            it = tableSyncClient.traverse(name, "mcc");
            count = 0;
            while(it.valid()) {
                it.next();
                count++;
            }
            Assert.assertEquals(3000, count);
            Assert.assertTrue(tableSyncClient.delete(name, "card1_1"));
            Assert.assertEquals(0, tableSyncClient.count(name, "card1_1"));
            Assert.assertEquals(1000, tableSyncClient.count(name, "mcc2", "mcc"));
            it = tableSyncClient.traverse(name);
            count = 0;
            while(it.valid()) {
                it.next();
                count++;
            }
            Assert.assertEquals(2999, count);
            it = tableSyncClient.traverse(name, "mcc");
            count = 0;
            while(it.valid()) {
                it.next();
                count++;
            }
            Assert.assertEquals(3000, count);
            Assert.assertTrue(tableSyncClient.delete(name, "mcc2", "mcc"));
            it = tableSyncClient.traverse(name, "mcc");
            count = 0;
            while(it.valid()) {
                it.next();
                count++;
            }
            Assert.assertEquals(2000, count);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        } finally {
            nsc.dropTable(name);
        }
    }

    @Test (dataProvider = "FormatVersion")
    public void testIndexIsPartitionKey(int formatVersion, String[] partitionKey) {
        String name = String.valueOf(id.incrementAndGet());
        nsc.dropTable(name);
        Common.ColumnDesc col0 = Common.ColumnDesc.newBuilder().setName("card").setAddTsIdx(true).setType("string").build();
        Common.ColumnDesc col1 = Common.ColumnDesc.newBuilder().setName("mcc").setAddTsIdx(true).setType("string").build();
        Common.ColumnDesc col2 = Common.ColumnDesc.newBuilder().setName("amt").setAddTsIdx(false).setType("double").build();
        NS.TableInfo.Builder builder = NS.TableInfo.newBuilder()
                .setName(name).setTtl(0)
                .addColumnDescV1(col0).addColumnDescV1(col1).addColumnDescV1(col2)
                .setFormatVersion(formatVersion);
        for (String key : partitionKey) {
            builder.addPartitionKey(key);
        }
        NS.TableInfo table = builder.build();
        Assert.assertTrue(nsc.createTable(table));
        client.refreshRouteTable();
        try {
            Map<String, Object> data = new HashMap<String, Object>();
            data.put("card", "card0");
            data.put("mcc", "mcc0");
            data.put("amt", 1.5);
            tableSyncClient.put(name, 1122, data);
            data.clear();
            data.put("card", "card0");
            data.put("mcc", "mcc1");
            data.put("amt", 1.6);
            tableSyncClient.put(name, 1234, data);
            KvIterator it = tableSyncClient.scan(name, "card0", "card", 1235, 0);
            Assert.assertTrue(it.valid());
            Assert.assertTrue(it.getCount() == 2);
            Object[] value = it.getDecodedValue();
            Assert.assertEquals(value[1], "mcc1");
            it = tableSyncClient.scan(name, "mcc0", "mcc", 0, 0);
            Assert.assertTrue(it.valid());
            Assert.assertTrue(it.getCount() == 1);
            value = it.getDecodedValue();
            Assert.assertEquals(value[1], "mcc0");

            Object[] row = tableSyncClient.getRow(name, "card0", "card", 0);
            Assert.assertEquals(row.length, 3);
            Assert.assertEquals(row[0], "card0");
            Assert.assertEquals(row[1], "mcc1");
            row = tableSyncClient.getRow(name, "mcc0", "mcc", 0);
            Assert.assertEquals(row.length, 3);
            Assert.assertEquals(row[0], "card0");
            Assert.assertEquals(row[1], "mcc0");

            it = tableSyncClient.scan(name, "cardxxx", "card", 0, 0);
            Assert.assertFalse(it.valid());
            it = tableSyncClient.scan(name, "mccxxx", "mcc", 0, 0);
            Assert.assertFalse(it.valid());
            row = tableSyncClient.getRow(name, "cardxxx", "card", 0);
            Assert.assertEquals(row, null);
            row = tableSyncClient.getRow(name, "mccxxx", "mcc", 0);
            Assert.assertEquals(row, null);
            Assert.assertEquals(0, tableSyncClient.count(name, "cardxxx", "card"));
            Assert.assertEquals(0, tableSyncClient.count(name, "mccxxx", "mcc"));
            Assert.assertTrue(tableSyncClient.delete(name, "card0", "card"));
            Assert.assertFalse(tableSyncClient.delete(name, "card0", "card"));
            Assert.assertFalse(tableSyncClient.delete(name, "cardxxx", "card"));
            Assert.assertFalse(tableSyncClient.delete(name, "cardxxx", "mcc"));
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        } finally {
            nsc.dropTable(name);
        }
    }

    @Test (dataProvider = "FormatVersion")
    public void testBigData(int formatVersion, String[] partitionKey) {
        String name = String.valueOf(id.incrementAndGet());
        nsc.dropTable(name);
        Common.ColumnDesc col0 = Common.ColumnDesc.newBuilder().setName("card").setAddTsIdx(true).setType("string").build();
        Common.ColumnDesc col1 = Common.ColumnDesc.newBuilder().setName("mcc").setAddTsIdx(true).setType("string").build();
        Common.ColumnDesc col2 = Common.ColumnDesc.newBuilder().setName("amt").setAddTsIdx(false).setType("double").build();
        NS.TableInfo.Builder builder = NS.TableInfo.newBuilder()
                .setName(name).setTtl(0)
                .addColumnDescV1(col0).addColumnDescV1(col1).addColumnDescV1(col2)
                .setFormatVersion(formatVersion);
        for (String key : partitionKey) {
            builder.addPartitionKey(key);
        }
        NS.TableInfo table = builder.build();
        Assert.assertTrue(nsc.createTable(table));
        client.refreshRouteTable();
        try {
            long ts = System.currentTimeMillis();
            for (int i = 0; i < 50; i++) {
                Map<String, Object> data = new HashMap<String, Object>();
                data.put("card", "card" + String.valueOf(i));
                data.put("mcc", "mcc" + String.valueOf(i));
                data.put("amt", 1.5);
                for (int j = 0; j < 100; j++) {
                    tableSyncClient.put(name, ts + j, data);
                }
            }
            Assert.assertEquals(100, tableSyncClient.count(name, "card10"));
            Assert.assertEquals(100, tableSyncClient.count(name, "mcc10", "mcc"));
            KvIterator it = tableSyncClient.scan(name, "card20", "card", 0, 0);
            Assert.assertEquals(100, it.getCount());
            long cur_ts = ts + 99;
            for (int i = 0; i < 100; i++) {
                Assert.assertEquals(cur_ts, it.getKey());
                cur_ts--;
                it.next();
            }
            it = tableSyncClient.scan(name, "mcc20", "mcc", 0, 0);
            Assert.assertEquals(100, it.getCount());
            cur_ts = ts + 99;
            for (int i = 0; i < 100; i++) {
                Assert.assertEquals(cur_ts, it.getKey());
                cur_ts--;
                it.next();
            }
            it = tableSyncClient.traverse(name);
            int count = 0;
            while(it.valid()) {
                it.next();
                count++;
            }
            Assert.assertEquals(5000, count);
            it = tableSyncClient.traverse(name, "mcc");
            count = 0;
            while(it.valid()) {
                it.next();
                count++;
            }
            Assert.assertEquals(5000, count);
            Assert.assertTrue(tableSyncClient.delete(name, "card10"));
            Assert.assertEquals(0, tableSyncClient.count(name, "card10"));
            Assert.assertEquals(100, tableSyncClient.count(name, "mcc10", "mcc"));
            it = tableSyncClient.traverse(name);
            count = 0;
            while(it.valid()) {
                it.next();
                count++;
            }
            Assert.assertEquals(4900, count);
            it = tableSyncClient.traverse(name, "mcc");
            count = 0;
            while(it.valid()) {
                it.next();
                count++;
            }
            Assert.assertEquals(5000, count);
            Assert.assertTrue(tableSyncClient.delete(name, "mcc10", "mcc"));
            it = tableSyncClient.traverse(name, "mcc");
            count = 0;
            while(it.valid()) {
                it.next();
                count++;
            }
            Assert.assertEquals(4900, count);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        } finally {
            nsc.dropTable(name);
        }
    }

    @DataProvider(name = "PartitionKey")
    public Object[][] PartitionKey() {
        return new Object[][] {
                new Object[] { 1, new String[] {"col1"}, new String[] {"col1"}},
                new Object[] { 0, new String[] {"col1"}, new String[] {"col1"}},
                new Object[] { 0, new String[] {"col2"}, new String[] {"col1"}},
                new Object[] { 1, new String[] {"col2"}, new String[] {"col1"}},
                new Object[] { 0, new String[] {"col1", "col2"}, new String[] {"col1"}},
                new Object[] { 1, new String[] {"col1", "col2"}, new String[] {"col1"}},
                new Object[] { 1, new String[] {"col1"}, new String[] {"col5"}},
                new Object[] { 0, new String[] {"col1"}, new String[] {"col5"}},
                new Object[] { 1, new String[] {"col1", "col2"}, new String[] {"col1", "col2"}},
                new Object[] { 0, new String[] {"col1", "col2"}, new String[] {"col1", "col2"}},
                new Object[] { 1, new String[] {"col1"}, new String[] {"col1", "col2"}},
                new Object[] { 0, new String[] {"col1"}, new String[] {"col1", "col2"}},
                new Object[] { 1, new String[] {"col1"}, new String[] {"col1", "col2", "col5"}},
                new Object[] { 0, new String[] {"col1"}, new String[] {"col1", "col2", "col5"}},
        };
    }

    private void CreateAndPut(String name, int formatVersion, Common.ColumnKey[] columnKeys, String[] partitionKey) {
        nsc.dropTable(name);
        NS.TableInfo.Builder builder = NS.TableInfo.newBuilder()
                .setName(name).setTtl(0)
                .addColumnDescV1(Common.ColumnDesc.newBuilder().setName("col1").setType("string").build())
                .addColumnDescV1(Common.ColumnDesc.newBuilder().setName("col2").setType("string").build())
                .addColumnDescV1(Common.ColumnDesc.newBuilder().setName("col3").setType("string").build())
                .addColumnDescV1(Common.ColumnDesc.newBuilder().setName("col4").setType("int64").build())
                .addColumnDescV1(Common.ColumnDesc.newBuilder().setName("col5").setType("int64").setIsTsCol(true).build())
                .setFormatVersion(formatVersion);
        for (Common.ColumnKey columnKey : columnKeys) {
            builder.addColumnKey(columnKey);
        }
        for (String key : partitionKey) {
            builder.addPartitionKey(key);
        }
        NS.TableInfo table = builder.build();
        Assert.assertTrue(nsc.createTable(table));
        client.refreshRouteTable();
        try {
            for (int idx = 0; idx < 10; idx++) {
                Map<String, Object> data = new HashMap<String, Object>();
                data.put("col1", "col1" + String.valueOf(idx));
                data.put("col2", "col2" + String.valueOf(idx));
                data.put("col3", "col3" + String.valueOf(idx));
                for (int j = 0; j < 5; j++) {
                    data.put("col4", 100l + j);
                    data.put("col5", 1000l + j);
                    Assert.assertTrue(tableSyncClient.put(name, data));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test (dataProvider = "PartitionKey")
    public void testParitionKey(int formatVersion, String[] columnKey, String[] partitionKey) {
        String name = String.valueOf(id.incrementAndGet());
        Common.ColumnKey.Builder builder = Common.ColumnKey.newBuilder();
        String indexName = "";
        for (String col : columnKey) {
            indexName += col;
            builder.addColName(col);
        }
        builder.setIndexName(indexName);
        CreateAndPut(name, formatVersion, new Common.ColumnKey[] { builder.build()}, partitionKey);
        try {
            Map<String, Object> scanMap = new HashMap<>();
            Object[] keyRow = new Object[columnKey.length];
            int idx = 0;
            for (String col : columnKey) {
                scanMap.put(col, col + String.valueOf(1));
                keyRow[idx++] = col + String.valueOf(1);
            }
            KvIterator it = tableSyncClient.scan(name, scanMap, indexName, 0, 0, "", 0);
            Assert.assertTrue(it.valid());
            Assert.assertTrue(it.getCount() == 5);
            Object[] value = it.getDecodedValue();
            Assert.assertEquals(value[0], "col11");
            Assert.assertEquals(value[1], "col21");

            Object[] row = tableSyncClient.getRow(name, scanMap, indexName, 0, "", Tablet.GetType.kSubKeyEq);
            Assert.assertEquals(row.length, 5);
            Assert.assertEquals(row[0], "col11");
            Assert.assertEquals(row[1], "col21");

            it = tableSyncClient.traverse(name);
            int count = 0;
            while(it.valid()) {
                it.next();
                count++;
            }
            Assert.assertEquals(50, count);
            Assert.assertEquals(5, tableSyncClient.count(name, scanMap, indexName, "", true));
            String key = TableClientCommon.getCombinedKey(keyRow, true);
            tableSyncClient.delete(name, key, indexName);
            Assert.assertEquals(0, tableSyncClient.count(name, scanMap, indexName, "", true));
            it = tableSyncClient.traverse(name);
            count = 0;
            while(it.valid()) {
                it.next();
                count++;
            }
            Assert.assertEquals(45, count);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        } finally {
            nsc.dropTable(name);
        }
    }

    @DataProvider(name = "MultiColumnKey")
    public Object[][] MultiColumnKey() {
        return new Object[][] {
                new Object[] { 1, new Object[] {new String[] {"col1"}, new String[] {"col2"} }, new String[] {"col1"}},
                new Object[] { 0, new Object[] {new String[] {"col1"}, new String[] {"col2"} }, new String[] {"col1"}},
                new Object[] { 1, new Object[] {new String[] {"col1", "col3"}, new String[] {"col2"} }, new String[] {"col1"}},
                new Object[] { 0, new Object[] {new String[] {"col1", "col3"}, new String[] {"col2"} }, new String[] {"col1"}},
                new Object[] { 1, new Object[] {new String[] {"col1", "col3"}, new String[] {"col2"} }, new String[] {"col2"}},
                new Object[] { 0, new Object[] {new String[] {"col1", "col3"}, new String[] {"col2"} }, new String[] {"col2"}},
                new Object[] { 1, new Object[] {new String[] {"col1", "col3"}, new String[] {"col2"} }, new String[] {"col1", "col3"}},
                new Object[] { 0, new Object[] {new String[] {"col1", "col3"}, new String[] {"col2"} }, new String[] {"col1", "col3"}},
                new Object[] { 1, new Object[] {new String[] {"col1", "col3"}, new String[] {"col2"} }, new String[] {"col5"}},
                new Object[] { 0, new Object[] {new String[] {"col1", "col3"}, new String[] {"col2"} }, new String[] {"col5"}},
                new Object[] { 1, new Object[] {new String[] {"col1"}, new String[] {"col2"}, new String[] {"col3"} }, new String[] {"col1"}},
                new Object[] { 0, new Object[] {new String[] {"col1"}, new String[] {"col2"}, new String[] {"col3"} }, new String[] {"col1"}},
                new Object[] { 1, new Object[] {new String[] {"col1"}, new String[] {"col2"}, new String[] {"col3"} }, new String[] {"col1", "col2", "col3"}},
                new Object[] { 0, new Object[] {new String[] {"col1"}, new String[] {"col2"}, new String[] {"col3"} }, new String[] {"col1", "col2", "col3"}},
                new Object[] { 1, new Object[] {new String[] {"col1"}, new String[] {"col2"}, new String[] {"col3"} }, new String[] {"col1", "col2", "col5"}},
                new Object[] { 0, new Object[] {new String[] {"col1"}, new String[] {"col2"}, new String[] {"col3"} }, new String[] {"col1", "col2", "col5"}},
                new Object[] { 1, new Object[] {new String[] {"col1", "col2", "col3"}}, new String[] {"col1", "col2", "col5"}},
                new Object[] { 0, new Object[] {new String[] {"col1", "col2", "col3"}}, new String[] {"col1", "col2", "col5"}},
        };
    }

    @Test (dataProvider = "MultiColumnKey")
    public void testParitionKeyMultiColumnKey(int formatVersion, Object[] columnKey, String[] partitionKey) {
        String name = String.valueOf(id.incrementAndGet());
        Common.ColumnKey[] keys = new Common.ColumnKey[columnKey.length];
        int idx = 0;
        for (Object arr : columnKey) {
            Common.ColumnKey.Builder builder = Common.ColumnKey.newBuilder();
            String indexName = "";
            for (String col : (String[])arr) {
                indexName += col;
                builder.addColName(col);
            }
            builder.setIndexName(indexName);
            keys[idx++] = builder.build();
        }
        CreateAndPut(name, formatVersion, keys, partitionKey);
        try {
            for (int i = 0; i < columnKey.length; i++) {
                Map<String, Object> scanMap = new HashMap<>();
                Object[] keyRow = new Object[((String[]) columnKey[0]).length];
                idx = 0;
                String indexName = "";
                for (String col : (String[]) (columnKey[i])) {
                    indexName += col;
                    scanMap.put(col, col + String.valueOf(1));
                }
                KvIterator it = tableSyncClient.scan(name, scanMap, indexName, 0, 0, "", 0);
                Assert.assertTrue(it.valid());
                Assert.assertTrue(it.getCount() == 5);
                Object[] value = it.getDecodedValue();
                Assert.assertEquals(value[0], "col11");
                Assert.assertEquals(value[1], "col21");

                Object[] row = tableSyncClient.getRow(name, scanMap, indexName, 0, "", Tablet.GetType.kSubKeyEq);
                Assert.assertEquals(row.length, 5);
                Assert.assertEquals(row[0], "col11");
                Assert.assertEquals(row[1], "col21");
                Assert.assertEquals(5, tableSyncClient.count(name, scanMap, indexName, "", true));
            }

            Map<String, Object> scanMap = new HashMap<>();
            Object[] keyRow = new Object[((String[]) columnKey[0]).length];
            idx = 0;
            String indexName = "";
            for (String col : (String[]) (columnKey[0])) {
                indexName += col;
                scanMap.put(col, col + String.valueOf(1));
                keyRow[idx++] = col + String.valueOf(1);
            }

            KvIterator it = tableSyncClient.traverse(name);
            int count = 0;
            while(it.valid()) {
                it.next();
                count++;
            }
            Assert.assertEquals(50, count);
            String key = TableClientCommon.getCombinedKey(keyRow, true);
            tableSyncClient.delete(name, key, indexName);
            Assert.assertEquals(0, tableSyncClient.count(name, scanMap, indexName, "", true));
            it = tableSyncClient.traverse(name);
            count = 0;
            while(it.valid()) {
                it.next();
                count++;
            }
            Assert.assertEquals(45, count);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        } finally {
            nsc.dropTable(name);
        }
    }
}
