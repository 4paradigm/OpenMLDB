package com._4paradigm.rtidb.client.ut.ha;

import com._4paradigm.rtidb.client.GetFuture;
import com._4paradigm.rtidb.client.KvIterator;
import com._4paradigm.rtidb.client.PutFuture;
import com._4paradigm.rtidb.client.ScanFuture;
import com._4paradigm.rtidb.client.base.TestCaseBase;
import com._4paradigm.rtidb.common.Common;
import com._4paradigm.rtidb.ns.NS;
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
                new Object[] { 1 },
                new Object[] { 0 },
        };
    }

    @Test (dataProvider = "FormatVersion")
    public void testPartitionKey(int formatVersion) {
        String name = String.valueOf(id.incrementAndGet());
        nsc.dropTable(name);
        Common.ColumnDesc col0 = Common.ColumnDesc.newBuilder().setName("card").setAddTsIdx(true).setType("string").build();
        Common.ColumnDesc col1 = Common.ColumnDesc.newBuilder().setName("mcc").setAddTsIdx(false).setType("string").build();
        Common.ColumnDesc col2 = Common.ColumnDesc.newBuilder().setName("amt").setAddTsIdx(false).setType("double").build();
        NS.TableInfo table = NS.TableInfo.newBuilder()
                .setName(name).setTtl(0)
                .addColumnDescV1(col0).addColumnDescV1(col1).addColumnDescV1(col2)
                .addPartitionKey("mcc")
                .setFormatVersion(formatVersion)
                .build();
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
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        }
        try {
            PutFuture pf = tableAsyncClient.put(name, 1133, new Object[]{"card0", "mcc1", 1.2});
            Assert.assertTrue(false);
        } catch (Exception e) {
            Assert.assertTrue(true);
        }
        try {
            ScanFuture sf = tableAsyncClient.scan(name, "card0", "card", 0, 0);
            Assert.assertTrue(false);
        } catch (Exception e) {
            Assert.assertTrue(true);
        }
        try {
            GetFuture gf = tableAsyncClient.get(name, "card0", "card", 0);
            Assert.assertTrue(false);
        } catch (Exception e) {
            Assert.assertTrue(true);
        }
        nsc.dropTable(name);
    }

    @Test (dataProvider = "FormatVersion")
    public void testIndexIsPartitionKey(int formatVersion) {
        String name = String.valueOf(id.incrementAndGet());
        nsc.dropTable(name);
        Common.ColumnDesc col0 = Common.ColumnDesc.newBuilder().setName("card").setAddTsIdx(true).setType("string").build();
        Common.ColumnDesc col1 = Common.ColumnDesc.newBuilder().setName("mcc").setAddTsIdx(true).setType("string").build();
        Common.ColumnDesc col2 = Common.ColumnDesc.newBuilder().setName("amt").setAddTsIdx(false).setType("double").build();
        NS.TableInfo table = NS.TableInfo.newBuilder()
                .setName(name).setTtl(0)
                .addColumnDescV1(col0).addColumnDescV1(col1).addColumnDescV1(col2)
                .addPartitionKey("mcc")
                .setFormatVersion(formatVersion)
                .build();
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
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        }
    }

    @Test (dataProvider = "FormatVersion")
    public void testBigData(int formatVersion) {
        String name = String.valueOf(id.incrementAndGet());
        nsc.dropTable(name);
        Common.ColumnDesc col0 = Common.ColumnDesc.newBuilder().setName("card").setAddTsIdx(true).setType("string").build();
        Common.ColumnDesc col1 = Common.ColumnDesc.newBuilder().setName("mcc").setAddTsIdx(true).setType("string").build();
        Common.ColumnDesc col2 = Common.ColumnDesc.newBuilder().setName("amt").setAddTsIdx(false).setType("double").build();
        NS.TableInfo table = NS.TableInfo.newBuilder()
                .setName(name).setTtl(0)
                .addColumnDescV1(col0).addColumnDescV1(col1).addColumnDescV1(col2)
                .addPartitionKey("mcc")
                .setFormatVersion(formatVersion)
                .build();
        boolean ok = nsc.createTable(table);
        Assert.assertTrue(ok);
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
            Assert.assertTrue(tableSyncClient.delete(name, "card10"));
            Assert.assertTrue(tableSyncClient.delete(name, "mcc10", "mcc"));
            Assert.assertEquals(0, tableSyncClient.count(name, "card10"));
            Assert.assertEquals(0, tableSyncClient.count(name, "mcc10", "mcc"));
            it = tableSyncClient.traverse(name);
            int count = 0;
            while(it.valid()) {
                it.next();
            }
            Assert.assertEquals(5000, count);
            it = tableSyncClient.traverse(name, "mcc");
            while(it.valid()) {
                it.next();
            }
            Assert.assertEquals(5000, count);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
