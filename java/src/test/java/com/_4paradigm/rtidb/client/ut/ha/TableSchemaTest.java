package com._4paradigm.rtidb.client.ut.ha;

import com._4paradigm.rtidb.client.base.Config;
import com._4paradigm.rtidb.client.base.TestCaseBase;
import com._4paradigm.rtidb.common.Common;
import com._4paradigm.rtidb.ns.NS.ColumnDesc;
import com._4paradigm.rtidb.ns.NS.PartitionMeta;
import com._4paradigm.rtidb.ns.NS.TableInfo;
import com._4paradigm.rtidb.ns.NS.TablePartition;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class TableSchemaTest extends TestCaseBase {
    private final static Logger logger = LoggerFactory.getLogger(TableSchemaTest.class);
    private static AtomicInteger id = new AtomicInteger(50000);
    private static String[] nodes = Config.NODES;

    @BeforeClass
    public void setUp() {
        super.setUp();
    }
    @AfterClass
    public void tearDown() {
        super.tearDown();
    }

    private String createSchemaTable() {
        String name = String.valueOf(id.incrementAndGet());
        nsc.dropTable(name);
        PartitionMeta pm0_0 = PartitionMeta.newBuilder().setEndpoint(nodes[0]).setIsLeader(true).build();
        PartitionMeta pm0_1 = PartitionMeta.newBuilder().setEndpoint(nodes[1]).setIsLeader(false).build();
        ColumnDesc col0 = ColumnDesc.newBuilder().setName("card").setAddTsIdx(true).setType("string").build();
        ColumnDesc col1 = ColumnDesc.newBuilder().setName("ts").setAddTsIdx(false).setType("timestamp").build();
        ColumnDesc col2 = ColumnDesc.newBuilder().setName("short").setAddTsIdx(false).setType("int16").build();
        ColumnDesc col3 = ColumnDesc.newBuilder().setName("date").setAddTsIdx(false).setType("date").build();
        ColumnDesc col4 = ColumnDesc.newBuilder().setName("bool").setAddTsIdx(false).setType("bool").build();
        TablePartition tp0 = TablePartition.newBuilder().addPartitionMeta(pm0_0).addPartitionMeta(pm0_1).setPid(0).build();
        TablePartition tp1 = TablePartition.newBuilder().addPartitionMeta(pm0_0).addPartitionMeta(pm0_1).setPid(1).build();
        TableInfo table = TableInfo.newBuilder().addTablePartition(tp0).addTablePartition(tp1)
                .setSegCnt(8).setName(name).setTtl(0)
                .addColumnDesc(col0).addColumnDesc(col1).addColumnDesc(col2).addColumnDesc(col3).addColumnDesc(col4)
                .build();
        boolean ok = nsc.createTable(table);
        Assert.assertTrue(ok);
        client.refreshRouteTable();
        return name;
    }
 
    private String createStringSchemaTable() {
        String name = String.valueOf(id.incrementAndGet());
        nsc.dropTable(name);
        PartitionMeta pm0_0 = PartitionMeta.newBuilder().setEndpoint(nodes[0]).setIsLeader(true).build();
        PartitionMeta pm0_1 = PartitionMeta.newBuilder().setEndpoint(nodes[1]).setIsLeader(false).build();
        ColumnDesc col0 = ColumnDesc.newBuilder().setName("card").setAddTsIdx(true).setType("string").build();
        ColumnDesc col1 = ColumnDesc.newBuilder().setName("ts").setAddTsIdx(false).setType("timestamp").build();
        ColumnDesc col2 = ColumnDesc.newBuilder().setName("str1").setAddTsIdx(false).setType("string").build();
        ColumnDesc col3 = ColumnDesc.newBuilder().setName("str2").setAddTsIdx(false).setType("string").build();
        TablePartition tp0 = TablePartition.newBuilder().addPartitionMeta(pm0_0).addPartitionMeta(pm0_1).setPid(0).build();
        TablePartition tp1 = TablePartition.newBuilder().addPartitionMeta(pm0_0).addPartitionMeta(pm0_1).setPid(1).build();
        TableInfo table = TableInfo.newBuilder().addTablePartition(tp0).addTablePartition(tp1)
                .setSegCnt(8).setName(name).setTtl(0)
                .addColumnDesc(col0).addColumnDesc(col1).addColumnDesc(col2).addColumnDesc(col3)
                .build();
        boolean ok = nsc.createTable(table);
        Assert.assertTrue(ok);
        client.refreshRouteTable();
        return name;
    }
 

    private String createMoreFieldTable(int schema_size) {
        String name = String.valueOf(id.incrementAndGet());
        nsc.dropTable(name);
        List<ColumnDesc> schema = new ArrayList<ColumnDesc>(schema_size);
        ColumnDesc col0 = ColumnDesc.newBuilder().setName("card").setAddTsIdx(true).setType("string").build();
        schema.add(col0);
        for (int idx = 0; idx < schema_size -1; idx++) {
            ColumnDesc col = ColumnDesc.newBuilder().setName("filed" + idx).setAddTsIdx(false).setType("double").build();
            schema.add(col);
        }
        TableInfo.Builder builder = TableInfo.newBuilder()
                .setSegCnt(8).setName(name).setTtl(0);
        for(ColumnDesc desc: schema) {
            builder.addColumnDesc(desc);
        }
        TableInfo table = builder.build();
        boolean ok = nsc.createTable(table);
        Assert.assertTrue(ok);
        client.refreshRouteTable();
        return name;
    }

    @Test
    public void testEmptyStringPut() {
        String name = createStringSchemaTable();
        long time = System.currentTimeMillis();
        try {
            boolean ok = tableSyncClient.put(name, time, new Object[] {"xxx", new DateTime(time), null, ""});
            Assert.assertTrue(ok);
            Object[] row = tableSyncClient.getRow(name, "xxx", 0);
            Assert.assertNotNull(row);
            Assert.assertEquals(4, row.length);
            Assert.assertEquals("xxx", row[0]);
            Assert.assertEquals(time,((DateTime)row[1]).getMillis());
            Assert.assertEquals(null, row[2]);
            Assert.assertEquals("", row[3]);
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("", e);
            Assert.assertTrue(false);
        }
    }
    @Test
    public void testNewTypesPut() {
        String name = createSchemaTable();
        long time = System.currentTimeMillis();
        LocalDate target = new LocalDate(time);
        try {
            boolean ok = tableSyncClient.put(name, time, new Object[] {"card0", new DateTime(time), (short)1, target, true});
            Assert.assertTrue(ok);
            Object[] row = tableSyncClient.getRow(name, "card0", 0);
            Assert.assertNotNull(row);
            Assert.assertEquals(5, row.length);

            Assert.assertEquals("card0", row[0]);
            Assert.assertEquals(time,((DateTime)row[1]).getMillis());
            Assert.assertEquals((short) 1, row[2]);
            Assert.assertEquals(target.toDate(), row[3]);
            Assert.assertEquals(true, row[4]);

            ok = tableSyncClient.put(name, time, new Object[] {"card0", new DateTime(time), (short)1, target, false});
            Assert.assertTrue(ok);
            row = tableSyncClient.getRow(name, "card0", 0);
            Assert.assertNotNull(row);
            Assert.assertEquals(5, row.length);
            Assert.assertEquals(false, row[4]);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        }
    }

    @Test
    public void testFieldPut() {
        int []schema_num_arr = {30, 126, 127, 128, 129, 256, 500, 1200, 2000};
        for (int idx = 0; idx < schema_num_arr.length; idx++) {
            int schema_num = schema_num_arr[idx];
            String name = createMoreFieldTable(schema_num);
            //String name = "50001";
            long time = System.currentTimeMillis();
            LocalDate target = new LocalDate(time);
            Map<String, Object> row1 = new HashMap<String, Object>();
            row1.put("card", "card0");
            for (int i = 0; i < schema_num - 1; i++) {
                row1.put("filed" + i, i + 1.5d);
            }
            try {

                boolean ok = tableSyncClient.put(name, time, row1);
                Assert.assertTrue(ok);
                Object[] row = tableSyncClient.getRow(name, "card0", 0);
                Assert.assertNotNull(row);
                Assert.assertEquals(schema_num, row.length);
                Assert.assertEquals("card0", row[0]);
                for (int i = 0; i < row.length - 1; i++) {
                    Assert.assertEquals(i + 1.5d, Double.parseDouble(row[i + 1].toString()), 0.000001);
                }
            } catch (Exception e) {
                e.printStackTrace();
                Assert.assertTrue(false);
            } finally {
                nsc.dropTable(name);
            }
        }
    }

    @Test
    public void testForAddTableField() {
        int[] schema_num_arr = {30, 127, 128};
        for (int idx = 0; idx < schema_num_arr.length; idx++) {
            int schema_num = schema_num_arr[idx];
            String name = createMoreFieldTable(schema_num);
            //String name = "50001";
            long time = System.currentTimeMillis();
            LocalDate target = new LocalDate(time);
            Map<String, Object> row1 = new HashMap<String, Object>();
            row1.put("card", "card0");
            for (int i = 0; i < schema_num - 1; i++) {
                row1.put("filed" + i, i + 1.5d);
            }
            try {
                Assert.assertEquals(tableSyncClient.getSchema(name).size(), schema_num);
                boolean ok = tableSyncClient.put(name, time, row1);
                Assert.assertTrue(ok);
                Object[] row = tableSyncClient.getRow(name, "card0", 0);
                Assert.assertNotNull(row);
                Assert.assertEquals(schema_num, row.length);
                Assert.assertEquals("card0", row[0]);
                for (int i = 0; i < row.length - 1; i++) {
                    Assert.assertEquals(i + 1.5d, Double.parseDouble(row[i + 1].toString()), 0.000001);
                }

                //put column count more than shema
                row1.put("mcc", "mcc0");
                ok = tableSyncClient.put(name, time, row1);
                Assert.assertTrue(ok);
                row = tableSyncClient.getRow(name, "card0", 0);
                Assert.assertNotNull(row);
                Assert.assertEquals(schema_num, row.length);
                Assert.assertEquals("card0", row[0]);
                for (int i = 0; i < row.length - 1; i++) {
                    Assert.assertEquals(i + 1.5d, Double.parseDouble(row[i + 1].toString()), 0.000001);
                }

                ok = nsc.addTableField(name, "aa", "string");
//            Thread.currentThread().sleep(1000);
                Assert.assertTrue(ok);
                client.refreshRouteTable();
                Assert.assertEquals(tableSyncClient.getSchema(name).size(), schema_num + 1);

                row1.clear();
                row1.put("card", "card1");
                for (int i = 0; i < schema_num - 1; i++) {
                    row1.put("filed" + i, i + 1.5d);
                }
                ok = tableSyncClient.put(name, time, row1);
                Assert.assertTrue(ok);
                row = tableSyncClient.getRow(name, "card1", 0);
                Assert.assertNotNull(row);
                Assert.assertEquals(schema_num + 1, row.length);
                Assert.assertEquals("card1", row[0]);
                for (int i = 0; i < row.length - 2; i++) {
                    Assert.assertEquals(i + 1.5d, Double.parseDouble(row[i + 1].toString()), 0.000001);
                }

                row1.clear();
                row1.put("card", "card2");
                for (int i = 0; i < schema_num - 1; i++) {
                    row1.put("filed" + i, i + 1.5d);
                }
                row1.put("aa", "card3");
                ok = tableSyncClient.put(name, time, row1);
                Assert.assertTrue(ok);
                row = tableSyncClient.getRow(name, "card2", 0);
                Assert.assertNotNull(row);
                Assert.assertEquals(schema_num + 1, row.length);
                Assert.assertEquals("card2", row[0]);
                for (int i = 0; i < row.length - 2; i++) {
                    Assert.assertEquals(i + 1.5d, Double.parseDouble(row[i + 1].toString()), 0.000001);
                }
                Assert.assertEquals("card3", row[row.length - 1]);

                //put column count more than shema
                row1.put("mcc", "mcc1");
                ok = tableSyncClient.put(name, time, row1);
                Assert.assertTrue(ok);
                row = tableSyncClient.getRow(name, "card2", 0);
                Assert.assertNotNull(row);
                Assert.assertEquals(schema_num + 1, row.length);
                Assert.assertEquals("card2", row[0]);
                for (int i = 0; i < row.length - 2; i++) {
                    Assert.assertEquals(i + 1.5d, Double.parseDouble(row[i + 1].toString()), 0.000001);
                }
                Assert.assertEquals("card3", row[row.length - 1]);

            } catch (Exception e) {
                e.printStackTrace();
                Assert.assertTrue(false);
            } finally {
                nsc.dropTable(name);
            }
        }
    }

    @Test
    public void testSchemaSize() {
        int[] schemaArr = {100, 127, 128, 129, 1000};
        for (int schemaSize : schemaArr) {
            String name = String.valueOf(id.incrementAndGet());
            nsc.dropTable(name);
            TableInfo.Builder builder = TableInfo.newBuilder().setName(name).setTtl(0);
            Common.ColumnDesc col = Common.ColumnDesc.newBuilder().setName("card").setAddTsIdx(true).setType("string").build();
            builder.addColumnDescV1(col);
            for (int i = 0; i < schemaSize - 1; i++) {
                col = Common.ColumnDesc.newBuilder().setName("card" + i).setType("string").build();
                builder.addColumnDescV1(col);
            }
            TableInfo table = builder.build();
            boolean ok = nsc.createTable(table);
            Assert.assertTrue(ok);
            client.refreshRouteTable();
            long time = System.currentTimeMillis();
            try {
                Object[] objects = new Object[schemaSize];
                for (int i = 0; i < schemaSize; i++) {
                    objects[i] = String.valueOf(i);
                }
                ok = tableSyncClient.put(name, time, objects);
                Assert.assertTrue(ok);
                Object[] row = tableSyncClient.getRow(name, "0", 0);
                Assert.assertNotNull(row);
                Assert.assertEquals(schemaSize, row.length);
                for (int i = 0; i < schemaSize; i++) {
                    Assert.assertEquals(String.valueOf(i), row[i]);
                }
            } catch (Exception e) {
                e.printStackTrace();
                Assert.assertTrue(false);
            } finally {
                nsc.dropTable(name);
            }
        }
    }
}
