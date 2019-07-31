package com._4paradigm.rtidb.client.ut.ha;

import com._4paradigm.rtidb.client.base.TestCaseBase;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com._4paradigm.rtidb.common.Common.ColumnDesc;
import com._4paradigm.rtidb.common.Common.ColumnKey;
import com._4paradigm.rtidb.ns.NS.TableInfo;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.UUID;

public class CreateTableTest extends TestCaseBase {

    private final static Logger logger = LoggerFactory.getLogger(TableSchemaTest.class);
    private static AtomicInteger id = new AtomicInteger(50000);
    @BeforeClass
    public void setUp() {
        super.setUp();
    }

    @AfterClass
    public void tearDown() {
        super.tearDown();
    }
    @Test
    public void testTSColumnType() {
        Map<String, Boolean> map = new HashMap<String, Boolean>();
        map.put("int16", false);
        map.put("int32", false);
        map.put("string", false);
        map.put("bool", false);
        map.put("float", false);
        map.put("double", false);
        map.put("date", false);
        map.put("int64", true);
        map.put("uint64", true);
        map.put("timestamp", true);
        for (Map.Entry<String, Boolean> entry : map.entrySet()) {
            String name = String.valueOf(id.incrementAndGet());
            nsc.dropTable(name);
            ColumnDesc col0 = ColumnDesc.newBuilder().setName("card").setAddTsIdx(true).setType("string").build();
            ColumnDesc col1 = ColumnDesc.newBuilder().setName("mcc").setAddTsIdx(false).setType("string").build();
            ColumnDesc col2 = ColumnDesc.newBuilder().setName("amt").setAddTsIdx(false).setType("double").build();
            ColumnDesc col3 = ColumnDesc.newBuilder().setName("ts").setAddTsIdx(false).setType(entry.getKey()).setIsTsCol(true).build();
            TableInfo table = TableInfo.newBuilder()
                    .setName(name).setTtl(0)
                    .addColumnDescV1(col0).addColumnDescV1(col1).addColumnDescV1(col2).addColumnDescV1(col3)
                    .build();
            boolean ok = nsc.createTable(table);
            if (entry.getValue().booleanValue()) {
                Assert.assertTrue(ok);
            } else {
                Assert.assertFalse(ok);
            }
            nsc.dropTable(name);
        }
    }

    @Test
    public void testTsColIndex() {
        {
            String name = String.valueOf(id.incrementAndGet());
            nsc.dropTable(name);
            ColumnDesc col0 = ColumnDesc.newBuilder().setName("card").setAddTsIdx(true).setType("string").setAddTsIdx(true).build();
            ColumnDesc col1 = ColumnDesc.newBuilder().setName("mcc").setAddTsIdx(false).setType("string").build();
            ColumnDesc col2 = ColumnDesc.newBuilder().setName("amt").setAddTsIdx(false).setType("double").build();
            ColumnDesc col3 = ColumnDesc.newBuilder().setName("ts").setAddTsIdx(false).setType("int64").setAddTsIdx(true).setIsTsCol(true).build();
            TableInfo table = TableInfo.newBuilder()
                    .setName(name).setTtl(0)
                    .addColumnDescV1(col0).addColumnDescV1(col1).addColumnDescV1(col2).addColumnDescV1(col3)
                    .build();
            boolean ok = nsc.createTable(table);
            Assert.assertFalse(ok);

            ColumnDesc col4 = ColumnDesc.newBuilder().setName("ts").setAddTsIdx(false).setType("int64").setIsTsCol(true).build();
            ColumnKey key1 = ColumnKey.newBuilder().setIndexName("card").addColName("ts").build();
            table = TableInfo.newBuilder()
                    .setName(name).setTtl(0)
                    .addColumnDescV1(col0).addColumnDescV1(col1).addColumnDescV1(col2).addColumnDescV1(col4)
                    .addColumnKey(key1)
                    .build();
            ok = nsc.createTable(table);
            Assert.assertFalse(ok);
        }
        {
            String name = String.valueOf(id.incrementAndGet());
            nsc.dropTable(name);
            ColumnDesc col0 = ColumnDesc.newBuilder().setName("card").setAddTsIdx(true).setType("string").setAddTsIdx(true).build();
            ColumnDesc col1 = ColumnDesc.newBuilder().setName("mcc").setAddTsIdx(false).setType("string").build();
            ColumnDesc col2 = ColumnDesc.newBuilder().setName("amt").setAddTsIdx(false).setType("double").setAddTsIdx(true).build();
            ColumnDesc col3 = ColumnDesc.newBuilder().setName("ts").setAddTsIdx(false).setType("int64").setAddTsIdx(true).setIsTsCol(true).build();
            TableInfo table = TableInfo.newBuilder()
                    .setName(name).setTtl(0)
                    .addColumnDescV1(col0).addColumnDescV1(col1).addColumnDescV1(col2).addColumnDescV1(col3)
                    .build();
            boolean ok = nsc.createTable(table);
            Assert.assertFalse(ok);

            ColumnDesc col4 = ColumnDesc.newBuilder().setName("ts").setAddTsIdx(false).setType("int64").setIsTsCol(true).build();
            ColumnKey key1 = ColumnKey.newBuilder().setIndexName("card").addColName("ts").build();
            table = TableInfo.newBuilder()
                    .setName(name).setTtl(0)
                    .addColumnDescV1(col0).addColumnDescV1(col1).addColumnDescV1(col2).addColumnDescV1(col4)
                    .addColumnKey(key1)
                    .build();
            ok = nsc.createTable(table);
            Assert.assertFalse(ok);
        }
    }


    @Test
    public void testColumnKey() {
        String name = String.valueOf(id.incrementAndGet());
        ColumnDesc col0 = ColumnDesc.newBuilder().setName("card").setAddTsIdx(true).setType("string").build();
        ColumnDesc col1 = ColumnDesc.newBuilder().setName("mcc").setAddTsIdx(false).setType("string").build();
        ColumnDesc col2 = ColumnDesc.newBuilder().setName("amt").setAddTsIdx(false).setType("double").build();
        ColumnDesc col3 = ColumnDesc.newBuilder().setName("col1").setAddTsIdx(false).setType("int64").build();
        ColumnDesc col4 = ColumnDesc.newBuilder().setName("ts1").setAddTsIdx(false).setIsTsCol(true).setType("int64").build();
        ColumnDesc col5 = ColumnDesc.newBuilder().setName("ts2").setAddTsIdx(false).setIsTsCol(true).setType("timestamp").build();
        TableInfo table = TableInfo.newBuilder()
                .setName(name).setTtl(0)
                .addColumnDescV1(col0).addColumnDescV1(col1).addColumnDescV1(col2).addColumnDescV1(col3)
                .addColumnDescV1(col4).addColumnDescV1(col5)
                .build();
        boolean ok = nsc.createTable(table);
        Assert.assertFalse(ok);

        ColumnKey colKey1 = ColumnKey.newBuilder().addColName("card").addTsName("ts1").build();
        TableInfo table1 = TableInfo.newBuilder()
                .setName(name).setTtl(0)
                .addColumnDescV1(col0).addColumnDescV1(col1).addColumnDescV1(col2).addColumnDescV1(col3)
                .addColumnDescV1(col4).addColumnDescV1(col5)
                .addColumnKey(colKey1)
                .build();

        Assert.assertTrue(nsc.createTable(table1));
        nsc.dropTable(name);

        ColumnKey colKeyErr = ColumnKey.newBuilder().addColName("card").addTsName("tsNull").build();
        TableInfo tableErr = TableInfo.newBuilder()
                .setName(name).setTtl(0)
                .addColumnDescV1(col0).addColumnDescV1(col1).addColumnDescV1(col2).addColumnDescV1(col3)
                .addColumnDescV1(col4).addColumnDescV1(col5)
                .addColumnKey(colKeyErr)
                .build();
        Assert.assertFalse(nsc.createTable(tableErr));

        ColumnKey colKeyErr1 = ColumnKey.newBuilder().addColName("card").addColName("NULL").addTsName("tsNull").build();
        TableInfo tableErr1 = TableInfo.newBuilder()
                .setName(name).setTtl(0)
                .addColumnDescV1(col0).addColumnDescV1(col1).addColumnDescV1(col2).addColumnDescV1(col3)
                .addColumnDescV1(col4).addColumnDescV1(col5)
                .addColumnKey(colKeyErr1)
                .build();
        Assert.assertFalse(nsc.createTable(tableErr1));

        ColumnKey colKey2 = ColumnKey.newBuilder().setIndexName("combined_key").addColName("card").addColName("mcc").addTsName("ts1").build();
        TableInfo table2 = TableInfo.newBuilder()
                .setName(name).setTtl(0)
                .addColumnDescV1(col0).addColumnDescV1(col1).addColumnDescV1(col2).addColumnDescV1(col3)
                .addColumnDescV1(col4).addColumnDescV1(col5)
                .addColumnKey(colKey2)
                .build();
        Assert.assertTrue(nsc.createTable(table2));
        nsc.dropTable(name);

        ColumnKey colKey3 = ColumnKey.newBuilder().setIndexName("combined_key").addColName("card").addColName("mcc").build();
        TableInfo table3 = TableInfo.newBuilder().setName(name).setTtl(0)
                .addColumnDescV1(col0).addColumnDescV1(col1).addColumnDescV1(col2).addColumnDescV1(col3)
                .addColumnKey(colKey3)
                .build();
        Assert.assertTrue(nsc.createTable(table3));
        nsc.dropTable(name);

        ColumnKey colKey4 = ColumnKey.newBuilder().setIndexName("combined_key").addColName("card").addColName("mcc")
                .addTsName("ts1").addTsName("ts1").build();
        TableInfo table4 = TableInfo.newBuilder().setName(name).setTtl(0)
                .addColumnDescV1(col0).addColumnDescV1(col1).addColumnDescV1(col2).addColumnDescV1(col3)
                .addColumnKey(colKey4)
                .build();
        Assert.assertFalse(nsc.createTable(table4));

        ColumnKey colKey5_1 = ColumnKey.newBuilder().setIndexName("combined_key1").addColName("card").addColName("mcc").build();
        ColumnKey colKey5_2 = ColumnKey.newBuilder().setIndexName("combined_key2").addColName("card").addColName("mcc").build();
        TableInfo table5 = TableInfo.newBuilder().setName(name).setTtl(0)
                .addColumnDescV1(col0).addColumnDescV1(col1).addColumnDescV1(col2).addColumnDescV1(col3)
                .addColumnKey(colKey5_1).addColumnKey(colKey5_2)
                .build();
        Assert.assertTrue(nsc.createTable(table5));

        nsc.dropTable(name);
        ColumnKey colKey6 = ColumnKey.newBuilder().setIndexName("combined_key").addColName("card").addColName("amt")
                .addTsName("ts1").addTsName("ts1").build();
        TableInfo table6 = TableInfo.newBuilder().setName(name).setTtl(0)
                .addColumnDescV1(col0).addColumnDescV1(col1).addColumnDescV1(col2).addColumnDescV1(col3)
                .addColumnKey(colKey4)
                .build();
        Assert.assertFalse(nsc.createTable(table6));

        ColumnKey colKey7 = ColumnKey.newBuilder().setIndexName("amt")
                .addTsName("ts1").addTsName("ts1").build();
        TableInfo table7 = TableInfo.newBuilder().setName(name).setTtl(0)
                .addColumnDescV1(col0).addColumnDescV1(col1).addColumnDescV1(col2).addColumnDescV1(col3)
                .addColumnKey(colKey4)
                .build();
        Assert.assertFalse(nsc.createTable(table7));

        ColumnKey colKey8 = ColumnKey.newBuilder().setIndexName(UUID.randomUUID().toString().replaceAll("-", ""))
                .addTsName("ts1").addTsName("ts1").build();
        TableInfo table8 = TableInfo.newBuilder().setName(name).setTtl(0)
                .addColumnDescV1(col0).addColumnDescV1(col1).addColumnDescV1(col2).addColumnDescV1(col3)
                .addColumnKey(colKey4)
                .build();
        Assert.assertFalse(nsc.createTable(table7));
    }

    @Test
    public void testCreateWithTTL() {
        String name = String.valueOf(id.incrementAndGet());
        nsc.dropTable(name);
        ColumnDesc col0 = ColumnDesc.newBuilder().setName("card").setAddTsIdx(true).setType("string").build();
        ColumnDesc col1 = ColumnDesc.newBuilder().setName("mcc").setAddTsIdx(false).setType("string").build();
        ColumnDesc col2 = ColumnDesc.newBuilder().setName("amt").setAddTsIdx(false).setType("double").build();
        ColumnDesc col3 = ColumnDesc.newBuilder().setName("col1").setAddTsIdx(false).setType("int64").build();
        ColumnDesc col4 = ColumnDesc.newBuilder().setName("ts1").setAddTsIdx(false).setIsTsCol(true).setType("int64").build();
        ColumnDesc col5 = ColumnDesc.newBuilder().setName("ts2").setAddTsIdx(false).setIsTsCol(true)
                .setTtl(1000).setType("timestamp").build();
        ColumnKey colKey1 = ColumnKey.newBuilder().setIndexName("card1").addColName("card").addTsName("ts1").addTsName("ts2").build();
        ColumnKey colKey2 = ColumnKey.newBuilder().setIndexName("mcc").addColName("mcc").addTsName("ts2").build();
        TableInfo table = TableInfo.newBuilder()
                .setName(name).setTtl(14400)
                .addColumnDescV1(col0).addColumnDescV1(col1).addColumnDescV1(col2).addColumnDescV1(col3)
                .addColumnDescV1(col4).addColumnDescV1(col5)
                .addColumnKey(colKey1).addColumnKey(colKey2)
                .build();
        boolean ok = nsc.createTable(table);
        Assert.assertTrue(ok);
        nsc.dropTable(name);

        ColumnDesc col6 = ColumnDesc.newBuilder().setName("ts1").setAddTsIdx(false).setIsTsCol(true)
                .setTtl(14400).setType("int64").build();
        ColumnDesc col7 = ColumnDesc.newBuilder().setName("ts2").setAddTsIdx(false).setIsTsCol(true)
                .setTtl(14400).setType("timestamp").build();
        TableInfo table1 = TableInfo.newBuilder()
                .setName(name).setTtl(14400)
                .addColumnDescV1(col0).addColumnDescV1(col1).addColumnDescV1(col2).addColumnDescV1(col3)
                .addColumnDescV1(col6).addColumnDescV1(col7)
                .addColumnKey(colKey1).addColumnKey(colKey2)
                .build();
        ok = nsc.createTable(table1);
        Assert.assertTrue(ok);
        nsc.dropTable(name);

        ColumnDesc col8 = ColumnDesc.newBuilder().setName("ts2").setAddTsIdx(false).setIsTsCol(true)
                .setTtl(157680000).setType("timestamp").build();
        TableInfo table2 = TableInfo.newBuilder()
                .setName(name).setTtl(14400)
                .addColumnDescV1(col0).addColumnDescV1(col1).addColumnDescV1(col2).addColumnDescV1(col3)
                .addColumnDescV1(col6).addColumnDescV1(col8)
                .addColumnKey(colKey1).addColumnKey(colKey2)
                .build();
        ok = nsc.createTable(table2);
        Assert.assertFalse(ok);

        ColumnDesc col9 = ColumnDesc.newBuilder().setName("ts2").setAddTsIdx(false).setIsTsCol(true)
                .setTtl(10000).setType("timestamp").build();
        TableInfo table3 = TableInfo.newBuilder()
                .setName(name).setTtl(10).setTtlType("kLatestTime")
                .addColumnDescV1(col0).addColumnDescV1(col1).addColumnDescV1(col2).addColumnDescV1(col3)
                .addColumnDescV1(col6).addColumnDescV1(col9)
                .addColumnKey(colKey1).addColumnKey(colKey2)
                .build();
        ok = nsc.createTable(table3);
        Assert.assertFalse(ok);

        ColumnDesc col10 = ColumnDesc.newBuilder().setName("ts2").setAddTsIdx(false).setIsTsCol(true)
                .setTtl(100).setType("timestamp").build();
        TableInfo table4 = TableInfo.newBuilder()
                .setName(name).setTtl(10).setTtlType("kLatestTime")
                .addColumnDescV1(col0).addColumnDescV1(col1).addColumnDescV1(col2).addColumnDescV1(col3)
                .addColumnDescV1(col4).addColumnDescV1(col10)
                .addColumnKey(colKey1).addColumnKey(colKey2)
                .build();
        ok = nsc.createTable(table4);
        Assert.assertTrue(ok);
        nsc.dropTable(name);
    }

    @Test
    public void testAddColumnKey() {
        String name = String.valueOf(id.incrementAndGet());
        nsc.dropTable(name);
        ColumnDesc col0 = ColumnDesc.newBuilder().setName("card").setAddTsIdx(true).setType("string").build();
        ColumnDesc col1 = ColumnDesc.newBuilder().setName("mcc").setAddTsIdx(true).setType("string").build();
        ColumnDesc col2 = ColumnDesc.newBuilder().setName("amt").setAddTsIdx(false).setType("double").build();
        ColumnDesc col3 = ColumnDesc.newBuilder().setName("col1").setAddTsIdx(false).setType("int64").build();
        TableInfo table = TableInfo.newBuilder()
                .setName(name).setTtl(14400)
                .addColumnDescV1(col0).addColumnDescV1(col1).addColumnDescV1(col2).addColumnDescV1(col3)
                .build();
        boolean ok = nsc.createTable(table);
        Assert.assertTrue(ok);
        List<TableInfo> tableInfo = nsc.showTable(name);
        Assert.assertEquals(tableInfo.size(), 1);
        Assert.assertEquals(tableInfo.get(0).getColumnKeyCount(), 2);
        Assert.assertEquals(tableInfo.get(0).getColumnKey(0).getIndexName(), "card");
        Assert.assertEquals(tableInfo.get(0).getColumnKey(0).getTsNameList().size(), 0);
        Assert.assertEquals(tableInfo.get(0).getColumnKey(1).getIndexName(), "mcc");
        nsc.dropTable(name);

        ColumnDesc col4 = ColumnDesc.newBuilder().setName("col1").setAddTsIdx(false).setIsTsCol(true).setType("int64").build();
        table = TableInfo.newBuilder()
                .setName(name).setTtl(14400)
                .addColumnDescV1(col0).addColumnDescV1(col1).addColumnDescV1(col2).addColumnDescV1(col4)
                .build();
        ok = nsc.createTable(table);
        Assert.assertTrue(ok);
        tableInfo = nsc.showTable(name);
        Assert.assertEquals(tableInfo.size(), 1);
        Assert.assertEquals(tableInfo.get(0).getColumnKeyCount(), 2);
        Assert.assertEquals(tableInfo.get(0).getColumnKey(0).getIndexName(), "card");
        Assert.assertEquals(tableInfo.get(0).getColumnKey(0).getTsNameList().size(), 1);
        Assert.assertEquals(tableInfo.get(0).getColumnKey(0).getTsName(0), "col1");
        Assert.assertEquals(tableInfo.get(0).getColumnKey(1).getIndexName(), "mcc");
        nsc.dropTable(name);
    }
}
