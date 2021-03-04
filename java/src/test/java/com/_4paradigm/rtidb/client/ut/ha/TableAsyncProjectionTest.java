package com._4paradigm.rtidb.client.ut.ha;

import com._4paradigm.rtidb.client.*;
import com._4paradigm.rtidb.client.base.TestCaseBase;
import com._4paradigm.rtidb.common.Common;
import com._4paradigm.rtidb.ns.NS.TableInfo;
import com._4paradigm.rtidb.type.Type;
import org.apache.commons.collections4.Put;
import org.joda.time.DateTime;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.sql.Date;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class TableAsyncProjectionTest extends TestCaseBase {
    private static AtomicInteger id = new AtomicInteger(21000);
    @BeforeClass
    public void setUp() {
        super.setUp();
    }

    @AfterClass
    public void tearDown() {
        super.tearDown();
    }
    class TestArgs {
        TableInfo tableInfo;
        ArrayList<String> projectionList;
        Object[] row;
        Object[] expected;
        String key;
        long ts;

        @Override
        public String toString() {
            return "TestArgs{" +
                    "tableInfo=" + tableInfo +
                    ", projectionList=" + projectionList +
                    ", row=" + Arrays.toString(row) +
                    ", expected=" + Arrays.toString(expected) +
                    ", key='" + key + '\'' +
                    ", ts=" + ts +
                    '}';
        }
    };



    private TableAsyncProjectionTest.TestArgs createArg(Object[] input,
                                                       ArrayList<String> projectList, Object[] expect, String key, long ts,
                                                       int formatVersion)  {
        String name = String.valueOf(id.incrementAndGet());
        TableInfo.Builder tbuilder = TableInfo.newBuilder();
        Common.ColumnDesc col0 = Common.ColumnDesc.newBuilder().setName("card").setType("string").build();
        Common.ColumnDesc col1 = Common.ColumnDesc.newBuilder().setName("mcc").setType("string").build();
        Common.ColumnDesc col2 = Common.ColumnDesc.newBuilder().setName("ts").setType("int64").setIsTsCol(true).build();
        Common.ColumnDesc col3 = Common.ColumnDesc.newBuilder().setName("date").setType("date").build();
        Common.ColumnDesc col4 = Common.ColumnDesc.newBuilder().setName("time").setType("timestamp").build();
        Common.ColumnDesc col5 = Common.ColumnDesc.newBuilder().setName("amt").setType("double").build();
        Common.ColumnDesc col6 = Common.ColumnDesc.newBuilder().setName("amt2").setType("float").build();
        tbuilder.addColumnDescV1(col0);
        tbuilder.addColumnDescV1(col1);
        tbuilder.addColumnDescV1(col2);
        tbuilder.addColumnDescV1(col3);
        tbuilder.addColumnDescV1(col4);
        tbuilder.addColumnDescV1(col5);
        tbuilder.addColumnDescV1(col6);
        Common.ColumnKey ck = Common.ColumnKey.newBuilder().setIndexName("card").addColName("card").addTsName("ts").build();
        tbuilder.addColumnKey(ck);
        tbuilder.setFormatVersion(formatVersion);
        tbuilder.setName(name);
        tbuilder.setSegCnt(8);
        tbuilder.setTableType(Type.TableType.kTimeSeries);
        TableInfo table = tbuilder.build();
        TableAsyncProjectionTest.TestArgs args = new TableAsyncProjectionTest.TestArgs();
        args.tableInfo = table;
        args.row = input;
        args.expected = expect;
        args.projectionList = projectList;
        args.key = key;
        args.ts = ts;
        return args;
    }

    @DataProvider(name="projection_case")
    public Object[][] genCase() {
        Date now = new Date(System.currentTimeMillis());
        DateTime dt = new DateTime();
        return new Object[][] {
                // the legacy format
                new Object[]{createArg(new Object[] {"card0", "mcc0", 10000l, new Date(now.getYear(), now.getMonth(), now.getDate()), dt, 64.0d, 32.0f}, new ArrayList<String>(Arrays.asList("card")), new Object[]{"card0"},
                        "card0", 10000l, 0)},
                new Object[]{createArg(new Object[] {"card1", null, 10000l, new Date(now.getYear(), now.getMonth(), now.getDate()), dt, 64.0d, 32.0f}, new ArrayList<String>(Arrays.asList("mcc")), new Object[]{null},
                        "card1", 10000l, 0 )},
                new Object[]{createArg(new Object[] {"card2", null, 10000l, new Date(now.getYear(), now.getMonth(), now.getDate()), dt, 64.0d, 32.0f}, new ArrayList<String>(Arrays.asList("ts")), new Object[]{10000l},
                        "card2", 10000l, 0)},
                new Object[]{createArg(new Object[] {"card3", null, 10000l, new Date(now.getYear(), now.getMonth(), now.getDate()), dt, 64.0d, 32.0f}, new ArrayList<String>(Arrays.asList("date")), new Object[]{new Date(now.getYear(), now.getMonth(), now.getDate())},
                        "card3", 10000l, 0)},
                new Object[]{createArg(new Object[] {"card3", null, 10000l, new Date(now.getYear(), now.getMonth(), now.getDate()), dt, 64.0d, 32.0f}, new ArrayList<String>(Arrays.asList("time")), new Object[]{dt},
                        "card3", 10000l, 0)},
                new Object[]{createArg(new Object[] {"card4", "mcc0", 10000l, new Date(now.getYear(), now.getMonth(), now.getDate()), dt, 64.0d, 32.0f}, new ArrayList<String>(Arrays.asList("mcc", "card", "mcc")), new Object[]{"mcc0", "card4", "mcc0"},
                        "card4", 10000l, 0)},
                new Object[]{createArg(new Object[] {"card5", "mcc0", 10000l, new Date(now.getYear(), now.getMonth(), now.getDate()), dt, 64.0d, 32.0f}, new ArrayList<String>(Arrays.asList("date", "card")), new Object[]{new Date(now.getYear(), now.getMonth(), now.getDate()), "card5"},
                        "card5", 10000l, 0 )},

                // the new format
                new Object[]{createArg(new Object[] {"card0", "mcc0", 10000l, new Date(now.getYear(), now.getMonth(), now.getDate()), dt, 64.0d, 32.0f}, new ArrayList<String>(),new Object[] {"card0", "mcc0", 10000l, new Date(now.getYear(), now.getMonth(), now.getDate()), dt, 64.0d, 32.0f},
                        "card0", 10000l, 1)},
                new Object[]{createArg(new Object[] {"card0", "mcc0", 10000l, new Date(now.getYear(), now.getMonth(), now.getDate()), dt, 64.0d, 32.0f}, new ArrayList<String>(Arrays.asList("card")), new Object[]{"card0"},
                        "card0", 10000l, 1)},
                new Object[]{createArg(new Object[] {"card1", null, 10000l, new Date(now.getYear(), now.getMonth(), now.getDate()), dt, 64.0d, 32.0f}, new ArrayList<String>(Arrays.asList("mcc")), new Object[]{null},
                        "card1", 10000l, 1)},
                new Object[]{createArg(new Object[] {"card2", null, 10000l, new Date(now.getYear(), now.getMonth(), now.getDate()), dt, 64.0d, 32.0f}, new ArrayList<String>(Arrays.asList("ts")), new Object[]{10000l},
                        "card2", 10000l, 1)},
                new Object[]{createArg(new Object[] {"card3", null, 10000l, new Date(now.getYear(), now.getMonth(), now.getDate()), dt, 64.0d, 32.0f}, new ArrayList<String>(Arrays.asList("date")), new Object[]{new Date(now.getYear(), now.getMonth(), now.getDate())},
                        "card3", 10000l, 1)},
                new Object[]{createArg(new Object[] {"card3", null, 10000l, new Date(now.getYear(), now.getMonth(), now.getDate()), dt, 64.0d, 32.0f}, new ArrayList<String>(Arrays.asList("time")), new Object[]{dt},
                        "card3", 10000l, 1)},
                new Object[]{createArg(new Object[] {"card3", null, 10000l, new Date(now.getYear(), now.getMonth(), now.getDate()), dt, 64.0d, 32.0f}, new ArrayList<String>(Arrays.asList("amt")), new Object[]{64.0d},
                        "card3", 10000l, 1)},
                new Object[]{createArg(new Object[] {"card3", null, 10000l, new Date(now.getYear(), now.getMonth(), now.getDate()), dt, 64.0d, 32.0f}, new ArrayList<String>(Arrays.asList("amt2")), new Object[]{32.0f},
                        "card3", 10000l, 1)},
                new Object[]{createArg(new Object[] {"card4", "mcc0", 10000l, new Date(now.getYear(), now.getMonth(), now.getDate()), dt, 64.0d, 32.0f}, new ArrayList<String>(Arrays.asList("mcc", "card", "mcc")), new Object[]{"mcc0", "card4", "mcc0"},
                        "card4", 10000l, 1)},
                new Object[]{createArg(new Object[] {"card5", "mcc0", 10000l, new Date(now.getYear(), now.getMonth(), now.getDate()), dt, 64.0d, 32.0f}, new ArrayList<String>(Arrays.asList("date", "card")), new Object[]{new Date(now.getYear(), now.getMonth(), now.getDate()), "card5"},
                        "card5", 10000l, 1)}

        };
    }
    @Test(dataProvider = "projection_case")
    public void testScanCase(TestArgs args) throws Exception{
        nsc.dropTable(args.tableInfo.getName());
        boolean ok = nsc.createTable(args.tableInfo);
        Assert.assertTrue(ok);
        List<TableInfo> tables = nsc.showTable(args.tableInfo.getName());
        client.refreshRouteTable();
        PutFuture pf = tableAsyncClient.put(args.tableInfo.getName(), args.row);
        Assert.assertTrue(pf.get());
        // make sure put ok
        GetOption getOption = new GetOption();
        GetFuture getFuture = tableAsyncClient.get(args.tableInfo.getName(), args.key, args.ts, getOption);
        Assert.assertEquals(args.row, getFuture.getRow());
        ScanOption option = new ScanOption();
        option.setProjection(args.projectionList);
        ScanFuture sf = tableAsyncClient.scan(args.tableInfo.getName(), args.key, args.ts, 0l, option);
        KvIterator it = sf.get();
        Assert.assertEquals(1, it.getCount());
        Assert.assertTrue(it.valid());
        Assert.assertEquals(args.expected, it.getDecodedValue());
        sf = tableAsyncClient.scan(args.tableInfo.getName(), args.key, args.ts, 0l, option);
        it = sf.get(1000, TimeUnit.MILLISECONDS);
        Assert.assertEquals(1, it.getCount());
        Assert.assertTrue(it.valid());
        Assert.assertEquals(args.expected, it.getDecodedValue());
    }


    @Test(dataProvider = "projection_case")
    public void testGetCase(TestArgs args) throws Exception{
        nsc.dropTable(args.tableInfo.getName());
        boolean ok = nsc.createTable(args.tableInfo);
        Assert.assertTrue(ok);
        List<TableInfo> tables = nsc.showTable(args.tableInfo.getName());
        client.refreshRouteTable();
        PutFuture pf = tableAsyncClient.put(args.tableInfo.getName(), args.row);
        Assert.assertTrue(pf.get());
        // make sure put ok
        GetOption getOption = new GetOption();
        GetFuture getFuture = tableAsyncClient.get(args.tableInfo.getName(), args.key, args.ts, getOption);
        Assert.assertEquals(args.row, getFuture.getRow());

        GetOption option = new GetOption();
        option.setProjection(args.projectionList);
        GetFuture gf = tableAsyncClient.get(args.tableInfo.getName(), args.key, args.ts, option);
        Assert.assertEquals(args.expected, gf.getRow());
    }
}
