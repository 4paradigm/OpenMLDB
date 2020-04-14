package com._4paradigm.rtidb.client.ut.ha;

import com._4paradigm.rtidb.client.*;
import com._4paradigm.rtidb.client.base.TestCaseBase;
import com._4paradigm.rtidb.common.Common;
import com._4paradigm.rtidb.ns.NS.TableInfo;
import com._4paradigm.rtidb.type.Type;
import org.apache.commons.collections4.Put;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class TableAsyncProjectionTest extends TestCaseBase {
    private static AtomicInteger id = new AtomicInteger(20000);
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
    };



    private TestArgs createArg(Object[] input, ArrayList<String> projectList, Object[] expect, String key, long ts)  {
        String name = String.valueOf(id.incrementAndGet());
        TableInfo.Builder tbuilder = TableInfo.newBuilder();
        Common.ColumnDesc col0 = Common.ColumnDesc.newBuilder().setName("card").setDataType(Type.DataType.kVarchar).setType("string").build();
        Common.ColumnDesc col1 = Common.ColumnDesc.newBuilder().setName("mcc").setNotNull(false).setDataType(Type.DataType.kVarchar).setType("string").build();
        Common.ColumnDesc col2 = Common.ColumnDesc.newBuilder().setName("ts").setDataType(Type.DataType.kBigInt).setType("int64").setIsTsCol(true).build();
        tbuilder.addColumnDescV1(col0);
        tbuilder.addColumnDescV1(col1);
        tbuilder.addColumnDescV1(col2);
        Common.ColumnKey ck = Common.ColumnKey.newBuilder().setIndexName("card").addColName("card").addTsName("ts").build();
        tbuilder.addColumnKey(ck);
        tbuilder.setFormatVersion(1);
        tbuilder.setName(name);
        tbuilder.setSegCnt(8);
        tbuilder.setTableType(Type.TableType.kTimeSeries);
        TableInfo table = tbuilder.build();
        TestArgs args = new TestArgs();
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
        return new Object[][] {
                new Object[]{createArg(new Object[] {"card0", "mcc0", 10000l}, new ArrayList<String>(Arrays.asList("card")), new Object[]{"card0"},
                        "card0", 10000l)},
                new Object[]{createArg(new Object[] {"card1", null, 10000l}, new ArrayList<String>(Arrays.asList("mcc")), new Object[]{null},
                                "card1", 10000l)}
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
