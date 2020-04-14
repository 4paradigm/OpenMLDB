package com._4paradigm.rtidb.client.ut.ha;

import com._4paradigm.rtidb.client.GetOption;
import com._4paradigm.rtidb.client.KvIterator;
import com._4paradigm.rtidb.client.ScanOption;
import com._4paradigm.rtidb.client.base.TestCaseBase;
import com._4paradigm.rtidb.common.Common;
import com._4paradigm.rtidb.ns.NS.TableInfo;
import com._4paradigm.rtidb.type.Type;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.sql.Date;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class TableSyncDateTest extends TestCaseBase {
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
        Object[] row;
        Object[] expected;
        String key;
        long ts;
    };


    private TestArgs createArg(Object[] input, Object[] expect, String key, long ts)  {
        String name = String.valueOf(id.incrementAndGet());
        TableInfo.Builder tbuilder = TableInfo.newBuilder();
        Common.ColumnDesc col0 = Common.ColumnDesc.newBuilder().setName("card").setDataType(Type.DataType.kVarchar).setType("string").build();
        Common.ColumnDesc col1 = Common.ColumnDesc.newBuilder().setName("mcc").setNotNull(false).setDataType(Type.DataType.kVarchar).setType("string").build();
        Common.ColumnDesc col2 = Common.ColumnDesc.newBuilder().setName("ts").setDataType(Type.DataType.kBigInt).setType("int64").setIsTsCol(true).build();
        Common.ColumnDesc col3 = Common.ColumnDesc.newBuilder().setName("date").setNotNull(false).setDataType(Type.DataType.kDate).setType("date").build();
        tbuilder.addColumnDescV1(col0);
        tbuilder.addColumnDescV1(col1);
        tbuilder.addColumnDescV1(col2);
        tbuilder.addColumnDescV1(col3);
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
        args.key = key;
        args.ts = ts;
        return args;
    }

    @DataProvider(name="date_case")
    public Object[][] genCase() {
        Date now = new Date(System.currentTimeMillis());
        return new Object[][] {
                new Object[]{createArg(new Object[] {"card0", "mcc0", 10000l, new Date(now.getYear(), now.getMonth(), now.getDate())},
                new Object[] {"card0", "mcc0", 10000l, new Date(now.getYear(), now.getMonth(), now.getDate())},
                        "card0", 10000l)},
                new Object[] {createArg(new Object[] {"card1", "mcc0", 10000l, null},
                                new Object[] {"card1", "mcc0", 10000l, null},
                                "card1", 10000l)}
        };
    }

    @Test(dataProvider = "date_case")
    public void testDateCase(TestArgs args) throws Exception{
        nsc.dropTable(args.tableInfo.getName());
        boolean ok = nsc.createTable(args.tableInfo);
        Assert.assertTrue(ok);
        List<TableInfo> tables = nsc.showTable(args.tableInfo.getName());
        client.refreshRouteTable();
        ok = tableSyncClient.put(args.tableInfo.getName(), args.row);
        Assert.assertTrue(ok);
        GetOption option = new GetOption();
        Object[] row = tableSyncClient.getRow(args.tableInfo.getName(), args.key, args.ts, option);
        Assert.assertEquals(row, args.expected);
    }

}
