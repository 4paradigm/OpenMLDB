package com._4paradigm.fesql_auto_test.entity;

import com._4paradigm.fesql.sqlcase.model.ExpectDesc;
import com._4paradigm.fesql.sqlcase.model.InputDesc;
import com._4paradigm.fesql.sqlcase.model.SQLCase;
import com._4paradigm.fesql_auto_test.checker.ResultChecker;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.FileNotFoundException;
import java.text.ParseException;
import java.util.List;

public class FesqlDataProviderTest {
    @Test
    public void getRtidbDirTest() {
        Assert.assertNotNull(FesqlDataProvider.rtidbDir());
        Assert.assertEquals("rtidb", FesqlDataProvider.rtidbDir().getName());
    }

    @Test
    public void getDataProviderTest() throws FileNotFoundException {
        FesqlDataProvider provider = FesqlDataProvider.dataProviderGenerator("/yaml/rtidb_demo.yaml");
        Assert.assertNotNull(provider);
        Assert.assertEquals(3, provider.getCases().length);

        SQLCase sqlCase = provider.getCases()[0];
        Assert.assertEquals(2, sqlCase.getInputs().size());
    }

    @Test
    public void getInsertTest() throws FileNotFoundException {
        FesqlDataProvider provider = FesqlDataProvider.dataProviderGenerator("/yaml/rtidb_demo2.yaml");
        Assert.assertNotNull(provider);
        Assert.assertEquals(1, provider.getCases().length);
        SQLCase sqlCase = provider.getCases()[0];
        Assert.assertEquals(2, sqlCase.getInputs().size());

        InputDesc input = sqlCase.getInputs().get(0);
        Assert.assertEquals(input.getInsert(), "insert into " + input.getName() + " values\n" +
                "('aa',2,3,1590738989000L),\n" +
                "(null,null,null,1590738990000L);");
    }

    @Test
    public void getCreateTest() throws FileNotFoundException {
        FesqlDataProvider provider = FesqlDataProvider.dataProviderGenerator("/yaml/rtidb_demo2.yaml");
        Assert.assertNotNull(provider);
        Assert.assertEquals(1, provider.getCases().length);
        SQLCase sqlCase = provider.getCases()[0];
        Assert.assertEquals(2, sqlCase.getInputs().size());

        InputDesc input = sqlCase.getInputs().get(0);
        Assert.assertEquals("create table " + input.getName() + "(\n" +
                "c1 string,\n" +
                "c2 int,\n" +
                "c3 bigint,\n" +
                "c4 timestamp,\n" +
                "index(key=(c1),ts=c4));", input.getCreate());
    }


    @Test
    public void converRowsTest() throws ParseException, FileNotFoundException {
        FesqlDataProvider provider = FesqlDataProvider.dataProviderGenerator("/yaml/rtidb_demo.yaml");
        Assert.assertNotNull(provider);
        Assert.assertEquals(3, provider.getCases().length);
        SQLCase sqlCase = provider.getCases()[0];
        Assert.assertEquals(2, sqlCase.getInputs().size());
        List<List<Object>> expect = ResultChecker.convertRows(sqlCase.getExpect().getRows(),
                sqlCase.getExpect().getColumns());
    }
}
