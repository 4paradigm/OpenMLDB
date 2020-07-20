package com._4paradigm.fesql.sqlcase.model;

import org.testng.Assert;
import org.testng.annotations.Test;

public class SQLCaseTest {
    @Test
    public void testSqlFormat() {
        String sql = "create table {0} (c1 int, c2 float, c3 double, c4 timestamp, index(key=(c1), ts=c4));";
        sql = SQLCase.formatSql(sql, 0, "auto_t1");
        Assert.assertEquals("create table auto_t1 (c1 int, c2 float, c3 double, c4 timestamp, index(key=(c1), ts=c4));", sql);
    }

    @Test
    public void testSqlFormatAuto() {
        String sql = "create table {auto} (c1 int, c2 float, c3 double, c4 timestamp, index(key=(c1), ts=c4));";
        sql = SQLCase.formatSql(sql, "auto_t1");
        Assert.assertEquals("create table auto_t1 (c1 int, c2 float, c3 double, c4 timestamp, index(key=(c1), ts=c4));", sql);
    }
}
