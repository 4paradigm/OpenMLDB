/*
 * Copyright 2021 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com._4paradigm.openmldb.test_common.case_test.sdk;


import com._4paradigm.openmldb.test_common.model.SQLCase;
import com._4paradigm.openmldb.test_common.model.Table;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.testng.collections.Lists;

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

    @Test
    public void testCreateBuilder() {
//        Assert.assertEquals(Table.buildCreateSQLFromColumnsIndexs("auto_t1",
//                Lists.newArrayList("c1 string", "c2 bigint", "c3 int", "c4 float",
//                "c5 timestamp"), Lists.newArrayList("index1:c1:c5", "index2:c1|c2:c5:365d",
//                        "index3:c1:c5:1000:absolute"), 1,1,null,"SSD"),
//                "create table auto_t1(\n" +
//                        "c1 string,\n" +
//                        "c2 bigint,\n" +
//                        "c3 int,\n" +
//                        "c4 float,\n" +
//                        "c5 timestamp,\n" +
//                        "index(key=(c1),ts=c5),\n" +
//                        "index(key=(c1,c2),ts=c5,ttl=365d),\n" +
//                        "index(key=(c1),ts=c5,ttl=1000,ttl_type=absolute)" +
//                        ");");

        String sql = Table.buildCreateSQLFromColumnsIndexs("auto_t1",
                Lists.newArrayList("c1 string", "c2 bigint", "c3 int", "c4 float",
                        "c5 timestamp"), Lists.newArrayList("index1:c1:c5"), 1, 1, null, null);
        System.out.println(sql);
    }
}
