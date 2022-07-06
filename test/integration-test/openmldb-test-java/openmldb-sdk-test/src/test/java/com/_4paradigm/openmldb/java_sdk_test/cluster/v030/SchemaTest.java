package com._4paradigm.openmldb.java_sdk_test.cluster.v030;

import com._4paradigm.openmldb.test_common.openmldb.OpenMLDBGlobalVar;
import com._4paradigm.openmldb.java_sdk_test.common.FedbTest;
import com._4paradigm.openmldb.test_common.util.TypeUtil;
import com._4paradigm.openmldb.test_common.util.SDKUtil;
import com._4paradigm.openmldb.sdk.Column;
import com._4paradigm.openmldb.sdk.Schema;
import io.qameta.allure.Feature;
import io.qameta.allure.Story;
import lombok.extern.slf4j.Slf4j;
import org.testng.Assert;
import org.testng.collections.Lists;

import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Feature("SchemaTest")
public class SchemaTest extends FedbTest {
    @Story("schema-sdk")
    // @Test
    public void testHaveIndexAndOption() throws SQLException {
        boolean dbOk = executor.createDB(OpenMLDBGlobalVar.dbName);
        log.info("create db:{},{}", OpenMLDBGlobalVar.dbName, dbOk);
        String tableName = "test_schema1";
        String createSql = "create table "+tableName+"(\n" +
                "c1 string,\n" +
                "c2 int not null,\n" +
                "c3 bigint,\n" +
                "c4 smallint,\n" +
                "c5 float,\n" +
                "c6 double not null,\n" +
                "c7 timestamp not null,\n" +
                "c8 date,\n" +
                "c9 bool not null,\n" +
                "index(key=(c1),ts=c7,ttl=10,ttl_type=latest))options(partitionnum=8,replicanum=3);";
        SDKUtil.sql(executor, OpenMLDBGlobalVar.dbName,createSql);
        Schema tableSchema = executor.getTableSchema(OpenMLDBGlobalVar.dbName, tableName);
        List<Column> columnList = tableSchema.getColumnList();
        List<String> actualList = columnList.stream()
                .map(column -> String.format("%s %s %s",
                        column.getColumnName(),
                        TypeUtil.fromJDBCTypeToString(column.getSqlType()),
                        column.isNotNull() ? "not null" : "").trim())
                .collect(Collectors.toList());
        List<String> expectList = Lists.newArrayList("c1 string","c2 int not null","c3 bigint","c4 smallint",
                "c5 float","c6 double not null","c7 timestamp not null","c8 date","c9 bool not null");
        Assert.assertEquals(actualList,expectList);
        String deleteSql = "drop table "+tableName+";";
        SDKUtil.sql(executor, OpenMLDBGlobalVar.dbName,deleteSql);
    }
}
