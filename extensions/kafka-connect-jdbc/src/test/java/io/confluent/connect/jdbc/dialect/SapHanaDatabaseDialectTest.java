/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.jdbc.dialect;

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.junit.Test;

import java.util.List;

import io.confluent.connect.jdbc.util.QuoteMethod;
import io.confluent.connect.jdbc.util.TableId;

import static org.junit.Assert.assertEquals;

public class SapHanaDatabaseDialectTest extends BaseDialectTest<SapHanaDatabaseDialect> {

  @Override
  protected SapHanaDatabaseDialect createDialect() {
    return new SapHanaDatabaseDialect(sourceConfigWithUrl("jdbc:sap://something"));
  }

  @Test
  public void shouldMapPrimitiveSchemaTypeToSqlTypes() {
    assertPrimitiveMapping(Type.INT8, "TINYINT");
    assertPrimitiveMapping(Type.INT16, "SMALLINT");
    assertPrimitiveMapping(Type.INT32, "INTEGER");
    assertPrimitiveMapping(Type.INT64, "BIGINT");
    assertPrimitiveMapping(Type.FLOAT32, "REAL");
    assertPrimitiveMapping(Type.FLOAT64, "DOUBLE");
    assertPrimitiveMapping(Type.BOOLEAN, "BOOLEAN");
    assertPrimitiveMapping(Type.BYTES, "BLOB");
    assertPrimitiveMapping(Type.STRING, "VARCHAR(1000)");
  }

  @Test
  public void shouldMapDecimalSchemaTypeToDecimalSqlType() {
    assertDecimalMapping(0, "DECIMAL");
    assertDecimalMapping(3, "DECIMAL");
    assertDecimalMapping(4, "DECIMAL");
    assertDecimalMapping(5, "DECIMAL");
  }

  @Test
  public void shouldMapDataTypes() {
    verifyDataTypeMapping("TINYINT", Schema.INT8_SCHEMA);
    verifyDataTypeMapping("SMALLINT", Schema.INT16_SCHEMA);
    verifyDataTypeMapping("INTEGER", Schema.INT32_SCHEMA);
    verifyDataTypeMapping("BIGINT", Schema.INT64_SCHEMA);
    verifyDataTypeMapping("REAL", Schema.FLOAT32_SCHEMA);
    verifyDataTypeMapping("DOUBLE", Schema.FLOAT64_SCHEMA);
    verifyDataTypeMapping("BOOLEAN", Schema.BOOLEAN_SCHEMA);
    verifyDataTypeMapping("VARCHAR(1000)", Schema.STRING_SCHEMA);
    verifyDataTypeMapping("BLOB", Schema.BYTES_SCHEMA);
    verifyDataTypeMapping("DECIMAL", Decimal.schema(0));
    verifyDataTypeMapping("DATE", Date.SCHEMA);
    verifyDataTypeMapping("DATE", Time.SCHEMA);
    verifyDataTypeMapping("TIMESTAMP", Timestamp.SCHEMA);
  }

  @Test
  public void shouldMapDateSchemaTypeToDateSqlType() {
    assertDateMapping("DATE");
  }

  @Test
  public void shouldMapTimeSchemaTypeToTimeSqlType() {
    assertTimeMapping("DATE");
  }

  @Test
  public void shouldMapTimestampSchemaTypeToTimestampSqlType() {
    assertTimestampMapping("TIMESTAMP");
  }

  @Test
  public void shouldBuildCreateQueryStatement() {
    String expected =
        "CREATE COLUMN TABLE \"myTable\" (" + System.lineSeparator() + "\"c1\" INTEGER NOT NULL," +
        System.lineSeparator() + "\"c2\" BIGINT NOT NULL," + System.lineSeparator() +
        "\"c3\" VARCHAR(1000) NOT NULL," + System.lineSeparator() + "\"c4\" VARCHAR(1000) NULL," +
        System.lineSeparator() + "\"c5\" DATE DEFAULT '2001-03-15'," + System.lineSeparator() +
        "\"c6\" DATE DEFAULT '00:00:00.000'," + System.lineSeparator() +
        "\"c7\" TIMESTAMP DEFAULT '2001-03-15 00:00:00.000'," + System.lineSeparator() +
        "\"c8\" DECIMAL NULL," + System.lineSeparator() +
        "\"c9\" BOOLEAN DEFAULT 1," + System.lineSeparator() + "PRIMARY KEY(\"c1\"))";
    String sql = dialect.buildCreateTableStatement(tableId, sinkRecordFields);
    assertEquals(expected, sql);
  }
  
   @Test
  public void shouldReturnCurrentTimestampDatabaseQuery() {
     String expected = "SELECT CURRENT_TIMESTAMP FROM DUMMY";
     String sql = dialect.currentTimestampDatabaseQuery();
     assertEquals(expected, sql);
  }

  @Test
  public void shouldBuildAlterTableStatement() {
    List<String> statements = dialect.buildAlterTable(tableId, sinkRecordFields);
    String[] sql = {
        "ALTER TABLE \"myTable\" ADD(" + System.lineSeparator() + "\"c1\" INTEGER NOT NULL," +
        System.lineSeparator() + "\"c2\" BIGINT NOT NULL," + System.lineSeparator() +
        "\"c3\" VARCHAR(1000) NOT NULL," + System.lineSeparator() + "\"c4\" VARCHAR(1000) NULL," +
        System.lineSeparator() + "\"c5\" DATE DEFAULT '2001-03-15'," + System.lineSeparator() +
        "\"c6\" DATE DEFAULT '00:00:00.000'," + System.lineSeparator() +
        "\"c7\" TIMESTAMP DEFAULT '2001-03-15 00:00:00.000'," + System.lineSeparator() +
        "\"c8\" DECIMAL NULL," + System.lineSeparator() +
        "\"c9\" BOOLEAN DEFAULT 1)"};
    assertStatements(sql, statements);
  }

  @Test
  public void shouldBuildUpsertStatement() {
    assertEquals(
        "UPSERT \"myTable\""
        + "(\"id1\",\"id2\",\"columnA\",\"columnB\",\"columnC\",\"columnD\") "
        + "VALUES(?,?,?,?,?,?) "
        + "WITH PRIMARY KEY",
        dialect.buildUpsertQueryStatement(tableId, pkColumns, columnsAtoD)
    );

    quoteIdentfiiers = QuoteMethod.NEVER;
    dialect = createDialect();
    assertEquals(
        "UPSERT myTable"
        + "(id1,id2,columnA,columnB,columnC,columnD) "
        + "VALUES(?,?,?,?,?,?) "
        + "WITH PRIMARY KEY",
        dialect.buildUpsertQueryStatement(tableId, pkColumns, columnsAtoD)
    );
  }


  @Test
  public void createOneColNoPk() {
    verifyCreateOneColNoPk("CREATE COLUMN TABLE \"myTable\" (" + System.lineSeparator() +
                           "\"col1\" INTEGER NOT NULL)");
  }

  @Test
  public void createOneColOnePk() {
    verifyCreateOneColOnePk(
        "CREATE COLUMN TABLE \"myTable\" (" + System.lineSeparator() + "\"pk1\" INTEGER NOT NULL," +
        System.lineSeparator() + "PRIMARY KEY(\"pk1\"))");
  }

  @Test
  public void createThreeColTwoPk() {
    verifyCreateThreeColTwoPk(
        "CREATE COLUMN TABLE \"myTable\" (" + System.lineSeparator() + "\"pk1\" INTEGER NOT NULL," +
        System.lineSeparator() + "\"pk2\" INTEGER NOT NULL," + System.lineSeparator() +
        "\"col1\" INTEGER NOT NULL," + System.lineSeparator() + "PRIMARY KEY(\"pk1\",\"pk2\"))");

    quoteIdentfiiers = QuoteMethod.NEVER;
    dialect = createDialect();
    verifyCreateThreeColTwoPk(
        "CREATE COLUMN TABLE myTable (" + System.lineSeparator() + "pk1 INTEGER NOT NULL," +
        System.lineSeparator() + "pk2 INTEGER NOT NULL," + System.lineSeparator() +
        "col1 INTEGER NOT NULL," + System.lineSeparator() + "PRIMARY KEY(pk1,pk2))");
  }

  @Test
  public void alterAddOneCol() {
    verifyAlterAddOneCol(
        "ALTER TABLE \"myTable\" ADD(" + System.lineSeparator() + "\"newcol1\" INTEGER NULL)");
  }

  @Test
  public void alterAddTwoCol() {
    verifyAlterAddTwoCols(
        "ALTER TABLE \"myTable\" ADD(" + System.lineSeparator() + "\"newcol1\" INTEGER NULL," +
        System.lineSeparator() + "\"newcol2\" INTEGER DEFAULT 42)");
  }

  @Test
  public void upsert() {
    TableId tableA = tableId("tableA");
    assertEquals(
        "UPSERT \"tableA\"(\"col1\",\"col2\",\"col3\",\"col4\") VALUES(?,?,?,?) WITH PRIMARY KEY",
        dialect.buildUpsertQueryStatement(
            tableA,
            columns(tableA, "col1"),
            columns(tableA, "col2", "col3", "col4")
        )
    );

    quoteIdentfiiers = QuoteMethod.NEVER;
    dialect = createDialect();
    assertEquals(
        "UPSERT tableA(col1,col2,col3,col4) VALUES(?,?,?,?) WITH PRIMARY KEY",
        dialect.buildUpsertQueryStatement(
            tableA,
            columns(tableA, "col1"),
            columns(tableA, "col2", "col3", "col4")
        )
    );
  }

  @Test
  public void shouldSanitizeUrlWithoutCredentialsInProperties() {
    assertSanitizedUrl(
        "jdbc:sap://something?key1=value1&key2=value2&key3=value3&&other=value",
        "jdbc:sap://something?key1=value1&key2=value2&key3=value3&&other=value"

    );
  }

  @Test
  public void shouldSanitizeUrlWithCredentialsInUrlProperties() {
    assertSanitizedUrl(
        "jdbc:sap://something?password=secret&key1=value1&key2=value2&key3=value3&"
        + "user=smith&password=secret&other=value",
        "jdbc:sap://something?password=****&key1=value1&key2=value2&key3=value3&"
        + "user=smith&password=****&other=value"
    );
  }
}
