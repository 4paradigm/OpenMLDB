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

import io.confluent.connect.jdbc.util.QuoteMethod;

import static org.junit.Assert.assertEquals;

public class VerticaDatabaseDialectTest extends BaseDialectTest<VerticaDatabaseDialect> {

  @Override
  protected VerticaDatabaseDialect createDialect() {
    return new VerticaDatabaseDialect(sourceConfigWithUrl("jdbc:vertica://something"));
  }


  @Test
  public void shouldMapPrimitiveSchemaTypeToSqlTypes() {
    assertPrimitiveMapping(Type.INT8, "INT");
    assertPrimitiveMapping(Type.INT16, "INT");
    assertPrimitiveMapping(Type.INT32, "INT");
    assertPrimitiveMapping(Type.INT64, "INT");
    assertPrimitiveMapping(Type.FLOAT32, "FLOAT");
    assertPrimitiveMapping(Type.FLOAT64, "FLOAT");
    assertPrimitiveMapping(Type.BOOLEAN, "BOOLEAN");
    assertPrimitiveMapping(Type.BYTES, "VARBINARY(1024)");
    assertPrimitiveMapping(Type.STRING, "VARCHAR(1024)");
  }

  @Test
  public void shouldMapDecimalSchemaTypeToDecimalSqlType() {
    assertDecimalMapping(0, "DECIMAL(18,0)");
    assertDecimalMapping(3, "DECIMAL(18,3)");
    assertDecimalMapping(4, "DECIMAL(18,4)");
    assertDecimalMapping(5, "DECIMAL(18,5)");
  }

  @Test
  public void shouldMapDataTypes() {
    verifyDataTypeMapping("INT", Schema.INT8_SCHEMA);
    verifyDataTypeMapping("INT", Schema.INT16_SCHEMA);
    verifyDataTypeMapping("INT", Schema.INT32_SCHEMA);
    verifyDataTypeMapping("INT", Schema.INT64_SCHEMA);
    verifyDataTypeMapping("FLOAT", Schema.FLOAT32_SCHEMA);
    verifyDataTypeMapping("FLOAT", Schema.FLOAT64_SCHEMA);
    verifyDataTypeMapping("BOOLEAN", Schema.BOOLEAN_SCHEMA);
    verifyDataTypeMapping("VARCHAR(1024)", Schema.STRING_SCHEMA);
    verifyDataTypeMapping("VARBINARY(1024)", Schema.BYTES_SCHEMA);
    verifyDataTypeMapping("DECIMAL(18,0)", Decimal.schema(0));
    verifyDataTypeMapping("DECIMAL(18,4)", Decimal.schema(4));
    verifyDataTypeMapping("DATE", Date.SCHEMA);
    verifyDataTypeMapping("TIME", Time.SCHEMA);
    verifyDataTypeMapping("TIMESTAMP", Timestamp.SCHEMA);
  }

  @Test
  public void shouldMapDateSchemaTypeToDateSqlType() {
    assertDateMapping("DATE");
  }

  @Test
  public void shouldMapTimeSchemaTypeToTimeSqlType() {
    assertTimeMapping("TIME");
  }

  @Test
  public void shouldMapTimestampSchemaTypeToTimestampSqlType() {
    assertTimestampMapping("TIMESTAMP");
  }

  @Test
  public void shouldBuildCreateQueryStatement() {
    assertEquals(
        "CREATE TABLE \"myTable\" (\n"
        + "\"c1\" INT NOT NULL,\n"
        + "\"c2\" INT NOT NULL,\n"
        + "\"c3\" VARCHAR(1024) NOT NULL,\n"
        + "\"c4\" VARCHAR(1024) NULL,\n"
        + "\"c5\" DATE DEFAULT '2001-03-15',\n"
        + "\"c6\" TIME DEFAULT '00:00:00.000',\n"
        + "\"c7\" TIMESTAMP DEFAULT '2001-03-15 00:00:00.000',\n"
        + "\"c8\" DECIMAL(18,4) NULL,\n"
        + "\"c9\" BOOLEAN DEFAULT 1,\n"
        + "PRIMARY KEY(\"c1\"))",
        dialect.buildCreateTableStatement(tableId, sinkRecordFields)
    );

    quoteIdentfiiers = QuoteMethod.NEVER;
    dialect = createDialect();
    assertEquals(
        "CREATE TABLE myTable (\n"
        + "c1 INT NOT NULL,\n"
        + "c2 INT NOT NULL,\n"
        + "c3 VARCHAR(1024) NOT NULL,\n"
        + "c4 VARCHAR(1024) NULL,\n"
        + "c5 DATE DEFAULT '2001-03-15',\n"
        + "c6 TIME DEFAULT '00:00:00.000',\n"
        + "c7 TIMESTAMP DEFAULT '2001-03-15 00:00:00.000',\n"
        + "c8 DECIMAL(18,4) NULL,\n"
        + "c9 BOOLEAN DEFAULT 1,\n"
        + "PRIMARY KEY(c1))",
        dialect.buildCreateTableStatement(tableId, sinkRecordFields)
    );
  }

  @Test
  public void shouldBuildAlterTableStatement() {
    assertStatements(
        new String[]{
            "ALTER TABLE \"myTable\" ADD \"c1\" INT NOT NULL",
            "ALTER TABLE \"myTable\" ADD \"c2\" INT NOT NULL",
            "ALTER TABLE \"myTable\" ADD \"c3\" VARCHAR(1024) NOT NULL",
            "ALTER TABLE \"myTable\" ADD \"c4\" VARCHAR(1024) NULL",
            "ALTER TABLE \"myTable\" ADD \"c5\" DATE DEFAULT '2001-03-15'",
            "ALTER TABLE \"myTable\" ADD \"c6\" TIME DEFAULT '00:00:00.000'",
            "ALTER TABLE \"myTable\" ADD \"c7\" TIMESTAMP DEFAULT '2001-03-15 00:00:00.000'",
            "ALTER TABLE \"myTable\" ADD \"c8\" DECIMAL(18,4) NULL",
            "ALTER TABLE \"myTable\" ADD \"c9\" BOOLEAN DEFAULT 1"
        },
        dialect.buildAlterTable(tableId, sinkRecordFields)
    );

    quoteIdentfiiers = QuoteMethod.NEVER;
    dialect = createDialect();
    assertStatements(
        new String[]{
            "ALTER TABLE myTable ADD c1 INT NOT NULL",
            "ALTER TABLE myTable ADD c2 INT NOT NULL",
            "ALTER TABLE myTable ADD c3 VARCHAR(1024) NOT NULL",
            "ALTER TABLE myTable ADD c4 VARCHAR(1024) NULL",
            "ALTER TABLE myTable ADD c5 DATE DEFAULT '2001-03-15'",
            "ALTER TABLE myTable ADD c6 TIME DEFAULT '00:00:00.000'",
            "ALTER TABLE myTable ADD c7 TIMESTAMP DEFAULT '2001-03-15 00:00:00.000'",
            "ALTER TABLE myTable ADD c8 DECIMAL(18,4) NULL",
            "ALTER TABLE myTable ADD c9 BOOLEAN DEFAULT 1"
        },
        dialect.buildAlterTable(tableId, sinkRecordFields)
    );
  }

  @Test(expected = UnsupportedOperationException.class)
  public void shouldBuildUpsertStatement() {
    dialect.buildUpsertQueryStatement(tableId, pkColumns, columnsAtoD);
  }


  @Test
  public void createOneColNoPk() {
    verifyCreateOneColNoPk(
        "CREATE TABLE \"myTable\" (" + System.lineSeparator() + "\"col1\" INT NOT NULL)");
  }

  @Test
  public void createOneColOnePk() {
    verifyCreateOneColOnePk(
        "CREATE TABLE \"myTable\" (" + System.lineSeparator() + "\"pk1\" INT NOT NULL," +
        System.lineSeparator() + "PRIMARY KEY(\"pk1\"))");
  }

  @Test
  public void createThreeColTwoPk() {
    verifyCreateThreeColTwoPk(
        "CREATE TABLE \"myTable\" (" + System.lineSeparator() + "\"pk1\" INT NOT NULL," +
        System.lineSeparator() + "\"pk2\" INT NOT NULL," + System.lineSeparator() +
        "\"col1\" INT NOT NULL," + System.lineSeparator() + "PRIMARY KEY(\"pk1\",\"pk2\"))");

    quoteIdentfiiers = QuoteMethod.NEVER;
    dialect = createDialect();
    verifyCreateThreeColTwoPk(
        "CREATE TABLE myTable (" + System.lineSeparator() + "pk1 INT NOT NULL," +
        System.lineSeparator() + "pk2 INT NOT NULL," + System.lineSeparator() +
        "col1 INT NOT NULL," + System.lineSeparator() + "PRIMARY KEY(pk1,pk2))");
  }

  @Test
  public void alterAddOneCol() {
    verifyAlterAddOneCol("ALTER TABLE \"myTable\" ADD \"newcol1\" INT NULL");
  }

  @Test
  public void alterAddTwoCol() {
    verifyAlterAddTwoCols("ALTER TABLE \"myTable\" ADD \"newcol1\" INT NULL",
                          "ALTER TABLE \"myTable\" ADD \"newcol2\" INT DEFAULT 42");
  }

  @Test
  public void shouldSanitizeUrlWithoutCredentialsInProperties() {
    assertSanitizedUrl(
        "jdbc:vertica://something?key1=value1&key2=value2&key3=value3&&other=value",
        "jdbc:vertica://something?key1=value1&key2=value2&key3=value3&&other=value"

    );
  }

  @Test
  public void shouldSanitizeUrlWithCredentialsInUrlProperties() {
    assertSanitizedUrl(
        "jdbc:vertica://something?password=secret&key1=value1&key2=value2&key3=value3&"
        + "user=smith&password=secret&other=value",
        "jdbc:vertica://something?password=****&key1=value1&key2=value2&key3=value3&"
        + "user=smith&password=****&other=value"
    );
  }
}