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

import io.confluent.connect.jdbc.util.ColumnDefinition;
import io.confluent.connect.jdbc.util.DateTimeUtils;
import java.io.ByteArrayInputStream;
import java.io.StringReader;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.time.ZoneOffset;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.junit.Test;

import io.confluent.connect.jdbc.util.QuoteMethod;
import io.confluent.connect.jdbc.util.TableId;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class OracleDatabaseDialectTest extends BaseDialectTest<OracleDatabaseDialect> {

  @Override
  protected OracleDatabaseDialect createDialect() {
    return new OracleDatabaseDialect(sourceConfigWithUrl("jdbc:oracle:thin://something"));
  }

  @Override
  @Test
  public void bindFieldStringValue() throws SQLException {
    int index = ThreadLocalRandom.current().nextInt();
    verifyBindField(++index, Schema.STRING_SCHEMA, "yep").setCharacterStream(eq(index), any(StringReader.class));
  }

  @Override
  @Test
  public void bindFieldBytesValue() throws SQLException {
    int index = ThreadLocalRandom.current().nextInt();
    verifyBindField(++index, Schema.BYTES_SCHEMA, new byte[]{42}).setBlob(eq(index), any(ByteArrayInputStream.class));
    verifyBindField(++index, Schema.BYTES_SCHEMA, ByteBuffer.wrap(new byte[]{42})).setBlob(eq(index), any(ByteArrayInputStream.class));
  }

  @Test
  public void shouldMapPrimitiveSchemaTypeToSqlTypes() {
    assertPrimitiveMapping(Type.INT8, "NUMBER(3,0)");
    assertPrimitiveMapping(Type.INT16, "NUMBER(5,0)");
    assertPrimitiveMapping(Type.INT32, "NUMBER(10,0)");
    assertPrimitiveMapping(Type.INT64, "NUMBER(19,0)");
    assertPrimitiveMapping(Type.FLOAT32, "BINARY_FLOAT");
    assertPrimitiveMapping(Type.FLOAT64, "BINARY_DOUBLE");
    assertPrimitiveMapping(Type.BOOLEAN, "NUMBER(1,0)");
    assertPrimitiveMapping(Type.BYTES, "BLOB");
    assertPrimitiveMapping(Type.STRING, "CLOB");
  }

  @Test
  public void shouldMapDecimalSchemaTypeToDecimalSqlType() {
    assertDecimalMapping(0, "NUMBER(*,0)");
    assertDecimalMapping(3, "NUMBER(*,3)");
    assertDecimalMapping(4, "NUMBER(*,4)");
    assertDecimalMapping(5, "NUMBER(*,5)");
  }

  @Test
  public void shouldMapDataTypes() {
    verifyDataTypeMapping("NUMBER(3,0)", Schema.INT8_SCHEMA);
    verifyDataTypeMapping("NUMBER(5,0)", Schema.INT16_SCHEMA);
    verifyDataTypeMapping("NUMBER(10,0)", Schema.INT32_SCHEMA);
    verifyDataTypeMapping("NUMBER(19,0)", Schema.INT64_SCHEMA);
    verifyDataTypeMapping("BINARY_FLOAT", Schema.FLOAT32_SCHEMA);
    verifyDataTypeMapping("BINARY_DOUBLE", Schema.FLOAT64_SCHEMA);
    verifyDataTypeMapping("NUMBER(1,0)", Schema.BOOLEAN_SCHEMA);
    verifyDataTypeMapping("CLOB", Schema.STRING_SCHEMA);
    verifyDataTypeMapping("BLOB", Schema.BYTES_SCHEMA);
    verifyDataTypeMapping("NUMBER(*,0)", Decimal.schema(0));
    verifyDataTypeMapping("NUMBER(*,42)", Decimal.schema(42));
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
    String expected = "CREATE TABLE \"myTable\" (\n" + "\"c1\" NUMBER(10,0) NOT NULL,\n" +
                      "\"c2\" NUMBER(19,0) NOT NULL,\n" + "\"c3\" CLOB NOT NULL,\n" +
                      "\"c4\" CLOB NULL,\n" + "\"c5\" DATE DEFAULT '2001-03-15',\n" +
                      "\"c6\" DATE DEFAULT '00:00:00.000',\n" +
                      "\"c7\" TIMESTAMP DEFAULT '2001-03-15 00:00:00.000',\n" +
                      "\"c8\" NUMBER(*,4) NULL,\n" +
                      "\"c9\" NUMBER(1,0) DEFAULT 1,\n" +
                      "PRIMARY KEY(\"c1\"))";
    String sql = dialect.buildCreateTableStatement(tableId, sinkRecordFields);
    assertEquals(expected, sql);
  }

  @Test
  public void shouldBuildAlterTableStatement() {
    assertStatements(
        new String[]{
            "ALTER TABLE \"myTable\" ADD(\n" +
            "\"c1\" NUMBER(10,0) NOT NULL,\n" +
            "\"c2\" NUMBER(19,0) NOT NULL,\n" +
            "\"c3\" CLOB NOT NULL,\n" +
            "\"c4\" CLOB NULL,\n" +
            "\"c5\" DATE DEFAULT '2001-03-15',\n" +
            "\"c6\" DATE DEFAULT '00:00:00.000',\n" +
            "\"c7\" TIMESTAMP DEFAULT '2001-03-15 00:00:00.000',\n" +
            "\"c8\" NUMBER(*,4) NULL,\n" +
            "\"c9\" NUMBER(1,0) DEFAULT 1)"
        },
        dialect.buildAlterTable(tableId, sinkRecordFields)
    );

    quoteIdentfiiers = QuoteMethod.NEVER;
    dialect = createDialect();

    assertStatements(
        new String[]{
            "ALTER TABLE myTable ADD(\n" +
            "c1 NUMBER(10,0) NOT NULL,\n" +
            "c2 NUMBER(19,0) NOT NULL,\n" +
            "c3 CLOB NOT NULL,\n" +
            "c4 CLOB NULL,\n" +
            "c5 DATE DEFAULT '2001-03-15',\n" +
            "c6 DATE DEFAULT '00:00:00.000',\n" +
            "c7 TIMESTAMP DEFAULT '2001-03-15 00:00:00.000',\n" +
            "c8 NUMBER(*,4) NULL,\n" +
            "c9 NUMBER(1,0) DEFAULT 1)"
        },
        dialect.buildAlterTable(tableId, sinkRecordFields)
    );
  }

  @Test
  public void shouldBuildUpsertStatement() {
    String expected = "merge into \"myTable\" using (select ? \"id1\", ? \"id2\", ? \"columnA\", " +
                      "? \"columnB\", ? \"columnC\", ? \"columnD\" FROM dual) incoming on" +
                      "(\"myTable\".\"id1\"=incoming.\"id1\" and \"myTable\".\"id2\"=incoming" +
                      ".\"id2\") when matched then update set \"myTable\".\"columnA\"=incoming" +
                      ".\"columnA\",\"myTable\".\"columnB\"=incoming.\"columnB\",\"myTable\"" +
                      ".\"columnC\"=incoming.\"columnC\",\"myTable\".\"columnD\"=incoming" +
                      ".\"columnD\" when not matched then insert(\"myTable\".\"columnA\"," +
                      "\"myTable\".\"columnB\",\"myTable\".\"columnC\",\"myTable\".\"columnD\"," +
                      "\"myTable\".\"id1\",\"myTable\".\"id2\") values(incoming.\"columnA\"," +
                      "incoming.\"columnB\",incoming.\"columnC\",incoming.\"columnD\",incoming" +
                      ".\"id1\",incoming.\"id2\")";
    String sql = dialect.buildUpsertQueryStatement(tableId, pkColumns, columnsAtoD);
    assertEquals(expected, sql);
  }

  @Test
  public void createOneColNoPk() {
    verifyCreateOneColNoPk(
        "CREATE TABLE \"myTable\" (" + System.lineSeparator() + "\"col1\" NUMBER(10,0) NOT NULL)");
  }

  @Test
  public void createOneColOnePk() {
    verifyCreateOneColOnePk(
        "CREATE TABLE \"myTable\" (" + System.lineSeparator() + "\"pk1\" NUMBER(10,0) NOT NULL," +
        System.lineSeparator() + "PRIMARY KEY(\"pk1\"))");
  }

  @Test
  public void createThreeColTwoPk() {
    verifyCreateThreeColTwoPk(
        "CREATE TABLE \"myTable\" (" + System.lineSeparator() + "\"pk1\" NUMBER(10,0) NOT NULL," +
        System.lineSeparator() + "\"pk2\" NUMBER(10,0) NOT NULL," + System.lineSeparator() +
        "\"col1\" NUMBER(10,0) NOT NULL," + System.lineSeparator() +
        "PRIMARY KEY(\"pk1\",\"pk2\"))");

    quoteIdentfiiers = QuoteMethod.NEVER;
    dialect = createDialect();

    verifyCreateThreeColTwoPk(
        "CREATE TABLE myTable (" + System.lineSeparator() + "pk1 NUMBER(10,0) NOT NULL," +
        System.lineSeparator() + "pk2 NUMBER(10,0) NOT NULL," + System.lineSeparator() +
        "col1 NUMBER(10,0) NOT NULL," + System.lineSeparator() +
        "PRIMARY KEY(pk1,pk2))");
  }

  @Test
  public void alterAddOneCol() {
    verifyAlterAddOneCol(
        "ALTER TABLE \"myTable\" ADD(" + System.lineSeparator() + "\"newcol1\" NUMBER(10,0) NULL)");
  }

  @Test
  public void alterAddTwoCol() {
    verifyAlterAddTwoCols(
        "ALTER TABLE \"myTable\" ADD(" + System.lineSeparator() +
        "\"newcol1\" NUMBER(10,0) NULL," +
        System.lineSeparator() + "\"newcol2\" NUMBER(10,0) DEFAULT 42)");
  }

  @Test
  public void upsert() {
    TableId article = tableId("ARTICLE");
    String expected = "merge into \"ARTICLE\" " +
                      "using (select ? \"title\", ? \"author\", ? \"body\" FROM dual) incoming on" +
                      "(\"ARTICLE\".\"title\"=incoming.\"title\" and \"ARTICLE\"" +
                      ".\"author\"=incoming.\"author\") " +
                      "when matched then update set \"ARTICLE\".\"body\"=incoming.\"body\" " +
                      "when not matched then insert(\"ARTICLE\".\"body\",\"ARTICLE\".\"title\"," +
                      "\"ARTICLE\".\"author\") " +
                      "values(incoming.\"body\",incoming.\"title\",incoming.\"author\")";
    String actual = dialect.buildUpsertQueryStatement(article, columns(article, "title", "author"),
                                                      columns(article, "body"));
    assertEquals(expected, actual);
  }

  @Test
  public void shouldSanitizeUrlWithCredentialsInHosts() {
    assertSanitizedUrl(
        "jdbc:oracle:thin:sandy/secret@myhost:1111/db?key1=value1",
        "jdbc:oracle:thin:sandy/****@myhost:1111/db?key1=value1"
    );
    assertSanitizedUrl(
        "jdbc:oracle:oci8:sandy/secret@host=myhost1,port=1111/db?key1=value1",
        "jdbc:oracle:oci8:sandy/****@host=myhost1,port=1111/db?key1=value1"
    );
  }

  @Test
  public void shouldSanitizeUrlWithCredentialsInUrlProperties() {
    assertSanitizedUrl(
        "jdbc:oracle:thin:@myhost:1111/db?password=secret&key1=value1&"
        + "key2=value2&key3=value3&user=smith&password=secret&other=value",
        "jdbc:oracle:thin:@myhost:1111/db?password=****&key1=value1&"
        + "key2=value2&key3=value3&user=smith&password=****&other=value"
    );
  }

  @Test
  public void shouldSanitizeUrlWithKerberosCredentialsInUrlProperties() {
    assertSanitizedUrl(
        "jdbc:oracle:thin:@myhost:1111/db?"
            + "password=secret&"
            + "javax.net.ssl.keyStorePassword=secret2&"
            + "key1=value1&"
            + "key2=value2&"
            + "key3=value3&"
            + "user=smith&"
            + "password=secret&"
            + "passworNotSanitized=not-secret&"
            + "passwordShouldBeSanitized=value3&"
            + "javax.net.ssl.trustStorePassword=superSecret&"
            + "OCINewPassword=secret2&"
            + "oracle.net.wallet_password=secret3&"
            + "proxy_password=secret4&"
            + "PROXY_USER_PASSWORD=secret5&"
            + "other=value",
        "jdbc:oracle:thin:@myhost:1111/db?"
            + "password=****&"
            + "javax.net.ssl.keyStorePassword=****&"
            + "key1=value1&"
            + "key2=value2&"
            + "key3=value3&"
            + "user=smith&"
            + "password=****&"
            + "passworNotSanitized=not-secret&"
            + "passwordShouldBeSanitized=****&"
            + "javax.net.ssl.trustStorePassword=****&"
            + "OCINewPassword=****&"
            + "oracle.net.wallet_password=****&"
            + "proxy_password=****&"
            + "PROXY_USER_PASSWORD=****&"
            + "other=value"
    );
  }

  @Test
  public void shouldBindStringAccordingToColumnDef() throws SQLException {
    int index = ThreadLocalRandom.current().nextInt();
    String value = "random text";
    Schema schema = Schema.STRING_SCHEMA;
    PreparedStatement stmtVarchar = mock(PreparedStatement.class);
    ColumnDefinition colDefVarchar = mock(ColumnDefinition.class);
    when(colDefVarchar.type()).thenReturn(Types.VARCHAR);

    PreparedStatement stmtNchar = mock(PreparedStatement.class);
    ColumnDefinition colDefNchar = mock(ColumnDefinition.class);
    when(colDefNchar.type()).thenReturn(Types.NCHAR);

    PreparedStatement stmtNvarchar = mock(PreparedStatement.class);
    ColumnDefinition colDefNvarchar = mock(ColumnDefinition.class);
    when(colDefNvarchar.type()).thenReturn(Types.NVARCHAR);

    PreparedStatement stmtClob = mock(PreparedStatement.class);
    ColumnDefinition colDefClob = mock(ColumnDefinition.class);
    when(colDefClob.type()).thenReturn(Types.CLOB);

    dialect.bindField(stmtVarchar, index, schema, value, colDefVarchar);
    verify(stmtVarchar, times(1)).setString(index, value);

    dialect.bindField(stmtNchar, index, schema, value, colDefNchar);
    verify(stmtNchar, times(1)).setNString(index, value);

    dialect.bindField(stmtNvarchar, index, schema, value, colDefNvarchar);
    verify(stmtNvarchar, times(1)).setNString(index, value);

    dialect.bindField(stmtClob, index, schema, value, colDefClob);
    verify(stmtClob, times(1)).setCharacterStream(eq(index), any(StringReader.class));
  }

  @Test
  public void shouldBindBytesAccordingToColumnDef() throws SQLException {
    int index = ThreadLocalRandom.current().nextInt();
    byte[] value = new byte[]{42};
    Schema schema = Schema.BYTES_SCHEMA;
    PreparedStatement statement = mock(PreparedStatement.class);
    ColumnDefinition colDefBlob = mock(ColumnDefinition.class);
    when(colDefBlob.type()).thenReturn(Types.BLOB);
    ColumnDefinition colDefBinary = mock(ColumnDefinition.class);
    when(colDefBinary.type()).thenReturn(Types.BINARY);

    dialect.bindField(statement, index, schema, value, colDefBlob);
    verify(statement, times(1)).setBlob(eq(index), any(ByteArrayInputStream.class));
    dialect.bindField(statement, index, schema, value, colDefBinary);
    verify(statement, times(1)).setBytes(index, value);
  }
}