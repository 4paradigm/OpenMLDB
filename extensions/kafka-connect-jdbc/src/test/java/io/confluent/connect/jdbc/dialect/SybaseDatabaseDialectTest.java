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

import static org.junit.Assert.assertEquals;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.time.ZoneOffset;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.ThreadLocalRandom;

import io.confluent.connect.jdbc.util.DateTimeUtils;
import io.confluent.connect.jdbc.util.QuoteMethod;
import io.confluent.connect.jdbc.util.TableId;

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.junit.Test;

public class SybaseDatabaseDialectTest extends BaseDialectTest<SybaseDatabaseDialect> {

  @Override
  protected SybaseDatabaseDialect createDialect() {
    return new SybaseDatabaseDialect(sourceConfigWithUrl("jdbc:jtds:sybase://something"));
  }


  @Test
  public void shouldMapPrimitiveSchemaTypeToSqlTypes() {
    assertPrimitiveMapping(Type.INT8, "smallint");
    assertPrimitiveMapping(Type.INT16, "smallint");
    assertPrimitiveMapping(Type.INT32, "int");
    assertPrimitiveMapping(Type.INT64, "bigint");
    assertPrimitiveMapping(Type.FLOAT32, "real");
    assertPrimitiveMapping(Type.FLOAT64, "float");
    assertPrimitiveMapping(Type.BOOLEAN, "bit");
    assertPrimitiveMapping(Type.BYTES, "image");
    assertPrimitiveMapping(Type.STRING, "text");
  }

  @Override
  @Test
  public void bindFieldByteValue() throws SQLException {
    int index = ThreadLocalRandom.current().nextInt();
    verifyBindField(++index, Schema.INT8_SCHEMA, (byte) 42).setShort(index, (byte) 42);
  }

  @Test
  public void shouldMapDecimalSchemaTypeToDecimalSqlType() {
    assertDecimalMapping(0, "decimal(38,0)");
    assertDecimalMapping(3, "decimal(38,3)");
    assertDecimalMapping(4, "decimal(38,4)");
    assertDecimalMapping(5, "decimal(38,5)");
  }

  @Test
  public void shouldMapDataTypes() {
    verifyDataTypeMapping("smallint", Schema.INT8_SCHEMA);
    verifyDataTypeMapping("smallint", Schema.INT16_SCHEMA);
    verifyDataTypeMapping("int", Schema.INT32_SCHEMA);
    verifyDataTypeMapping("bigint", Schema.INT64_SCHEMA);
    verifyDataTypeMapping("real", Schema.FLOAT32_SCHEMA);
    verifyDataTypeMapping("float", Schema.FLOAT64_SCHEMA);
    verifyDataTypeMapping("bit", Schema.BOOLEAN_SCHEMA);
    verifyDataTypeMapping("text", Schema.STRING_SCHEMA);
    verifyDataTypeMapping("image", Schema.BYTES_SCHEMA);
    verifyDataTypeMapping("decimal(38,0)", Decimal.schema(0));
    verifyDataTypeMapping("decimal(38,4)", Decimal.schema(4));
    verifyDataTypeMapping("date", Date.SCHEMA);
    verifyDataTypeMapping("time", Time.SCHEMA);
    verifyDataTypeMapping("datetime", Timestamp.SCHEMA);
  }

  @Test
  public void shouldMapDateSchemaTypeToDateSqlType() {
    assertDateMapping("date");
  }

  @Test
  public void shouldMapTimeSchemaTypeToTimeSqlType() {
    assertTimeMapping("time");
  }

  @Test
  public void shouldMapTimestampSchemaTypeToTimestampSqlType() {
    assertTimestampMapping("datetime");
  }

  @Test
  public void shouldBuildCreateTableStatement() {
    assertEquals(
        "CREATE TABLE \"myTable\" (\n"
        + "\"c1\" int NOT NULL,\n"
        + "\"c2\" bigint NOT NULL,\n"
        +"\"c3\" text NOT NULL,\n"
        + "\"c4\" text NULL,\n"
        + "\"c5\" date DEFAULT '2001-03-15',\n"
        + "\"c6\" time DEFAULT '00:00:00.000',\n"
        + "\"c7\" datetime DEFAULT '2001-03-15 00:00:00.000',\n"
        + "\"c8\" decimal(38,4) NULL,\n"
        + "\"c9\" bit DEFAULT 1,\n" +
        "PRIMARY KEY(\"c1\"))",
        dialect.buildCreateTableStatement(tableId, sinkRecordFields)
    );

    quoteIdentfiiers = QuoteMethod.NEVER;
    dialect = createDialect();
    assertEquals(
        "CREATE TABLE myTable (\n"
        + "c1 int NOT NULL,\n"
        + "c2 bigint NOT NULL,\n"
        + "c3 text NOT NULL,\n"
        + "c4 text NULL,\n"
        + "c5 date DEFAULT '2001-03-15',\n"
        + "c6 time DEFAULT '00:00:00.000',\n"
        + "c7 datetime DEFAULT '2001-03-15 00:00:00.000',\n"
        + "c8 decimal(38,4) NULL,\n"
        + "c9 bit DEFAULT 1,\n" +
        "PRIMARY KEY(c1))",
        dialect.buildCreateTableStatement(tableId, sinkRecordFields)
    );
  }

  @Test
  public void shouldBuildDropTableStatement() {
    String expected = "DROP TABLE \"myTable\"";
    String sql = dialect.buildDropTableStatement(tableId, new DropOptions().setIfExists(false));
    assertEquals(expected, sql);
  }

  @Test
  public void shouldBuildDropTableStatementWithIfExistsClause() {
    String expected = "IF EXISTS (SELECT 1 FROM sysobjects WHERE name='myTable' AND type='U') "
                      + "DROP TABLE \"myTable\"";
    String sql = dialect.buildDropTableStatement(tableId, new DropOptions().setIfExists(true));
    assertEquals(expected, sql);
  }

  @Test
  public void shouldBuildDropTableStatementWithIfExistsClauseAndSchemaNameInTableId() {
    tableId = new TableId("dbName","dbo", "myTable");
    assertEquals(
        "IF EXISTS (SELECT 1 FROM sysobjects INNER JOIN sysusers ON sysobjects.uid"
        + "=sysusers.uid WHERE sysusers.name='dbo' AND sysobjects.name='myTable'"
        + " AND type='U') DROP TABLE \"dbName\".\"dbo\".\"myTable\"",
        dialect.buildDropTableStatement(tableId, new DropOptions().setIfExists(true))
    );

    quoteIdentfiiers = QuoteMethod.NEVER;
    dialect = createDialect();
    assertEquals(
        "IF EXISTS (SELECT 1 FROM sysobjects INNER JOIN sysusers ON sysobjects.uid"
        + "=sysusers.uid WHERE sysusers.name='dbo' AND sysobjects.name='myTable'"
        + " AND type='U') DROP TABLE dbName.dbo.myTable",
        dialect.buildDropTableStatement(tableId, new DropOptions().setIfExists(true))
    );
  }

  @Test
  public void shouldBuildAlterTableStatement() {
    List<String> statements = dialect.buildAlterTable(tableId, sinkRecordFields);
    String[] sql = {
        "ALTER TABLE \"myTable\" ADD\n" + "\"c1\" int NOT NULL,\n" + "\"c2\" bigint NOT NULL,\n" +
        "\"c3\" text NOT NULL,\n" + "\"c4\" text NULL,\n" +
        "\"c5\" date DEFAULT '2001-03-15',\n" + "\"c6\" time DEFAULT '00:00:00.000',\n" +
        "\"c7\" datetime DEFAULT '2001-03-15 00:00:00.000',\n" + "\"c8\" decimal(38,4) NULL,\n" +
        "\"c9\" bit DEFAULT 1"};
    assertStatements(sql, statements);
  }

  @Test
  public void shouldBuildUpsertStatement() {
    assertEquals(
        "merge into \"myTable\" AS target using (select ? AS \"id1\", ?" +
        " AS \"id2\", ? AS \"columnA\", ? AS \"columnB\", ? AS \"columnC\", ? AS \"columnD\")" +
        " AS incoming on (target.\"id1\"=incoming.\"id1\" and target.\"id2\"=incoming" +
        ".\"id2\") when matched then update set \"columnA\"=incoming.\"columnA\"," +
        "\"columnB\"=incoming.\"columnB\",\"columnC\"=incoming.\"columnC\"," +
        "\"columnD\"=incoming.\"columnD\" when not matched then insert (\"columnA\", " +
        "\"columnB\", \"columnC\", \"columnD\", \"id1\", \"id2\") values (incoming.\"columnA\"," +
        "incoming.\"columnB\",incoming.\"columnC\",incoming.\"columnD\",incoming.\"id1\"," +
        "incoming.\"id2\")",
        dialect.buildUpsertQueryStatement(tableId, pkColumns, columnsAtoD)
    );

    quoteIdentfiiers = QuoteMethod.NEVER;
    dialect = createDialect();
    assertEquals(
        "merge into myTable AS target using (select ? AS id1, ?" +
        " AS id2, ? AS columnA, ? AS columnB, ? AS columnC, ? AS columnD)" +
        " AS incoming on (target.id1=incoming.id1 and target.id2=incoming" +
        ".id2) when matched then update set columnA=incoming.columnA," +
        "columnB=incoming.columnB,columnC=incoming.columnC," +
        "columnD=incoming.columnD when not matched then insert (columnA, " +
        "columnB, columnC, columnD, id1, id2) values (incoming.columnA," +
        "incoming.columnB,incoming.columnC,incoming.columnD,incoming.id1," +
        "incoming.id2)",
        dialect.buildUpsertQueryStatement(tableId, pkColumns, columnsAtoD)
    );
  }

  @Test
  public void createOneColNoPk() {
    verifyCreateOneColNoPk(
        "CREATE TABLE \"myTable\" (" + System.lineSeparator() + "\"col1\" int NOT NULL)");
  }

  @Test
  public void createOneColOnePk() {
    verifyCreateOneColOnePk(
        "CREATE TABLE \"myTable\" (" + System.lineSeparator() + "\"pk1\" int NOT NULL," +
        System.lineSeparator() + "PRIMARY KEY(\"pk1\"))");
  }

  @Test
  public void createThreeColTwoPk() {
    verifyCreateThreeColTwoPk(
        "CREATE TABLE \"myTable\" (" + System.lineSeparator() + "\"pk1\" int NOT NULL," +
        System.lineSeparator() + "\"pk2\" int NOT NULL," + System.lineSeparator() +
        "\"col1\" int NOT NULL," + System.lineSeparator() + "PRIMARY KEY(\"pk1\",\"pk2\"))");

    quoteIdentfiiers = QuoteMethod.NEVER;
    dialect = createDialect();
    verifyCreateThreeColTwoPk(
        "CREATE TABLE myTable (" + System.lineSeparator() + "pk1 int NOT NULL," +
        System.lineSeparator() + "pk2 int NOT NULL," + System.lineSeparator() +
        "col1 int NOT NULL," + System.lineSeparator() + "PRIMARY KEY(pk1,pk2))");
  }

  @Test
  public void alterAddOneCol() {
    verifyAlterAddOneCol(
        "ALTER TABLE \"myTable\" ADD" + System.lineSeparator() + "\"newcol1\" int NULL");
  }

  @Test
  public void alterAddTwoCol() {
    verifyAlterAddTwoCols(
        "ALTER TABLE \"myTable\" ADD" + System.lineSeparator() + "\"newcol1\" int NULL," +
        System.lineSeparator() + "\"newcol2\" int DEFAULT 42");
  }

  @Test
  public void upsert1() {
    TableId customer = tableId("Customer");
    assertEquals(
        "merge into \"Customer\" AS target using (select ? AS \"id\", ? AS \"name\", ? " +
        "AS \"salary\", ? AS \"address\") AS incoming on (target.\"id\"=incoming.\"id\") when matched then update set " +
        "\"name\"=incoming.\"name\",\"salary\"=incoming.\"salary\",\"address\"=incoming" +
        ".\"address\" when not matched then insert " +
        "(\"name\", \"salary\", \"address\", \"id\") values (incoming.\"name\",incoming" +
        ".\"salary\",incoming.\"address\",incoming.\"id\")",
        dialect.buildUpsertQueryStatement(
            customer,
            columns(customer, "id"),
            columns(customer, "name", "salary", "address")
        )
    );

    quoteIdentfiiers = QuoteMethod.NEVER;
    dialect = createDialect();
    assertEquals(
        "merge into Customer AS target using (select ? AS id, ? AS name, ? " +
        "AS salary, ? AS address) AS incoming on (target.id=incoming.id) when matched then update set " +
        "name=incoming.name,salary=incoming.salary,address=incoming" +
        ".address when not matched then insert " +
        "(name, salary, address, id) values (incoming.name,incoming" +
        ".salary,incoming.address,incoming.id)",
        dialect.buildUpsertQueryStatement(
            customer,
            columns(customer, "id"),
            columns(customer, "name", "salary", "address")
        )
    );
  }

  @Test
  public void upsert2() {
    TableId book = new TableId(null, null, "Book");
    assertEquals(
        "merge into \"Book\" AS target using (select ? AS \"author\", ? AS \"title\", ?" +
        " AS \"ISBN\", ? AS \"year\", ? AS \"pages\")" +
        " AS incoming on (target.\"author\"=incoming.\"author\" and target.\"title\"=incoming.\"title\")" +
        " when matched then update set \"ISBN\"=incoming.\"ISBN\",\"year\"=incoming.\"year\"," +
        "\"pages\"=incoming.\"pages\" when not " +
        "matched then insert (\"ISBN\", \"year\", \"pages\", \"author\", \"title\") values (incoming" +
        ".\"ISBN\",incoming.\"year\"," + "incoming.\"pages\",incoming.\"author\",incoming.\"title\")",
        dialect.buildUpsertQueryStatement(
            book,
            columns(book, "author", "title"),
            columns(book, "ISBN", "year", "pages")
        )
    );

    quoteIdentfiiers = QuoteMethod.NEVER;
    dialect = createDialect();
    assertEquals(
        "merge into Book AS target using (select ? AS author, ? AS title, ?" +
        " AS ISBN, ? AS year, ? AS pages)" +
        " AS incoming on (target.author=incoming.author and target.title=incoming.title)" +
        " when matched then update set ISBN=incoming.ISBN,year=incoming.year," +
        "pages=incoming.pages when not " +
        "matched then insert (ISBN, year, pages, author, title) values (incoming" +
        ".ISBN,incoming.year," + "incoming.pages,incoming.author,incoming.title)",
        dialect.buildUpsertQueryStatement(
            book,
            columns(book, "author", "title"),
            columns(book, "ISBN", "year", "pages")
        )
    );
  }

  @Test
  public void bindFieldPrimitiveValuesExceptString() throws SQLException {
    int index = ThreadLocalRandom.current().nextInt();
    verifyBindField(++index, Schema.INT8_SCHEMA, (short) 42).setShort(index, (short) 42);
    verifyBindField(++index, Schema.INT8_SCHEMA, (short) -42).setShort(index, (short) -42);
    verifyBindField(++index, Schema.INT16_SCHEMA, (short) 42).setShort(index, (short) 42);
    verifyBindField(++index, Schema.INT32_SCHEMA, 42).setInt(index, 42);
    verifyBindField(++index, Schema.INT64_SCHEMA, 42L).setLong(index, 42L);
    verifyBindField(++index, Schema.BOOLEAN_SCHEMA, false).setBoolean(index, false);
    verifyBindField(++index, Schema.BOOLEAN_SCHEMA, true).setBoolean(index, true);
    verifyBindField(++index, Schema.FLOAT32_SCHEMA, -42f).setFloat(index, -42f);
    verifyBindField(++index, Schema.FLOAT64_SCHEMA, 42d).setDouble(index, 42d);
    verifyBindField(++index, Schema.BYTES_SCHEMA, new byte[]{42}).setBytes(index, new byte[]{42});
    verifyBindField(++index, Schema.BYTES_SCHEMA, ByteBuffer.wrap(new byte[]{42})).setBytes(index, new byte[]{42});
    verifyBindField(++index, Schema.STRING_SCHEMA, "yep").setString(index, "yep");
    verifyBindField(++index, Decimal.schema(0), new BigDecimal("1.5").setScale(0, BigDecimal.ROUND_HALF_EVEN)).setBigDecimal(index, new BigDecimal(2));
    Calendar utcCalendar = DateTimeUtils.getTimeZoneCalendar(TimeZone.getTimeZone(ZoneOffset.UTC));
    verifyBindField(
      ++index,
      Date.SCHEMA,
      new java.util.Date(0)
    ).setDate(index, new java.sql.Date(0), utcCalendar);
    verifyBindField(
      ++index,
      Time.SCHEMA,
      new java.util.Date(1000)
    ).setTime(index, new java.sql.Time(1000), utcCalendar);
    verifyBindField(
      ++index,
      Timestamp.SCHEMA,
      new java.util.Date(100)
    ).setTimestamp(index, new java.sql.Timestamp(100), utcCalendar);
  }

  @Test
  public void shouldSanitizeUrlWithoutCredentialsInProperties() {
    assertSanitizedUrl(
        "jdbc:jtds:sybase://something?key1=value1&key2=value2&key3=value3&&other=value",
        "jdbc:jtds:sybase://something?key1=value1&key2=value2&key3=value3&&other=value"

    );
  }

  @Test
  public void shouldSanitizeUrlWithCredentialsInUrlProperties() {
    assertSanitizedUrl(
        "jdbc:jtds:sybase://something?password=secret&key1=value1&key2=value2&key3=value3&"
        + "user=smith&password=secret&other=value",
        "jdbc:jtds:sybase://something?password=****&key1=value1&key2=value2&key3=value3&"
        + "user=smith&password=****&other=value"
    );
  }
}
