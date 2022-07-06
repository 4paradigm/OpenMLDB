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

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;

import io.confluent.connect.jdbc.util.ColumnId;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.Test;

import io.confluent.connect.jdbc.util.QuoteMethod;
import io.confluent.connect.jdbc.util.TableId;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;

public class SqlServerDatabaseDialectTest extends BaseDialectTest<SqlServerDatabaseDialect> {

  public class MockSqlServerDatabaseDialect extends SqlServerDatabaseDialect {
    public MockSqlServerDatabaseDialect() {
      super(sourceConfigWithUrl("jdbc:jtds:sqlsserver://something"));
    }
    @Override
    public boolean versionWithBreakingDatetimeChange() {
      return true;
    }
  }

  @Override
  protected SqlServerDatabaseDialect createDialect() {
    return new MockSqlServerDatabaseDialect();
  }

  @Test
  public void shouldConvertFromDateTimeOffset() {
    ZoneId utc = ZoneId.of("UTC");
    TimeZone timeZone = TimeZone.getTimeZone(utc.getId());

    String value = "2016-12-08 12:34:56.7850000 -07:00";
    java.sql.Timestamp ts = SqlServerDatabaseDialect.dateTimeOffsetFrom(value, timeZone);
    assertTimestamp(ZonedDateTime.of(2016, 12, 8, 19, 34, 56, 785000000, utc), ts);

    value = "2019-12-08 12:34:56.7850200 -00:00";
    ts = SqlServerDatabaseDialect.dateTimeOffsetFrom(value, timeZone);
    assertTimestamp(ZonedDateTime.of(2019, 12, 8, 12, 34, 56, 785020000, utc), ts);
  }

  @Test
  public void testCustomColumnConverters() {
    assertColumnConverter(SqlServerDatabaseDialect.DATETIMEOFFSET, null, Timestamp.SCHEMA, Timestamp.class);
  }

  protected void assertTimestamp(ZonedDateTime expected, java.sql.Timestamp actual) {
    ZonedDateTime zdt = ZonedDateTime.ofInstant(actual.toInstant(), ZoneId.of("UTC"));
    assertEquals(expected.getYear(), zdt.getYear());
    assertEquals(expected.getMonthValue(), zdt.getMonthValue());
    assertEquals(expected.getDayOfMonth(), zdt.getDayOfMonth());
    assertEquals(expected.getHour(), zdt.getHour());
    assertEquals(expected.getMinute(), zdt.getMinute());
    assertEquals(expected.getSecond(), zdt.getSecond());
    assertEquals(expected.getNano(), zdt.getNano());
  }

  @Test
  public void shouldMapPrimitiveSchemaTypeToSqlTypes() {
    assertPrimitiveMapping(Type.INT8, "tinyint");
    assertPrimitiveMapping(Type.INT16, "smallint");
    assertPrimitiveMapping(Type.INT32, "int");
    assertPrimitiveMapping(Type.INT64, "bigint");
    assertPrimitiveMapping(Type.FLOAT32, "real");
    assertPrimitiveMapping(Type.FLOAT64, "float");
    assertPrimitiveMapping(Type.BOOLEAN, "bit");
    assertPrimitiveMapping(Type.BYTES, "varbinary(max)");
    assertPrimitiveMapping(Type.STRING, "varchar(max)");
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
    verifyDataTypeMapping("tinyint", Schema.INT8_SCHEMA);
    verifyDataTypeMapping("smallint", Schema.INT16_SCHEMA);
    verifyDataTypeMapping("int", Schema.INT32_SCHEMA);
    verifyDataTypeMapping("bigint", Schema.INT64_SCHEMA);
    verifyDataTypeMapping("real", Schema.FLOAT32_SCHEMA);
    verifyDataTypeMapping("float", Schema.FLOAT64_SCHEMA);
    verifyDataTypeMapping("bit", Schema.BOOLEAN_SCHEMA);
    verifyDataTypeMapping("varchar(max)", Schema.STRING_SCHEMA);
    verifyDataTypeMapping("varbinary(max)", Schema.BYTES_SCHEMA);
    verifyDataTypeMapping("decimal(38,0)", Decimal.schema(0));
    verifyDataTypeMapping("decimal(38,4)", Decimal.schema(4));
    verifyDataTypeMapping("date", Date.SCHEMA);
    verifyDataTypeMapping("time", Time.SCHEMA);
    verifyDataTypeMapping("datetime2", Timestamp.SCHEMA);
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
    assertTimestampMapping("datetime2");
  }

  @Test
  public void shouldBuildCreateQueryStatement() {
    assertEquals(
        "CREATE TABLE [myTable] (\n"
        + "[c1] int NOT NULL,\n"
        + "[c2] bigint NOT NULL,\n"
        + "[c3] varchar(max) NOT NULL,\n"
        + "[c4] varchar(max) NULL,\n"
        + "[c5] date DEFAULT '2001-03-15',\n"
        + "[c6] time DEFAULT '00:00:00.000',\n"
        + "[c7] datetime2 DEFAULT '2001-03-15 00:00:00.000',\n"
        + "[c8] decimal(38,4) NULL,\n"
        + "[c9] bit DEFAULT 1,\n" +
        "PRIMARY KEY([c1]))",
        dialect.buildCreateTableStatement(tableId, sinkRecordFields)
    );

    quoteIdentfiiers = QuoteMethod.NEVER;
    dialect = createDialect();
    assertEquals(
        "CREATE TABLE myTable (\n"
        + "c1 int NOT NULL,\n"
        + "c2 bigint NOT NULL,\n"
        + "c3 varchar(max) NOT NULL,\n"
        + "c4 varchar(max) NULL,\n"
        + "c5 date DEFAULT '2001-03-15',\n"
        + "c6 time DEFAULT '00:00:00.000',\n"
        + "c7 datetime2 DEFAULT '2001-03-15 00:00:00.000',\n"
        + "c8 decimal(38,4) NULL,\n"
        + "c9 bit DEFAULT 1,\n" +
        "PRIMARY KEY(c1))",
        dialect.buildCreateTableStatement(tableId, sinkRecordFields)
    );
  }

  @Test
  public void shouldBuildAlterTableStatement() {
    assertStatements(
        new String[]{
            "ALTER TABLE [myTable] ADD\n"
            + "[c1] int NOT NULL,\n"
            + "[c2] bigint NOT NULL,\n"
            + "[c3] varchar(max) NOT NULL,\n"
            + "[c4] varchar(max) NULL,\n"
            + "[c5] date DEFAULT '2001-03-15',\n"
            + "[c6] time DEFAULT '00:00:00.000',\n"
            + "[c7] datetime2 DEFAULT '2001-03-15 00:00:00.000',\n"
            + "[c8] decimal(38,4) NULL,\n"
            + "[c9] bit DEFAULT 1"
        },
        dialect.buildAlterTable(tableId, sinkRecordFields)
    );

    quoteIdentfiiers = QuoteMethod.NEVER;
    dialect = createDialect();
    assertStatements(
        new String[]{
            "ALTER TABLE myTable ADD\n"
            + "c1 int NOT NULL,\n"
            + "c2 bigint NOT NULL,\n"
            + "c3 varchar(max) NOT NULL,\n"
            + "c4 varchar(max) NULL,\n"
            + "c5 date DEFAULT '2001-03-15',\n"
            + "c6 time DEFAULT '00:00:00.000',\n"
            + "c7 datetime2 DEFAULT '2001-03-15 00:00:00.000',\n"
            + "c8 decimal(38,4) NULL,\n"
            + "c9 bit DEFAULT 1"
        },
        dialect.buildAlterTable(tableId, sinkRecordFields)
    );
  }

  @Test
  public void shouldBuildUpsertStatement() {
    assertEquals(
        "merge into [myTable] with (HOLDLOCK) AS target using (select ? AS [id1], ?" +
        " AS [id2], ? AS [columnA], ? AS [columnB], ? AS [columnC], ? AS [columnD])" +
        " AS incoming on (target.[id1]=incoming.[id1] and target.[id2]=incoming" +
        ".[id2]) when matched then update set [columnA]=incoming.[columnA]," +
        "[columnB]=incoming.[columnB],[columnC]=incoming.[columnC]," +
        "[columnD]=incoming.[columnD] when not matched then insert ([columnA], " +
        "[columnB], [columnC], [columnD], [id1], [id2]) values (incoming.[columnA]," +
        "incoming.[columnB],incoming.[columnC],incoming.[columnD],incoming.[id1]," +
        "incoming.[id2]);",
        dialect.buildUpsertQueryStatement(tableId, pkColumns, columnsAtoD)
    );

    quoteIdentfiiers = QuoteMethod.NEVER;
    dialect = createDialect();
    assertEquals(
        "merge into myTable with (HOLDLOCK) AS target using (select ? AS id1, ?" +
        " AS id2, ? AS columnA, ? AS columnB, ? AS columnC, ? AS columnD)" +
        " AS incoming on (target.id1=incoming.id1 and target.id2=incoming" +
        ".id2) when matched then update set columnA=incoming.columnA," +
        "columnB=incoming.columnB,columnC=incoming.columnC," +
        "columnD=incoming.columnD when not matched then insert (columnA, " +
        "columnB, columnC, columnD, id1, id2) values (incoming.columnA," +
        "incoming.columnB,incoming.columnC,incoming.columnD,incoming.id1," +
        "incoming.id2);",
        dialect.buildUpsertQueryStatement(tableId, pkColumns, columnsAtoD)
    );
  }

  @Test
  public void createOneColNoPk() {
    verifyCreateOneColNoPk(
        "CREATE TABLE [myTable] (" + System.lineSeparator() + "[col1] int NOT NULL)");
  }

  @Test
  public void createOneColOnePk() {
    verifyCreateOneColOnePk(
        "CREATE TABLE [myTable] (" + System.lineSeparator() + "[pk1] int NOT NULL," +
        System.lineSeparator() + "PRIMARY KEY([pk1]))");
  }

  @Test
  public void createOneColOnePkInString() {
    verifyCreateOneColOnePkAsString(
        "CREATE TABLE [myTable] (" + System.lineSeparator() + "[pk1] varchar(900) NOT NULL," +
          System.lineSeparator() + "PRIMARY KEY([pk1]))");
  }

  @Test
  public void createThreeColTwoPk() {
    verifyCreateThreeColTwoPk(
        "CREATE TABLE [myTable] (" + System.lineSeparator() + "[pk1] int NOT NULL," +
        System.lineSeparator() + "[pk2] int NOT NULL," + System.lineSeparator() +
        "[col1] int NOT NULL," + System.lineSeparator() + "PRIMARY KEY([pk1],[pk2]))");
  }

  @Test
  public void alterAddOneCol() {
    verifyAlterAddOneCol(
        "ALTER TABLE [myTable] ADD" + System.lineSeparator() + "[newcol1] int NULL");
  }

  @Test
  public void alterAddTwoCol() {
    verifyAlterAddTwoCols(
        "ALTER TABLE [myTable] ADD" + System.lineSeparator() + "[newcol1] int NULL," +
        System.lineSeparator() + "[newcol2] int DEFAULT 42");
  }

  @Test
  public void upsert1() {
    TableId customer = tableId("Customer");
    assertEquals(
        "merge into [Customer] with (HOLDLOCK) AS target using (select ? AS [id], ? AS [name], ? " +
        "AS [salary], ? AS [address]) AS incoming on (target.[id]=incoming.[id]) when matched then update set " +
        "[name]=incoming.[name],[salary]=incoming.[salary],[address]=incoming" +
        ".[address] when not matched then insert " +
        "([name], [salary], [address], [id]) values (incoming.[name],incoming" +
        ".[salary],incoming.[address],incoming.[id]);",
        dialect.buildUpsertQueryStatement(
            customer,
            columns(customer, "id"),
            columns(customer, "name", "salary", "address")
        )
    );

    quoteIdentfiiers = QuoteMethod.NEVER;
    dialect = createDialect();
    assertEquals(
        "merge into Customer with (HOLDLOCK) AS target using (select ? AS id, ? AS name, ? " +
        "AS salary, ? AS address) AS incoming on (target.id=incoming.id) when matched then update set " +
        "name=incoming.name,salary=incoming.salary,address=incoming" +
        ".address when not matched then insert " +
        "(name, salary, address, id) values (incoming.name,incoming" +
        ".salary,incoming.address,incoming.id);",
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
        "merge into [Book] with (HOLDLOCK) AS target using (select ? AS [author], ? AS [title], ?" +
        " AS [ISBN], ? AS [year], ? AS [pages])" +
        " AS incoming on (target.[author]=incoming.[author] and target.[title]=incoming.[title])" +
        " when matched then update set [ISBN]=incoming.[ISBN],[year]=incoming.[year]," +
        "[pages]=incoming.[pages] when not " +
        "matched then insert ([ISBN], [year], [pages], [author], [title]) values (incoming" +
        ".[ISBN],incoming.[year]," + "incoming.[pages],incoming.[author],incoming.[title]);",
        dialect.buildUpsertQueryStatement(
            book,
            columns(book, "author", "title"),
            columns(book, "ISBN", "year", "pages")
        )
    );

    quoteIdentfiiers = QuoteMethod.NEVER;
    dialect = createDialect();
    assertEquals(
        "merge into Book with (HOLDLOCK) AS target using (select ? AS author, ? AS title, ?" +
        " AS ISBN, ? AS year, ? AS pages)" +
        " AS incoming on (target.author=incoming.author and target.title=incoming.title)" +
        " when matched then update set ISBN=incoming.ISBN,year=incoming.year," +
        "pages=incoming.pages when not " +
        "matched then insert (ISBN, year, pages, author, title) values (incoming" +
        ".ISBN,incoming.year," + "incoming.pages,incoming.author,incoming.title);",
        dialect.buildUpsertQueryStatement(
            book,
            columns(book, "author", "title"),
            columns(book, "ISBN", "year", "pages")
        )
    );
  }

  @Test(expected=ConnectException.class)
  public void shouldFailDatetimeColumnAsTimeStampColumn() throws SQLException, ConnectException {
    String timeStampColumnName = "start_time";
    List<ColumnId> timestampColumns = new ArrayList<>();
    timestampColumns.add(new ColumnId(tableId, timeStampColumnName));
    ResultSetMetaData spyRsMetadata = Mockito.spy(ResultSetMetaData.class);
    Mockito.doReturn(1).when(spyRsMetadata).getColumnCount();

    Mockito.doReturn(timeStampColumnName).when(spyRsMetadata).getColumnName(1);
    Mockito.doReturn("datetime").when(spyRsMetadata).getColumnTypeName(1);

    dialect.validateSpecificColumnTypes(spyRsMetadata, timestampColumns);
  }

  @Test
  public void shouldNotFailDatetimeColumnAsRegularColumn() throws SQLException, ConnectException {
    String timeStampColumnName = "start_time";
    String regularColumnName = "datetime_as_regular";

    List<ColumnId> timestampColumns = new ArrayList<>();
    timestampColumns.add(new ColumnId(tableId, timeStampColumnName));
    ResultSetMetaData spyRsMetadata = Mockito.spy(ResultSetMetaData.class);
    Mockito.doReturn(2).when(spyRsMetadata).getColumnCount();

    Mockito.doReturn(regularColumnName).when(spyRsMetadata).getColumnName(1);
    Mockito.doReturn("datetime").when(spyRsMetadata).getColumnTypeName(1);

    Mockito.doReturn(timeStampColumnName).when(spyRsMetadata).getColumnName(2);
    Mockito.doReturn("datetime2").when(spyRsMetadata).getColumnTypeName(2);

    dialect.validateSpecificColumnTypes(spyRsMetadata, timestampColumns);
  }


  @Test
  public void shouldSanitizeUrlWithoutCredentialsInProperties() {
    assertSanitizedUrl(
        "jdbc:sqlserver://;servername=server_name;"
        + "integratedSecurity=true;authenticationScheme=JavaKerberos",
        "jdbc:sqlserver://;servername=server_name;"
        + "integratedSecurity=true;authenticationScheme=JavaKerberos"
    );
  }

  @Test
  public void shouldSanitizeUrlWithCredentialsInUrlProperties() {
    assertSanitizedUrl(
        "jdbc:sqlserver://;servername=server_name;password=secret;keyStoreSecret=secret;"
        + "gsscredential=secret;integratedSecurity=true;authenticationScheme=JavaKerberos",
        "jdbc:sqlserver://;servername=server_name;password=****;keyStoreSecret=****;"
        + "gsscredential=****;integratedSecurity=true;authenticationScheme=JavaKerberos"
    );
    assertSanitizedUrl(
        "jdbc:sqlserver://;password=secret;servername=server_name;keyStoreSecret=secret;"
        + "gsscredential=secret;integratedSecurity=true;authenticationScheme=JavaKerberos",
        "jdbc:sqlserver://;password=****;servername=server_name;keyStoreSecret=****;"
        + "gsscredential=****;integratedSecurity=true;authenticationScheme=JavaKerberos"
    );
  }
}