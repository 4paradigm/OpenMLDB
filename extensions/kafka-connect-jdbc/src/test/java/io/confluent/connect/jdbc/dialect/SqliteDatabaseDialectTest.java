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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.SQLException;
import java.sql.Types;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import io.confluent.connect.jdbc.sink.SqliteHelper;
import io.confluent.connect.jdbc.util.ColumnDefinition;
import io.confluent.connect.jdbc.util.DateTimeUtils;
import io.confluent.connect.jdbc.util.QuoteMethod;
import io.confluent.connect.jdbc.util.TableDefinition;
import io.confluent.connect.jdbc.util.TableId;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

public class SqliteDatabaseDialectTest extends BaseDialectTest<SqliteDatabaseDialect> {

  private final SqliteHelper sqliteHelper = new SqliteHelper(getClass().getSimpleName());

  @Before
  public void beforeEach() throws Exception {
    sqliteHelper.setUp();
  }

  @After
  public void afterEach() throws Exception {
    sqliteHelper.tearDown();
  }

  @Override
  protected SqliteDatabaseDialect createDialect() {
    return new SqliteDatabaseDialect(sourceConfigWithUrl("jdbc:sqlite://something"));
  }


  @Test
  public void shouldMapPrimitiveSchemaTypeToSqlTypes() {
    assertPrimitiveMapping(Type.INT8, "INTEGER");
    assertPrimitiveMapping(Type.INT16, "INTEGER");
    assertPrimitiveMapping(Type.INT32, "INTEGER");
    assertPrimitiveMapping(Type.INT64, "INTEGER");
    assertPrimitiveMapping(Type.FLOAT32, "REAL");
    assertPrimitiveMapping(Type.FLOAT64, "REAL");
    assertPrimitiveMapping(Type.BOOLEAN, "INTEGER");
    assertPrimitiveMapping(Type.BYTES, "BLOB");
    assertPrimitiveMapping(Type.STRING, "TEXT");
  }

  @Test
  public void shouldMapDecimalSchemaTypeToDecimalSqlType() {
    assertDecimalMapping(0, "NUMERIC");
    assertDecimalMapping(3, "NUMERIC");
    assertDecimalMapping(4, "NUMERIC");
    assertDecimalMapping(5, "NUMERIC");
  }

  @Test
  public void shouldMapDataTypes() {
    verifyDataTypeMapping("INTEGER", Schema.INT8_SCHEMA);
    verifyDataTypeMapping("INTEGER", Schema.INT16_SCHEMA);
    verifyDataTypeMapping("INTEGER", Schema.INT32_SCHEMA);
    verifyDataTypeMapping("INTEGER", Schema.INT64_SCHEMA);
    verifyDataTypeMapping("REAL", Schema.FLOAT32_SCHEMA);
    verifyDataTypeMapping("REAL", Schema.FLOAT64_SCHEMA);
    verifyDataTypeMapping("INTEGER", Schema.BOOLEAN_SCHEMA);
    verifyDataTypeMapping("TEXT", Schema.STRING_SCHEMA);
    verifyDataTypeMapping("BLOB", Schema.BYTES_SCHEMA);
    verifyDataTypeMapping("NUMERIC", Decimal.schema(0));
    verifyDataTypeMapping("NUMERIC", Date.SCHEMA);
    verifyDataTypeMapping("NUMERIC", Time.SCHEMA);
    verifyDataTypeMapping("NUMERIC", Timestamp.SCHEMA);
  }

  @Test
  public void shouldMapDateSchemaTypeToDateSqlType() {
    assertDateMapping("NUMERIC");
  }

  @Test
  public void shouldMapTimeSchemaTypeToTimeSqlType() {
    assertTimeMapping("NUMERIC");
  }

  @Test
  public void shouldMapTimestampSchemaTypeToTimestampSqlType() {
    assertTimestampMapping("NUMERIC");
  }

  @Test
  public void shouldBuildCreateQueryStatement() {
    assertEquals(
        "CREATE TABLE `myTable` (\n"
        + "`c1` INTEGER NOT NULL,\n"
        + "`c2` INTEGER NOT NULL,\n"
        + "`c3` TEXT NOT NULL,\n"
        + "`c4` TEXT NULL,\n"
        + "`c5` NUMERIC DEFAULT '2001-03-15',\n"
        + "`c6` NUMERIC DEFAULT '00:00:00.000',\n"
        + "`c7` NUMERIC DEFAULT '2001-03-15 00:00:00.000',\n"
        + "`c8` NUMERIC NULL,\n"
        + "`c9` INTEGER DEFAULT 1,\n"
        + "PRIMARY KEY(`c1`))",
        dialect.buildCreateTableStatement(tableId, sinkRecordFields)
    );

    quoteIdentfiiers = QuoteMethod.NEVER;
    dialect = createDialect();
    assertEquals(
        "CREATE TABLE myTable (\n"
        + "c1 INTEGER NOT NULL,\n"
        + "c2 INTEGER NOT NULL,\n"
        + "c3 TEXT NOT NULL,\n"
        + "c4 TEXT NULL,\n"
        + "c5 NUMERIC DEFAULT '2001-03-15',\n"
        + "c6 NUMERIC DEFAULT '00:00:00.000',\n"
        + "c7 NUMERIC DEFAULT '2001-03-15 00:00:00.000',\n"
        + "c8 NUMERIC NULL,\n"
        + "c9 INTEGER DEFAULT 1,\n"
        + "PRIMARY KEY(c1))",
        dialect.buildCreateTableStatement(tableId, sinkRecordFields)
    );
  }

  @Test
  public void shouldBuildAlterTableStatement() {
    assertStatements(
        new String[]{
            "ALTER TABLE `myTable` ADD `c1` INTEGER NOT NULL",
            "ALTER TABLE `myTable` ADD `c2` INTEGER NOT NULL",
            "ALTER TABLE `myTable` ADD `c3` TEXT NOT NULL",
            "ALTER TABLE `myTable` ADD `c4` TEXT NULL",
            "ALTER TABLE `myTable` ADD `c5` NUMERIC DEFAULT '2001-03-15'",
            "ALTER TABLE `myTable` ADD `c6` NUMERIC DEFAULT '00:00:00.000'",
            "ALTER TABLE `myTable` ADD `c7` NUMERIC DEFAULT '2001-03-15 00:00:00.000'",
            "ALTER TABLE `myTable` ADD `c8` NUMERIC NULL",
            "ALTER TABLE `myTable` ADD `c9` INTEGER DEFAULT 1"
        },
        dialect.buildAlterTable(tableId, sinkRecordFields)
    );

    quoteIdentfiiers = QuoteMethod.NEVER;
    dialect = createDialect();
    assertStatements(
        new String[]{
            "ALTER TABLE myTable ADD c1 INTEGER NOT NULL",
            "ALTER TABLE myTable ADD c2 INTEGER NOT NULL",
            "ALTER TABLE myTable ADD c3 TEXT NOT NULL",
            "ALTER TABLE myTable ADD c4 TEXT NULL",
            "ALTER TABLE myTable ADD c5 NUMERIC DEFAULT '2001-03-15'",
            "ALTER TABLE myTable ADD c6 NUMERIC DEFAULT '00:00:00.000'",
            "ALTER TABLE myTable ADD c7 NUMERIC DEFAULT '2001-03-15 00:00:00.000'",
            "ALTER TABLE myTable ADD c8 NUMERIC NULL",
            "ALTER TABLE myTable ADD c9 INTEGER DEFAULT 1"
        },
        dialect.buildAlterTable(tableId, sinkRecordFields)
    );
  }

  @Test
  public void shouldBuildUpsertStatement() {
    String expected = "INSERT OR REPLACE INTO `myTable`(`id1`,`id2`,`columnA`,`columnB`," +
                      "`columnC`,`columnD`) VALUES(?,?,?,?,?,?)";
    String sql = dialect.buildUpsertQueryStatement(tableId, pkColumns, columnsAtoD);
    assertEquals(expected, sql);
  }

  @Test
  public void createOneColNoPk() {
    verifyCreateOneColNoPk(
        "CREATE TABLE `myTable` (" + System.lineSeparator() + "`col1` INTEGER NOT NULL)");
  }

  @Test
  public void createOneColOnePk() {
    verifyCreateOneColOnePk(
        "CREATE TABLE `myTable` (" + System.lineSeparator() + "`pk1` INTEGER NOT NULL," +
        System.lineSeparator() + "PRIMARY KEY(`pk1`))");
  }

  @Test
  public void createThreeColTwoPk() {
    verifyCreateThreeColTwoPk(
        "CREATE TABLE `myTable` (" + System.lineSeparator() + "`pk1` INTEGER NOT NULL," +
        System.lineSeparator() + "`pk2` INTEGER NOT NULL," + System.lineSeparator() +
        "`col1` INTEGER NOT NULL," + System.lineSeparator() + "PRIMARY KEY(`pk1`,`pk2`))");

    quoteIdentfiiers = QuoteMethod.NEVER;
    dialect = createDialect();
    verifyCreateThreeColTwoPk(
        "CREATE TABLE myTable (" + System.lineSeparator() + "pk1 INTEGER NOT NULL," +
        System.lineSeparator() + "pk2 INTEGER NOT NULL," + System.lineSeparator() +
        "col1 INTEGER NOT NULL," + System.lineSeparator() + "PRIMARY KEY(pk1,pk2))");
  }

  @Test
  public void alterAddOneCol() {
    verifyAlterAddOneCol("ALTER TABLE `myTable` ADD `newcol1` INTEGER NULL");
  }

  @Test
  public void alterAddTwoCol() {
    verifyAlterAddTwoCols("ALTER TABLE `myTable` ADD `newcol1` INTEGER NULL",
                          "ALTER TABLE `myTable` ADD `newcol2` INTEGER DEFAULT 42");
  }

  @Test
  public void upsert() {
    TableId book = new TableId(null, null, "Book");
    assertEquals(
        "INSERT OR REPLACE INTO `Book`(`author`,`title`,`ISBN`,`year`,`pages`) VALUES(?,?,?,?,?)",
        dialect.buildUpsertQueryStatement(
            book,
            columns(book, "author", "title"),
            columns(book, "ISBN", "year", "pages")
        )
    );

    quoteIdentfiiers = QuoteMethod.NEVER;
    dialect = createDialect();
    assertEquals(
        "INSERT OR REPLACE INTO Book(author,title,ISBN,year,pages) VALUES(?,?,?,?,?)",
        dialect.buildUpsertQueryStatement(
            book,
            columns(book, "author", "title"),
            columns(book, "ISBN", "year", "pages")
        )
    );
  }

  @Test(expected = SQLException.class)
  public void tableOnEmptyDb() throws SQLException {
    TableId tableId = new TableId(null, null, "x");
    dialect.describeTable(sqliteHelper.connection, tableId);
  }

  @Test
  public void testDescribeTable() throws SQLException {
    TableId tableId = new TableId(null, null, "x");
    sqliteHelper.createTable(
        "create table x (id int primary key, name text not null, optional_age int null)");
    TableDefinition defn = dialect.describeTable(sqliteHelper.connection, tableId);
    assertEquals(tableId, defn.id());
    ColumnDefinition columnDefn = defn.definitionForColumn("id");
    assertEquals("INT", columnDefn.typeName());
    assertEquals(Types.INTEGER, columnDefn.type());
    assertEquals(true, columnDefn.isPrimaryKey());
    assertEquals(false, columnDefn.isOptional());

    columnDefn = defn.definitionForColumn("name");
    assertEquals("TEXT", columnDefn.typeName());
    assertEquals(Types.VARCHAR, columnDefn.type());
    assertEquals(false, columnDefn.isPrimaryKey());
    assertEquals(false, columnDefn.isOptional());

    columnDefn = defn.definitionForColumn("optional_age");
    assertEquals("INT", columnDefn.typeName());
    assertEquals(Types.INTEGER, columnDefn.type());
    assertEquals(false, columnDefn.isPrimaryKey());
    assertEquals(true, columnDefn.isOptional());
  }

  @Test
  public void useCurrentTimestampValue() throws SQLException {
    Calendar cal = DateTimeUtils.getTimeZoneCalendar(TimeZone.getTimeZone("UTC"));

    //Regular expression to check if the timestamp is of the format %Y-%m-%d %H:%M:%S.%f
    Pattern p = Pattern.compile("(\\p{Nd}++)\\Q-\\E(\\p{Nd}++)\\Q-\\E(\\p{Nd}++)\\Q \\E(\\p{Nd}++)"
        + "\\Q:\\E(\\p{Nd}++)"
        + "\\Q:\\E"
        + "(\\p{Nd}++)\\Q.\\E(\\p{Nd}++)");

    java.util.Date timeOnDB = dialect.currentTimeOnDB(sqliteHelper.connection, cal);
    java.util.Date currentTime = new java.util.Date();
    Matcher matcher = p.matcher(timeOnDB.toString());

    long timeOnDBInSeconds = timeOnDB.getTime() / 1000;
    long currentTimeInSeconds = currentTime.getTime() / 1000;
    long differenceInTime = Math.abs(timeOnDBInSeconds - currentTimeInSeconds);

    assertTrue(differenceInTime < 5);
    assertTrue(matcher.matches());
  }
}