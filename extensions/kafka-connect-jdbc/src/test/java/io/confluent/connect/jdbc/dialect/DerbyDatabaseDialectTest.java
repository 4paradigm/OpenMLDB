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
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;

import io.confluent.connect.jdbc.util.QuoteMethod;
import io.confluent.connect.jdbc.util.TableId;

import static org.junit.Assert.assertEquals;

public class DerbyDatabaseDialectTest extends BaseDialectTest<DerbyDatabaseDialect> {

  @Override
  protected DerbyDatabaseDialect createDialect() {
    return new DerbyDatabaseDialect(sourceConfigWithUrl("jdbc:derby://something"));
  }

  @Test
  public void shouldMapPrimitiveSchemaTypeToSqlTypes() {
    assertPrimitiveMapping(Type.INT8, "SMALLINT");
    assertPrimitiveMapping(Type.INT16, "SMALLINT");
    assertPrimitiveMapping(Type.INT32, "INTEGER");
    assertPrimitiveMapping(Type.INT64, "BIGINT");
    assertPrimitiveMapping(Type.FLOAT32, "FLOAT");
    assertPrimitiveMapping(Type.FLOAT64, "DOUBLE");
    assertPrimitiveMapping(Type.BOOLEAN, "SMALLINT");
    assertPrimitiveMapping(Type.BYTES, "BLOB(64000)");
    assertPrimitiveMapping(Type.STRING, "VARCHAR(32672)");
  }

  @Test
  public void shouldMapDecimalSchemaTypeToDecimalSqlType() {
    assertDecimalMapping(0, "DECIMAL(31,0)");
    assertDecimalMapping(5, "DECIMAL(31,5)");
  }

  @Test
  public void shouldMapDataTypes() {
    verifyDataTypeMapping("SMALLINT", Schema.INT8_SCHEMA);
    verifyDataTypeMapping("SMALLINT", Schema.INT16_SCHEMA);
    verifyDataTypeMapping("INTEGER", Schema.INT32_SCHEMA);
    verifyDataTypeMapping("BIGINT", Schema.INT64_SCHEMA);
    verifyDataTypeMapping("FLOAT", Schema.FLOAT32_SCHEMA);
    verifyDataTypeMapping("DOUBLE", Schema.FLOAT64_SCHEMA);
    verifyDataTypeMapping("SMALLINT", Schema.BOOLEAN_SCHEMA);
    verifyDataTypeMapping("VARCHAR(32672)", Schema.STRING_SCHEMA);
    verifyDataTypeMapping("BLOB(64000)", Schema.BYTES_SCHEMA);
    verifyDataTypeMapping("DECIMAL(31,0)", Decimal.schema(0));
    verifyDataTypeMapping("DECIMAL(31,2)", Decimal.schema(2));
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
    String expected =
        "CREATE TABLE \"myTable\" (\n" + "\"c1\" INTEGER NOT NULL,\n" + "\"c2\" BIGINT NOT NULL,\n"
        + "\"c3\" VARCHAR(32672) NOT NULL,\n" + "\"c4\" VARCHAR(32672) NULL,\n"
        + "\"c5\" DATE DEFAULT '2001-03-15',\n" + "\"c6\" TIME DEFAULT '00:00:00.000',\n"
        + "\"c7\" TIMESTAMP DEFAULT '2001-03-15 00:00:00.000',\n" + "\"c8\" DECIMAL(31,4) NULL,\n"
        + "\"c9\" SMALLINT DEFAULT 1,\n"
        + "PRIMARY KEY(\"c1\"))";
    String sql = dialect.buildCreateTableStatement(tableId, sinkRecordFields);
    assertEquals(expected, sql);
  }

  @Test
  public void shouldBuildCreateQueryStatementWithNoIdentifierQuoting() {
    quoteIdentfiiers = QuoteMethod.NEVER;
    dialect = createDialect();

    String expected =
        "CREATE TABLE myTable (\nc1 INTEGER NOT NULL,\nc2 BIGINT NOT NULL,\n"
        + "c3 VARCHAR(32672) NOT NULL,\nc4 VARCHAR(32672) NULL,\n"
        + "c5 DATE DEFAULT '2001-03-15',\nc6 TIME DEFAULT '00:00:00.000',\n"
        + "c7 TIMESTAMP DEFAULT '2001-03-15 00:00:00.000',\nc8 DECIMAL(31,4) NULL,\n"
        + "c9 SMALLINT DEFAULT 1,\n"
        + "PRIMARY KEY(c1))";
    String sql = dialect.buildCreateTableStatement(tableId, sinkRecordFields);
    assertEquals(expected, sql);
  }

  @Test
  public void shouldBuildAlterTableStatement() {
    List<String> statements = dialect.buildAlterTable(tableId, sinkRecordFields);
    String[] sql = {"ALTER TABLE \"myTable\" \n" + "ADD \"c1\" INTEGER NOT NULL,\n"
                    + "ADD \"c2\" BIGINT NOT NULL,\n" + "ADD \"c3\" VARCHAR(32672) NOT NULL,\n"
                    + "ADD \"c4\" VARCHAR(32672) NULL,\n"
                    + "ADD \"c5\" DATE DEFAULT '2001-03-15',\n"
                    + "ADD \"c6\" TIME DEFAULT '00:00:00.000',\n"
                    + "ADD \"c7\" TIMESTAMP DEFAULT '2001-03-15 00:00:00.000',\n"
                    + "ADD \"c8\" DECIMAL(31,4) NULL,\n"
                    + "ADD \"c9\" SMALLINT DEFAULT 1"};
    assertStatements(sql, statements);
  }

  @Test
  public void shouldBuildAlterTableStatementWithNoIdentifierQuoting() {
    quoteIdentfiiers = QuoteMethod.NEVER;
    dialect = createDialect();

    List<String> statements = dialect.buildAlterTable(tableId, sinkRecordFields);
    String[] sql = {"ALTER TABLE myTable \n"
                    + "ADD c1 INTEGER NOT NULL,\n"
                    + "ADD c2 BIGINT NOT NULL,\n"
                    + "ADD c3 VARCHAR(32672) NOT NULL,\n"
                    + "ADD c4 VARCHAR(32672) NULL,\n"
                    + "ADD c5 DATE DEFAULT '2001-03-15',\n"
                    + "ADD c6 TIME DEFAULT '00:00:00.000',\n"
                    + "ADD c7 TIMESTAMP DEFAULT '2001-03-15 00:00:00.000',\n"
                    + "ADD c8 DECIMAL(31,4) NULL,\n"
                    + "ADD c9 SMALLINT DEFAULT 1"};
    assertStatements(sql, statements);
  }

  @Test
  public void createOneColNoPk() {
    verifyCreateOneColNoPk(
        "CREATE TABLE \"myTable\" (" + System.lineSeparator() + "\"col1\" INTEGER NOT NULL)");
  }

  @Test
  public void createOneColOnePk() {
    verifyCreateOneColOnePk(
        "CREATE TABLE \"myTable\" (" + System.lineSeparator() + "\"pk1\" INTEGER NOT NULL," + System
            .lineSeparator() + "PRIMARY KEY(\"pk1\"))");
  }

  @Test
  public void createThreeColTwoPk() {
    verifyCreateThreeColTwoPk(
        "CREATE TABLE \"myTable\" (" + System.lineSeparator() + "\"pk1\" INTEGER NOT NULL," + System
            .lineSeparator() + "\"pk2\" INTEGER NOT NULL," + System.lineSeparator()
        + "\"col1\" INTEGER NOT NULL," + System.lineSeparator() + "PRIMARY KEY(\"pk1\",\"pk2\"))");
  }

  @Test
  public void alterAddOneCol() {
    verifyAlterAddOneCol("ALTER TABLE \"myTable\" ADD \"newcol1\" INTEGER NULL");
  }

  @Test
  public void alterAddTwoCol() {
    verifyAlterAddTwoCols(
        "ALTER TABLE \"myTable\" " + System.lineSeparator() + "ADD \"newcol1\" INTEGER NULL,"
        + System.lineSeparator() + "ADD \"newcol2\" INTEGER DEFAULT 42");
  }

  @Test
  public void insert() {
    TableId customers = tableId("customers");
    String expected = "INSERT INTO \"customers\"(\"age\",\"firstName\",\"lastName\") VALUES(?,?,?)";
    String sql = dialect.buildInsertStatement(customers, columns(customers),
                                              columns(customers, "age", "firstName", "lastName")
    );
    assertEquals(expected, sql);
  }

  @Test
  public void update() {
    TableId customers = tableId("customers");
    String expected =
        "UPDATE \"customers\" SET \"age\" = ?, \"firstName\" = ?, \"lastName\" = ? WHERE "
        + "\"id\" = ?";
    String sql = dialect.buildUpdateStatement(customers, columns(customers, "id"),
                                              columns(customers, "age", "firstName", "lastName")
    );
    assertEquals(expected, sql);
  }

  @Ignore
  @Test
  public void shouldBuildUpsertStatement() {
    String expected =
        "merge into \"myTable\" using (values(?, ?, ?, ?, ?, ?)) "
        + "as DAT(\"id1\", \"id2\", \"columnA\", \"columnB\", \"columnC\", \"columnD\") "
        + "on \"myTable\".\"id1\"=DAT.\"id1\" and \"myTable\".\"id2\"=DAT.\"id2\" "
        + "when matched then update set "
        + "\"myTable\".\"columnA\"=DAT.\"columnA\", "
        + "\"myTable\".\"columnB\"=DAT.\"columnB\", "
        + "\"myTable\".\"columnC\"=DAT.\"columnC\", "
        + "\"myTable\".\"columnD\"=DAT.\"columnD\" "
        + "when not matched then "
        + "insert(\"myTable\".\"columnA\",\"myTable\".\"columnB\",\"myTable\".\"columnC\","
        + "\"myTable\".\"columnD\",\"myTable\".\"id1\",\"myTable\""
        + ".\"id2\") "
        + "values(DAT.\"columnA\",DAT.\"columnB\",DAT.\"columnC\",DAT.\"columnD\",DAT.\"id1\","
        + "DAT.\"id2\")";
    String sql = dialect.buildUpsertQueryStatement(tableId, pkColumns, columnsAtoD);
    assertEquals(expected, sql);
  }

  @Ignore
  @Test
  public void upsert() {
    TableId actor = tableId("actor");
    String expected = "merge into \"actor\" using (values(?, ?, ?, ?)) as DAT(\"actor_id\", "
                      + "\"first_name\", \"last_name\", \"score\") on \"actor\".\"actor_id\"=DAT"
                      + ".\"actor_id\" when matched then update set \"actor\".\"first_name\"=DAT"
                      + ".\"first_name\", \"actor\".\"last_name\"=DAT.\"last_name\", "
                      + "\"actor\".\"score\"=DAT.\"score\" when not matched then insert(\"actor\""
                      + ".\"first_name\",\"actor\".\"last_name\",\"actor\".\"score\",\"actor\""
                      + ".\"actor_id\") values(DAT.\"first_name\",DAT.\"last_name\",DAT"
                      + ".\"score\",DAT.\"actor_id\")";
    String sql = dialect.buildUpsertQueryStatement(actor, columns(actor, "actor_id"),
                                                   columns(actor, "first_name", "last_name",
                                                           "score"
                                                   )
    );
    assertEquals(expected, sql);
  }

  @Test
  public void upsertOnlyKeyCols() {
    TableId actor = tableId("actor");
    String expected = "merge into \"actor\" using (values(?)) as DAT(\"actor_id\") on \"actor\""
                      + ".\"actor_id\"=DAT.\"actor_id\" when not matched then insert(\"actor\""
                      + ".\"actor_id\") values(DAT.\"actor_id\")";
    String sql = dialect.buildUpsertQueryStatement(
        actor, columns(actor, "actor_id"), columns(actor));
    assertEquals(expected, sql);
  }

  @Test
  public void upsertOnlyKeyColsWithNoIdentifiernQuoting() {
    quoteIdentfiiers = QuoteMethod.NEVER;
    dialect = createDialect();

    TableId actor = tableId("actor");
    String expected = "merge into actor using (values(?)) as DAT(actor_id) on actor"
                      + ".actor_id=DAT.actor_id when not matched then insert(actor"
                      + ".actor_id) values(DAT.actor_id)";
    String sql = dialect.buildUpsertQueryStatement(
        actor, columns(actor, "actor_id"), columns(actor));
    assertEquals(expected, sql);
  }

  @Test
  public void shouldSanitizeUrlWithoutCredentialsInProperties() {
    assertSanitizedUrl(
        "jdbc:derby:sample;user=jill;other=toFetchAPail",
        "jdbc:derby:sample;user=jill;other=toFetchAPail"
    );
  }

  @Test
  public void shouldSanitizeUrlWithCredentialsInUrlProperties() {
    assertSanitizedUrl(
        "jdbc:derby:sample;user=jill;password=toFetchAPail",
        "jdbc:derby:sample;user=jill;password=****"
    );
    assertSanitizedUrl(
        "jdbc:derby:sample;password=toFetchAPail;user=jill",
        "jdbc:derby:sample;password=****;user=jill"
    );
  }
}