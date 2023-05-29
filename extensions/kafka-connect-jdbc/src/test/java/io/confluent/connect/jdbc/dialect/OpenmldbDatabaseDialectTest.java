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
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.Test;

import java.util.List;

import io.confluent.connect.jdbc.util.QuoteMethod;
import io.confluent.connect.jdbc.util.TableId;

import static org.junit.Assert.assertEquals;

public class OpenmldbDatabaseDialectTest extends BaseDialectTest<OpenmldbDatabaseDialect> {

    // OpenMLDB JDBC will create connection when create dialect, so we need to use the real connection hold on
    @Override
    protected OpenmldbDatabaseDialect createDialect() {
        return new OpenmldbDatabaseDialect(sourceConfigWithUrl("jdbc:openmldb://something"));
    }

    @Test
    public void shouldMapPrimitiveSchemaTypeToSqlTypes() {
//        assertPrimitiveMapping(Schema.Type.INT8, "TINYINT");
        assertPrimitiveMapping(Schema.Type.INT16, "SMALLINT");
        assertPrimitiveMapping(Schema.Type.INT32, "INT");
        assertPrimitiveMapping(Schema.Type.INT64, "BIGINT");
        assertPrimitiveMapping(Schema.Type.FLOAT32, "FLOAT");
        assertPrimitiveMapping(Schema.Type.FLOAT64, "DOUBLE");
        assertPrimitiveMapping(Schema.Type.BOOLEAN, "BOOL"); // in OpenMLDB, it's bool
//        assertPrimitiveMapping(Schema.Type.BYTES, "BLOB");
        assertPrimitiveMapping(Schema.Type.STRING, "VARCHAR");
    }

    @Test(expected = ConnectException.class)
    public void shouldMapDecimalSchemaTypeToDecimalSqlType() {
        // no decimal type in OpenMLDB
        assertDecimalMapping(0, "DECIMAL");
        assertDecimalMapping(3, "DECIMAL");
        assertDecimalMapping(4, "DECIMAL");
        assertDecimalMapping(5, "DECIMAL");
    }

    @Test
    public void shouldMapDataTypes() {
//        verifyDataTypeMapping("TINYINT", Schema.INT8_SCHEMA);
        verifyDataTypeMapping("SMALLINT", Schema.INT16_SCHEMA);
        verifyDataTypeMapping("INT", Schema.INT32_SCHEMA);
        verifyDataTypeMapping("BIGINT", Schema.INT64_SCHEMA);
        verifyDataTypeMapping("FLOAT", Schema.FLOAT32_SCHEMA);
        verifyDataTypeMapping("DOUBLE", Schema.FLOAT64_SCHEMA);
        verifyDataTypeMapping("BOOL", Schema.BOOLEAN_SCHEMA);
        verifyDataTypeMapping("VARCHAR", Schema.STRING_SCHEMA);
//        verifyDataTypeMapping("BLOB", Schema.BYTES_SCHEMA);
//        verifyDataTypeMapping("DECIMAL", Decimal.schema(0));
        verifyDataTypeMapping("DATE", Date.SCHEMA);
        verifyDataTypeMapping("DATE", Time.SCHEMA);
        verifyDataTypeMapping("TIMESTAMP", Timestamp.SCHEMA);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void shouldBuildCreateQueryStatement() {
        // no pk, no decimal. need to test new sink record fields
        String expected =
                "CREATE TABLE `myTable` (\n" + "`c1` INT NOT NULL,\n" + "`c2` BIGINT NOT NULL,\n" +
                        "`c3` TEXT NOT NULL,\n" + "`c4` TEXT NULL,\n" +
                        "`c5` DATE DEFAULT '2001-03-15',\n" + "`c6` TIME(3) DEFAULT '00:00:00.000',\n" +
                        "`c7` DATETIME(3) DEFAULT '2001-03-15 00:00:00.000',\n" + "`c8` DECIMAL(65,4) NULL,\n" +
                        "`c9` TINYINT DEFAULT 1);";
        String sql = dialect.buildCreateTableStatement(tableId, sinkRecordFields);
        assertEquals(expected, sql);
    }

    @Test
    public void shouldReturnCurrentTimestampDatabaseQuery() {
        String expected = null;
        String sql = dialect.currentTimestampDatabaseQuery();
        assertEquals(expected, sql);
    }

    @Test
    public void createOneColNoPk() {
        verifyCreateOneColNoPk(
                "CREATE TABLE `myTable` (" + System.lineSeparator() + "`col1` INT NOT NULL)");

        quoteIdentfiiers = QuoteMethod.NEVER;
        dialect = createDialect();

        verifyCreateOneColNoPk(
                "CREATE TABLE myTable (" + System.lineSeparator() + "col1 INT NOT NULL)");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void alterUnsupported() {
        verifyAlterAddOneCol(
                "ALTER TABLE \"myTable\" ADD(" + System.lineSeparator() + "\"newcol1\" INTEGER NULL)");
    }


    @Test
    public void shouldMapDateSchemaTypeToDateSqlType() {
        assertDateMapping("DATE");
    }

    @Test
    public void shouldMapTimeSchemaTypeToDateSqlType() {
        assertTimeMapping("DATE");
    }

    @Test
    public void shouldMapTimestampSchemaTypeToTimestampSqlType() {
        assertTimestampMapping("TIMESTAMP");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void shouldBuildAlterTableStatement() {
        List<String> statements = dialect.buildAlterTable(tableId, sinkRecordFields);
        String[] sql = {
                "ALTER TABLE `myTable` \n" + "ADD `c1` INT NOT NULL,\n" + "ADD `c2` BIGINT NOT NULL,\n" +
                        "ADD `c3` TEXT NOT NULL,\n" + "ADD `c4` TEXT NULL,\n" +
                        "ADD `c5` DATE DEFAULT '2001-03-15',\n" + "ADD `c6` TIME(3) DEFAULT '00:00:00.000',\n" +
                        "ADD `c7` DATETIME(3) DEFAULT '2001-03-15 00:00:00.000',\n" +
                        "ADD `c8` DECIMAL(65,4) NULL,\n" +
                        "ADD `c9` TINYINT DEFAULT 1"};
        assertStatements(sql, statements);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void shouldBuildUpsertStatement() {
        String expected = "insert into `myTable`(`id1`,`id2`,`columnA`,`columnB`,`columnC`,`columnD`)" +
                " values(?,?,?,?,?,?) on duplicate key update `columnA`=values(`columnA`)," +
                "`columnB`=values(`columnB`),`columnC`=values(`columnC`),`columnD`=values" +
                "(`columnD`)";
        String sql = dialect.buildUpsertQueryStatement(tableId, pkColumns, columnsAtoD);
        assertEquals(expected, sql);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void createOneColOnePk() {
        verifyCreateOneColOnePk(
                "CREATE TABLE `myTable` (" + System.lineSeparator() + "`pk1` INT NOT NULL," +
                        System.lineSeparator() + "PRIMARY KEY(`pk1`))");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void alterAddOneCol() {
        verifyAlterAddOneCol("ALTER TABLE `myTable` ADD `newcol1` INT NULL");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void upsert() {
        TableId actor = tableId("actor");
        String expected = "insert into `actor`(`actor_id`,`first_name`,`last_name`,`score`) " +
                "values(?,?,?,?) on duplicate key update `first_name`=values(`first_name`)," +
                "`last_name`=values(`last_name`),`score`=values(`score`)";
        String sql = dialect.buildUpsertQueryStatement(actor, columns(actor, "actor_id"),
                columns(actor, "first_name", "last_name",
                        "score"));
        assertEquals(expected, sql);

        quoteIdentfiiers = QuoteMethod.NEVER;
        dialect = createDialect();

        expected = "insert into actor(actor_id,first_name,last_name,score) " +
                "values(?,?,?,?) on duplicate key update first_name=values(first_name)," +
                "last_name=values(last_name),score=values(score)";
        sql = dialect.buildUpsertQueryStatement(actor, columns(actor, "actor_id"),
                columns(actor, "first_name", "last_name",
                        "score"));
        assertEquals(expected, sql);
    }

    @Test
    public void insert() {
        TableId customers = tableId("customers");
        String expected = "INSERT INTO `customers`(`age`,`firstName`,`lastName`) VALUES(?,?,?)";
        String sql = dialect.buildInsertStatement(customers, columns(customers),
                columns(customers, "age", "firstName", "lastName"));
        assertEquals(expected, sql);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void update() {
        TableId customers = tableId("customers");
        String expected =
                "UPDATE `customers` SET `age` = ?, `firstName` = ?, `lastName` = ? WHERE " + "`id` = ?";
        String sql = dialect.buildUpdateStatement(customers, columns(customers, "id"),
                columns(customers, "age", "firstName", "lastName"));
        assertEquals(expected, sql);
    }
}
