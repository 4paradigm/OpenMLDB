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

package io.confluent.connect.jdbc.source;

import java.time.ZoneOffset;
import java.util.Collections;
import java.util.TimeZone;

import io.confluent.connect.jdbc.util.DateTimeUtils;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig.TimestampGranularity;
import io.confluent.connect.jdbc.util.ColumnId;
import io.confluent.connect.jdbc.util.ExpressionBuilder;
import io.confluent.connect.jdbc.util.IdentifierRules;
import io.confluent.connect.jdbc.util.QuoteMethod;
import io.confluent.connect.jdbc.util.TableId;

import static org.junit.Assert.assertEquals;

public class TimestampIncrementingCriteriaTest {

  private static final TableId TABLE_ID = new TableId(null, null,"myTable");
  private static final ColumnId INCREMENTING_COLUMN = new ColumnId(TABLE_ID, "id");
  private static final ColumnId TS1_COLUMN = new ColumnId(TABLE_ID, "ts1");
  private static final ColumnId TS2_COLUMN = new ColumnId(TABLE_ID, "ts2");
  private static final List<ColumnId> TS_COLUMNS = Arrays.asList(TS1_COLUMN, TS2_COLUMN);
  private static final java.sql.Timestamp TS0 = new java.sql.Timestamp(0);
  private static final java.sql.Timestamp TS1 = new java.sql.Timestamp(4761);
  private static final java.sql.Timestamp TS2 = new java.sql.Timestamp(176400L);

  private IdentifierRules rules;
  private QuoteMethod identifierQuoting;
  private ExpressionBuilder builder;
  private TimestampIncrementingCriteria criteria;
  private TimestampIncrementingCriteria criteriaInc;
  private TimestampIncrementingCriteria criteriaTs;
  private TimestampIncrementingCriteria criteriaIncTs;
  private Schema schema;
  private Struct record;
  private TimeZone utcTimeZone = TimeZone.getTimeZone(ZoneOffset.UTC);

  @Before
  public void beforeEach() {
    criteria = new TimestampIncrementingCriteria(null, null, utcTimeZone);
    criteriaInc = new TimestampIncrementingCriteria(INCREMENTING_COLUMN, null, utcTimeZone);
    criteriaTs = new TimestampIncrementingCriteria(null, TS_COLUMNS, utcTimeZone);
    criteriaIncTs = new TimestampIncrementingCriteria(INCREMENTING_COLUMN, TS_COLUMNS, utcTimeZone);
    identifierQuoting = null;
    rules = null;
    builder = null;
  }

  protected void assertExtractedOffset(long expected,
      java.sql.Timestamp expectedT,
      Schema schema,
      Struct record,
      TimestampGranularity timestampGranularity) {
    TimestampIncrementingCriteria criteria;
    if (schema.field(INCREMENTING_COLUMN.name()) != null) {
      if (schema.field(TS1_COLUMN.name()) != null) {
        criteria = criteriaIncTs;
      } else {
        criteria = criteriaInc;
      }
    } else if (schema.field(TS1_COLUMN.name()) != null) {
      criteria = criteriaTs;
    } else {
      criteria = this.criteria;
    }
    TimestampIncrementingOffset offset = criteria.extractValues(schema, record,
        null, timestampGranularity);
    assertEquals(expectedT, offset.getTimestampOffset());
    assertEquals(expected, offset.getIncrementingOffset());
  }

  @Test
  public void extractIntOffset() throws SQLException {
    schema = SchemaBuilder.struct().field("id", SchemaBuilder.INT32_SCHEMA).build();
    record = new Struct(schema).put("id", 42);
    assertExtractedOffset(42L, TS0, schema, record, TimestampGranularity.CONNECT_LOGICAL);
  }

  @Test
  public void extractLongOffset() throws SQLException {
    schema = SchemaBuilder.struct().field("id", SchemaBuilder.INT64_SCHEMA).build();
    record = new Struct(schema).put("id", 42L);
    assertExtractedOffset(42L, TS0, schema, record, TimestampGranularity.CONNECT_LOGICAL);
  }

  @Test
  public void extractDecimalOffset() throws SQLException {
    final Schema decimalSchema = Decimal.schema(0);
    schema = SchemaBuilder.struct().field("id", decimalSchema).build();
    record = new Struct(schema).put("id", new BigDecimal(42));
    assertExtractedOffset(42L, TS0, schema, record, TimestampGranularity.CONNECT_LOGICAL);
  }

  @Test(expected = ConnectException.class)
  public void extractTooLargeDecimalOffset() throws SQLException {
    final Schema decimalSchema = Decimal.schema(0);
    schema = SchemaBuilder.struct().field("id", decimalSchema).build();
    record = new Struct(schema).put("id", new BigDecimal(Long.MAX_VALUE).add(new BigDecimal(1)));
    assertExtractedOffset(42L, TS0, schema, record, TimestampGranularity.CONNECT_LOGICAL);
  }

  @Test(expected = ConnectException.class)
  public void extractFractionalDecimalOffset() throws SQLException {
    final Schema decimalSchema = Decimal.schema(2);
    schema = SchemaBuilder.struct().field("id", decimalSchema).build();
    record = new Struct(schema).put("id", new BigDecimal("42.42"));
    assertExtractedOffset(42L, TS0, schema, record, TimestampGranularity.CONNECT_LOGICAL);
  }

  @Test
  public void extractWithIncTsColumn() throws SQLException {
    schema = SchemaBuilder.struct()
                          .field("id", SchemaBuilder.INT32_SCHEMA)
                          .field(TS1_COLUMN.name(), Timestamp.SCHEMA)
                          .field(TS2_COLUMN.name(), Timestamp.SCHEMA)
                          .build();
    record = new Struct(schema)
        .put("id", 42)
        .put(TS1_COLUMN.name(), TS1);
    assertExtractedOffset(42L, TS1, schema, record,
        TimestampGranularity.CONNECT_LOGICAL);
  }

  @Test(expected = DataException.class)
  public void extractWithIncColumnNotExisting() throws Exception {
    schema = SchemaBuilder.struct()
            .field("real-id", SchemaBuilder.INT32_SCHEMA)
            .field(TS1_COLUMN.name(), Timestamp.SCHEMA)
            .field(TS2_COLUMN.name(), Timestamp.SCHEMA)
            .build();
    record = new Struct(schema).put("real-id", 42);
    criteriaIncTs.extractValues(schema, record, null, TimestampGranularity.CONNECT_LOGICAL);
  }

  @Test
  public void extractWithTsColumn() throws Exception {
    schema = SchemaBuilder.struct()
        .field(TS1_COLUMN.name(), Timestamp.SCHEMA)
        .field(TS2_COLUMN.name(), Timestamp.SCHEMA)
        .build();
    record = new Struct(schema)
        .put(TS1_COLUMN.name(), TS1)
        .put(TS2_COLUMN.name(), TS2);
    assertExtractedOffset(-1, TS1, schema, record, TimestampGranularity.CONNECT_LOGICAL);
  }

  @Test
  public void extractWithTsColumnConnectLogical() throws Exception {
    schema = SchemaBuilder.struct()
        .field(TS1_COLUMN.name(), Timestamp.SCHEMA)
        .field(TS2_COLUMN.name(), Timestamp.SCHEMA)
        .build();
    record = new Struct(schema)
        .put(TS1_COLUMN.name(), TS1)
        .put(TS2_COLUMN.name(), TS2);
    assertExtractedOffset(-1, TS1, schema, record,
        TimestampGranularity.CONNECT_LOGICAL);
  }

  @Test
  public void extractWithTsColumnNanosLong() throws Exception {
    schema = SchemaBuilder.struct()
        .field(TS1_COLUMN.name(), SchemaBuilder.INT64_SCHEMA)
        .field(TS2_COLUMN.name(), SchemaBuilder.INT64_SCHEMA)
        .build();
    record = new Struct(schema)
        .put(TS1_COLUMN.name(), DateTimeUtils.toEpochNanos(TS1))
        .put(TS2_COLUMN.name(), DateTimeUtils.toEpochNanos(TS2));
    assertExtractedOffset(-1, TS1, schema, record,
        TimestampGranularity.NANOS_LONG);
  }

  @Test
  public void extractWithTsColumnNanosString() throws Exception {
    schema = SchemaBuilder.struct()
        .field(TS1_COLUMN.name(), SchemaBuilder.STRING_SCHEMA)
        .field(TS2_COLUMN.name(), SchemaBuilder.STRING_SCHEMA)
        .build();
    record = new Struct(schema)
        .put(TS1_COLUMN.name(), String.valueOf(DateTimeUtils.toEpochNanos(TS1)))
        .put(TS2_COLUMN.name(), String.valueOf(DateTimeUtils.toEpochNanos(TS2)));
    assertExtractedOffset(-1, TS1, schema, record,
        TimestampGranularity.NANOS_STRING);
  }

  @Test
  public void extractWithTsColumnIsoDateTimeString() throws Exception {
    schema = SchemaBuilder.struct()
        .field(TS1_COLUMN.name(), SchemaBuilder.STRING_SCHEMA)
        .field(TS2_COLUMN.name(), SchemaBuilder.STRING_SCHEMA)
        .build();
    record = new Struct(schema)
        .put(TS1_COLUMN.name(), DateTimeUtils.toIsoDateTimeString(TS1, utcTimeZone))
        .put(TS2_COLUMN.name(), DateTimeUtils.toIsoDateTimeString(TS2, utcTimeZone));
    assertExtractedOffset(-1, TS1, schema, record,
        TimestampGranularity.NANOS_ISO_DATETIME_STRING);
  }

  @Test
  public void extractWithTsColumnNanosLongNull() throws Exception {
    schema = SchemaBuilder.struct()
        .field(TS1_COLUMN.name(), SchemaBuilder.OPTIONAL_INT64_SCHEMA)
        .field(TS2_COLUMN.name(), SchemaBuilder.OPTIONAL_INT64_SCHEMA)
        .build();
    record = new Struct(schema)
        .put(TS1_COLUMN.name(), null)
        .put(TS2_COLUMN.name(), null);
    assertExtractedOffset(-1, TS0, schema, record,
        TimestampGranularity.NANOS_LONG);
  }

  @Test
  public void extractWithTsColumnNanosStringNull() throws Exception {
    schema = SchemaBuilder.struct()
        .field(TS1_COLUMN.name(), SchemaBuilder.OPTIONAL_STRING_SCHEMA)
        .field(TS2_COLUMN.name(), SchemaBuilder.OPTIONAL_STRING_SCHEMA)
        .build();
    record = new Struct(schema)
        .put(TS1_COLUMN.name(), null)
        .put(TS2_COLUMN.name(), null);
    assertExtractedOffset(-1, TS0, schema, record,
        TimestampGranularity.NANOS_STRING);
  }

  @Test
  public void extractWithTsColumnIsoDateTimeStringNull() throws Exception {
    schema = SchemaBuilder.struct()
        .field(TS1_COLUMN.name(), SchemaBuilder.OPTIONAL_STRING_SCHEMA)
        .field(TS2_COLUMN.name(), SchemaBuilder.OPTIONAL_STRING_SCHEMA)
        .build();
    record = new Struct(schema)
        .put(TS1_COLUMN.name(), null)
        .put(TS2_COLUMN.name(), null);
    assertExtractedOffset(-1, TS0, schema, record,
        TimestampGranularity.NANOS_ISO_DATETIME_STRING);
  }

  @Test(expected = ConnectException.class)
  public void extractWithTsColumnIsoDateTimeStringNanosConfig() throws Exception {
    schema = SchemaBuilder.struct()
        .field(TS1_COLUMN.name(), SchemaBuilder.STRING_SCHEMA)
        .field(TS2_COLUMN.name(), SchemaBuilder.STRING_SCHEMA)
        .build();
    record = new Struct(schema)
        .put(TS1_COLUMN.name(), DateTimeUtils.toIsoDateTimeString(TS1, utcTimeZone))
        .put(TS2_COLUMN.name(), DateTimeUtils.toIsoDateTimeString(TS2, utcTimeZone));
    assertExtractedOffset(-1, TS1, schema, record,
        TimestampGranularity.NANOS_STRING);
  }

  @Test
  public void extractWithFallbackTsColumn() throws Exception {
    schema = SchemaBuilder.struct()
        .field(TS1_COLUMN.name(), Timestamp.builder().optional().build())
        .field(TS2_COLUMN.name(), Timestamp.SCHEMA)
        .build();
    record = new Struct(schema)
        .put(TS1_COLUMN.name(), null)
        .put(TS2_COLUMN.name(), TS2);
    assertExtractedOffset(-1, TS2, schema, record, TimestampGranularity.CONNECT_LOGICAL);
  }

  @Test
  public void extractWithCaseInsensitiveTsColumn() throws Exception {
    schema = SchemaBuilder.struct()
        .field(TS1_COLUMN.name().toUpperCase(), Timestamp.SCHEMA)
        .field(TS2_COLUMN.name(), Timestamp.SCHEMA)
        .build();
    record = new Struct(schema)
        .put(TS1_COLUMN.name().toUpperCase(), TS1)
        .put(TS2_COLUMN.name(), TS2);
    TimestampIncrementingOffset offset = criteriaTs.extractValues(schema, record, null, TimestampGranularity.CONNECT_LOGICAL);
    assertEquals(TS1, offset.getTimestampOffset());
  }

  @Test
  public void extractWithCaseInsensitiveFallbackTsColumn() throws Exception {
    schema = SchemaBuilder.struct()
        .field(TS1_COLUMN.name(), Timestamp.builder().optional().build())
        .field(TS2_COLUMN.name().toUpperCase(), Timestamp.SCHEMA)
        .build();
    record = new Struct(schema)
        .put(TS1_COLUMN.name(), null)
        .put(TS2_COLUMN.name().toUpperCase(), TS2);
    assertExtractedOffset(-1, TS2, schema, record, TimestampGranularity.CONNECT_LOGICAL);
  }

  @Test(expected = DataException.class)
  public void extractWithTsColumnNotExisting() throws Exception {
    schema = SchemaBuilder.struct()
        .field(TS1_COLUMN.name(), Timestamp.SCHEMA)
        .build();
    record = new Struct(schema)
        .put(TS1_COLUMN.name(), TS1);
    assertExtractedOffset(-1, TS2, schema, record, TimestampGranularity.CONNECT_LOGICAL);
  }

  @Test(expected = DataException.class)
  public void extractWithConflictingCaseInsensitiveTsColumns() throws Exception {
    String lowerCaseColumnName = "ts1";
    String upperCaseColumnName = "TS1";
    String invalidColumnName = "tS1";

    criteriaTs = new TimestampIncrementingCriteria(
        null,
        Collections.singletonList(new ColumnId(TABLE_ID, invalidColumnName)),
        utcTimeZone
    );

    schema = SchemaBuilder.struct()
        .field(lowerCaseColumnName, Timestamp.SCHEMA)
        .field(upperCaseColumnName, Timestamp.SCHEMA)
        .build();
    record = new Struct(schema)
        .put(lowerCaseColumnName, TS1)
        .put(upperCaseColumnName, TS2);
    criteriaTs.extractValues(schema, record, null, TimestampGranularity.CONNECT_LOGICAL);
  }

  @Test
  public void extractWithCaseSensitiveTsColumnGivenPrecedence() throws Exception {
    String lowerCaseColumnName = "ts1";
    String upperCaseColumnName = "TS1";

    criteriaTs = new TimestampIncrementingCriteria(
        null,
        Collections.singletonList(new ColumnId(TABLE_ID, lowerCaseColumnName)),
        utcTimeZone
    );

    schema = SchemaBuilder.struct()
        .field(lowerCaseColumnName, Timestamp.SCHEMA)
        .field(upperCaseColumnName, Timestamp.SCHEMA)
        .build();
    record = new Struct(schema)
        .put(lowerCaseColumnName, TS1)
        .put(upperCaseColumnName, TS2);
    TimestampIncrementingOffset offset = criteriaTs.extractValues(schema, record, null, TimestampGranularity.CONNECT_LOGICAL);
    assertEquals(TS1, offset.getTimestampOffset());
  }

  @Test
  public void createIncrementingWhereClause() {
    builder = builder();
    criteriaInc.incrementingWhereClause(builder);
    assertEquals(
        " WHERE \"myTable\".\"id\" > ? ORDER BY \"myTable\".\"id\" ASC",
        builder.toString()
    );

    identifierQuoting = QuoteMethod.NEVER;
    builder = builder();
    criteriaInc.incrementingWhereClause(builder);
    assertEquals(
        " WHERE myTable.id > ? ORDER BY myTable.id ASC",
        builder.toString()
    );
  }

  @Test
  public void createTimestampWhereClause() {
    builder = builder();
    criteriaTs.timestampWhereClause(builder);
    assertEquals(
        " WHERE "
        + "COALESCE(\"myTable\".\"ts1\",\"myTable\".\"ts2\") > ? "
        + "AND "
        + "COALESCE(\"myTable\".\"ts1\",\"myTable\".\"ts2\") < ? "
        + "ORDER BY "
        + "COALESCE(\"myTable\".\"ts1\",\"myTable\".\"ts2\") "
        + "ASC",
        builder.toString()
    );

    identifierQuoting = QuoteMethod.NEVER;
    builder = builder();
    criteriaTs.timestampWhereClause(builder);
    assertEquals(
        " WHERE "
        + "COALESCE(myTable.ts1,myTable.ts2) > ? "
        + "AND "
        + "COALESCE(myTable.ts1,myTable.ts2) < ? "
        + "ORDER BY "
        + "COALESCE(myTable.ts1,myTable.ts2) "
        + "ASC",
        builder.toString()
    );
  }

  @Test
  public void createTimestampIncrementingWhereClause() {
    builder = builder();
    criteriaIncTs.timestampIncrementingWhereClause(builder);
    assertEquals(
        " WHERE "
        + "COALESCE(\"myTable\".\"ts1\",\"myTable\".\"ts2\") < ? "
        + "AND ("
        + "(COALESCE(\"myTable\".\"ts1\",\"myTable\".\"ts2\") = ? AND \"myTable\".\"id\" > ?) "
        + "OR "
        + "COALESCE(\"myTable\".\"ts1\",\"myTable\".\"ts2\") > ?) "
        + "ORDER BY COALESCE(\"myTable\".\"ts1\",\"myTable\".\"ts2\"),"
        + "\"myTable\".\"id\" ASC",
        builder.toString()
    );

    identifierQuoting = QuoteMethod.NEVER;
    builder = builder();
    criteriaIncTs.timestampIncrementingWhereClause(builder);
    assertEquals(
        " WHERE "
        + "COALESCE(myTable.ts1,myTable.ts2) < ? "
        + "AND ("
        + "(COALESCE(myTable.ts1,myTable.ts2) = ? AND myTable.id > ?) "
        + "OR "
        + "COALESCE(myTable.ts1,myTable.ts2) > ?) "
        + "ORDER BY COALESCE(myTable.ts1,myTable.ts2),"
        + "myTable.id ASC",
        builder.toString()
    );
  }

  protected ExpressionBuilder builder() {
    ExpressionBuilder result = new ExpressionBuilder(rules);
    result.setQuoteIdentifiers(identifierQuoting);
    return result;
  }
}