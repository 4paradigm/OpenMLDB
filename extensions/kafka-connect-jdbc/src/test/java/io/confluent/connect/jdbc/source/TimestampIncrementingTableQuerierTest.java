/**
 * Copyright 2016 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.connect.jdbc.source;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.util.ExpressionBuilder;
import io.confluent.connect.jdbc.util.TableId;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.annotation.Mock;
import org.powermock.api.easymock.annotation.MockNice;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.TimeZone;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.mock;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.powermock.api.easymock.PowerMock.expectLastCall;
import static org.powermock.api.easymock.PowerMock.mockStatic;
import static org.powermock.api.easymock.PowerMock.replay;
import static org.powermock.api.easymock.PowerMock.replayAll;

@RunWith(PowerMockRunner.class)
@PrepareForTest(SchemaMapping.class)
public class TimestampIncrementingTableQuerierTest {

  private static final Timestamp INITIAL_TS = new Timestamp(71);
  private static final long INITIAL_INC = 4761;
  private static final List<String> TIMESTAMP_COLUMNS = Arrays.asList("ts1", "ts2");
  private static final String INCREMENTING_COLUMN = "inc";

  @Mock
  private PreparedStatement stmt;
  @Mock
  private ResultSet resultSet;
  @Mock
  private Connection db;
  @MockNice
  private ExpressionBuilder expressionBuilder;
  @Mock
  private TimestampIncrementingCriteria criteria;
  @Mock
  private SchemaMapping schemaMapping;
  private DatabaseDialect dialect;

  @Before
  public void setUp() {
    dialect = mock(DatabaseDialect.class);
    mockStatic(SchemaMapping.class);
  }

  private TimestampIncrementingTableQuerier querier(
      TimestampIncrementingOffset initialOffset,
      boolean timestampMode
  ) {
    final String tableName = "table";
    expect(dialect.parseTableIdentifier(tableName)).andReturn(new TableId("", "", tableName));

    // Have to replay the dialect here since it's used to the table ID in the querier's constructor
    replay(dialect);

    return new TimestampIncrementingTableQuerier(
        dialect,
        TableQuerier.QueryMode.TABLE,
        tableName,
        "",
        timestampMode ? TIMESTAMP_COLUMNS : null,
        INCREMENTING_COLUMN,
        initialOffset.toMap(),
        10211197100L, // Timestamp delay
        TimeZone.getTimeZone("UTC"),
        "",
        JdbcSourceConnectorConfig.TimestampGranularity.CONNECT_LOGICAL
    );
  }

  private Schema schema() {
    SchemaBuilder result =SchemaBuilder.struct();
    result.field(INCREMENTING_COLUMN, Schema.INT64_SCHEMA);
    TIMESTAMP_COLUMNS.forEach(
        col -> result.field(col, org.apache.kafka.connect.data.Timestamp.builder().build())
    );
    return result.build();
  }

  private void expectNewQuery() throws Exception {
    expect(dialect.createPreparedStatement(eq(db), anyObject())).andReturn(stmt);
    expect(dialect.expressionBuilder()).andReturn(expressionBuilder);
    expect(dialect.criteriaFor(anyObject(), anyObject())).andReturn(criteria);
    dialect.validateSpecificColumnTypes(anyObject(), anyObject());
    expectLastCall();
    criteria.whereClause(expressionBuilder);
    expectLastCall();
    criteria.setQueryParameters(eq(stmt), anyObject());
    expectLastCall();
    expect(stmt.executeQuery()).andReturn(resultSet);
    expect(resultSet.getMetaData()).andReturn(null);
    expect(SchemaMapping.create(anyObject(), anyObject(), anyObject())).andReturn(schemaMapping);
  }

  @Test
  public void testEmptyResultSet() throws Exception {
    expectNewQuery();
    TimestampIncrementingTableQuerier querier = querier(offset(INITIAL_TS, INITIAL_INC), false);
    expect(resultSet.next()).andReturn(false);

    replayAll();

    querier.maybeStartQuery(db);

    assertFalse(querier.next());
  }

  @Test
  public void testTimestampAndIncrementingMode() throws Exception {
    Timestamp firstNewTimestamp = new Timestamp(INITIAL_TS.getTime() + 1);
    TimestampIncrementingOffset firstNewOffset = offset(firstNewTimestamp, INITIAL_INC + 1);
    TimestampIncrementingOffset secondNewOffset = offset(new Timestamp(INITIAL_TS.getTime() + 2), INITIAL_INC + 2);
    expectNewQuery();
    TimestampIncrementingTableQuerier querier = querier(offset(INITIAL_TS, INITIAL_INC), false);
    expectRecord(firstNewOffset);
    expectRecord(firstNewOffset);
    expectRecord(secondNewOffset);
    expectRecord(secondNewOffset);
    expect(resultSet.next()).andReturn(false);

    replayAll();

    querier.maybeStartQuery(db);

    // We just commit timestamp offsets immediately in this mode since the incrementing column
    // provides an additional layer of granularity; as long as there aren't two updates to the same
    // row that take place with the same timestamp, no data loss should occur
    assertNextRecord(querier, firstNewOffset);
    assertNextRecord(querier, firstNewOffset);
    assertNextRecord(querier, secondNewOffset);
    assertNextRecord(querier, secondNewOffset);

    assertFalse(querier.next());
  }

  @Test
  public void testIncrementingMode() throws Exception {
    TimestampIncrementingOffset firstNewOffset = offset(INITIAL_INC + 1);
    TimestampIncrementingOffset secondNewOffset = offset(INITIAL_INC + 2);
    expectNewQuery();
    TimestampIncrementingTableQuerier querier = querier(offset(INITIAL_INC), true);
    expectRecord(firstNewOffset);
    expectRecord(firstNewOffset);
    expectRecord(secondNewOffset);
    expectRecord(secondNewOffset);
    expect(resultSet.next()).andReturn(false);

    replayAll();

    querier.maybeStartQuery(db);

    // We commit offsets immediately in this mode
    assertNextRecord(querier, firstNewOffset);
    assertNextRecord(querier, firstNewOffset);
    assertNextRecord(querier, secondNewOffset);
    assertNextRecord(querier, secondNewOffset);

    assertFalse(querier.next());
  }

  @Test
  public void testMultipleSingleRecordResultSets() throws Exception {
    TimestampIncrementingOffset initialOffset = offset(INITIAL_TS, INITIAL_INC);
    expectNewQuery();
    expectNewQuery();
    TimestampIncrementingTableQuerier querier = querier(initialOffset, true);
    expectRecord(initialOffset);
    expect(resultSet.next()).andReturn(false);
    expectReset();
    expectRecord(initialOffset);
    expect(resultSet.next()).andReturn(false);

    replayAll();

    querier.maybeStartQuery(db);

    assertNextRecord(querier, initialOffset);

    assertFalse(querier.next());

    querier.reset(0, true);
    querier.maybeStartQuery(db);

    assertNextRecord(querier, initialOffset);

    assertFalse(querier.next());
  }

  private void assertNextRecord(
      TimestampIncrementingTableQuerier querier, TimestampIncrementingOffset offset
  ) throws Exception {
    assertTrue(querier.next());
    assertEquals(offset.toMap(), querier.extractRecord().sourceOffset());
  }

  private void expectRecord(TimestampIncrementingOffset offset) throws Exception {
    expect(schemaMapping.schema()).andReturn(schema()).times(2);
    expect(resultSet.next()).andReturn(true);
    expect(schemaMapping.fieldSetters()).andReturn(Collections.emptyList());
    expect(criteria.extractValues(anyObject(), anyObject(), anyObject(), anyObject())).andReturn(offset);
  }

  private void expectReset() throws Exception {
    resultSet.close();
    expectLastCall();
    stmt.close();
    expectLastCall();
    db.commit();
    expectLastCall();
  }

  private static TimestampIncrementingOffset offset(Long inc) {
    return new TimestampIncrementingOffset(null, inc);
  }

  private static TimestampIncrementingOffset offset(Timestamp ts, Long inc) {
    return new TimestampIncrementingOffset(ts, inc);
  }
}
