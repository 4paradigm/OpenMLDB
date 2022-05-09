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
import org.apache.kafka.connect.source.SourceRecord;
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
public class TimestampTableQuerierTest {

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

  private TimestampIncrementingTableQuerier querier(Timestamp initialTimestampOffset) {
    final String tableName = "table";
    expect(dialect.parseTableIdentifier(tableName)).andReturn(new TableId("", "", tableName));

    // Have to replay the dialect here since it's used to the table ID in the querier's constructor
    replay(dialect);

    return new TimestampTableQuerier(
        dialect,
        TableQuerier.QueryMode.TABLE,
        tableName,
        "",
        TIMESTAMP_COLUMNS,
        new TimestampIncrementingOffset(initialTimestampOffset, null).toMap(),
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
    TimestampIncrementingTableQuerier querier = querier(INITIAL_TS);
    expect(resultSet.next()).andReturn(false);

    replayAll();

    querier.maybeStartQuery(db);

    assertFalse(querier.next());
  }

  @Test
  public void testSingleRecordInResultSet() throws Exception {
    Timestamp newTimestamp = new Timestamp(INITIAL_TS.getTime() + 1);
    expectNewQuery();
    TimestampIncrementingTableQuerier querier = querier(INITIAL_TS);
    expectRecord(newTimestamp);
    expect(resultSet.next()).andReturn(false);

    replayAll();

    querier.maybeStartQuery(db);

    assertNextRecord(querier, newTimestamp);

    assertFalse(querier.next());
  }

  @Test
  public void testTwoRecordsWithSameTimestampInResultSet() throws Exception {
    Timestamp newTimestamp = new Timestamp(INITIAL_TS.getTime() + 1);
    expectNewQuery();
    TimestampIncrementingTableQuerier querier = querier(INITIAL_TS);
    expectRecord(newTimestamp);
    expectRecord(newTimestamp);
    expect(resultSet.next()).andReturn(false);

    replayAll();

    querier.maybeStartQuery(db);

    // This isn't the last record in the result set with the new timestamp, so we can't commit an
    // offset that includes that timestamp yet
    assertNextRecord(querier, INITIAL_TS);

    // Now we can commit an offset with that timestamp, since this is the last record in the result
    // set
    assertNextRecord(querier, newTimestamp);

    assertFalse(querier.next());
  }

  @Test
  public void testTwoRecordsWithSameTimestampFollowedByRecordWithNewTimestampInResultSet() throws Exception {
    Timestamp firstNewTimestamp = new Timestamp(INITIAL_TS.getTime() + 1);
    Timestamp secondNewTimestamp = new Timestamp(INITIAL_TS.getTime() + 2);
    expectNewQuery();
    TimestampIncrementingTableQuerier querier = querier(INITIAL_TS);
    expectRecord(firstNewTimestamp);
    expectRecord(firstNewTimestamp);
    expectRecord(secondNewTimestamp);
    expect(resultSet.next()).andReturn(false);

    replayAll();

    querier.maybeStartQuery(db);

    // This isn't the last record in the result set with the new timestamp, so we can't commit an
    // offset that includes that timestamp yet
    assertNextRecord(querier, INITIAL_TS);

    // Now we can commit an offset with that timestamp, since this is the last record in the result
    // set
    assertNextRecord(querier, firstNewTimestamp);

    // And again, now we can commit an offset with the second new timestamp as we've exhausted the
    // batch
    assertNextRecord(querier, secondNewTimestamp);

    assertFalse(querier.next());
  }

  @Test
  public void testTwoRecordsWithSameTimestampFollowedByTwoRecordsWithNewTimestampInResultSet() throws Exception {
    Timestamp firstNewTimestamp = new Timestamp(INITIAL_TS.getTime() + 1);
    Timestamp secondNewTimestamp = new Timestamp(INITIAL_TS.getTime() + 2);
    expectNewQuery();
    TimestampIncrementingTableQuerier querier = querier(INITIAL_TS);
    expectRecord(firstNewTimestamp);
    expectRecord(firstNewTimestamp);
    expectRecord(secondNewTimestamp);
    expectRecord(secondNewTimestamp);
    expect(resultSet.next()).andReturn(false);

    replayAll();

    querier.maybeStartQuery(db);

    // This isn't the last record in the result set with the new timestamp, so we can't commit an
    // offset that includes that timestamp yet
    assertNextRecord(querier, INITIAL_TS);

    // Now we can commit an offset with that timestamp, since this is the last record in the result
    // set
    assertNextRecord(querier, firstNewTimestamp);

    // Again, have to reuse the timestamp since there's another record waiting that has the same
    // timestamp as the one we're about to query
    assertNextRecord(querier, firstNewTimestamp);

    // And again, now we can commit an offset with the second new timestamp as we've exhausted the
    // batch
    assertNextRecord(querier, secondNewTimestamp);

    assertFalse(querier.next());
  }

  @Test
  public void testMultipleSingleRecordResultSets() throws Exception {
    expectNewQuery();
    expectNewQuery();
    TimestampIncrementingTableQuerier querier = querier(INITIAL_TS);
    expectRecord(INITIAL_TS);
    expect(resultSet.next()).andReturn(false);
    expectReset();
    expectRecord(INITIAL_TS);
    expect(resultSet.next()).andReturn(false);

    replayAll();

    querier.maybeStartQuery(db);

    // We have to commit for the last record in the batch
    assertNextRecord(querier, INITIAL_TS);

    assertFalse(querier.next());

    querier.reset(0, true);
    querier.maybeStartQuery(db);

    // We have to commit for the last record in the batch
    assertNextRecord(querier, INITIAL_TS);

    assertFalse(querier.next());
  }

  @Test
  public void testMultipleDoubleRecordResultSetsOffsetReset() throws Exception {
    Timestamp firstNewTimestamp = new Timestamp(INITIAL_TS.getTime() + 1);
    expectNewQuery();
    expectNewQuery();
    TimestampIncrementingTableQuerier querier = querier(INITIAL_TS);
    expectRecord(INITIAL_TS);
    expectRecord(firstNewTimestamp);
    expect(resultSet.next()).andReturn(false);
    expectReset();

    replayAll();

    querier.maybeStartQuery(db);

    // We have to commit for the last record in the batch
    assertNextRecord(querier, INITIAL_TS);
    assertNextRecord(querier, firstNewTimestamp);

    assertFalse(querier.next());

    querier.reset(0, true);
    querier.maybeStartQuery(db);

    assertEquals(querier.offset.getTimestampOffset(), INITIAL_TS);
  }

  @Test
  public void testMultipleDoubleRecordResultSetsNoOffsetReset() throws Exception {
    Timestamp firstNewTimestamp = new Timestamp(INITIAL_TS.getTime() + 1);
    expectNewQuery();
    expectNewQuery();
    TimestampIncrementingTableQuerier querier = querier(INITIAL_TS);
    expectRecord(INITIAL_TS);
    expectRecord(firstNewTimestamp);
    expect(resultSet.next()).andReturn(false);
    expectReset();

    replayAll();

    querier.maybeStartQuery(db);

    // We have to commit for the last record in the batch
    assertNextRecord(querier, INITIAL_TS);
    assertNextRecord(querier, firstNewTimestamp);

    assertFalse(querier.next());

    querier.reset(0, false);
    querier.maybeStartQuery(db);

    assertEquals(querier.offset.getTimestampOffset(), firstNewTimestamp);
  }

  private void assertNextRecord(
      TimestampIncrementingTableQuerier querier, Timestamp expectedTimestampOffset
  ) throws Exception {
    assertTrue(querier.next());
    SourceRecord record = querier.extractRecord();
    TimestampIncrementingOffset actualOffset =TimestampIncrementingOffset.fromMap(record.sourceOffset()); 
    assertEquals(expectedTimestampOffset, actualOffset.getTimestampOffset());
  }

  private void expectRecord(Timestamp timestamp) throws Exception {
    expect(schemaMapping.schema()).andReturn(schema()).times(2);
    expect(resultSet.next()).andReturn(true);
    expect(schemaMapping.fieldSetters()).andReturn(Collections.emptyList());
    TimestampIncrementingOffset offset = new TimestampIncrementingOffset(timestamp, null);
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

  private static TimestampIncrementingOffset offset(Timestamp ts) {
    return offset(ts, null);
  }

  private static TimestampIncrementingOffset offset(Timestamp ts, Long inc) {
    return new TimestampIncrementingOffset(ts, inc);
  }
}
