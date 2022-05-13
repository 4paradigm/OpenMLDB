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

import java.util.TimeZone;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig.TimestampGranularity;
import io.confluent.connect.jdbc.source.SchemaMapping.FieldSetter;

/**
 * A specialized subclass of the {@link TimestampIncrementingTableQuerier} that only advances the
 * to-be-committed timestamp offset for its records after either all rows have been read from the
 * current table query, or all rows with the to-be-committed timestamp have been read successfully
 * (and the next row has a timestamp that is strictly greater than it).
 * This prevents data loss in cases where the table has multiple rows with identical timestamps and
 * the connector is shut down in the middle of reading these rows. However, if the connector is
 * configured with additional query logic (such as a suffix containing a LIMIT clause), data loss
 * may still occur, since the timestamp offset for the last row of each query is unconditionally
 * committed.
 */
public class TimestampTableQuerier extends TimestampIncrementingTableQuerier {
  private static final Logger log = LoggerFactory.getLogger(TimestampTableQuerier.class);

  private boolean exhaustedResultSet;
  private PendingRecord nextRecord;
  private Timestamp latestCommittableTimestamp;

  public TimestampTableQuerier(
      DatabaseDialect dialect,
      QueryMode mode,
      String name,
      String topicPrefix,
      List<String> timestampColumnNames,
      Map<String, Object> offsetMap,
      Long timestampDelay,
      TimeZone timeZone,
      String suffix,
      TimestampGranularity timestampGranularity
  ) {
    super(
        dialect,
        mode,
        name,
        topicPrefix,
        timestampColumnNames,
        null,
        offsetMap,
        timestampDelay,
        timeZone,
        suffix,
        timestampGranularity
    );

    this.latestCommittableTimestamp = this.offset.getTimestampOffset();
    this.exhaustedResultSet = false;
    this.nextRecord = null;
  }

  @Override
  public boolean next() throws SQLException {
    if (exhaustedResultSet && nextRecord == null) {
      // We've reached the end of the result set and returned our cached record; nothing left until
      // we execute a new query
      return false;
    }

    if (nextRecord == null) {
      // We haven't read any rows from the table yet; happens when this method is invoked for the
      // first time after a new query is executed
      if (resultSet.next()) {
        // At least one row is available; cache it immediately
        nextRecord = doExtractRecord();
      } else {
        // No rows were available from the result set; make note that we've exhausted it, and
        // indicate to the caller that no more rows are available until a new query is executed
        exhaustedResultSet = true;
        return false;
      }
    }

    if (!resultSet.next()) {
      exhaustedResultSet = true;
    }
    // Even if we've exhausted the result set, our cached record is still available
    return true;
  }

  @Override
  protected ResultSet executeQuery() throws SQLException {
    ResultSet result = super.executeQuery();
    exhaustedResultSet = false;
    return result;
  }

  @Override
  public SourceRecord extractRecord() {
    if (nextRecord == null) {
      throw new IllegalStateException("No more records are available");
    }
    PendingRecord currentRecord = nextRecord;
    nextRecord = exhaustedResultSet ? null : doExtractRecord();
    if (nextRecord == null
        || canCommitTimestamp(currentRecord.timestamp(), nextRecord.timestamp())) {
      latestCommittableTimestamp = currentRecord.timestamp();
    }
    return currentRecord.record(latestCommittableTimestamp);
  }

  private PendingRecord doExtractRecord() {
    Struct record = new Struct(schemaMapping.schema());
    for (FieldSetter setter : schemaMapping.fieldSetters()) {
      try {
        setter.setField(record, resultSet);
      } catch (IOException e) {
        log.warn("Error mapping fields into Connect record", e);
        throw new ConnectException(e);
      } catch (SQLException e) {
        log.warn("SQL error mapping fields into Connect record", e);
        throw new DataException(e);
      }
    }
    this.offset = criteria.extractValues(schemaMapping.schema(), record, offset,
        timestampGranularity);
    Timestamp timestamp = offset.hasTimestampOffset() ? offset.getTimestampOffset() : null;
    return new PendingRecord(partition, timestamp, topic, record.schema(), record);
  }

  private boolean canCommitTimestamp(Timestamp current, Timestamp next) {
    return current == null || next == null || current.before(next);
  }

  @Override
  public void reset(long now, boolean resetOffset) {
    this.nextRecord = null;
    super.reset(now, resetOffset);
  }

  @Override
  public String toString() {
    return "TimestampTableQuerier{"
        + "table=" + tableId
        + ", query='" + query + '\''
        + ", topicPrefix='" + topicPrefix + '\''
        + ", timestampColumns=" + timestampColumnNames
        + '}';
  }

  private static class PendingRecord {
    private final Map<String, String> partition;
    private final Timestamp timestamp;
    private final String topic;
    private final Schema valueSchema;
    private final Object value;

    public PendingRecord(
        Map<String, String> partition,
        Timestamp timestamp,
        String topic,
        Schema valueSchema,
        Object value
    ) {
      this.partition = partition;
      this.timestamp = timestamp;
      this.topic = topic;
      this.valueSchema = valueSchema;
      this.value = value;
    }

    /**
     * @return the timestamp value for the row that generated this record
     */
    public Timestamp timestamp() {
      return timestamp;
    }

    /**
     * @param offsetTimestamp the timestamp to use for the record's offset; may be null
     * @return a {@link SourceRecord} whose source offset contains the provided timestamp
     */
    public SourceRecord record(Timestamp offsetTimestamp) {
      TimestampIncrementingOffset offset = new TimestampIncrementingOffset(offsetTimestamp, null);
      return new SourceRecord(
          partition, offset.toMap(), topic, valueSchema, value
      );
    }
  }
}
