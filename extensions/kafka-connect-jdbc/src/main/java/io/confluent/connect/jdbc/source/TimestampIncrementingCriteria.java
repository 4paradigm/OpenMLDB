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

import java.util.ArrayList;
import java.util.Map;
import java.util.TimeZone;

import io.confluent.connect.jdbc.util.LruCache;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import io.confluent.connect.jdbc.util.ColumnId;
import io.confluent.connect.jdbc.util.DateTimeUtils;
import io.confluent.connect.jdbc.util.ExpressionBuilder;

public class TimestampIncrementingCriteria {

  /**
   * The values that can be used in a statement's WHERE clause.
   */
  public interface CriteriaValues {

    /**
     * Get the beginning of the time period.
     *
     * @return the beginning timestamp; may be null
     * @throws SQLException if there is a problem accessing the value
     */
    Timestamp beginTimestampValue() throws SQLException;

    /**
     * Get the end of the time period.
     *
     * @return the ending timestamp; never null
     * @throws SQLException if there is a problem accessing the value
     */
    Timestamp endTimestampValue() throws SQLException;

    /**
     * Get the last incremented value seen.
     *
     * @return the last incremented value from one of the rows
     * @throws SQLException if there is a problem accessing the value
     */
    Long lastIncrementedValue() throws SQLException;
  }

  protected static final BigDecimal LONG_MAX_VALUE_AS_BIGDEC = new BigDecimal(Long.MAX_VALUE);

  protected final Logger log = LoggerFactory.getLogger(getClass());
  protected final List<ColumnId> timestampColumns;
  protected final ColumnId incrementingColumn;
  protected final TimeZone timeZone;
  private final LruCache<Schema, List<String>> caseAdjustedTimestampColumns;


  public TimestampIncrementingCriteria(
      ColumnId incrementingColumn,
      List<ColumnId> timestampColumns,
      TimeZone timeZone
  ) {
    this.timestampColumns =
        timestampColumns != null ? timestampColumns : Collections.<ColumnId>emptyList();
    this.incrementingColumn = incrementingColumn;
    this.timeZone = timeZone;
    this.caseAdjustedTimestampColumns =
        timestampColumns != null ? new LruCache<>(16) : null;
  }

  protected boolean hasTimestampColumns() {
    return !timestampColumns.isEmpty();
  }

  protected boolean hasIncrementedColumn() {
    return incrementingColumn != null;
  }

  /**
   * Build the WHERE clause for the columns used in this criteria.
   *
   * @param builder the string builder to which the WHERE clause should be appended; never null
   */
  public void whereClause(ExpressionBuilder builder) {
    if (hasTimestampColumns() && hasIncrementedColumn()) {
      timestampIncrementingWhereClause(builder);
    } else if (hasTimestampColumns()) {
      timestampWhereClause(builder);
    } else if (hasIncrementedColumn()) {
      incrementingWhereClause(builder);
    }
  }

  /**
   * Set the query parameters on the prepared statement whose WHERE clause was generated with the
   * previous call to {@link #whereClause(ExpressionBuilder)}.
   *
   * @param stmt   the prepared statement; never null
   * @param values the values that can be used in the criteria parameters; never null
   * @throws SQLException if there is a problem using the prepared statement
   */
  public void setQueryParameters(
      PreparedStatement stmt,
      CriteriaValues values
  ) throws SQLException {
    if (hasTimestampColumns() && hasIncrementedColumn()) {
      setQueryParametersTimestampIncrementing(stmt, values);
    } else if (hasTimestampColumns()) {
      setQueryParametersTimestamp(stmt, values);
    } else if (hasIncrementedColumn()) {
      setQueryParametersIncrementing(stmt, values);
    }
  }

  protected void setQueryParametersTimestampIncrementing(
      PreparedStatement stmt,
      CriteriaValues values
  ) throws SQLException {
    Timestamp beginTime = values.beginTimestampValue();
    Timestamp endTime = values.endTimestampValue();
    Long incOffset = values.lastIncrementedValue();
    stmt.setTimestamp(1, endTime, DateTimeUtils.getTimeZoneCalendar(timeZone));
    stmt.setTimestamp(2, beginTime, DateTimeUtils.getTimeZoneCalendar(timeZone));
    stmt.setLong(3, incOffset);
    stmt.setTimestamp(4, beginTime, DateTimeUtils.getTimeZoneCalendar(timeZone));
    log.debug(
        "Executing prepared statement with start time value = {} end time = {} and incrementing"
        + " value = {}", DateTimeUtils.formatTimestamp(beginTime, timeZone),
        DateTimeUtils.formatTimestamp(endTime, timeZone), incOffset
    );
  }

  protected void setQueryParametersIncrementing(
      PreparedStatement stmt,
      CriteriaValues values
  ) throws SQLException {
    Long incOffset = values.lastIncrementedValue();
    stmt.setLong(1, incOffset);
    log.debug("Executing prepared statement with incrementing value = {}", incOffset);
  }

  protected void setQueryParametersTimestamp(
      PreparedStatement stmt,
      CriteriaValues values
  ) throws SQLException {
    Timestamp beginTime = values.beginTimestampValue();
    Timestamp endTime = values.endTimestampValue();
    stmt.setTimestamp(1, beginTime, DateTimeUtils.getTimeZoneCalendar(timeZone));
    stmt.setTimestamp(2, endTime, DateTimeUtils.getTimeZoneCalendar(timeZone));
    log.debug("Executing prepared statement with timestamp value = {} end time = {}",
        DateTimeUtils.formatTimestamp(beginTime, timeZone),
        DateTimeUtils.formatTimestamp(endTime, timeZone)
    );
  }

  /**
   * Extract the offset values from the row.
   *
   * @param schema the record's schema; never null
   * @param record the record's struct; never null
   * @param previousOffset a previous timestamp offset if the table has timestamp columns
   * @param timestampGranularity defines the configured granularity of the timestamp field
   * @return the timestamp for this row; may not be null
   */
  public TimestampIncrementingOffset extractValues(
      Schema schema,
      Struct record,
      TimestampIncrementingOffset previousOffset,
      JdbcSourceConnectorConfig.TimestampGranularity timestampGranularity
  ) {
    Timestamp extractedTimestamp = null;
    if (hasTimestampColumns()) {
      extractedTimestamp = extractOffsetTimestamp(schema, record, timestampGranularity);
      assert previousOffset == null || (previousOffset.getTimestampOffset() != null
                                        && previousOffset.getTimestampOffset().compareTo(
          extractedTimestamp) <= 0
      );
    }
    Long extractedId = null;
    if (hasIncrementedColumn()) {
      extractedId = extractOffsetIncrementedId(schema, record);

      // If we are only using an incrementing column, then this must be incrementing.
      // If we are also using a timestamp, then we may see updates to older rows.
      assert previousOffset == null || previousOffset.getIncrementingOffset() == -1L
             || extractedId > previousOffset.getIncrementingOffset() || hasTimestampColumns();
    }
    return new TimestampIncrementingOffset(extractedTimestamp, extractedId);
  }

  /**
   * Extract the timestamp from the row.
   *
   * @param schema the record's schema; never null
   * @param record the record's struct; never null
   * @param timestampGranularity defines the configured granularity of the timestamp field
   * @return the timestamp for this row; may not be null
   */
  protected Timestamp extractOffsetTimestamp(
      Schema schema,
      Struct record,
      JdbcSourceConnectorConfig.TimestampGranularity timestampGranularity
  ) {
    caseAdjustedTimestampColumns.computeIfAbsent(schema, this::findCaseSensitiveTimestampColumns);
    for (String timestampColumn : caseAdjustedTimestampColumns.get(schema)) {
      Timestamp ts = timestampGranularity.toTimestamp.apply(record.get(timestampColumn), timeZone);
      if (ts != null) {
        return ts;
      }
    }
    return null;
  }

  /**
   * Extract the incrementing column value from the row.
   *
   * @param schema the record's schema; never null
   * @param record the record's struct; never null
   * @return the incrementing ID for this row; may not be null
   */
  protected Long extractOffsetIncrementedId(
      Schema schema,
      Struct record
  ) {
    final Long extractedId;
    final Field field = schema.field(incrementingColumn.name());
    if (field == null) {
      throw new DataException("Incrementing column " + incrementingColumn.name() + " not found in "
              + schema.fields().stream().map(Field::name).collect(Collectors.joining(",")));
    }

    final Schema incrementingColumnSchema = field.schema();
    final Object incrementingColumnValue = record.get(incrementingColumn.name());
    if (incrementingColumnValue == null) {
      throw new ConnectException(
          "Null value for incrementing column of type: " + incrementingColumnSchema.type());
    } else if (isIntegralPrimitiveType(incrementingColumnValue)) {
      extractedId = ((Number) incrementingColumnValue).longValue();
    } else if (incrementingColumnSchema.name() != null && incrementingColumnSchema.name().equals(
        Decimal.LOGICAL_NAME)) {
      extractedId = extractDecimalId(incrementingColumnValue);
    } else {
      throw new ConnectException(
          "Invalid type for incrementing column: " + incrementingColumnSchema.type());
    }
    log.trace("Extracted incrementing column value: {}", extractedId);
    return extractedId;
  }

  protected Long extractDecimalId(Object incrementingColumnValue) {
    final BigDecimal decimal = ((BigDecimal) incrementingColumnValue);
    if (decimal.compareTo(LONG_MAX_VALUE_AS_BIGDEC) > 0) {
      throw new ConnectException("Decimal value for incrementing column exceeded Long.MAX_VALUE");
    }
    if (decimal.scale() != 0) {
      throw new ConnectException("Scale of Decimal value for incrementing column must be 0");
    }
    return decimal.longValue();
  }

  protected boolean isIntegralPrimitiveType(Object incrementingColumnValue) {
    return incrementingColumnValue instanceof Long || incrementingColumnValue instanceof Integer
           || incrementingColumnValue instanceof Short || incrementingColumnValue instanceof Byte;
  }

  protected String coalesceTimestampColumns(ExpressionBuilder builder) {
    if (timestampColumns.size() == 1) {
      builder.append(timestampColumns.get(0));
    } else {
      builder.append("COALESCE(");
      builder.appendList().delimitedBy(",").of(timestampColumns);
      builder.append(")");
    }
    return builder.toString();
  }

  protected void timestampIncrementingWhereClause(ExpressionBuilder builder) {
    // This version combines two possible conditions. The first checks timestamp == last
    // timestamp and incrementing > last incrementing. The timestamp alone would include
    // duplicates, but adding the incrementing condition ensures no duplicates, e.g. you would
    // get only the row with id = 23:
    //  timestamp 1234, id 22 <- last
    //  timestamp 1234, id 23
    // The second check only uses the timestamp > last timestamp. This covers everything new,
    // even if it is an update of the existing row. If we previously had:
    //  timestamp 1234, id 22 <- last
    // and then these rows were written:
    //  timestamp 1235, id 22
    //  timestamp 1236, id 23
    // We should capture both id = 22 (an update) and id = 23 (a new row)
    builder.append(" WHERE ");
    coalesceTimestampColumns(builder);
    builder.append(" < ? AND ((");
    coalesceTimestampColumns(builder);
    builder.append(" = ? AND ");
    builder.append(incrementingColumn);
    builder.append(" > ?");
    builder.append(") OR ");
    coalesceTimestampColumns(builder);
    builder.append(" > ?)");
    builder.append(" ORDER BY ");
    coalesceTimestampColumns(builder);
    builder.append(",");
    builder.append(incrementingColumn);
    builder.append(" ASC");
  }

  protected void incrementingWhereClause(ExpressionBuilder builder) {
    builder.append(" WHERE ");
    builder.append(incrementingColumn);
    builder.append(" > ?");
    builder.append(" ORDER BY ");
    builder.append(incrementingColumn);
    builder.append(" ASC");
  }

  protected void timestampWhereClause(ExpressionBuilder builder) {
    builder.append(" WHERE ");
    coalesceTimestampColumns(builder);
    builder.append(" > ? AND ");
    coalesceTimestampColumns(builder);
    builder.append(" < ? ORDER BY ");
    coalesceTimestampColumns(builder);
    builder.append(" ASC");
  }

  private List<String> findCaseSensitiveTimestampColumns(Schema schema) {
    Map<String, List<String>> caseInsensitiveColumns = schema.fields().stream()
        .map(Field::name)
        .collect(Collectors.groupingBy(String::toLowerCase));

    List<String> result = new ArrayList<>();
    for (ColumnId timestampColumn : timestampColumns) {
      String columnName = timestampColumn.name();
      if (schema.field(columnName) != null) {
        log.trace(
            "Timestamp column name {} case-sensitively matches column read from database",
            columnName
        );
        result.add(columnName);
      } else {
        log.debug(
            "Timestamp column name {} not found in columns read from database; "
                + "falling back to a case-insensitive search",
            columnName
        );
        List<String> caseInsensitiveMatches = caseInsensitiveColumns.get(columnName.toLowerCase());
        if (caseInsensitiveMatches == null || caseInsensitiveMatches.isEmpty()) {
          throw new DataException("Timestamp column " + columnName + " not found in "
              + schema.fields().stream().map(Field::name).collect(Collectors.joining(",")));
        } else if (caseInsensitiveMatches.size() > 1) {
          throw new DataException("Timestamp column " + columnName
              + " not found in columns read from database: "
              + schema.fields().stream().map(Field::name).collect(Collectors.joining(",")) + ". "
              + "Could not fall back to case-insensitively selecting a column "
              + "because there were multiple columns whose names "
              + "case-insensitively matched the specified name: "
              + String.join(",", caseInsensitiveMatches) + ". "
              + "To force the connector to choose between these columns, "
              + "specify a value for the timestamp column configuration property "
              + "that matches the desired column case-sensitively."
          );
        } else {
          String caseAdjustedColumnName = caseInsensitiveMatches.get(0);
          log.debug(
              "Falling back on column {} for user-specified timestamp column {} "
                  + "(this is the only column that case-insensitively matches)",
              caseAdjustedColumnName,
              columnName
          );
          result.add(caseAdjustedColumnName);
        }
      }
    }

    return result;
  }

}
