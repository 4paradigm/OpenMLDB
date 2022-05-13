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

import java.sql.SQLNonTransientException;
import java.util.TimeZone;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.dialect.DatabaseDialects;
import io.confluent.connect.jdbc.util.CachedConnectionProvider;
import io.confluent.connect.jdbc.util.ColumnDefinition;
import io.confluent.connect.jdbc.util.ColumnId;
import io.confluent.connect.jdbc.util.TableId;
import io.confluent.connect.jdbc.util.Version;
import io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig.TransactionIsolationMode;

/**
 * JdbcSourceTask is a Kafka Connect SourceTask implementation that reads from JDBC databases and
 * generates Kafka Connect records.
 */
public class JdbcSourceTask extends SourceTask {
  // When no results, periodically return control flow to caller to give it a chance to pause us.
  private static final int CONSECUTIVE_EMPTY_RESULTS_BEFORE_RETURN = 3;

  private static final Logger log = LoggerFactory.getLogger(JdbcSourceTask.class);

  private Time time;
  private JdbcSourceTaskConfig config;
  private DatabaseDialect dialect;
  //Visible for Testing
  CachedConnectionProvider cachedConnectionProvider;
  PriorityQueue<TableQuerier> tableQueue = new PriorityQueue<>();
  private final AtomicBoolean running = new AtomicBoolean(false);
  private final AtomicLong taskThreadId = new AtomicLong(0);

  int maxRetriesPerQuerier;

  public JdbcSourceTask() {
    this.time = new SystemTime();
  }

  public JdbcSourceTask(Time time) {
    this.time = time;
  }

  @Override
  public String version() {
    return Version.getVersion();
  }

  @Override
  public void start(Map<String, String> properties) {
    log.info("Starting JDBC source task");
    try {
      config = new JdbcSourceTaskConfig(properties);
    } catch (ConfigException e) {
      throw new ConnectException("Couldn't start JdbcSourceTask due to configuration error", e);
    }

    List<String> tables = config.getList(JdbcSourceTaskConfig.TABLES_CONFIG);
    String query = config.getString(JdbcSourceTaskConfig.QUERY_CONFIG);

    if ((tables.isEmpty() && query.isEmpty())) {
      throw new ConnectException("Task is being killed because"
              + " it was not assigned a table nor a query to execute."
              + " If run in table mode please make sure that the tables"
              + " exist on the database. If the table does exist on"
              + " the database, we recommend using the fully qualified"
              + " table name.");
    }

    if ((!tables.isEmpty() && !query.isEmpty())) {
      throw new ConnectException("Invalid configuration: a JdbcSourceTask"
              + " cannot have both a table and a query assigned to it");
    }


    final String url = config.getString(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG);
    final int maxConnAttempts = config.getInt(JdbcSourceConnectorConfig.CONNECTION_ATTEMPTS_CONFIG);
    final long retryBackoff = config.getLong(JdbcSourceConnectorConfig.CONNECTION_BACKOFF_CONFIG);

    final String dialectName = config.getString(JdbcSourceConnectorConfig.DIALECT_NAME_CONFIG);
    if (dialectName != null && !dialectName.trim().isEmpty()) {
      dialect = DatabaseDialects.create(dialectName, config);
    } else {
      dialect = DatabaseDialects.findBestFor(url, config);
    }
    log.info("Using JDBC dialect {}", dialect.name());

    cachedConnectionProvider = connectionProvider(maxConnAttempts, retryBackoff);


    dialect.setConnectionIsolationMode(
            cachedConnectionProvider.getConnection(),
            TransactionIsolationMode
                    .valueOf(
                            config.getString(
                                    JdbcSourceConnectorConfig
                                            .TRANSACTION_ISOLATION_MODE_CONFIG
                            )
                    )
    );
    TableQuerier.QueryMode queryMode = !query.isEmpty() ? TableQuerier.QueryMode.QUERY :
                                       TableQuerier.QueryMode.TABLE;
    List<String> tablesOrQuery = queryMode == TableQuerier.QueryMode.QUERY
                                 ? Collections.singletonList(query) : tables;

    String mode = config.getString(JdbcSourceTaskConfig.MODE_CONFIG);
    //used only in table mode
    Map<String, List<Map<String, String>>> partitionsByTableFqn = new HashMap<>();
    Map<Map<String, String>, Map<String, Object>> offsets = null;
    if (mode.equals(JdbcSourceTaskConfig.MODE_INCREMENTING)
        || mode.equals(JdbcSourceTaskConfig.MODE_TIMESTAMP)
        || mode.equals(JdbcSourceTaskConfig.MODE_TIMESTAMP_INCREMENTING)) {
      List<Map<String, String>> partitions = new ArrayList<>(tables.size());
      switch (queryMode) {
        case TABLE:
          log.trace("Starting in TABLE mode");
          for (String table : tables) {
            // Find possible partition maps for different offset protocols
            // We need to search by all offset protocol partition keys to support compatibility
            List<Map<String, String>> tablePartitions = possibleTablePartitions(table);
            partitions.addAll(tablePartitions);
            partitionsByTableFqn.put(table, tablePartitions);
          }
          break;
        case QUERY:
          log.trace("Starting in QUERY mode");
          partitions.add(Collections.singletonMap(JdbcSourceConnectorConstants.QUERY_NAME_KEY,
                                                  JdbcSourceConnectorConstants.QUERY_NAME_VALUE));
          break;
        default:
          throw new ConnectException("Unknown query mode: " + queryMode);
      }
      offsets = context.offsetStorageReader().offsets(partitions);
      log.trace("The partition offsets are {}", offsets);
    }

    String incrementingColumn
        = config.getString(JdbcSourceTaskConfig.INCREMENTING_COLUMN_NAME_CONFIG);
    List<String> timestampColumns
        = config.getList(JdbcSourceTaskConfig.TIMESTAMP_COLUMN_NAME_CONFIG);
    Long timestampDelayInterval
        = config.getLong(JdbcSourceTaskConfig.TIMESTAMP_DELAY_INTERVAL_MS_CONFIG);
    boolean validateNonNulls
        = config.getBoolean(JdbcSourceTaskConfig.VALIDATE_NON_NULL_CONFIG);
    TimeZone timeZone = config.timeZone();
    String suffix = config.getString(JdbcSourceTaskConfig.QUERY_SUFFIX_CONFIG).trim();

    for (String tableOrQuery : tablesOrQuery) {
      final List<Map<String, String>> tablePartitionsToCheck;
      final Map<String, String> partition;
      switch (queryMode) {
        case TABLE:
          if (validateNonNulls) {
            validateNonNullable(
                mode,
                tableOrQuery,
                incrementingColumn,
                timestampColumns
            );
          }
          tablePartitionsToCheck = partitionsByTableFqn.get(tableOrQuery);
          break;
        case QUERY:
          partition = Collections.singletonMap(
              JdbcSourceConnectorConstants.QUERY_NAME_KEY,
              JdbcSourceConnectorConstants.QUERY_NAME_VALUE
          );
          tablePartitionsToCheck = Collections.singletonList(partition);
          break;
        default:
          throw new ConnectException("Unexpected query mode: " + queryMode);
      }

      // The partition map varies by offset protocol. Since we don't know which protocol each
      // table's offsets are keyed by, we need to use the different possible partitions
      // (newest protocol version first) to find the actual offsets for each table.
      Map<String, Object> offset = null;
      if (offsets != null) {
        for (Map<String, String> toCheckPartition : tablePartitionsToCheck) {
          offset = offsets.get(toCheckPartition);
          if (offset != null) {
            log.info("Found offset {} for partition {}", offsets, toCheckPartition);
            break;
          }
        }
      }
      offset = computeInitialOffset(tableOrQuery, offset, timeZone);

      String topicPrefix = config.topicPrefix();
      JdbcSourceConnectorConfig.TimestampGranularity timestampGranularity
          = JdbcSourceConnectorConfig.TimestampGranularity.get(config);

      if (mode.equals(JdbcSourceTaskConfig.MODE_BULK)) {
        tableQueue.add(
            new BulkTableQuerier(
                dialect, 
                queryMode, 
                tableOrQuery, 
                topicPrefix, 
                suffix
            )
        );
      } else if (mode.equals(JdbcSourceTaskConfig.MODE_INCREMENTING)) {
        tableQueue.add(
            new TimestampIncrementingTableQuerier(
                dialect,
                queryMode,
                tableOrQuery,
                topicPrefix,
                null,
                incrementingColumn,
                offset,
                timestampDelayInterval,
                timeZone,
                suffix,
                timestampGranularity
            )
        );
      } else if (mode.equals(JdbcSourceTaskConfig.MODE_TIMESTAMP)) {
        tableQueue.add(
            new TimestampTableQuerier(
                dialect,
                queryMode,
                tableOrQuery,
                topicPrefix,
                timestampColumns,
                offset,
                timestampDelayInterval,
                timeZone,
                suffix,
                timestampGranularity
            )
        );
      } else if (mode.endsWith(JdbcSourceTaskConfig.MODE_TIMESTAMP_INCREMENTING)) {
        tableQueue.add(
            new TimestampIncrementingTableQuerier(
                dialect,
                queryMode,
                tableOrQuery,
                topicPrefix,
                timestampColumns,
                incrementingColumn,
                offset,
                timestampDelayInterval,
                timeZone,
                suffix,
                timestampGranularity
            )
        );
      }
    }

    running.set(true);
    taskThreadId.set(Thread.currentThread().getId());
    log.info("Started JDBC source task");

    maxRetriesPerQuerier = config.getInt(JdbcSourceConnectorConfig.QUERY_RETRIES_CONFIG);
  }

  protected CachedConnectionProvider connectionProvider(int maxConnAttempts, long retryBackoff) {
    return new CachedConnectionProvider(dialect, maxConnAttempts, retryBackoff) {
      @Override
      protected void onConnect(final Connection connection) throws SQLException {
        super.onConnect(connection);
        connection.setAutoCommit(false);
      }
    };
  }

  //This method returns a list of possible partition maps for different offset protocols
  //This helps with the upgrades
  private List<Map<String, String>> possibleTablePartitions(String table) {
    TableId tableId = dialect.parseTableIdentifier(table);
    return Arrays.asList(
        OffsetProtocols.sourcePartitionForProtocolV1(tableId),
        OffsetProtocols.sourcePartitionForProtocolV0(tableId)
    );
  }

  protected Map<String, Object> computeInitialOffset(
          String tableOrQuery,
          Map<String, Object> partitionOffset,
          TimeZone timezone) {
    if (!(partitionOffset == null)) {
      return partitionOffset;
    } else {
      Map<String, Object> initialPartitionOffset = null;
      // no offsets found
      Long timestampInitial = config.getLong(JdbcSourceConnectorConfig.TIMESTAMP_INITIAL_CONFIG);
      if (timestampInitial != null) {
        // start at the specified timestamp
        if (timestampInitial == JdbcSourceConnectorConfig.TIMESTAMP_INITIAL_CURRENT) {
          // use the current time
          try {
            final Connection con = cachedConnectionProvider.getConnection();
            Calendar cal = Calendar.getInstance(timezone);
            timestampInitial = dialect.currentTimeOnDB(con, cal).getTime();
          } catch (SQLException e) {
            throw new ConnectException("Error while getting initial timestamp from database", e);
          }
        }
        initialPartitionOffset = new HashMap<String, Object>();
        initialPartitionOffset.put(TimestampIncrementingOffset.TIMESTAMP_FIELD, timestampInitial);
        log.info("No offsets found for '{}', so using configured timestamp {}", tableOrQuery,
                timestampInitial);
      }
      return initialPartitionOffset;
    }
  }

  @Override
  public void stop() throws ConnectException {
    log.info("Stopping JDBC source task");

    // In earlier versions of Kafka, stop() was not called from the task thread. In this case, all
    // resources are closed at the end of 'poll()' when no longer running or if there is an error.
    running.set(false);

    if (taskThreadId.longValue() == Thread.currentThread().getId()) {
      shutdown();
    }
  }

  protected void closeResources() {
    log.info("Closing resources for JDBC source task");
    try {
      if (cachedConnectionProvider != null) {
        cachedConnectionProvider.close();
      }
    } catch (Throwable t) {
      log.warn("Error while closing the connections", t);
    } finally {
      cachedConnectionProvider = null;
      try {
        if (dialect != null) {
          dialect.close();
        }
      } catch (Throwable t) {
        log.warn("Error while closing the {} dialect: ", dialect.name(), t);
      } finally {
        dialect = null;
      }
    }
  }

  @Override
  public List<SourceRecord> poll() throws InterruptedException {
    log.trace("{} Polling for new data");

    Map<TableQuerier, Integer> consecutiveEmptyResults = tableQueue.stream().collect(
        Collectors.toMap(Function.identity(), (q) -> 0));
    while (running.get()) {
      final TableQuerier querier = tableQueue.peek();

      if (!querier.querying()) {
        // If not in the middle of an update, wait for next update time
        final long nextUpdate = querier.getLastUpdate()
            + config.getInt(JdbcSourceTaskConfig.POLL_INTERVAL_MS_CONFIG);
        final long now = time.milliseconds();
        final long sleepMs = Math.min(nextUpdate - now, 100);

        if (sleepMs > 0) {
          log.trace("Waiting {} ms to poll {} next", nextUpdate - now, querier.toString());
          time.sleep(sleepMs);
          continue; // Re-check stop flag before continuing
        }
      }

      final List<SourceRecord> results = new ArrayList<>();
      try {
        log.debug("Checking for next block of results from {}", querier.toString());
        querier.maybeStartQuery(cachedConnectionProvider.getConnection());

        int batchMaxRows = config.getInt(JdbcSourceTaskConfig.BATCH_MAX_ROWS_CONFIG);
        boolean hadNext = true;
        while (results.size() < batchMaxRows && (hadNext = querier.next())) {
          results.add(querier.extractRecord());
        }
        querier.resetRetryCount();

        if (!hadNext) {
          // If we finished processing the results from the current query, we can reset and send
          // the querier to the tail of the queue
          resetAndRequeueHead(querier, false);
        }

        if (results.isEmpty()) {
          consecutiveEmptyResults.compute(querier, (k, v) -> v + 1);
          log.trace("No updates for {}", querier.toString());

          if (Collections.min(consecutiveEmptyResults.values())
              >= CONSECUTIVE_EMPTY_RESULTS_BEFORE_RETURN) {
            log.trace("More than " + CONSECUTIVE_EMPTY_RESULTS_BEFORE_RETURN
                + " consecutive empty results for all queriers, returning");
            return null;
          } else {
            continue;
          }
        } else {
          consecutiveEmptyResults.put(querier, 0);
        }

        log.debug("Returning {} records for {}", results.size(), querier);
        return results;
      } catch (SQLNonTransientException sqle) {
        log.error("Non-transient SQL exception while running query for table: {}",
            querier, sqle);
        resetAndRequeueHead(querier, true);
        // This task has failed, so close any resources (may be reopened if needed) before throwing
        closeResources();
        throw new ConnectException(sqle);
      } catch (SQLException sqle) {
        log.error(
                "SQL exception while running query for table: {}, {}."
                        + " Attempting retry {} of {} attempts.",
                querier,
                sqle,
                querier.getAttemptedRetryCount() + 1,
                maxRetriesPerQuerier
        );
        resetAndRequeueHead(querier, true);
        if (maxRetriesPerQuerier > 0
                && querier.getAttemptedRetryCount() >= maxRetriesPerQuerier) {
          closeResources();
          throw new ConnectException("Failed to Query table after retries", sqle);
        }
        querier.incrementRetryCount();
        return null;
      } catch (Throwable t) {
        log.error("Failed to run query for table: {}", querier, t);
        resetAndRequeueHead(querier, true);
        // This task has failed, so close any resources (may be reopened if needed) before throwing
        closeResources();
        throw t;
      }
    }

    shutdown();
    return null;
  }

  private void shutdown() {
    final TableQuerier querier = tableQueue.peek();
    if (querier != null) {
      resetAndRequeueHead(querier, true);
    }
    closeResources();
  }

  private void resetAndRequeueHead(TableQuerier expectedHead, boolean resetOffset) {
    log.debug("Resetting querier {}", expectedHead.toString());
    TableQuerier removedQuerier = tableQueue.poll();
    assert removedQuerier == expectedHead;
    expectedHead.reset(time.milliseconds(), resetOffset);
    tableQueue.add(expectedHead);
  }

  private void validateNonNullable(
      String incrementalMode,
      String table,
      String incrementingColumn,
      List<String> timestampColumns
  ) {
    try {
      Set<String> lowercaseTsColumns = new HashSet<>();
      for (String timestampColumn: timestampColumns) {
        lowercaseTsColumns.add(timestampColumn.toLowerCase(Locale.getDefault()));
      }

      boolean incrementingOptional = false;
      boolean atLeastOneTimestampNotOptional = false;
      final Connection conn = cachedConnectionProvider.getConnection();
      boolean autoCommit = conn.getAutoCommit();
      try {
        conn.setAutoCommit(true);
        Map<ColumnId, ColumnDefinition> defnsById = dialect.describeColumns(conn, table, null);
        for (ColumnDefinition defn : defnsById.values()) {
          String columnName = defn.id().name();
          if (columnName.equalsIgnoreCase(incrementingColumn)) {
            incrementingOptional = defn.isOptional();
          } else if (lowercaseTsColumns.contains(columnName.toLowerCase(Locale.getDefault()))) {
            if (!defn.isOptional()) {
              atLeastOneTimestampNotOptional = true;
            }
          }
        }
      } finally {
        conn.setAutoCommit(autoCommit);
      }

      // Validate that requested columns for offsets are NOT NULL. Currently this is only performed
      // for table-based copying because custom query mode doesn't allow this to be looked up
      // without a query or parsing the query since we don't have a table name.
      if ((incrementalMode.equals(JdbcSourceConnectorConfig.MODE_INCREMENTING)
           || incrementalMode.equals(JdbcSourceConnectorConfig.MODE_TIMESTAMP_INCREMENTING))
          && incrementingOptional) {
        throw new ConnectException("Cannot make incremental queries using incrementing column "
                                   + incrementingColumn + " on " + table + " because this column "
                                   + "is nullable.");
      }
      if ((incrementalMode.equals(JdbcSourceConnectorConfig.MODE_TIMESTAMP)
           || incrementalMode.equals(JdbcSourceConnectorConfig.MODE_TIMESTAMP_INCREMENTING))
          && !atLeastOneTimestampNotOptional) {
        throw new ConnectException("Cannot make incremental queries using timestamp columns "
                                   + timestampColumns + " on " + table + " because all of these "
                                   + "columns "
                                   + "nullable.");
      }
    } catch (SQLException e) {
      throw new ConnectException("Failed trying to validate that columns used for offsets are NOT"
                                 + " NULL", e);
    }
  }
}
