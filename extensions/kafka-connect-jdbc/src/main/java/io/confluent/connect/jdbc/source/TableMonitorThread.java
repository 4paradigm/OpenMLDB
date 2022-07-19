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

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.util.ConnectionProvider;
import io.confluent.connect.jdbc.util.QuoteMethod;
import io.confluent.connect.jdbc.util.TableId;

import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * Thread that monitors the database for changes to the set of tables in the database that this
 * connector should load data from.
 */
public class TableMonitorThread extends Thread {
  private static final Logger log = LoggerFactory.getLogger(TableMonitorThread.class);

  private final DatabaseDialect dialect;
  private final ConnectionProvider connectionProvider;
  private final ConnectorContext context;
  private final CountDownLatch shutdownLatch;
  private final long startupMs;
  private final long pollMs;
  private final Set<String> whitelist;
  private final Set<String> blacklist;
  private final AtomicReference<List<TableId>> tables;
  private final Time time;

  public TableMonitorThread(DatabaseDialect dialect,
      ConnectionProvider connectionProvider,
      ConnectorContext context,
      long startupMs,
      long pollMs,
      Set<String> whitelist,
      Set<String> blacklist,
      Time time
  ) {
    this.dialect = dialect;
    this.connectionProvider = connectionProvider;
    this.context = context;
    this.shutdownLatch = new CountDownLatch(1);
    this.startupMs = startupMs;
    this.pollMs = pollMs;
    this.whitelist = whitelist;
    this.blacklist = blacklist;
    this.tables = new AtomicReference<>();
    this.time = time;
  }

  @Override
  public void run() {
    log.info("Starting thread to monitor tables.");
    while (shutdownLatch.getCount() > 0) {
      try {
        if (updateTables()) {
          context.requestTaskReconfiguration();
        }
      } catch (Exception e) {
        throw fail(e);
      }

      try {
        log.debug("Waiting {} ms to check for changed.", pollMs);
        boolean shuttingDown = shutdownLatch.await(pollMs, TimeUnit.MILLISECONDS);
        if (shuttingDown) {
          return;
        }
      } catch (InterruptedException e) {
        log.error("Unexpected InterruptedException, ignoring: ", e);
      }
    }
  }

  /**
   * @return the latest set of tables from the database that should be read by the connector, or
   *         {@code null} if the connector has not been able to read any tables from the database
   *         successfully yet
   */
  public List<TableId> tables() {
    awaitTablesReady(startupMs);
    List<TableId> tablesSnapshot = tables.get();
    if (tablesSnapshot == null) {
      return null;
    }

    Map<String, List<TableId>> duplicates = tablesSnapshot.stream()
        .collect(Collectors.groupingBy(TableId::tableName))
        .entrySet().stream()
        .filter(entry -> entry.getValue().size() > 1)
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    if (tablesSnapshot.isEmpty()) {
      log.debug(
          "Based on the supplied filtering rules, there are no matching tables to read from"
      );
    } else {
      log.debug(
          "Based on the supplied filtering rules, the tables available to read from include: {}",
          dialect.expressionBuilder()
              .appendList()
              .delimitedBy(",")
              .of(tablesSnapshot)
      );
    }

    if (!duplicates.isEmpty()) {
      String configText;
      if (whitelist != null) {
        configText = "'" + JdbcSourceConnectorConfig.TABLE_WHITELIST_CONFIG + "'";
      } else if (blacklist != null) {
        configText = "'" + JdbcSourceConnectorConfig.TABLE_BLACKLIST_CONFIG + "'";
      } else {
        configText = "'" + JdbcSourceConnectorConfig.TABLE_WHITELIST_CONFIG + "' or '"
            + JdbcSourceConnectorConfig.TABLE_BLACKLIST_CONFIG + "'";
      }
      String msg = "The connector uses the unqualified table name as the topic name and has "
          + "detected duplicate unqualified table names. This could lead to mixed data types in "
          + "the topic and downstream processing errors. To prevent such processing errors, the "
          + "JDBC Source connector fails to start when it detects duplicate table name "
          + "configurations. Update the connector's " + configText + " config to include exactly "
          + "one table in each of the tables listed below.\n\t";
      RuntimeException exception = new ConnectException(msg + duplicates.values());
      throw fail(exception);
    }
    return tablesSnapshot;
  }

  private void awaitTablesReady(long timeoutMs) {
    try {
      time.waitObject(tables, () -> tables.get() != null, time.milliseconds() + timeoutMs);
    } catch (InterruptedException | TimeoutException e) {
      log.warn("Timed out or interrupted while awaiting for tables being read.");
      return;
    }
  }

  public void shutdown() {
    log.info("Shutting down thread monitoring tables.");
    shutdownLatch.countDown();
  }

  private boolean updateTables() {
    final List<TableId> allTables;
    try {
      allTables = dialect.tableIds(connectionProvider.getConnection());
      log.debug("Got the following tables: {}", allTables);
    } catch (SQLException e) {
      log.error(
          "Error while trying to get updated table list, ignoring and waiting for next table poll"
          + " interval",
          e
      );
      connectionProvider.close();
      return false;
    }

    final List<TableId> filteredTables = new ArrayList<>(allTables.size());
    if (whitelist != null) {
      for (TableId table : allTables) {
        String fqn1 = dialect.expressionBuilder().append(table, QuoteMethod.NEVER).toString();
        String fqn2 = dialect.expressionBuilder().append(table, QuoteMethod.ALWAYS).toString();
        if (whitelist.contains(fqn1) || whitelist.contains(fqn2)
            || whitelist.contains(table.tableName())) {
          filteredTables.add(table);
        }
      }
    } else if (blacklist != null) {
      for (TableId table : allTables) {
        String fqn1 = dialect.expressionBuilder().append(table, QuoteMethod.NEVER).toString();
        String fqn2 = dialect.expressionBuilder().append(table, QuoteMethod.ALWAYS).toString();
        if (!(blacklist.contains(fqn1) || blacklist.contains(fqn2)
              || blacklist.contains(table.tableName()))) {
          filteredTables.add(table);
        }
      }
    } else {
      filteredTables.addAll(allTables);
    }

    List<TableId> priorTablesSnapshot = tables.getAndSet(filteredTables);
    synchronized (tables) {
      tables.notifyAll();
    }
    return !Objects.equals(priorTablesSnapshot, filteredTables);
  }

  /**
   * Fail the connector with an unrecoverable error and stop the table monitoring thread
   * @param t the cause of the failure
   * @return a {@link RuntimeException} that can be thrown from the calling method, for convenience
   */
  private RuntimeException fail(Throwable t) {
    String message = "Encountered an unrecoverable error while reading tables from the database";
    log.error(message, t);

    RuntimeException exception = new ConnectException(message, t);
    context.raiseError(exception);

    // Preemptively shut down the monitoring thread
    // so that we don't keep trying to read from the database
    shutdownLatch.countDown();
    return exception;
  }
}
