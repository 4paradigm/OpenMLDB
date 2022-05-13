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

package io.confluent.connect.jdbc;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.util.ConnectorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.dialect.DatabaseDialects;
import io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig;
import io.confluent.connect.jdbc.source.JdbcSourceTask;
import io.confluent.connect.jdbc.source.JdbcSourceTaskConfig;
import io.confluent.connect.jdbc.source.TableMonitorThread;
import io.confluent.connect.jdbc.util.CachedConnectionProvider;
import io.confluent.connect.jdbc.util.ExpressionBuilder;
import io.confluent.connect.jdbc.util.TableId;
import io.confluent.connect.jdbc.util.Version;

/**
 * JdbcConnector is a Kafka Connect Connector implementation that watches a JDBC database and
 * generates tasks to ingest database contents.
 */
public class JdbcSourceConnector extends SourceConnector {

  private static final Logger log = LoggerFactory.getLogger(JdbcSourceConnector.class);

  private static final long MAX_TIMEOUT = 10000L;

  private Map<String, String> configProperties;
  private JdbcSourceConnectorConfig config;
  private CachedConnectionProvider cachedConnectionProvider;
  private TableMonitorThread tableMonitorThread;
  private DatabaseDialect dialect;

  @Override
  public String version() {
    return Version.getVersion();
  }

  @Override
  public void start(Map<String, String> properties) throws ConnectException {
    log.info("Starting JDBC Source Connector");
    try {
      configProperties = properties;
      config = new JdbcSourceConnectorConfig(configProperties);
    } catch (ConfigException e) {
      throw new ConnectException("Couldn't start JdbcSourceConnector due to configuration error",
                                 e);
    }

    final String dbUrl = config.getString(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG);
    final int maxConnectionAttempts = config.getInt(
        JdbcSourceConnectorConfig.CONNECTION_ATTEMPTS_CONFIG
    );
    final long connectionRetryBackoff = config.getLong(
        JdbcSourceConnectorConfig.CONNECTION_BACKOFF_CONFIG
    );
    dialect = DatabaseDialects.findBestFor(
        dbUrl,
        config
    );
    cachedConnectionProvider = connectionProvider(maxConnectionAttempts, connectionRetryBackoff);

    // Initial connection attempt
    cachedConnectionProvider.getConnection();

    long tablePollMs = config.getLong(JdbcSourceConnectorConfig.TABLE_POLL_INTERVAL_MS_CONFIG);
    long tableStartupLimitMs =
        config.getLong(JdbcSourceConnectorConfig.TABLE_MONITORING_STARTUP_POLLING_LIMIT_MS_CONFIG);
    List<String> whitelist = config.getList(JdbcSourceConnectorConfig.TABLE_WHITELIST_CONFIG);
    Set<String> whitelistSet = whitelist.isEmpty() ? null : new HashSet<>(whitelist);
    List<String> blacklist = config.getList(JdbcSourceConnectorConfig.TABLE_BLACKLIST_CONFIG);
    Set<String> blacklistSet = blacklist.isEmpty() ? null : new HashSet<>(blacklist);

    if (whitelistSet != null && blacklistSet != null) {
      throw new ConnectException(JdbcSourceConnectorConfig.TABLE_WHITELIST_CONFIG + " and "
                                 + JdbcSourceConnectorConfig.TABLE_BLACKLIST_CONFIG + " are "
                                 + "exclusive.");
    }
    String query = config.getString(JdbcSourceConnectorConfig.QUERY_CONFIG);
    if (!query.isEmpty()) {
      if (whitelistSet != null || blacklistSet != null) {
        throw new ConnectException(JdbcSourceConnectorConfig.QUERY_CONFIG + " may not be combined"
                                   + " with whole-table copying settings.");
      }
      // Force filtering out the entire set of tables since the one task we'll generate is for the
      // query.
      whitelistSet = Collections.emptySet();

    }
    tableMonitorThread = new TableMonitorThread(
        dialect,
        cachedConnectionProvider,
        context,
        tableStartupLimitMs,
        tablePollMs,
        whitelistSet,
        blacklistSet,
        Time.SYSTEM
    );
    if (query.isEmpty()) {
      tableMonitorThread.start();
    }
  }

  protected CachedConnectionProvider connectionProvider(int maxConnAttempts, long retryBackoff) {
    return new CachedConnectionProvider(dialect, maxConnAttempts, retryBackoff);
  }

  @Override
  public Class<? extends Task> taskClass() {
    return JdbcSourceTask.class;
  }

  @Override
  public Config validate(Map<String, String> connectorConfigs) {
    Config config = super.validate(connectorConfigs);
    JdbcSourceConnectorConfig jdbcSourceConnectorConfig
            = new JdbcSourceConnectorConfig(connectorConfigs);
    jdbcSourceConnectorConfig.validateMultiConfigs(config);
    return config;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    String query = config.getString(JdbcSourceConnectorConfig.QUERY_CONFIG);
    List<Map<String, String>> taskConfigs;
    if (!query.isEmpty()) {
      Map<String, String> taskProps = new HashMap<>(configProperties);
      taskProps.put(JdbcSourceTaskConfig.TABLES_CONFIG, "");
      taskConfigs = Collections.singletonList(taskProps);
      log.trace("Producing task configs with custom query");
      return taskConfigs;
    } else {
      List<TableId> currentTables = tableMonitorThread.tables();
      if (currentTables == null) {
        taskConfigs = Collections.emptyList();
        log.info(
            "No tasks will be run because the connector has not been able to read "
                + "the list of tables from the database yet"
        );
      } else if (currentTables.isEmpty()) {
        taskConfigs = new ArrayList<>(1);
        log.warn("No tables were found so there's no work to be done.");
        Map<String, String> taskProps = new HashMap<>(configProperties);
        taskProps.put(JdbcSourceTaskConfig.TABLES_CONFIG, "[]");
        taskConfigs.add(taskProps);
      } else {
        int numGroups = Math.min(currentTables.size(), maxTasks);
        List<List<TableId>> tablesGrouped =
            ConnectorUtils.groupPartitions(currentTables, numGroups);
        taskConfigs = new ArrayList<>(tablesGrouped.size());
        for (List<TableId> taskTables : tablesGrouped) {
          Map<String, String> taskProps = new HashMap<>(configProperties);
          ExpressionBuilder builder = dialect.expressionBuilder();
          builder.appendList().delimitedBy(",").of(taskTables);
          taskProps.put(JdbcSourceTaskConfig.TABLES_CONFIG, builder.toString());
          taskConfigs.add(taskProps);
        }
        log.trace(
            "Producing task configs with no custom query for tables: {}",
            currentTables.toArray()
        );
      }
    }
    return taskConfigs;
  }

  @Override
  public void stop() throws ConnectException {
    log.info("Stopping table monitoring thread");
    tableMonitorThread.shutdown();
    try {
      tableMonitorThread.join(MAX_TIMEOUT);
    } catch (InterruptedException e) {
      // Ignore, shouldn't be interrupted
    } finally {
      try {
        cachedConnectionProvider.close();
      } finally {
        try {
          if (dialect != null) {
            dialect.close();
          }
        } catch (Throwable t) {
          log.warn("Error while closing the {} dialect: ", dialect, t);
        } finally {
          dialect = null;
        }
      }
    }
  }

  @Override
  public ConfigDef config() {
    return JdbcSourceConnectorConfig.CONFIG_DEF;
  }
}
