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
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.errors.ConnectException;
import org.easymock.EasyMock;
import org.easymock.Mock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.source.EmbeddedDerby;
import io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig;
import io.confluent.connect.jdbc.source.JdbcSourceTask;
import io.confluent.connect.jdbc.source.JdbcSourceTaskConfig;
import io.confluent.connect.jdbc.util.CachedConnectionProvider;
import io.confluent.connect.jdbc.util.ExpressionBuilder;
import io.confluent.connect.jdbc.util.TableId;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(PowerMockRunner.class)
@PrepareForTest({JdbcSourceConnector.class, DatabaseDialect.class})
@PowerMockIgnore("javax.management.*")
public class JdbcSourceConnectorTest {

  private JdbcSourceConnector connector;
  private EmbeddedDerby db;
  private Map<String, String> props;

  public static class MockJdbcSourceConnector extends JdbcSourceConnector {
    CachedConnectionProvider provider;
    public MockJdbcSourceConnector() {}
    public MockJdbcSourceConnector(CachedConnectionProvider provider) {
      this.provider = provider;
    }
    @Override
    protected CachedConnectionProvider connectionProvider(
            int maxConnAttempts,
            long retryBackoff
    ) {
      return provider;
    }
  }

  @Mock
  private DatabaseDialect dialect;
  @Mock
  private ConnectorContext connectorContext;

  @Before
  public void setup() {
    connector = new JdbcSourceConnector();
    db = new EmbeddedDerby();
    props = new HashMap<>();
    props.put(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, db.getUrl());
    props.put(JdbcSourceConnectorConfig.MODE_CONFIG, JdbcSourceConnectorConfig.MODE_BULK);
    props.put(JdbcSourceConnectorConfig.TOPIC_PREFIX_CONFIG, "test-");
  }

  @After
  public void tearDown() throws Exception {
    db.close();
    db.dropDatabase();
  }

  @Test
  public void testTaskClass() {
    assertEquals(JdbcSourceTask.class, connector.taskClass());
  }

  @Test(expected = ConnectException.class)
  public void testMissingUrlConfig() throws Exception {
    HashMap<String, String> connProps = new HashMap<>();
    connProps.put(JdbcSourceConnectorConfig.MODE_CONFIG, JdbcSourceConnectorConfig.MODE_BULK);
    connector.start(connProps);
  }

  @Test(expected = ConnectException.class)
  public void testMissingModeConfig() throws Exception {
    HashMap<String, String> connProps = new HashMap<>();
    connProps.put(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, db.getUrl());
    connector.start(Collections.<String, String>emptyMap());
  }

  @Test(expected = ConnectException.class)
  public void testStartConnectionFailure() throws Exception {
    // Invalid URL
    connector.start(Collections.singletonMap(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, "jdbc:foo"));
  }

  @Test
  public void testStartStop() throws Exception {
    CachedConnectionProvider mockCachedConnectionProvider = PowerMock.createMock(CachedConnectionProvider.class);
    connector  = new MockJdbcSourceConnector(mockCachedConnectionProvider);
    // Should request a connection, then should close it on stop(). The background thread may also
    // request connections any time it performs updates.
    Connection conn = PowerMock.createMock(Connection.class);
    EasyMock.expect(mockCachedConnectionProvider.getConnection()).andReturn(conn).anyTimes();

    // Since we're just testing start/stop, we don't worry about the value here but need to stub
    // something since the background thread will be started and try to lookup metadata.
    EasyMock.expect(conn.getMetaData()).andStubThrow(new SQLException());
    // Close will be invoked both for the SQLExeption and when the connector is stopped
    mockCachedConnectionProvider.close();
    PowerMock.expectLastCall().atLeastOnce();

    PowerMock.replayAll();

    connector.start(props);
    connector.stop();

    PowerMock.verifyAll();
  }

  @Test
  public void testNoTablesSpawnsSingleTask() throws Exception {
    // Tests case where there are no readable tables and ensures that no tasks
    // are returned to be run
    connector.start(props);
    List<Map<String, String>> configs = connector.taskConfigs(3);
    assertEquals(1, configs.size());
    connector.stop();
  }

  @Test
  public void testPartitioningOneTable() throws Exception {
    // Tests simplest case where we have exactly 1 table and also ensures we return fewer tasks
    // if there aren't enough tables for the max # of tasks
    db.createTable("test", "id", "INT NOT NULL");

    CountDownLatch taskReconfigurationLatch = new CountDownLatch(1);
    connectorContext.requestTaskReconfiguration();
    EasyMock.expectLastCall().andAnswer(() -> {
      taskReconfigurationLatch.countDown();
      return null;
    });
    EasyMock.replay(connectorContext);
    connector.initialize(connectorContext);

    connector.start(props);
    assertTrue(
        "Connector should have request task reconfiguration after reading tables from the database",
        taskReconfigurationLatch.await(10, TimeUnit.SECONDS)
    );

    List<Map<String, String>> configs = connector.taskConfigs(10);
    assertEquals(1, configs.size());
    assertTaskConfigsHaveParentConfigs(configs);
    assertEquals(tables("test"), configs.get(0).get(JdbcSourceTaskConfig.TABLES_CONFIG));
    assertNull(configs.get(0).get(JdbcSourceTaskConfig.QUERY_CONFIG));
    connector.stop();
  }

  @Test
  public void testPartitioningManyTables() throws Exception {
    // Tests distributing tables across multiple tasks, in this case unevenly
    db.createTable("test1", "id", "INT NOT NULL");
    db.createTable("test2", "id", "INT NOT NULL");
    db.createTable("test3", "id", "INT NOT NULL");
    db.createTable("test4", "id", "INT NOT NULL");

    CountDownLatch taskReconfigurationLatch = new CountDownLatch(1);
    connectorContext.requestTaskReconfiguration();
    EasyMock.expectLastCall().andAnswer(() -> {
      taskReconfigurationLatch.countDown();
      return null;
    });
    EasyMock.replay(connectorContext);
    connector.initialize(connectorContext);

    connector.start(props);
    assertTrue(
        "Connector should have request task reconfiguration after reading tables from the database",
        taskReconfigurationLatch.await(10, TimeUnit.SECONDS)
    );

    List<Map<String, String>> configs = connector.taskConfigs(3);
    assertEquals(3, configs.size());
    assertTaskConfigsHaveParentConfigs(configs);

    assertEquals(tables("test1","test2"), configs.get(0).get(JdbcSourceTaskConfig.TABLES_CONFIG));
    assertNull(configs.get(0).get(JdbcSourceTaskConfig.QUERY_CONFIG));
    assertEquals(tables("test3"), configs.get(1).get(JdbcSourceTaskConfig.TABLES_CONFIG));
    assertNull(configs.get(1).get(JdbcSourceTaskConfig.QUERY_CONFIG));
    assertEquals(tables("test4"), configs.get(2).get(JdbcSourceTaskConfig.TABLES_CONFIG));
    assertNull(configs.get(2).get(JdbcSourceTaskConfig.QUERY_CONFIG));

    connector.stop();
  }

  @Test
  public void testPartitioningQuery() throws Exception {
    // Tests "partitioning" when config specifies running a custom query
    db.createTable("test1", "id", "INT NOT NULL");
    db.createTable("test2", "id", "INT NOT NULL");
    final String sample_query = "SELECT foo, bar FROM sample_table";
    props.put(JdbcSourceConnectorConfig.QUERY_CONFIG, sample_query);
    connector.start(props);
    List<Map<String, String>> configs = connector.taskConfigs(3);
    assertEquals(1, configs.size());
    assertTaskConfigsHaveParentConfigs(configs);

    assertEquals("", configs.get(0).get(JdbcSourceTaskConfig.TABLES_CONFIG));
    assertEquals(sample_query, configs.get(0).get(JdbcSourceTaskConfig.QUERY_CONFIG));

    connector.stop();
  }

  @Test(expected = ConnectException.class)
  public void testConflictingQueryTableSettings() {
    final String sample_query = "SELECT foo, bar FROM sample_table";
    props.put(JdbcSourceConnectorConfig.QUERY_CONFIG, sample_query);
    props.put(JdbcSourceConnectorConfig.TABLE_WHITELIST_CONFIG, "foo,bar");
    connector.start(props);
  }

  private void assertTaskConfigsHaveParentConfigs(List<Map<String, String>> configs) {
    for (Map<String, String> config : configs) {
      assertEquals(this.db.getUrl(),
                   config.get(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG));
    }
  }


  @Test
  public void testSqlServerIsolationModeWithCorrectDialect() {

    props.put(JdbcSourceConnectorConfig.TRANSACTION_ISOLATION_MODE_CONFIG, "SQL_SERVER_SNAPSHOT");
    props.put(JdbcSourceConnectorConfig.DIALECT_NAME_CONFIG, "SqlServerDatabaseDialect");

    Config config = connector.validate(props);
    HashMap<String, ConfigValue> configValues = new HashMap<>();
    config.configValues().stream()
            .filter((configValue) ->
                    configValue.name().equals(
                            JdbcSourceConnectorConfig.TRANSACTION_ISOLATION_MODE_CONFIG
                    )
            ).forEach(configValue -> configValues.putIfAbsent(configValue.name(), configValue));

    assertTrue(
            configValues.get(
                    JdbcSourceConnectorConfig.TRANSACTION_ISOLATION_MODE_CONFIG
            ).errorMessages().isEmpty()
    );
  }

  @Test
  public void testSqlServerIsolationModeIncorrectDialect() {
    props.put(JdbcSourceConnectorConfig.TRANSACTION_ISOLATION_MODE_CONFIG, "SQL_SERVER_SNAPSHOT");
    props.put(JdbcSourceConnectorConfig.DIALECT_NAME_CONFIG, "MySqlDatabaseDialect");

    Config config = connector.validate(props);
    HashMap<String, ConfigValue> configValues = new HashMap<>();
    config.configValues().stream()
            .filter((configValue) ->
                    configValue.name().equals(
                            JdbcSourceConnectorConfig.TRANSACTION_ISOLATION_MODE_CONFIG
                    )
            ).forEach(configValue -> configValues.putIfAbsent(configValue.name(), configValue));

    List<String> errors = configValues.get(
            JdbcSourceConnectorConfig.TRANSACTION_ISOLATION_MODE_CONFIG
    ).errorMessages();
    assertFalse(errors.isEmpty());
    assertEquals(1, errors.size());
    assertTrue(errors.get(0).contains(
            "Isolation mode of `SQL_SERVER_SNAPSHOT` can only be"
                    + " configured with a Sql Server Dialect")
    );
  }

  @Test
  public void testSqlServerIsolationModeWithCorrectUrl() {
    List<String> sqlServerConnectionUrlTypes = new ArrayList<>();
    sqlServerConnectionUrlTypes.add("jdbc:sqlserver://localhost;user=Me");
    sqlServerConnectionUrlTypes.add("jdbc:microsoft:sqlserver://localhost;user=Me");
    sqlServerConnectionUrlTypes.add("jdbc:jtds:sqlserver://localhost;user=Me");

    for (String urlType : sqlServerConnectionUrlTypes) {
      props.put(JdbcSourceConnectorConfig.TRANSACTION_ISOLATION_MODE_CONFIG, "SQL_SERVER_SNAPSHOT");
      props.put(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, urlType);

      Config config = connector.validate(props);
      HashMap<String, ConfigValue> configValues = new HashMap<>();
      config.configValues().stream()
              .filter((configValue) ->
                      configValue.name().equals(
                              JdbcSourceConnectorConfig.TRANSACTION_ISOLATION_MODE_CONFIG
                      )
              ).forEach(configValue -> configValues.putIfAbsent(configValue.name(), configValue));

      assertTrue(
              configValues.get(
                      JdbcSourceConnectorConfig.TRANSACTION_ISOLATION_MODE_CONFIG
              ).errorMessages().isEmpty()
      );
    }
  }

  @Test
  public void testSqlServerIsolationModeWithIncorrectUrl() {
    props.put(JdbcSourceConnectorConfig.TRANSACTION_ISOLATION_MODE_CONFIG, "SQL_SERVER_SNAPSHOT");
    props.put(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, "jdbc:mysql://localhost:3306/sakila?profileSQL=true");

    Config config = connector.validate(props);
    HashMap<String, ConfigValue> configValues = new HashMap<>();
    config.configValues().stream()
            .filter((configValue) ->
                    configValue.name().equals(
                            JdbcSourceConnectorConfig.TRANSACTION_ISOLATION_MODE_CONFIG
                    )
            ).forEach(configValue -> configValues.putIfAbsent(configValue.name(), configValue));

    List<String> errors = configValues.get(
            JdbcSourceConnectorConfig.TRANSACTION_ISOLATION_MODE_CONFIG
    ).errorMessages();
    assertFalse(errors.isEmpty());
    assertEquals(1, errors.size());
    assertTrue(errors.get(0).contains(
            "Isolation mode of `SQL_SERVER_SNAPSHOT` can only be"
                    + " configured with a Sql Server Dialect")
    );


  }

  private String tables(String... names) {
    List<TableId> tableIds = new ArrayList<>();
    for (String name : names) {
      tableIds.add(new TableId(null, "APP", name));
    }
    ExpressionBuilder builder = ExpressionBuilder.create();
    builder.appendList().delimitedBy(",").of(tableIds);
    return builder.toString();
  }
}
