package io.confluent.connect.jdbc.source.integration;

import ch.vorburger.mariadb4j.DBConfigurationBuilder;
import ch.vorburger.mariadb4j.junit.MariaDB4jRule;
import org.apache.kafka.connect.runtime.AbstractStatus;
import org.apache.kafka.connect.runtime.AbstractStatus.State;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import io.confluent.common.utils.IntegrationTest;
import io.confluent.connect.jdbc.JdbcSourceConnector;
import io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig;
import io.confluent.connect.jdbc.source.JdbcSourceTaskConfig;

import static org.apache.kafka.connect.runtime.ConnectorConfig.TASKS_MAX_CONFIG;

@Category(IntegrationTest.class)
public class PauseResumeIT {
  private static final long CONNECTOR_STARTUP_DURATION_MS = TimeUnit.SECONDS.toMillis(60);
  private static final long POLLING_INTERVAL_MS = TimeUnit.SECONDS.toMillis(10);
  private static final String CONNECTOR_NAME = "JdbcSourceConnector";

  private static Logger log = LoggerFactory.getLogger(PauseResumeIT.class);

  @Rule
  public MariaDB4jRule dbRule = new MariaDB4jRule(
      DBConfigurationBuilder.newBuilder().setPort(0).build(),
      "testdb", null);

  EmbeddedConnectCluster connect;
  Map<String, String> props;

  @Before
  public void before() throws Exception {
    props = new HashMap<>();
    props.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, JdbcSourceConnector.class.getName());
    props.put(TASKS_MAX_CONFIG, "1");
    props.put(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, dbRule.getURL());
    props.put(JdbcSourceConnectorConfig.CONNECTION_USER_CONFIG, "root");
    props.put(JdbcSourceConnectorConfig.MODE_CONFIG, JdbcSourceConnectorConfig.MODE_INCREMENTING);
    props.put(JdbcSourceConnectorConfig.INCREMENTING_COLUMN_NAME_CONFIG, "id");
    props.put(JdbcSourceConnectorConfig.POLL_INTERVAL_MS_CONFIG, Long.toString(POLLING_INTERVAL_MS));
    props.put(JdbcSourceTaskConfig.TOPIC_PREFIX_CONFIG, "topic_");

    connect = new EmbeddedConnectCluster.Builder()
        .name("connect-cluster")
        .numWorkers(1)
        .brokerProps(new Properties())
        .build();

    // start the clusters
    log.debug("Starting embedded Connect worker, Kafka broker, and ZK");
    connect.start();
  }

  @After
  public void after() {
    if (connect != null) {
      connect.stop();
    }
  }

  @Test
  public void testPauseResume() throws Exception {
    try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
      stmt.executeUpdate(
          "CREATE TABLE accounts(id INTEGER AUTO_INCREMENT NOT NULL, name VARCHAR(255), PRIMARY KEY (id))" );
    }

    connect.configureConnector(CONNECTOR_NAME, props);

    waitForConnectorToStart(CONNECTOR_NAME, 1);

    Thread.sleep(POLLING_INTERVAL_MS);

    connect.requestPut(connect.endpointForResource(String.format("connectors/%s/pause", CONNECTOR_NAME)), "");

    waitForConnectorState(CONNECTOR_NAME, 1,
        3*POLLING_INTERVAL_MS, State.PAUSED);

    connect.requestPut(connect.endpointForResource(String.format("connectors/%s/resume", CONNECTOR_NAME)), "");
    waitForConnectorState(CONNECTOR_NAME, 1,
        3*POLLING_INTERVAL_MS, State.RUNNING);
  }

  protected Optional<Boolean> assertConnectorAndTasksStatus(String connectorName, int numTasks, AbstractStatus.State expectedStatus) {
    try {
      ConnectorStateInfo info = connect.connectorStatus(connectorName);
      boolean result = info != null
          && info.tasks().size() >= numTasks
          && info.connector().state().equals(expectedStatus.toString())
          && info.tasks().stream().allMatch(s -> s.state().equals(expectedStatus.toString()));
      return Optional.of(result);
    } catch (Exception e) {
      log.debug("Could not check connector state info.", e);
      return Optional.empty();
    }
  }

  protected long waitForConnectorToStart(String name, int numTasks) throws InterruptedException {
    return waitForConnectorState(name, numTasks, CONNECTOR_STARTUP_DURATION_MS, State.RUNNING);
  }

  protected long waitForConnectorState(String name, int numTasks, long timeoutMs, State state) throws InterruptedException {
    TestUtils.waitForCondition(
        () -> assertConnectorAndTasksStatus(name, numTasks, state).orElse(false),
        timeoutMs,
        "Connector tasks did not transition to state " + state + " in time"
    );
    return System.currentTimeMillis();
  }

  private Connection getConnection() throws SQLException {
    return DriverManager.getConnection(dbRule.getURL(), "root", "");
  }
}
