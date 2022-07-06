/*
 * Copyright [2017 - 2019] Confluent Inc.
 */

package io.confluent.connect.jdbc.source.integration;

import java.sql.SQLException;
import java.time.ZoneId;
import java.time.ZonedDateTime;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.storage.StringConverter;
import org.testcontainers.containers.FixedHostPortGenericContainer;

import org.apache.kafka.test.IntegrationTest;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.ClassRule;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.Timestamp;
import java.sql.PreparedStatement;
import java.time.Duration;

import io.confluent.connect.jdbc.integration.BaseConnectorIT;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import static org.junit.Assert.assertEquals;
import static io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG;
import static io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig.CONNECTION_USER_CONFIG;
import static io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig.CONNECTION_PASSWORD_CONFIG;
import static io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig.TABLE_WHITELIST_CONFIG;
import static io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig.MODE_CONFIG;
import static io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig.MODE_TIMESTAMP;
import static io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig.MODE_TIMESTAMP_INCREMENTING;
import static io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig.INCREMENTING_COLUMN_NAME_CONFIG;
import static io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig.TIMESTAMP_COLUMN_NAME_CONFIG;
import static io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig.TOPIC_PREFIX_CONFIG;
import static io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig.POLL_INTERVAL_MS_CONFIG;

import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.TASKS_MAX_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG;

/**
 * This test is to verify that JDBC source connector forbids Datetime column type
 * for MSSQL Server (2016 and after).
 *
 * Datetime is forbidden because JDBC handles all time stamp columns as {@link java.sql.Time}.
 * Datetime is only accurate to 3.33 MS but JDBC provides much higher accuracy
 * for SQL operations against Datetime. Therefore MSSQL casts Datetime to a higher precision
 * but it does so recursively (3.333333 MS). Since JDBC casts to higher precision
 * non recursively (3.330000 MS) our {@link io.confluent.connect.jdbc.source.TimestampIncrementingTableQuerier}
 * loops on the most recent record.
 *
 * Older MSSQL Server instances do not have this problem.
 */
@Category(IntegrationTest.class)
public class MSSQLDateTimeIT extends BaseConnectorIT {

    private static final Logger log = LoggerFactory.getLogger(MSSQLDateTimeIT.class);
    private static final String CONNECTOR_NAME = "JdbcSourceConnector";
    private static final int NUM_RECORDS_PRODUCED = 1;
    private static final long CONSUME_MAX_DURATION_MS = TimeUnit.MINUTES.toMillis(2);
    private static final int TASKS_MAX = 3;
    private static final String MSSQL_URL = "jdbc:sqlserver://0.0.0.0:1433";
    private static final String MSSQL_Table = "TestTable";
    private static final String TOPIC_PREFIX = "test-";
    private static final List<String> KAFKA_TOPICS = Collections.singletonList(TOPIC_PREFIX + MSSQL_Table );

    private Map<String, String> props;

    private static final String USER = "sa"; // test creds
    private static final String PASS = "reallyStrongPwd123"; // test creds

    private Connection connection;

    @ClassRule
    @SuppressWarnings("deprecation")
    public static final FixedHostPortGenericContainer mssqlServer =
            new FixedHostPortGenericContainer<>("microsoft/mssql-server-linux:latest")
                .withEnv("ACCEPT_EULA","Y")
                .withEnv("SA_PASSWORD","reallyStrongPwd123")
                .withFixedExposedPort(1433, 1433);

    @Before
    public void setup() throws Exception {
        //Set up JDBC Driver
        Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        connection = DriverManager.getConnection(MSSQL_URL, USER, PASS);
        startConnect();
    }

    @After
    public void close() throws SQLException {
        deleteTable();
        connect.deleteConnector(CONNECTOR_NAME);
        connection.close();
        // Stop all Connect, Kafka and Zk threads.
        stopConnect();
    }

    /**
     * Verify that Datetime as a timestamp column in Timestamp mode kills task.
     */
    @Test
    public void verifyTimeStampModeFailsWithDateTime() throws Exception {
        // Setup up props for the source connector
        props = configProperties();
        props.put(MODE_CONFIG, MODE_TIMESTAMP); // set to TimeStamp mode

        String sql = "CREATE TABLE " + MSSQL_Table
                + " (start_time DATETIME not NULL, record VARCHAR(255))";
        PreparedStatement createStmt = connection.prepareStatement(sql);
        executeSQL(createStmt);

        // Create topic in Kafka
        KAFKA_TOPICS.forEach(topic -> connect.kafka().createTopic(topic, 1));
        // Configure source connector
        configureAndWaitForConnector();

        Timestamp t = Timestamp.from(
                ZonedDateTime.of(2016, 12, 8, 19, 34, 56, 0, ZoneId.of("UTC")).toInstant()
        );

        sql = "INSERT INTO " + MSSQL_Table + " (start_time, record) "
                + "values (?, ?)";
        PreparedStatement insertStmt = connection.prepareStatement(sql);
        insertStmt.setTimestamp(1, t);
        insertStmt.setString(2, "example record value");
        executeSQL(insertStmt);

        connect.assertions().assertConnectorIsRunningAndTasksHaveFailed(
                CONNECTOR_NAME,
                Math.min(KAFKA_TOPICS.size(), TASKS_MAX),
                "failed to verify that tasks have failed"
        );
    }


    /**
     * Verify that Datetime as a timestamp column in Timestamp and Incrementing mode
     * kills task.
     */
    @Test
    public void verifyTimeStampAndIncrementingModeFailsWithDatetime() throws Exception {
        // Setup up props for the source connector
        props = configProperties();
        // set to TimeStamp + Incrementing mode
        props.put(MODE_CONFIG, MODE_TIMESTAMP_INCREMENTING);
        props.put(INCREMENTING_COLUMN_NAME_CONFIG, "incrementing_col");


        String sql = "CREATE TABLE " + MSSQL_Table
                + " (start_time DATETIME not NULL, incrementing_col int not NULL, "
                + "record VARCHAR(255))";
        PreparedStatement createStmt = connection.prepareStatement(sql);
        executeSQL(createStmt);


        // Create topic in Kafka
        KAFKA_TOPICS.forEach(topic -> connect.kafka().createTopic(topic, 1));

        // Configure source connector
        configureAndWaitForConnector();

        Timestamp t = Timestamp.from(
                ZonedDateTime.of(2016, 12, 8, 19, 34, 56, 0, ZoneId.of("UTC")).toInstant()
        );

        sql = "INSERT INTO " + MSSQL_Table + " (start_time, incrementing_col, record) "
                + "values (?, ?, ?)";
        PreparedStatement insertStmt = connection.prepareStatement(sql);
        insertStmt.setTimestamp(1, t);
        insertStmt.setInt(2, 1);
        insertStmt.setString(3, "example record value");
        executeSQL(insertStmt);


        connect.assertions().assertConnectorIsRunningAndTasksHaveFailed(
                CONNECTOR_NAME,
                Math.min(KAFKA_TOPICS.size(), TASKS_MAX),
                "failed to verify that tasks have failed"
        );
    }


    /**
     * Verify that Datetime as a non timestamp column does not kill task
     *
     * Also Verify that after a number of poll intervals the number of records in topic
     * still equal to the number of records written to the MSSQL Server instance.
     * This confirms that DateTime2 works as expected.
     *
     */
    @Test
    public void verifyDateTimeAllowedAsNonTimeStamp() throws Exception {
        props = configProperties();
        props.put(MODE_CONFIG, MODE_TIMESTAMP);


        String sql = "CREATE TABLE " + MSSQL_Table
                + " (start_time DATETIME2 not NULL, record DATETIME)";
        PreparedStatement createStmt = connection.prepareStatement(sql);
        executeSQL(createStmt);

        // Create topic in Kafka
        KAFKA_TOPICS.forEach(topic -> connect.kafka().createTopic(topic, 1));

        // Configure source connector
        configureAndWaitForConnector();

        Timestamp t = Timestamp.from(
                ZonedDateTime.of(2016, 12, 8, 19, 34, 56, 0, ZoneId.of("UTC")).toInstant()
        );

        sql = "INSERT INTO " + MSSQL_Table + " (start_time, record) "
                + "values (?, ?)";
        PreparedStatement insertStmt = connection.prepareStatement(sql);
        insertStmt.setTimestamp(1, t);
        insertStmt.setTimestamp(2, t);
        executeSQL(insertStmt);

        // Wait long enough for connector to query table for new records
        // if works as expected regardless of how long we wait, only one record would be found
        // verifies that there is no looping on the most recent record for datetime2 in MSSQL Server
        Thread.sleep(Duration.ofSeconds(30).toMillis());
        for (String topic: KAFKA_TOPICS) {
            ConsumerRecords<byte[], byte[]> records = connect.kafka().consume(
                    NUM_RECORDS_PRODUCED,
                    CONSUME_MAX_DURATION_MS,
                    topic);
            // Assert that records in topic == NUM_RECORDS_PRODUCED
            assertEquals(NUM_RECORDS_PRODUCED, records.count());
        }
    }

    private Map<String, String> configProperties() {
        // Create a hashmap to setup source connector config properties
        Map<String, String> props = new HashMap<>();
        props.put(CONNECTOR_CLASS_CONFIG, "JdbcSourceConnector");
        props.put(CONNECTION_URL_CONFIG, MSSQL_URL);
        props.put(CONNECTION_USER_CONFIG, "sa");
        props.put(CONNECTION_PASSWORD_CONFIG, "reallyStrongPwd123");
        props.put(TABLE_WHITELIST_CONFIG, MSSQL_Table);
        props.put(TIMESTAMP_COLUMN_NAME_CONFIG, "start_time");
        props.put(TOPIC_PREFIX_CONFIG, "test-");
        props.put(POLL_INTERVAL_MS_CONFIG, "30");
        props.put(TASKS_MAX_CONFIG, Integer.toString(TASKS_MAX));
        props.put(KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        props.put(VALUE_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        return props;
    }

    private void executeSQL(PreparedStatement stmt) throws Exception {
        try {
            stmt.executeUpdate();
        } catch (Exception ex) {
            log.error("Could not execute SQL: " + stmt.toString());
            throw ex;
        }
    }

    private void deleteTable() throws SQLException {
        try {
            PreparedStatement stmt = connection.prepareStatement(
                    "DROP TABLE " + MSSQL_Table
            );
            stmt.executeUpdate();
        } catch (SQLException ex) {
            log.error("Could delete all rows in the MSSQL table");
            throw ex;
        }
    }

    private void configureAndWaitForConnector() throws Exception {
        // start a source connector
        connect.configureConnector(CONNECTOR_NAME, props);

        // wait for tasks to spin up
        int minimumNumTasks = Math.min(KAFKA_TOPICS.size(), TASKS_MAX);
        waitForConnectorToStart(CONNECTOR_NAME, minimumNumTasks);
    }
}
