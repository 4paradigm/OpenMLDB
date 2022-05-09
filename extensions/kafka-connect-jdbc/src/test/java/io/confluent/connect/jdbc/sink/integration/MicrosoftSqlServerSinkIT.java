package io.confluent.connect.jdbc.sink.integration;

import io.confluent.connect.jdbc.integration.BaseConnectorIT;
import io.confluent.connect.jdbc.sink.JdbcSinkConfig;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.storage.StringConverter;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.FixedHostPortGenericContainer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG;

public class MicrosoftSqlServerSinkIT extends BaseConnectorIT {
    private static final Logger log = LoggerFactory.getLogger(MicrosoftSqlServerSinkIT.class);
    private static final String CONNECTOR_NAME = "jdbc-sink-connector";
    private static final int TASKS_MAX = 3;
    private static final String MSSQL_URL = "jdbc:sqlserver://0.0.0.0:1433";
    private static final String MSSQL_Table = "example_table";
    private static final List<String> KAFKA_TOPICS = Collections.singletonList(MSSQL_Table);
    private Map<String, String> props;
    private Connection connection;
    private JsonConverter jsonConverter;

    private static final String USER = "sa"; // test creds
    private static final String PASS = "reallyStrongPwd123"; // test creds

    @ClassRule
    @SuppressWarnings("deprecation")
    public static final FixedHostPortGenericContainer mssqlServer =
            new FixedHostPortGenericContainer<>("microsoft/mssql-server-linux:latest")
                    .withEnv("ACCEPT_EULA","Y")
                    .withEnv("SA_PASSWORD", PASS)
                    .withFixedExposedPort(1433, 1433);

    @Before
    public void setup() throws Exception {
        //Set up JDBC Driver
        Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        connection = DriverManager.getConnection(MSSQL_URL, USER, PASS);
        startConnect();
        jsonConverter =  jsonConverter();
    }

    @After
    public void close() throws SQLException {
        connect.deleteConnector(CONNECTOR_NAME);
        connection.close();
        // Stop all Connect, Kafka and Zk threads.
        stopConnect();
    }

    /**
     * Verify that if a table is specified without a schema/owner
     * and the table schema/owner is not dbo the connector fails
     * with error of form Table is missing and auto-creation is disabled
     */
    @Test
    public void verifyConnectorFailsWhenTableNameS() throws Exception {
        // Setup up props for the sink connector
        props = configProperties();

        // create table
        String sql = "CREATE TABLE guest." + MSSQL_Table
                + " (id int NULL, last_name VARCHAR(50), created_at DATETIME2 NOT NULL);";
        PreparedStatement createStmt = connection.prepareStatement(sql);
        executeSQL(createStmt);

        // Create topic in Kafka
        KAFKA_TOPICS.forEach(topic -> connect.kafka().createTopic(topic, 1));

        // Configure sink connector
        configureAndWaitForConnector();

        //create record and produce it
        Timestamp t = Timestamp.from(
                ZonedDateTime.of(2017, 12, 8, 19, 34, 56, 0, ZoneId.of("UTC")).toInstant()
        );
        final Schema schema = SchemaBuilder.struct().name("com.example.Person")
                .field("id", Schema.INT32_SCHEMA)
                .field("last_name", Schema.STRING_SCHEMA)
                .field("created_at", org.apache.kafka.connect.data.Timestamp.SCHEMA)
                .build();
        final Struct struct = new Struct(schema)
                .put("id", 1)
                .put("last_name", "Brams")
                .put("created_at", t);

        String kafkaValue = new String(jsonConverter.fromConnectData(MSSQL_Table, schema, struct));
        connect.kafka().produce(MSSQL_Table, null, kafkaValue);

        //sleep till it fails
        Thread.sleep(Duration.ofSeconds(30).toMillis());

        //verify that connector failed because it cannot find the table.
        assertTasksFailedWithTrace(
                CONNECTOR_NAME,
                Math.min(KAFKA_TOPICS.size(), TASKS_MAX),
                "Table \"dbo\".\""
                    + MSSQL_Table
                    + "\" is missing and auto-creation is disabled"
        );
    }

    private Map<String, String> configProperties() {
        // Create a hashmap to setup sink connector config properties
        Map<String, String> props = new HashMap<>();

        props.put(CONNECTOR_CLASS_CONFIG, "JdbcSinkConnector");
        // converters
        props.put(KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        props.put(VALUE_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());
        // license properties
        props.put("confluent.topic.bootstrap.servers", connect.kafka().bootstrapServers());
        props.put("confluent.topic.replication.factor", "1");

        props.put(JdbcSinkConfig.CONNECTION_URL, MSSQL_URL);
        props.put(JdbcSinkConfig.CONNECTION_USER, USER);
        props.put(JdbcSinkConfig.CONNECTION_PASSWORD, PASS);
        props.put("pk.mode", "none");
        props.put("topics", MSSQL_Table);
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

    private void configureAndWaitForConnector() throws Exception {
        // start a sink connector
        connect.configureConnector(CONNECTOR_NAME, props);

        // wait for tasks to spin up
        int minimumNumTasks = Math.min(KAFKA_TOPICS.size(), TASKS_MAX);
        waitForConnectorToStart(CONNECTOR_NAME, minimumNumTasks);
    }
}
