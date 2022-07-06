package io.confluent.connect.jdbc.sink.integration;

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.json.JsonConverter;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.testcontainers.containers.OracleContainer;
import org.testcontainers.utility.ThrowingFunction;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.ZoneId;
import java.util.Calendar;
import java.util.Collections;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import io.confluent.common.utils.IntegrationTest;
import io.confluent.connect.jdbc.integration.BaseConnectorIT;
import io.confluent.connect.jdbc.sink.JdbcSinkConfig;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Category(IntegrationTest.class)
public class OracleDatatypeIT extends BaseConnectorIT {
    @SuppressWarnings( "deprecation" )
    @Rule
    public OracleContainer oracle = new OracleContainer();

    Connection connection;
    private JsonConverter jsonConverter;
    private Map<String, String> props;
    private String tableName;

    @Before
    public void setup() throws SQLException {
        startConnect();

        jsonConverter = jsonConverter();
        props = baseSinkProps();

        tableName = "TEST";
        props.put(JdbcSinkConfig.CONNECTION_URL, oracle.getJdbcUrl());
        props.put(JdbcSinkConfig.CONNECTION_USER, oracle.getUsername());
        props.put(JdbcSinkConfig.CONNECTION_PASSWORD, oracle.getPassword());
        props.put(JdbcSinkConfig.PK_MODE, "record_value");
        props.put(JdbcSinkConfig.PK_FIELDS, "KEY");
        props.put(JdbcSinkConfig.AUTO_CREATE, "false");
        props.put(JdbcSinkConfig.MAX_RETRIES, "0");
        props.put("topics", tableName);

        // create topic in Kafka
        connect.kafka().createTopic(tableName, 1);

        connection = DriverManager.getConnection(oracle.getJdbcUrl(),
            oracle.getUsername(), oracle.getPassword());
    }

    @After
    public void tearDown() throws SQLException {
        connection.close();

        stopConnect();
    }

    @Test
    public void testPrimitiveAndLogicalTypesInsert() throws Exception {
        createPrimitiveAndLogicalTypesTable();

        testPrimitiveAndLogicalTypes("insert");
    }

    @Test
    public void testPrimitiveAndLogicalTypesUpsert() throws Exception {
        createPrimitiveAndLogicalTypesTable();

        testPrimitiveAndLogicalTypes("upsert");
    }

    @Test
    public void testPrimitiveAndLogicalTypesUpdate() throws Exception {
        createPrimitiveAndLogicalTypesTable();

        try (Statement s = connection.createStatement()) {
            s.execute("INSERT INTO " + tableName + " VALUES (0, 0, 0, 0, 0, 0, 0, '0', EMPTY_BLOB(), DATE '2022-01-01', DATE '2022-01-01', TIMESTAMP '2022-01-01 09:26:50.12', 0, 1)");
        }

        testPrimitiveAndLogicalTypes("update");
    }

    private void createPrimitiveAndLogicalTypesTable() throws SQLException {
        try (Statement s = connection.createStatement()) {
            s.execute("CREATE TABLE " + tableName + "("
                + "\"boolean\" NUMBER(1,0),"
                + "\"int8\" NUMBER, "
                + "\"int16\" NUMBER, "
                + "\"int32\" NUMBER, "
                + "\"int64\" NUMBER, "
                + "\"float32\" NUMBER, "
                + "\"float64\" NUMBER, "
                + "\"string\" VARCHAR2(100), "
                + "\"bytes\" BLOB, "
                + "\"date\" DATE, "
                + "\"time\" DATE, "
                + "\"timestamp\" TIMESTAMP, "
                + "\"decimal\" NUMBER, "
                + "KEY NUMBER NOT NULL, PRIMARY KEY (KEY)"
                + ")");
        }
    }

    private void testPrimitiveAndLogicalTypes(String insertMode) throws Exception {
        props.put(JdbcSinkConfig.INSERT_MODE, insertMode);

        final Schema schema = SchemaBuilder.struct()
            .field("boolean", Schema.BOOLEAN_SCHEMA)
            .field("int8", Schema.INT8_SCHEMA)
            .field("int16", Schema.INT16_SCHEMA)
            .field("int32", Schema.INT32_SCHEMA)
            .field("int64", Schema.INT64_SCHEMA)
            .field("float32", Schema.FLOAT32_SCHEMA)
            .field("float64", Schema.FLOAT64_SCHEMA)
            .field("string", Schema.STRING_SCHEMA)
            .field("bytes", Schema.BYTES_SCHEMA)
            .field("date", Date.SCHEMA)
            .field("time", Time.SCHEMA)
            .field("timestamp", Timestamp.SCHEMA)
            .field("decimal", Decimal.schema(0))
            .field("KEY", Schema.INT32_SCHEMA)
            .build();

        java.util.Date date = new java.util.Date(0);
        java.util.Date time = new java.util.Date(0);
        java.util.Date timestamp = new java.util.Date();
        BigDecimal decimal = new BigDecimal(2022);

        final Struct value = new Struct(schema)
            .put("boolean", false)
            .put("int8", (byte) 1)
            .put("int16", (short) 2)
            .put("int32", 3)
            .put("int64", 4L)
            .put("float32", (float) 5.0)
            .put("float64", (double) 6.0)
            .put("string", "7")
            .put("bytes", "8".getBytes(StandardCharsets.UTF_8))
            .put("date", date)
            .put("time", time)
            .put("timestamp", timestamp)
            .put("decimal", decimal)
            .put("KEY", 1);

        assertProduced(schema, value, (rs) -> {
            assertFalse(rs.getBoolean(1));
            assertEquals((byte) 1, rs.getByte(2));
            assertEquals((short) 2, rs.getShort(3));
            assertEquals(3, rs.getInt(4));
            assertEquals(4L, rs.getLong(5));
            assertEquals((float) 5.0, rs.getFloat(6), 0.01);
            assertEquals((double) 6.0, rs.getDouble(7), 0.01);
            assertEquals("7", rs.getString(8));
            assertEquals("8", new String(rs.getBytes(9)));
            assertEquals(date, rs.getDate(10,
                Calendar.getInstance(TimeZone.getTimeZone(ZoneId.of("UTC")))));
            assertEquals(time, rs.getTime(11,
                Calendar.getInstance(TimeZone.getTimeZone(ZoneId.of("UTC")))));
            assertEquals(timestamp, rs.getTimestamp(12,
                Calendar.getInstance(TimeZone.getTimeZone(ZoneId.of("UTC")))));
            assertEquals(decimal, rs.getBigDecimal(13));
            return null;
        });
    }

    @Test
    public void testShortAndLongStringInsert() throws Exception {
        createShortAndLongStringTable();
        testShortAndLongString("insert");
    }

    @Test
    public void testShortAndLongStringUpsert() throws Exception {
        createShortAndLongStringTable();

        testShortAndLongString("upsert");
    }

    @Test
    public void testShortAndLongStringUpdate() throws Exception {
        createShortAndLongStringTable();

        try (Statement s = connection.createStatement()) {
            s.execute("INSERT INTO " + tableName + " VALUES ('', EMPTY_CLOB(), 1)");
        }

        testShortAndLongString("update");
    }

    private void createShortAndLongStringTable() throws SQLException {
        try (Statement s = connection.createStatement()) {
            s.execute("CREATE TABLE " + tableName + "("
                + "\"shortString\" VARCHAR2(100),"
                + "\"longString\" CLOB, "
                + "KEY NUMBER NOT NULL, PRIMARY KEY (KEY)"
                + ")");
        }
    }

    @SuppressWarnings( "deprecation" )
    private void testShortAndLongString(String insertMode) throws Exception {
        props.put(JdbcSinkConfig.INSERT_MODE, insertMode);

        final String shortString = "shortString";
        final String longString = org.apache.commons.lang3.RandomStringUtils
            .randomAlphanumeric(40001);

        final Schema schema = SchemaBuilder.struct()
            .field("shortString", Schema.STRING_SCHEMA)
            .field("longString", Schema.STRING_SCHEMA)
            .field("KEY", Schema.INT32_SCHEMA)
            .build();
        final Struct value = new Struct(schema)
            .put("shortString", shortString)
            .put("longString", longString)
            .put("KEY", 1);
        assertProduced(schema, value, (rs) -> {
            assertEquals(shortString, rs.getString(1));
            assertEquals(longString, rs.getString(2));
            return null;
        });
    }

    private void assertProduced(
        Schema schema, Struct value, ThrowingFunction<ResultSet, Void> assertion) throws Exception {

        connect.configureConnector("jdbc-sink-connector", props);
        waitForConnectorToStart("jdbc-sink-connector", 1);

        produceRecord(schema, value);

        waitForCommittedRecords("jdbc-sink-connector", Collections.singleton(tableName),
            1, 1,
            TimeUnit.MINUTES.toMillis(3));

        try (Statement s = connection.createStatement()) {
            ResultSet rs = s.executeQuery(
                "SELECT * FROM " + tableName + " ORDER BY KEY DESC FETCH FIRST 1 ROWS ONLY");
            assertTrue(rs.next());
            assertion.apply(rs);
        }
    }

    private void produceRecord(Schema schema, Struct struct) {
        String kafkaValue = new String(jsonConverter.fromConnectData(tableName, schema, struct));
        connect.kafka().produce(tableName, null, kafkaValue);
    }
}
