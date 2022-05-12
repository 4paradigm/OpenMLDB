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

package io.confluent.connect.jdbc.sink;

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.dialect.SqliteDatabaseDialect;
import io.confluent.connect.jdbc.util.CachedConnectionProvider;
import io.confluent.connect.jdbc.util.TableDefinition;
import io.confluent.connect.jdbc.util.TableId;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;

public class JdbcDbWriterTest {

  private final SqliteHelper sqliteHelper = new SqliteHelper(getClass().getSimpleName());

  private JdbcDbWriter writer = null;
  private DatabaseDialect dialect;

  @Before
  public void setUp() throws IOException, SQLException {
    sqliteHelper.setUp();
  }

  @After
  public void tearDown() throws IOException, SQLException {
    if (writer != null)
      writer.closeQuietly();
    sqliteHelper.tearDown();
  }

  private JdbcDbWriter newWriter(Map<String, String> props) {
    final JdbcSinkConfig config = new JdbcSinkConfig(props);
    dialect = new SqliteDatabaseDialect(config);
    final DbStructure dbStructure = new DbStructure(dialect);
    return new JdbcDbWriter(config, dialect, dbStructure);
  }

  private JdbcDbWriter newWriterWithMockConnection(Map<String, String> props, Connection mockConnection) {
    final JdbcSinkConfig config = new JdbcSinkConfig(props);
    dialect = mock(DatabaseDialect.class);
    final DbStructure dbStructure = mock(DbStructure.class);
    return new JdbcDbWriter(config, dialect, dbStructure) {
      protected CachedConnectionProvider connectionProvider(int maxConnAttempts, long retryBackoff) {
        CachedConnectionProvider mockConnectionProvider = mock(CachedConnectionProvider.class);
        when(mockConnectionProvider.getConnection()).thenReturn(mockConnection);
        return mockConnectionProvider;
      }
    };
  }

  private class MockRollbackException extends SQLException {
    public MockRollbackException() {
      super();
    }
  }

  @Test
  public void verifyConnectionRollbackFailed() throws SQLException {
    SQLException e = verifyConnectionRollback(false);

    Throwable[] suppressed = e.getSuppressed();
    assertEquals(suppressed.length, 1);
    assertTrue(suppressed[0] instanceof MockRollbackException);
  }

  @Test
  public void verifyConnectionRollbackSucceeded() throws SQLException {
    SQLException e = verifyConnectionRollback(true);

    Throwable[] suppressed = e.getSuppressed();
    assertEquals(suppressed.length, 0);
  }

  private SQLException verifyConnectionRollback(boolean succeedOnRollBack) throws SQLException {
    Connection mockConnection = mock(Connection.class);

    doThrow(new SQLException()).when(mockConnection).commit();
    if (!succeedOnRollBack) {
      doThrow(new MockRollbackException()).when(mockConnection).rollback();
    }

    Map<String, String> props = new HashMap<>();
    props.put("connection.url", sqliteHelper.sqliteUri());
    props.put("auto.create", "true");
    props.put("auto.evolve", "true");
    props.put("insert.mode", "upsert");
    props.put("pk.mode", "record_key");
    props.put("pk.fields", "id"); // assigned name for the primitive key

    JdbcDbWriter writer = newWriterWithMockConnection(props, mockConnection);

    PreparedStatement mockStatement = mock(PreparedStatement.class);
    when(dialect.parseTableIdentifier(any())).thenReturn(mock(TableId.class));
    when(dialect.createPreparedStatement(any(), any())).thenReturn(mockStatement);
    when(dialect.statementBinder(any(), any(), any(), any(), any(), any()))
        .thenReturn(mock(PreparedStatementBinder.class));
    when(mockStatement.executeBatch()).thenReturn(new int[3]);

    Schema keySchema = Schema.INT64_SCHEMA;

    Schema valueSchema1 = SchemaBuilder.struct()
        .field("author", Schema.STRING_SCHEMA)
        .field("title", Schema.STRING_SCHEMA)
        .build();

    Struct valueStruct1 = new Struct(valueSchema1)
        .put("author", "Tom Robbins")
        .put("title", "Villa Incognito");

    SQLException e = assertThrows(
        SQLException.class,
        () ->
            writer.write(Collections.singleton(new SinkRecord(
                "books", 0,
                keySchema,
                1L,
                valueSchema1,
                valueStruct1,
                0)
            )));

    verify(mockConnection, times(1)).rollback();
    return e;
  }

  @Test
  public void autoCreateWithAutoEvolve() throws SQLException {
    String topic = "books";
    TableId tableId = new TableId(null, null, topic);

    Map<String, String> props = new HashMap<>();
    props.put("connection.url", sqliteHelper.sqliteUri());
    props.put("auto.create", "true");
    props.put("auto.evolve", "true");
    props.put("pk.mode", "record_key");
    props.put("pk.fields", "id"); // assigned name for the primitive key

    writer = newWriter(props);

    Schema keySchema = Schema.INT64_SCHEMA;

    Schema valueSchema1 = SchemaBuilder.struct()
                                       .field("author", Schema.STRING_SCHEMA)
                                       .field("title", Schema.STRING_SCHEMA)
                                       .build();

    Struct valueStruct1 = new Struct(valueSchema1)
        .put("author", "Tom Robbins")
        .put("title", "Villa Incognito");

    writer.write(Collections.singleton(new SinkRecord(topic, 0, keySchema, 1L, valueSchema1,
                                                      valueStruct1, 0)));

    TableDefinition metadata = dialect.describeTable(writer.cachedConnectionProvider.getConnection(),
                                                     tableId);
    assertTrue(metadata.definitionForColumn("id").isPrimaryKey());
    for (Field field : valueSchema1.fields()) {
      assertNotNull(metadata.definitionForColumn(field.name()));
    }

    Schema valueSchema2 = SchemaBuilder.struct()
                                       .field("author", Schema.STRING_SCHEMA)
                                       .field("title", Schema.STRING_SCHEMA)
                                       .field("year", Schema.OPTIONAL_INT32_SCHEMA) // new field
                                       .field("review", SchemaBuilder.string().defaultValue("").build()); // new field

    Struct valueStruct2 = new Struct(valueSchema2)
        .put("author", "Tom Robbins")
        .put("title", "Fierce Invalids")
        .put("year", 2016);

    writer.write(Collections.singleton(new SinkRecord(topic, 0, keySchema, 2L, valueSchema2, valueStruct2, 0)));

    TableDefinition refreshedMetadata = dialect.describeTable(sqliteHelper.connection, tableId);
    assertTrue(refreshedMetadata.definitionForColumn("id").isPrimaryKey());
    for (Field field : valueSchema2.fields()) {
      assertNotNull(refreshedMetadata.definitionForColumn(field.name()));
    }
  }

  @Test(expected = SQLException.class)
  public void multiInsertWithKafkaPkFailsDueToUniqueConstraint() throws SQLException {
    writeSameRecordTwiceExpectingSingleUpdate(JdbcSinkConfig.InsertMode.INSERT, JdbcSinkConfig.PrimaryKeyMode.KAFKA, "");
  }

  @Test
  public void idempotentUpsertWithKafkaPk() throws SQLException {
    writeSameRecordTwiceExpectingSingleUpdate(JdbcSinkConfig.InsertMode.UPSERT, JdbcSinkConfig.PrimaryKeyMode.KAFKA, "");
  }

  @Test(expected = SQLException.class)
  public void multiInsertWithRecordKeyPkFailsDueToUniqueConstraint() throws SQLException {
    writeSameRecordTwiceExpectingSingleUpdate(JdbcSinkConfig.InsertMode.INSERT, JdbcSinkConfig.PrimaryKeyMode.RECORD_KEY, "");
  }

  @Test
  public void idempotentUpsertWithRecordKeyPk() throws SQLException {
    writeSameRecordTwiceExpectingSingleUpdate(JdbcSinkConfig.InsertMode.UPSERT, JdbcSinkConfig.PrimaryKeyMode.RECORD_KEY, "");
  }

  @Test(expected = SQLException.class)
  public void multiInsertWithRecordValuePkFailsDueToUniqueConstraint() throws SQLException {
    writeSameRecordTwiceExpectingSingleUpdate(JdbcSinkConfig.InsertMode.INSERT, JdbcSinkConfig.PrimaryKeyMode.RECORD_VALUE, "author,title");
  }

  @Test
  public void idempotentUpsertWithRecordValuePk() throws SQLException {
    writeSameRecordTwiceExpectingSingleUpdate(JdbcSinkConfig.InsertMode.UPSERT, JdbcSinkConfig.PrimaryKeyMode.RECORD_VALUE, "author,title");
  }

  private void writeSameRecordTwiceExpectingSingleUpdate(
      JdbcSinkConfig.InsertMode insertMode,
      JdbcSinkConfig.PrimaryKeyMode pkMode,
      String pkFields
  ) throws SQLException {
    String topic = "books";
    int partition = 7;
    long offset = 42;

    Map<String, String> props = new HashMap<>();
    props.put("connection.url", sqliteHelper.sqliteUri());
    props.put("auto.create", "true");
    props.put("pk.mode", pkMode.toString());
    props.put("pk.fields", pkFields);
    props.put("insert.mode", insertMode.toString());

    writer = newWriter(props);

    Schema keySchema = SchemaBuilder.struct()
        .field("id", SchemaBuilder.INT64_SCHEMA);

    Struct keyStruct = new Struct(keySchema).put("id", 0L);

    Schema valueSchema = SchemaBuilder.struct()
        .field("author", Schema.STRING_SCHEMA)
        .field("title", Schema.STRING_SCHEMA)
        .build();

    Struct valueStruct = new Struct(valueSchema)
        .put("author", "Tom Robbins")
        .put("title", "Villa Incognito");

    SinkRecord record = new SinkRecord(topic, partition, keySchema, keyStruct, valueSchema, valueStruct, offset);

    writer.write(Collections.nCopies(2, record));

    assertEquals(
        1,
        sqliteHelper.select("select count(*) from books", new SqliteHelper.ResultSetReadCallback() {
          @Override
          public void read(ResultSet rs) throws SQLException {
            assertEquals(1, rs.getInt(1));
          }
        })
    );
  }

  @Test
  public void idempotentDeletes() throws SQLException {
    String topic = "books";
    int partition = 7;
    long offset = 42;

    Map<String, String> props = new HashMap<>();
    props.put("connection.url", sqliteHelper.sqliteUri());
    props.put("auto.create", "true");
    props.put("delete.enabled", "true");
    props.put("pk.mode", "record_key");
    props.put("insert.mode", "upsert");

    writer = newWriter(props);

    Schema keySchema = SchemaBuilder.struct()
        .field("id", SchemaBuilder.INT64_SCHEMA);

    Struct keyStruct = new Struct(keySchema).put("id", 0L);

    Schema valueSchema = SchemaBuilder.struct()
        .field("author", Schema.STRING_SCHEMA)
        .field("title", Schema.STRING_SCHEMA)
        .build();

    Struct valueStruct = new Struct(valueSchema)
        .put("author", "Tom Robbins")
        .put("title", "Villa Incognito");

    SinkRecord record = new SinkRecord(topic, partition, keySchema, keyStruct, valueSchema, valueStruct, offset);

    writer.write(Collections.nCopies(2, record));

    SinkRecord deleteRecord = new SinkRecord(topic, partition, keySchema, keyStruct, null, null, offset);
    writer.write(Collections.nCopies(2, deleteRecord));

    assertEquals(
        1,
        sqliteHelper.select("select count(*) from books", new SqliteHelper.ResultSetReadCallback() {
          @Override
          public void read(ResultSet rs) throws SQLException {
            assertEquals(0, rs.getInt(1));
          }
        })
    );
  }

  @Test
  public void insertDeleteSameRecord() throws SQLException {
    String topic = "books";
    int partition = 7;
    long offset = 42;

    Map<String, String> props = new HashMap<>();
    props.put("connection.url", sqliteHelper.sqliteUri());
    props.put("auto.create", "true");
    props.put("delete.enabled", "true");
    props.put("pk.mode", "record_key");
    props.put("insert.mode", "upsert");

    writer = newWriter(props);

    Schema keySchema = SchemaBuilder.struct()
        .field("id", SchemaBuilder.INT64_SCHEMA);

    Struct keyStruct = new Struct(keySchema).put("id", 0L);

    Schema valueSchema = SchemaBuilder.struct()
        .field("author", Schema.STRING_SCHEMA)
        .field("title", Schema.STRING_SCHEMA)
        .build();

    Struct valueStruct = new Struct(valueSchema)
        .put("author", "Tom Robbins")
        .put("title", "Villa Incognito");

    SinkRecord record = new SinkRecord(topic, partition, keySchema, keyStruct, valueSchema, valueStruct, offset);
    SinkRecord deleteRecord = new SinkRecord(topic, partition, keySchema, keyStruct, null, null, offset);
    writer.write(Collections.singletonList(record));
    writer.write(Collections.singletonList(deleteRecord));

    assertEquals(
        1,
        sqliteHelper.select("select count(*) from books", new SqliteHelper.ResultSetReadCallback() {
          @Override
          public void read(ResultSet rs) throws SQLException {
            assertEquals(0, rs.getInt(1));
          }
        })
    );
  }

  @Test
  public void insertDeleteInsertSameRecord() throws SQLException {
    String topic = "books";
    int partition = 7;
    long offset = 42;

    Map<String, String> props = new HashMap<>();
    props.put("connection.url", sqliteHelper.sqliteUri());
    props.put("auto.create", "true");
    props.put("delete.enabled", "true");
    props.put("pk.mode", "record_key");
    props.put("insert.mode", "upsert");

    writer = newWriter(props);

    Schema keySchema = SchemaBuilder.struct()
        .field("id", SchemaBuilder.INT64_SCHEMA);

    Struct keyStruct = new Struct(keySchema).put("id", 0L);

    Schema valueSchema = SchemaBuilder.struct()
        .field("author", Schema.STRING_SCHEMA)
        .field("title", Schema.STRING_SCHEMA)
        .build();

    Struct valueStruct = new Struct(valueSchema)
        .put("author", "Tom Robbins")
        .put("title", "Villa Incognito");

    SinkRecord record = new SinkRecord(topic, partition, keySchema, keyStruct, valueSchema, valueStruct, offset);
    SinkRecord deleteRecord = new SinkRecord(topic, partition, keySchema, keyStruct, null, null, offset);
    writer.write(Collections.singletonList(record));
    writer.write(Collections.singletonList(deleteRecord));
    writer.write(Collections.singletonList(record));

    assertEquals(
        1,
        sqliteHelper.select("select count(*) from books", new SqliteHelper.ResultSetReadCallback() {
          @Override
          public void read(ResultSet rs) throws SQLException {
            assertEquals(1, rs.getInt(1));
          }
        })
    );
  }

  @Test
  public void sameRecordNTimes() throws SQLException {
    String testId = "sameRecordNTimes";
    String createTable = "CREATE TABLE " + testId + " (" +
                         "    the_byte  INTEGER," +
                         "    the_short INTEGER," +
                         "    the_int INTEGER," +
                         "    the_long INTEGER," +
                         "    the_float REAL," +
                         "    the_double REAL," +
                         "    the_bool  INTEGER," +
                         "    the_string TEXT," +
                         "    the_bytes BLOB, " +
                         "    the_decimal  NUMERIC," +
                         "    the_date  NUMERIC," +
                         "    the_time  NUMERIC," +
                         "    the_timestamp  NUMERIC" +
                         ");";

    sqliteHelper.deleteTable(testId);
    sqliteHelper.createTable(createTable);

    Schema schema = SchemaBuilder.struct().name(testId)
        .field("the_byte", Schema.INT8_SCHEMA)
        .field("the_short", Schema.INT16_SCHEMA)
        .field("the_int", Schema.INT32_SCHEMA)
        .field("the_long", Schema.INT64_SCHEMA)
        .field("the_float", Schema.FLOAT32_SCHEMA)
        .field("the_double", Schema.FLOAT64_SCHEMA)
        .field("the_bool", Schema.BOOLEAN_SCHEMA)
        .field("the_string", Schema.STRING_SCHEMA)
        .field("the_bytes", Schema.BYTES_SCHEMA)
        .field("the_decimal", Decimal.schema(2).schema())
        .field("the_date", Date.SCHEMA)
        .field("the_time", Time.SCHEMA)
        .field("the_timestamp", Timestamp.SCHEMA);

    final java.util.Date instant = new java.util.Date(1474661402123L);

    final Struct struct = new Struct(schema)
        .put("the_byte", (byte) -32)
        .put("the_short", (short) 1234)
        .put("the_int", 42)
        .put("the_long", 12425436L)
        .put("the_float", 2356.3f)
        .put("the_double", -2436546.56457d)
        .put("the_bool", true)
        .put("the_string", "foo")
        .put("the_bytes", new byte[]{-32, 124})
        .put("the_decimal", new BigDecimal("1234.567"))
        .put("the_date", instant)
        .put("the_time", instant)
        .put("the_timestamp", instant);

    int numRecords = ThreadLocalRandom.current().nextInt(20, 80);

    Map<String, String> props = new HashMap<>();
    props.put("connection.url", sqliteHelper.sqliteUri());
    props.put("table.name.format", testId);
    props.put("batch.size", String.valueOf(ThreadLocalRandom.current().nextInt(20, 100)));

    writer = newWriter(props);

    writer.write(Collections.nCopies(
        numRecords,
        new SinkRecord("topic", 0, null, null, schema, struct, 0)
    ));

    assertEquals(
        numRecords,
        sqliteHelper.select(
            "SELECT * FROM " + testId,
            new SqliteHelper.ResultSetReadCallback() {
              @Override
              public void read(ResultSet rs) throws SQLException {
                assertEquals(struct.getInt8("the_byte").byteValue(), rs.getByte("the_byte"));
                assertEquals(struct.getInt16("the_short").shortValue(), rs.getShort("the_short"));
                assertEquals(struct.getInt32("the_int").intValue(), rs.getInt("the_int"));
                assertEquals(struct.getInt64("the_long").longValue(), rs.getLong("the_long"));
                assertEquals(struct.getFloat32("the_float"), rs.getFloat("the_float"), 0.01);
                assertEquals(struct.getFloat64("the_double"), rs.getDouble("the_double"), 0.01);
                assertEquals(struct.getBoolean("the_bool"), rs.getBoolean("the_bool"));
                assertEquals(struct.getString("the_string"), rs.getString("the_string"));
                assertArrayEquals(struct.getBytes("the_bytes"), rs.getBytes("the_bytes"));
                assertEquals(struct.get("the_decimal"), rs.getBigDecimal("the_decimal"));
                assertEquals(new java.sql.Date(((java.util.Date) struct.get("the_date")).getTime()), rs.getDate("the_date"));
                assertEquals(new java.sql.Time(((java.util.Date) struct.get("the_time")).getTime()), rs.getTime("the_time"));
                assertEquals(new java.sql.Timestamp(((java.util.Date) struct.get("the_time")).getTime()), rs.getTimestamp("the_timestamp"));
              }
            }
        )
    );
  }

}