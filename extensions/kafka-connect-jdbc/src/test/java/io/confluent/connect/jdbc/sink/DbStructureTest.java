package io.confluent.connect.jdbc.sink;

import org.apache.kafka.connect.data.Schema;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.sink.metadata.FieldsMetadata;
import io.confluent.connect.jdbc.sink.metadata.SinkRecordField;
import io.confluent.connect.jdbc.util.TableDefinition;
import io.confluent.connect.jdbc.util.TableId;
import io.confluent.connect.jdbc.util.TableType;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

public class DbStructureTest {

  DatabaseDialect dbDialect = mock(DatabaseDialect.class);
  DbStructure structure = new DbStructure(dbDialect);
  Connection connection = mock(Connection.class);
  TableId tableId = mock(TableId.class);
  JdbcSinkConfig config = mock(JdbcSinkConfig.class);
  FieldsMetadata fieldsMetadata = new FieldsMetadata(new HashSet<>(), new HashSet<>(), new HashMap<>());

  @Test
  public void testNoMissingFields() {
    assertTrue(missingFields(sinkRecords("aaa"), columns("aaa", "bbb")).isEmpty());
  }

  @Test
  public void testMissingFieldsWithSameCase() {
    assertEquals(1, missingFields(sinkRecords("aaa", "bbb"), columns("aaa")).size());
  }

  @Test
  public void testSameNamesDifferentCases() {
    assertTrue(missingFields(sinkRecords("aaa"), columns("aAa", "AaA")).isEmpty());
  }

  @Test
  public void testMissingFieldsWithDifferentCase() {
    assertTrue(missingFields(sinkRecords("aaa", "bbb"), columns("AaA", "BbB")).isEmpty());
    assertTrue(missingFields(sinkRecords("AaA", "bBb"), columns("aaa", "bbb")).isEmpty());
    assertTrue(missingFields(sinkRecords("AaA", "bBb"), columns("aAa", "BbB")).isEmpty());
  }

  @Test (expected = TableAlterOrCreateException.class)
  public void testMissingTableNoAutoCreate() throws Exception {
    structure.create(config, connection, tableId,
        fieldsMetadata);
  }

  @Test (expected = TableAlterOrCreateException.class)
  public void testCreateOrAlterNoAutoEvolve() throws Exception {
    when(dbDialect.tableExists(any(), any())).thenReturn(false);

    SinkRecordField sinkRecordField = new SinkRecordField(
        Schema.OPTIONAL_INT32_SCHEMA,
        "test",
        false
    );

    fieldsMetadata = new FieldsMetadata(
        Collections.emptySet(),
        Collections.singleton(sinkRecordField.name()),
        Collections.singletonMap(sinkRecordField.name(), sinkRecordField));

    structure.createOrAmendIfNecessary(config, connection, tableId, fieldsMetadata);
  }

  @Test (expected = TableAlterOrCreateException.class)
  public void testAlterNoAutoEvolve() throws Exception {
    TableDefinition tableDefinition = mock(TableDefinition.class);
    when(dbDialect.tableExists(any(), any())).thenReturn(true);
    when(dbDialect.describeTable(any(), any())).thenReturn(tableDefinition);
    when(tableDefinition.type()).thenReturn(TableType.TABLE);

    SinkRecordField sinkRecordField = new SinkRecordField(
        Schema.OPTIONAL_INT32_SCHEMA,
        "test",
        false
    );

    fieldsMetadata = new FieldsMetadata(
        Collections.emptySet(),
        Collections.singleton(sinkRecordField.name()),
        Collections.singletonMap(sinkRecordField.name(), sinkRecordField));

    structure.amendIfNecessary(config, connection, tableId,
        fieldsMetadata, 5);
  }

  @Test (expected = TableAlterOrCreateException.class)
  public void testAlterNotSupported() throws Exception {
    TableDefinition tableDefinition = mock(TableDefinition.class);
    when(dbDialect.tableExists(any(), any())).thenReturn(true);
    when(dbDialect.describeTable(any(), any())).thenReturn(tableDefinition);
    when(tableDefinition.type()).thenReturn(TableType.VIEW);

    SinkRecordField sinkRecordField = new SinkRecordField(
        Schema.OPTIONAL_INT32_SCHEMA,
        "test",
        true
    );
    fieldsMetadata = new FieldsMetadata(
        Collections.emptySet(),
        Collections.singleton(sinkRecordField.name()),
        Collections.singletonMap(sinkRecordField.name(), sinkRecordField));

    structure.amendIfNecessary(config, connection, tableId,
        fieldsMetadata, 5);
  }

  @Test (expected = TableAlterOrCreateException.class)
  public void testCannotAlterBecauseFieldNotOptionalAndNoDefaultValue() throws Exception {
    TableDefinition tableDefinition = mock(TableDefinition.class);
    when(dbDialect.tableExists(any(), any())).thenReturn(true);
    when(dbDialect.describeTable(any(), any())).thenReturn(tableDefinition);
    when(tableDefinition.type()).thenReturn(TableType.VIEW);

    SinkRecordField sinkRecordField = new SinkRecordField(
        Schema.INT32_SCHEMA,
        "test",
        true
    );
    fieldsMetadata = new FieldsMetadata(
        Collections.emptySet(),
        Collections.singleton(sinkRecordField.name()),
        Collections.singletonMap(sinkRecordField.name(), sinkRecordField));

    structure.amendIfNecessary(config, connection, tableId,
        fieldsMetadata, 5);
  }

  @Test (expected = TableAlterOrCreateException.class)
  public void testFailedToAmendExhaustedRetry() throws Exception {
    TableDefinition tableDefinition = mock(TableDefinition.class);
    when(dbDialect.tableExists(any(), any())).thenReturn(true);
    when(dbDialect.describeTable(any(), any())).thenReturn(tableDefinition);
    when(tableDefinition.type()).thenReturn(TableType.VIEW);

    SinkRecordField sinkRecordField = new SinkRecordField(
        Schema.OPTIONAL_INT32_SCHEMA,
        "test",
        false
    );
    fieldsMetadata = new FieldsMetadata(
        Collections.emptySet(),
        Collections.singleton(sinkRecordField.name()),
        Collections.singletonMap(sinkRecordField.name(), sinkRecordField));

    Map<String, String> props = new HashMap<>();

    // Required configurations, set to empty strings because they are irrelevant for the test
    props.put("connection.url", "");
    props.put("connection.user", "");
    props.put("connection.password", "");

    // Set to true so that the connector does not throw the exception on a different condition
    props.put("auto.evolve", "true");
    JdbcSinkConfig config = new JdbcSinkConfig(props);

    doThrow(new SQLException()).when(dbDialect).applyDdlStatements(any(), any());

    structure.amendIfNecessary(config, connection, tableId,
        fieldsMetadata, 0);
  }

  private Set<SinkRecordField> missingFields(
      Collection<SinkRecordField> fields,
      Set<String> dbColumnNames
  ) {
    return structure.missingFields(fields, dbColumnNames);
  }

  static Set<String> columns(String... names) {
    return new HashSet<>(Arrays.asList(names));
  }

  static List<SinkRecordField> sinkRecords(String... names) {
    List<SinkRecordField> fields = new ArrayList<>();
    for (String n : names) {
      fields.add(field(n));
    }
    return fields;
  }

  static SinkRecordField field(String name) {
    return new SinkRecordField(Schema.STRING_SCHEMA, name, false);
  }
}
