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

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.dialect.DatabaseDialect.StatementBinder;
import io.confluent.connect.jdbc.sink.metadata.FieldsMetadata;
import io.confluent.connect.jdbc.sink.metadata.SchemaPair;
import io.confluent.connect.jdbc.util.ColumnDefinition;
import io.confluent.connect.jdbc.util.ColumnId;
import io.confluent.connect.jdbc.util.TableDefinition;
import io.confluent.connect.jdbc.util.TableId;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.text.ParseException;

import org.apache.commons.lang3.time.DateUtils;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

public class BufferedRecords {
  private static final Logger log = LoggerFactory.getLogger(BufferedRecords.class);

  private final TableId tableId;
  private final JdbcSinkConfig config;
  private final DatabaseDialect dbDialect;
  private final DbStructure dbStructure;
  private final Connection connection;

  private List<SinkRecord> records = new ArrayList<>();
  private Schema keySchema;
  private Schema valueSchema;
  private RecordValidator recordValidator;
  private FieldsMetadata fieldsMetadata;
  private PreparedStatement updatePreparedStatement;
  private PreparedStatement deletePreparedStatement;
  private StatementBinder updateStatementBinder;
  private StatementBinder deleteStatementBinder;
  private boolean deletesInBatch = false;

  private static String[] parsePatterns = {"yyyy-MM-dd",
    "yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd HH:mm",
    "yyyy/MM/dd","yyyy-MM-dd HH:mm:ss.S",
    "yyyy/MM/dd HH:mm:ss", "yyyy/MM/dd HH:mm", 
    "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"};

  public BufferedRecords(JdbcSinkConfig config, TableId tableId, DatabaseDialect dbDialect,
      DbStructure dbStructure, Connection connection) {
    this.tableId = tableId;
    this.config = config;
    this.dbDialect = dbDialect;
    this.dbStructure = dbStructure;
    this.connection = connection;
    this.recordValidator = RecordValidator.create(config);
    if (config.autoSchema) {
      TableDefinition tableDefn;
      try {
        tableDefn = dbStructure.tableDefinition(connection, tableId);
        if (tableDefn == null) {
          throw new SQLException("can't get table info from sink db");
        }
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
      SchemaBuilder builder = SchemaBuilder.struct();
      for (ColumnDefinition colDefn : tableDefn.definitionsForColumns()) {
        dbDialect.addFieldToSchema(colDefn, builder);
      }
      valueSchema = builder.build();
    }
  }

  private static Object convertToLogicalType(Field field, Object value) {
    // may don't have logical name
    if (field.schema().name() == null) {
      return null;
    }
    switch (field.schema().name()) {
      case Date.LOGICAL_NAME:
        // support string to Date(able to have time, ref spark)
        if (value instanceof String) {
          try {
            return DateUtils.parseDate((String) value, parsePatterns);
          } catch (ParseException e) {
            throw new IllegalArgumentException("Invalid date format");
          }
        }
        // date in json can be day int, convert to millisecond
        return new java.util.Date((Long) value * 24 * 60 * 60 * 1000);
      case Time.LOGICAL_NAME:
      case Timestamp.LOGICAL_NAME:
        // support string to Date, int type to Date(if not, let it crash)
        if (value instanceof String) {
          try {
            return DateUtils.parseDate((String) value, parsePatterns);
          } catch (ParseException e) {
            throw new IllegalArgumentException("Invalid timestamp format");
          }
        }
        return new java.util.Date((Long) value);
      default:
        throw new IllegalArgumentException("Unsupported logical type " + field.schema().name());
    }
  }

  private static Object convertToSchemaType(Field field, Object value) {
    Object result = value;
    switch (field.schema().type()) {
      case INT16: {
        if (value instanceof Long) {
          result = ((Long) value).shortValue();
        }
        break;
      }
      case INT32: {
        if (value instanceof Long) {
          result = ((Long) value).intValue();
        }
        break;
      }
      case FLOAT32: {
        if (value instanceof Double) {
          result = ((Double) value).floatValue();
        }
        break;
      }
      case FLOAT64: {
        if (!(value instanceof Double)) {
          log.warn("json row type is not double");
        }
        break;
      }
      default: {
        log.debug("field {}-type {}, value type {}, stay", field.name(), field.schema().type(),
            value.getClass().getSimpleName());
      }
    }
    // no need to convert type, use origin type(e.g. Long, Double, String, etc.)
    return result;
  }

  public static Object convertToStruct(Schema valueSchema, Object value) {
    SchemaBuilder validSchemaBuilder = SchemaBuilder.struct();
    HashMap<String, Object> valueCache = new HashMap<String, Object>();
    @SuppressWarnings("unchecked")
    HashMap<String, Object> map = (HashMap<String, Object>) value;
    for (Field field : valueSchema.fields()) {
      Object v = map.get(field.name());
      Object newV = v;
      if (v != null) {
        // convert to the right type with schema(logical type first, if not, schema type)
        newV = convertToLogicalType(field, v);
        if (newV == null) {
          newV = convertToSchemaType(field, v);
        }
      }
      // if no value, don't put it into struct, so the column won't be in insert sql
      if (newV != null) {
        validSchemaBuilder.field(field.name(), field.schema());
        valueCache.put(field.name(), newV);
      }
    }

    Struct structValue = new Struct(validSchemaBuilder.build());
    valueCache.forEach((k, v) -> {
      structValue.put(k, v);
    });
    return structValue;
  }

  public List<SinkRecord> add(SinkRecord record) throws SQLException, TableAlterOrCreateException {
    // If auto.schema=true, we will use the insert stmt schema, not the record.valueSchema().
    // OpenMLDB doesn't support pk mode, so we only handle value schema here. Leave key
    //  schema as it is.
    if (config.autoSchema && record.valueSchema() == null) {
      // no value schema, value is a map, must convert to Struct
      if (!(record.value() instanceof HashMap)) {
        log.warn("auto schema convertToStruct only support hashmap to struct");
      }
      Object structValue = convertToStruct(valueSchema, record.value());
      log.debug("auto schema convertToStruct {}", structValue);
      record = new SinkRecord(record.topic(), record.kafkaPartition(), record.keySchema(),
          record.key(), valueSchema, structValue, record.kafkaOffset(), record.timestamp(),
          record.timestampType(), record.headers());
    }

    recordValidator.validate(record);
    final List<SinkRecord> flushed = new ArrayList<>();

    boolean schemaChanged = false;
    if (!Objects.equals(keySchema, record.keySchema())) {
      keySchema = record.keySchema();
      schemaChanged = true;
    }
    if (isNull(record.valueSchema())) {
      // For deletes, value and optionally value schema come in as null.
      // We don't want to treat this as a schema change if key schemas is the same
      // otherwise we flush unnecessarily.
      if (config.deleteEnabled) {
        deletesInBatch = true;
      }
    } else if (Objects.equals(valueSchema, record.valueSchema())) {
      if (config.deleteEnabled && deletesInBatch) {
        // flush so an insert after a delete of same record isn't lost
        flushed.addAll(flush());
      }
    } else {
      // value schema is not null and has changed. This is a real schema change.
      valueSchema = record.valueSchema();
      schemaChanged = true;
    }
    if (schemaChanged || updateStatementBinder == null) {
      // Each batch needs to have the same schemas, so get the buffered records out
      flushed.addAll(flush());

      // re-initialize everything that depends on the record schema
      final SchemaPair schemaPair = new SchemaPair(record.keySchema(), record.valueSchema());
      fieldsMetadata = FieldsMetadata.extract(
          tableId.tableName(), config.pkMode, config.pkFields, config.fieldsWhitelist, schemaPair);
      dbStructure.createOrAmendIfNecessary(config, connection, tableId, fieldsMetadata);
      final String insertSql = getInsertSql();
      final String deleteSql = getDeleteSql();
      log.debug("{} sql: {} deleteSql: {} meta: {}", config.insertMode, insertSql, deleteSql,
          fieldsMetadata);
      close();
      updatePreparedStatement = dbDialect.createPreparedStatement(connection, insertSql);
      updateStatementBinder =
          dbDialect.statementBinder(updatePreparedStatement, config.pkMode, schemaPair,
              fieldsMetadata, dbStructure.tableDefinition(connection, tableId), config.insertMode);
      if (config.deleteEnabled && nonNull(deleteSql)) {
        deletePreparedStatement = dbDialect.createPreparedStatement(connection, deleteSql);
        deleteStatementBinder = dbDialect.statementBinder(deletePreparedStatement, config.pkMode,
            schemaPair, fieldsMetadata, dbStructure.tableDefinition(connection, tableId),
            config.insertMode);
      }
    }

    // set deletesInBatch if schema value is not null
    if (isNull(record.value()) && config.deleteEnabled) {
      deletesInBatch = true;
    }

    records.add(record);

    if (records.size() >= config.batchSize) {
      flushed.addAll(flush());
    }
    return flushed;
  }

  public List<SinkRecord> flush() throws SQLException {
    if (records.isEmpty()) {
      log.debug("Records is empty");
      return new ArrayList<>();
    }
    log.debug("Flushing {} buffered records", records.size());
    for (SinkRecord record : records) {
      if (isNull(record.value()) && nonNull(deleteStatementBinder)) {
        deleteStatementBinder.bindRecord(record);
      } else {
        updateStatementBinder.bindRecord(record);
      }
    }
    executeUpdates();
    executeDeletes();

    final List<SinkRecord> flushedRecords = records;
    records = new ArrayList<>();
    deletesInBatch = false;
    return flushedRecords;
  }

  private void executeUpdates() throws SQLException {
    int[] batchStatus = updatePreparedStatement.executeBatch();
    for (int updateCount : batchStatus) {
      if (updateCount == Statement.EXECUTE_FAILED) {
        throw new BatchUpdateException(
            "Execution failed for part of the batch update", batchStatus);
      }
    }
  }

  private void executeDeletes() throws SQLException {
    if (nonNull(deletePreparedStatement)) {
      int[] batchStatus = deletePreparedStatement.executeBatch();
      for (int updateCount : batchStatus) {
        if (updateCount == Statement.EXECUTE_FAILED) {
          throw new BatchUpdateException(
              "Execution failed for part of the batch delete", batchStatus);
        }
      }
    }
  }

  public void close() throws SQLException {
    log.debug(
        "Closing BufferedRecords with updatePreparedStatement: {} deletePreparedStatement: {}",
        updatePreparedStatement, deletePreparedStatement);
    if (nonNull(updatePreparedStatement)) {
      updatePreparedStatement.close();
      updatePreparedStatement = null;
    }
    if (nonNull(deletePreparedStatement)) {
      deletePreparedStatement.close();
      deletePreparedStatement = null;
    }
  }

  private String getInsertSql() throws SQLException {
    switch (config.insertMode) {
      case INSERT:
        return dbDialect.buildInsertStatement(tableId, asColumns(fieldsMetadata.keyFieldNames),
            asColumns(fieldsMetadata.nonKeyFieldNames),
            dbStructure.tableDefinition(connection, tableId));
      case UPSERT:
        if (fieldsMetadata.keyFieldNames.isEmpty()) {
          throw new ConnectException(String.format(
              "Write to table '%s' in UPSERT mode requires key field names to be known, check the"
                  + " primary key configuration",
              tableId));
        }
        try {
          return dbDialect.buildUpsertQueryStatement(tableId,
              asColumns(fieldsMetadata.keyFieldNames), asColumns(fieldsMetadata.nonKeyFieldNames),
              dbStructure.tableDefinition(connection, tableId));
        } catch (UnsupportedOperationException e) {
          throw new ConnectException(String.format(
              "Write to table '%s' in UPSERT mode is not supported with the %s dialect.", tableId,
              dbDialect.name()));
        }
      case UPDATE:
        return dbDialect.buildUpdateStatement(tableId, asColumns(fieldsMetadata.keyFieldNames),
            asColumns(fieldsMetadata.nonKeyFieldNames),
            dbStructure.tableDefinition(connection, tableId));
      default:
        throw new ConnectException("Invalid insert mode");
    }
  }

  private String getDeleteSql() {
    String sql = null;
    if (config.deleteEnabled) {
      switch (config.pkMode) {
        case RECORD_KEY:
          if (fieldsMetadata.keyFieldNames.isEmpty()) {
            throw new ConnectException("Require primary keys to support delete");
          }
          try {
            sql = dbDialect.buildDeleteStatement(tableId, asColumns(fieldsMetadata.keyFieldNames));
          } catch (UnsupportedOperationException e) {
            throw new ConnectException(
                String.format("Deletes to table '%s' are not supported with the %s dialect.",
                    tableId, dbDialect.name()));
          }
          break;

        default:
          throw new ConnectException("Deletes are only supported for pk.mode record_key");
      }
    }
    return sql;
  }

  private Collection<ColumnId> asColumns(Collection<String> names) {
    return names.stream().map(name -> new ColumnId(tableId, name)).collect(Collectors.toList());
  }
}
