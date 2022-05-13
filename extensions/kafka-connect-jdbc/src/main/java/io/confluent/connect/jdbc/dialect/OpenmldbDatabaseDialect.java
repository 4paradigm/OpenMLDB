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

package io.confluent.connect.jdbc.dialect;

import java.util.Collection;
import java.util.List;

import io.confluent.connect.jdbc.sink.metadata.SinkRecordField;
import io.confluent.connect.jdbc.util.ColumnId;
import io.confluent.connect.jdbc.util.ExpressionBuilder;
import io.confluent.connect.jdbc.util.IdentifierRules;
import io.confluent.connect.jdbc.util.TableId;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link DatabaseDialect} for OpenMLDB.
 */
public class OpenmldbDatabaseDialect extends GenericDatabaseDialect {
  private final Logger log = LoggerFactory.getLogger(OpenmldbDatabaseDialect.class);

  /**
   * The provider for {@link OpenmldbDatabaseDialect}.
   */
  public static class Provider extends DatabaseDialectProvider.SubprotocolBasedProvider {
    public Provider() {
      super(OpenmldbDatabaseDialect.class.getSimpleName(), "openmldb");
    }

    @Override
    public DatabaseDialect create(AbstractConfig config) {
      return new OpenmldbDatabaseDialect(config);
    }
  }

  /**
   * Create a new dialect instance with the given connector configuration.
   *
   * @param config the connector configuration; may not be null
   */
  public OpenmldbDatabaseDialect(AbstractConfig config) {
    super(config, new IdentifierRules(".", "`", "`"));
  }

  @Override
  protected String currentTimestampDatabaseQuery() {
    // TODO(hw): getting current timestamp is unsupported
    return null;
  }

  @Override
  protected String getSqlType(SinkRecordField field) {
    if (field.schemaName() != null) {
      switch (field.schemaName()) {
        case Date.LOGICAL_NAME:
        case Time.LOGICAL_NAME:
          return "DATE";
        case Timestamp.LOGICAL_NAME:
          return "TIMESTAMP";
        default:
          // pass through to primitive types
      }
    }
    switch (field.schemaType()) {
      // INT8 is unsupported
      case INT16:
        return "SMALLINT";
      case INT32:
        return "INT";
      case INT64:
        return "BIGINT";
      case FLOAT32:
        return "FLOAT";
      case FLOAT64:
        return "DOUBLE";
      case BOOLEAN:
        return "BOOL";
      case STRING:
        return "VARCHAR";
      default:
        return super.getSqlType(field);
    }
  }

  @Override
  public String buildCreateTableStatement(
          TableId table,
          Collection<SinkRecordField> fields
  ) {
    final List<String> pkFieldNames = extractPrimaryKeyFieldNames(fields);
    if (!pkFieldNames.isEmpty()) {
      throw new UnsupportedOperationException("pk is unsupported in openmldb");
    }
    return super.buildCreateTableStatement(table, fields);
  }

  @Override
  protected void writeColumnSpec(
          ExpressionBuilder builder,
          SinkRecordField f
  ) {
    builder.appendColumnName(f.name());
    builder.append(" ");
    String sqlType = getSqlType(f);
    builder.append(sqlType);
    if (f.defaultValue() != null) {
      builder.append(" DEFAULT ");
      formatColumnValue(
              builder,
              f.schemaName(),
              f.schemaParameters(),
              f.schemaType(),
              f.defaultValue()
      );
    } else if (!isColumnOptional(f)) {
      builder.append(" NOT NULL");
    }
  }

  @Override
  public String buildDropTableStatement(
          TableId table,
          DropOptions options
  ) {
    // no ifExists, no cascade
    ExpressionBuilder builder = expressionBuilder();

    builder.append("DROP TABLE ");
    builder.append(table);
    return builder.toString();
  }

  @Override
  public List<String> buildAlterTable(
          TableId table,
          Collection<SinkRecordField> fields
  ) {
    throw new UnsupportedOperationException("alter is unsupported");
  }

  @Override
  public String buildUpdateStatement(
          TableId table,
          Collection<ColumnId> keyColumns,
          Collection<ColumnId> nonKeyColumns
  ) {
    throw new UnsupportedOperationException("update is unsupported");
  }

  @Override
  public final String buildDeleteStatement(
          TableId table,
          Collection<ColumnId> keyColumns
  ) {
    throw new UnsupportedOperationException("delete is unsupported");
  }

  // type is useless, just a placeholder
  protected Integer getSqlTypeForSchema(Schema schema) {
    return 0;
  }
}

