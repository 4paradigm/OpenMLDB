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

import io.confluent.connect.jdbc.sink.metadata.SinkRecordField;
import io.confluent.connect.jdbc.util.ColumnDefinition;
import io.confluent.connect.jdbc.util.ColumnId;
import io.confluent.connect.jdbc.util.ExpressionBuilder;
import io.confluent.connect.jdbc.util.IdentifierRules;
import io.confluent.connect.jdbc.util.TableId;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Types;
import java.util.Collection;
import java.util.List;

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
  public String buildCreateTableStatement(TableId table, Collection<SinkRecordField> fields) {
    final List<String> pkFieldNames = extractPrimaryKeyFieldNames(fields);
    if (!pkFieldNames.isEmpty()) {
      throw new UnsupportedOperationException("pk is unsupported in openmldb");
    }
    return super.buildCreateTableStatement(table, fields);
  }

  @Override
  protected void writeColumnSpec(ExpressionBuilder builder, SinkRecordField f) {
    builder.appendColumnName(f.name());
    builder.append(" ");
    String sqlType = getSqlType(f);
    builder.append(sqlType);
    if (f.defaultValue() != null) {
      builder.append(" DEFAULT ");
      formatColumnValue(
          builder, f.schemaName(), f.schemaParameters(), f.schemaType(), f.defaultValue());
    } else if (!isColumnOptional(f)) {
      builder.append(" NOT NULL");
    }
  }

  @Override
  public String buildDropTableStatement(TableId table, DropOptions options) {
    // no ifExists, no cascade
    ExpressionBuilder builder = expressionBuilder();

    builder.append("DROP TABLE ");
    builder.append(table);
    return builder.toString();
  }

  @Override
  public List<String> buildAlterTable(TableId table, Collection<SinkRecordField> fields) {
    throw new UnsupportedOperationException("alter is unsupported");
  }

  @Override
  public String buildUpdateStatement(
      TableId table, Collection<ColumnId> keyColumns, Collection<ColumnId> nonKeyColumns) {
    throw new UnsupportedOperationException("update is unsupported");
  }

  @Override
  public final String buildDeleteStatement(TableId table, Collection<ColumnId> keyColumns) {
    throw new UnsupportedOperationException("delete is unsupported");
  }

  // type is useless, just a placeholder
  protected Integer getSqlTypeForSchema(Schema schema) {
    return 0;
  }

  // set name in schema
  @Override
  protected String addFieldToSchema(final ColumnDefinition columnDefn, final SchemaBuilder builder,
      final String fieldName, final int sqlType, final boolean optional) {
    SchemaBuilder schemaBuilder = null;
    switch (sqlType) {
      // 16 bit ints
      case Types.SMALLINT: {
        // TODO(hw): openmldb doesn't support unsigned, but jdbc metadata returns false,
        //   fix it later. columnDefn.isSignedNumber()
        schemaBuilder = SchemaBuilder.int16();
        break;
      }

      // 32 bit int
      case Types.INTEGER: {
        schemaBuilder = SchemaBuilder.int32();
        break;
      }

      // 64 bit int
      case Types.BIGINT: {
        schemaBuilder = SchemaBuilder.int64();
        break;
      }
      // openmldb jdbc use java float, not double
      case Types.FLOAT: {
        schemaBuilder = SchemaBuilder.float32();
        break;
      }

      // Double is just double
      // Date is day + moth + year
      // Time is a time of day -- hour, minute, seconds, nanoseconds
      // Timestamp is a date + time, openmldb jdbc setTimestamp is compatible
      default: { }
    }
    if (schemaBuilder == null) {
      log.warn("openmldb schema builder for sqlType {} is null, "
              + "use GenericDatabaseDialect method",
          sqlType);
      return super.addFieldToSchema(columnDefn, builder, fieldName, sqlType, optional);
    }
    builder.field(fieldName, optional ? schemaBuilder.optional().build() : schemaBuilder.build());
    return fieldName;
  }
}
