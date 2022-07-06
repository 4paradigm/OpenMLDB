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

import io.confluent.connect.jdbc.dialect.DatabaseDialectProvider.SubprotocolBasedProvider;
import io.confluent.connect.jdbc.sink.metadata.SinkRecordField;
import io.confluent.connect.jdbc.source.ColumnMapping;
import io.confluent.connect.jdbc.util.ColumnDefinition;
import io.confluent.connect.jdbc.util.ColumnId;
import io.confluent.connect.jdbc.util.ExpressionBuilder;
import io.confluent.connect.jdbc.util.ExpressionBuilder.Transform;
import io.confluent.connect.jdbc.util.IdentifierRules;
import io.confluent.connect.jdbc.util.TableDefinition;
import io.confluent.connect.jdbc.util.TableId;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.DataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * A {@link DatabaseDialect} for PostgreSQL.
 */
public class PostgreSqlDatabaseDialect extends GenericDatabaseDialect {

  private static final Logger log = LoggerFactory.getLogger(PostgreSqlDatabaseDialect.class);

  // Visible for testing
  volatile int maxIdentifierLength = 0;

  /**
   * The provider for {@link PostgreSqlDatabaseDialect}.
   */
  public static class Provider extends SubprotocolBasedProvider {
    public Provider() {
      super(PostgreSqlDatabaseDialect.class.getSimpleName(), "postgresql");
    }

    @Override
    public DatabaseDialect create(AbstractConfig config) {
      return new PostgreSqlDatabaseDialect(config);
    }
  }

  static final String JSON_TYPE_NAME = "json";
  static final String JSONB_TYPE_NAME = "jsonb";
  static final String UUID_TYPE_NAME = "uuid";

  /**
   * Define the PG datatypes that require casting upon insert/update statements.
   */
  private static final Set<String> CAST_TYPES = Collections.unmodifiableSet(
      Utils.mkSet(
          JSON_TYPE_NAME,
          JSONB_TYPE_NAME,
          UUID_TYPE_NAME
      )
  );

  /**
   * Create a new dialect instance with the given connector configuration.
   *
   * @param config the connector configuration; may not be null
   */
  public PostgreSqlDatabaseDialect(AbstractConfig config) {
    super(config, new IdentifierRules(".", "\"", "\""));
  }

  @Override
  public Connection getConnection() throws SQLException {
    Connection result = super.getConnection();
    synchronized (this) {
      if (maxIdentifierLength <= 0) {
        maxIdentifierLength = computeMaxIdentifierLength(result);
      }
    }
    return result;
  }

  static int computeMaxIdentifierLength(Connection connection) {
    String warningMessage = "Unable to query database for maximum table name length; "
        + "the connector may fail to write to tables with long names";
    // https://stackoverflow.com/questions/27865770/how-long-can-postgresql-table-names-be/27865772#27865772
    String nameLengthQuery = "SELECT length(repeat('1234567890', 1000)::NAME);";
    
    int result;
    try (ResultSet rs = connection.createStatement().executeQuery(nameLengthQuery)) {
      if (rs.next()) {
        result = rs.getInt(1);
        if (result <= 0) {
          log.warn(
              "Cannot accommodate maximum table name length of {} as it is not positive; "
                  + "table name truncation will be disabled, "
                  + "and the connector may fail to write to tables with long names",
              result);
          result = Integer.MAX_VALUE;
        } else {
          log.info(
              "Maximum table name length for database is {} bytes",
              result
          );
        }
      } else {
        log.warn(warningMessage);
        result = Integer.MAX_VALUE;
      }
    } catch (SQLException e) {
      log.warn(warningMessage, e);
      result = Integer.MAX_VALUE;
    }
    return result;
  }

  @Override
  public TableId parseTableIdentifier(String fqn) {
    TableId result = super.parseTableIdentifier(fqn);
    if (maxIdentifierLength > 0 && result.tableName().length() > maxIdentifierLength) {
      String newTableName = result.tableName().substring(0, maxIdentifierLength);
      log.debug(
          "Truncating table name from {} to {} in order to respect maximum name length",
          result.tableName(),
          newTableName
      );
      result = new TableId(
          result.catalogName(),
          result.schemaName(),
          newTableName
      );
    }
    return result;
  }

  /**
   * Perform any operations on a {@link PreparedStatement} before it is used. This is called from
   * the {@link #createPreparedStatement(Connection, String)} method after the statement is
   * created but before it is returned/used.
   *
   * <p>This method sets the {@link PreparedStatement#setFetchDirection(int) fetch direction}
   * to {@link ResultSet#FETCH_FORWARD forward} as an optimization for the driver to allow it to
   * scroll more efficiently through the result set and prevent out of memory errors.
   *
   * @param stmt the prepared statement; never null
   * @throws SQLException the error that might result from initialization
   */
  @Override
  protected void initializePreparedStatement(PreparedStatement stmt) throws SQLException {
    super.initializePreparedStatement(stmt);

    log.trace("Initializing PreparedStatement fetch direction to FETCH_FORWARD for '{}'", stmt);
    stmt.setFetchDirection(ResultSet.FETCH_FORWARD);
  }


  @Override
  public String addFieldToSchema(
      ColumnDefinition columnDefn,
      SchemaBuilder builder
  ) {
    // Add the PostgreSQL-specific types first
    final String fieldName = fieldNameFor(columnDefn);
    switch (columnDefn.type()) {
      case Types.BIT: {
        // PostgreSQL allows variable length bit strings, but when length is 1 then the driver
        // returns a 't' or 'f' string value to represent the boolean value, so we need to handle
        // this as well as lengths larger than 8.
        boolean optional = columnDefn.isOptional();
        int numBits = columnDefn.precision();
        Schema schema;
        if (numBits <= 1) {
          schema = optional ? Schema.OPTIONAL_BOOLEAN_SCHEMA : Schema.BOOLEAN_SCHEMA;
        } else if (numBits <= 8) {
          // For consistency with what the connector did before ...
          schema = optional ? Schema.OPTIONAL_INT8_SCHEMA : Schema.INT8_SCHEMA;
        } else {
          schema = optional ? Schema.OPTIONAL_BYTES_SCHEMA : Schema.BYTES_SCHEMA;
        }
        builder.field(fieldName, schema);
        return fieldName;
      }
      case Types.OTHER: {
        // Some of these types will have fixed size, but we drop this from the schema conversion
        // since only fixed byte arrays can have a fixed size
        if (isJsonType(columnDefn)) {
          builder.field(
              fieldName,
              columnDefn.isOptional() ? Schema.OPTIONAL_STRING_SCHEMA : Schema.STRING_SCHEMA
          );
          return fieldName;
        }

        if (UUID.class.getName().equals(columnDefn.classNameForType())) {
          builder.field(
                  fieldName,
                  columnDefn.isOptional()
                          ?
                          Schema.OPTIONAL_STRING_SCHEMA :
                          Schema.STRING_SCHEMA
          );
          return fieldName;
        }

        break;
      }
      default:
        break;
    }

    // Delegate for the remaining logic
    return super.addFieldToSchema(columnDefn, builder);
  }

  @Override
  protected ColumnConverter columnConverterFor(
      ColumnMapping mapping,
      ColumnDefinition defn,
      int col,
      boolean isJdbc4
  ) {
    // First handle any PostgreSQL-specific types
    ColumnDefinition columnDefn = mapping.columnDefn();
    switch (columnDefn.type()) {
      case Types.BIT: {
        // PostgreSQL allows variable length bit strings, but when length is 1 then the driver
        // returns a 't' or 'f' string value to represent the boolean value, so we need to handle
        // this as well as lengths larger than 8.
        final int numBits = columnDefn.precision();
        if (numBits <= 1) {
          return rs -> rs.getBoolean(col);
        } else if (numBits <= 8) {
          // Do this for consistency with earlier versions of the connector
          return rs -> rs.getByte(col);
        }
        return rs -> rs.getBytes(col);
      }
      case Types.OTHER: {
        if (isJsonType(columnDefn)) {
          return rs -> rs.getString(col);
        }

        if (UUID.class.getName().equals(columnDefn.classNameForType())) {
          return rs -> rs.getString(col);
        }
        break;
      }
      default:
        break;
    }

    // Delegate for the remaining logic
    return super.columnConverterFor(mapping, defn, col, isJdbc4);
  }

  protected boolean isJsonType(ColumnDefinition columnDefn) {
    String typeName = columnDefn.typeName();
    return JSON_TYPE_NAME.equalsIgnoreCase(typeName) || JSONB_TYPE_NAME.equalsIgnoreCase(typeName);
  }

  @Override
  protected String getSqlType(SinkRecordField field) {
    if (field.schemaName() != null) {
      switch (field.schemaName()) {
        case Decimal.LOGICAL_NAME:
          return "DECIMAL";
        case Date.LOGICAL_NAME:
          return "DATE";
        case Time.LOGICAL_NAME:
          return "TIME";
        case Timestamp.LOGICAL_NAME:
          return "TIMESTAMP";
        default:
          // fall through to normal types
      }
    }
    switch (field.schemaType()) {
      case INT8:
      case INT16:
        return "SMALLINT";
      case INT32:
        return "INT";
      case INT64:
        return "BIGINT";
      case FLOAT32:
        return "REAL";
      case FLOAT64:
        return "DOUBLE PRECISION";
      case BOOLEAN:
        return "BOOLEAN";
      case STRING:
        return "TEXT";
      case BYTES:
        return "BYTEA";
      case ARRAY:
        SinkRecordField childField = new SinkRecordField(
              field.schema().valueSchema(),
              field.name(),
              field.isPrimaryKey()
            );
        return getSqlType(childField) + "[]";
      default:
        return super.getSqlType(field);
    }
  }

  @Override
  public String buildInsertStatement(
      TableId table,
      Collection<ColumnId> keyColumns,
      Collection<ColumnId> nonKeyColumns,
      TableDefinition definition
  ) {
    ExpressionBuilder builder = expressionBuilder();
    builder.append("INSERT INTO ");
    builder.append(table);
    builder.append(" (");
    builder.appendList()
           .delimitedBy(",")
           .transformedBy(ExpressionBuilder.columnNames())
           .of(keyColumns, nonKeyColumns);
    builder.append(") VALUES (");
    builder.appendList()
           .delimitedBy(",")
           .transformedBy(this.columnValueVariables(definition))
           .of(keyColumns, nonKeyColumns);
    builder.append(")");
    return builder.toString();
  }

  @Override
  public String buildUpdateStatement(
      TableId table,
      Collection<ColumnId> keyColumns,
      Collection<ColumnId> nonKeyColumns,
      TableDefinition definition
  ) {
    ExpressionBuilder builder = expressionBuilder();
    builder.append("UPDATE ");
    builder.append(table);
    builder.append(" SET ");
    builder.appendList()
           .delimitedBy(", ")
           .transformedBy(this.columnNamesWithValueVariables(definition))
           .of(nonKeyColumns);
    if (!keyColumns.isEmpty()) {
      builder.append(" WHERE ");
      builder.appendList()
             .delimitedBy(" AND ")
             .transformedBy(ExpressionBuilder.columnNamesWith(" = ?"))
             .of(keyColumns);
    }
    return builder.toString();
  }

  @Override
  public String buildUpsertQueryStatement(
      TableId table,
      Collection<ColumnId> keyColumns,
      Collection<ColumnId> nonKeyColumns,
      TableDefinition definition
  ) {
    final Transform<ColumnId> transform = (builder, col) -> {
      builder.appendColumnName(col.name())
             .append("=EXCLUDED.")
             .appendColumnName(col.name());
    };

    ExpressionBuilder builder = expressionBuilder();
    builder.append("INSERT INTO ");
    builder.append(table);
    builder.append(" (");
    builder.appendList()
           .delimitedBy(",")
           .transformedBy(ExpressionBuilder.columnNames())
           .of(keyColumns, nonKeyColumns);
    builder.append(") VALUES (");
    builder.appendList()
           .delimitedBy(",")
           .transformedBy(this.columnValueVariables(definition))
           .of(keyColumns, nonKeyColumns);
    builder.append(") ON CONFLICT (");
    builder.appendList()
           .delimitedBy(",")
           .transformedBy(ExpressionBuilder.columnNames())
           .of(keyColumns);
    if (nonKeyColumns.isEmpty()) {
      builder.append(") DO NOTHING");
    } else {
      builder.append(") DO UPDATE SET ");
      builder.appendList()
              .delimitedBy(",")
              .transformedBy(transform)
              .of(nonKeyColumns);
    }
    return builder.toString();
  }

  @Override
  protected void formatColumnValue(
      ExpressionBuilder builder,
      String schemaName,
      Map<String, String> schemaParameters,
      Schema.Type type,
      Object value
  ) {
    if (schemaName == null && Type.BOOLEAN.equals(type)) {
      builder.append((Boolean) value ? "TRUE" : "FALSE");
    } else {
      super.formatColumnValue(builder, schemaName, schemaParameters, type, value);
    }
  }

  @Override
  protected boolean maybeBindPrimitive(
      PreparedStatement statement,
      int index,
      Schema schema,
      Object value
  ) throws SQLException {

    switch (schema.type()) {
      case ARRAY: {
        Class<?> valueClass = value.getClass();
        Object newValue = null;
        Collection<?> valueCollection;
        if (Collection.class.isAssignableFrom(valueClass)) {
          valueCollection = (Collection<?>) value;
        } else if (valueClass.isArray()) {
          valueCollection = Arrays.asList((Object[]) value);
        } else {
          throw new DataException(
              String.format("Type '%s' is not supported for Array.", valueClass.getName())
          );
        }

        // All typecasts below are based on pgjdbc's documentation on how to use primitive arrays
        // - https://jdbc.postgresql.org/documentation/head/arrays.html
        switch (schema.valueSchema().type()) {
          case INT8: {
            // Gotta do this the long way, as Postgres has no single-byte integer,
            // so we want to cast to short as the next best thing, and we can't do that with
            // toArray.

            newValue = valueCollection.stream()
                .map(o -> ((Byte) o).shortValue())
                .toArray(Short[]::new);
            break;
          }
          case INT32:
            newValue = valueCollection.toArray(new Integer[0]);
            break;
          case INT16:
            newValue = valueCollection.toArray(new Short[0]);
            break;
          case BOOLEAN:
            newValue = valueCollection.toArray(new Boolean[0]);
            break;
          case STRING:
            newValue = valueCollection.toArray(new String[0]);
            break;
          case FLOAT64:
            newValue = valueCollection.toArray(new Double[0]);
            break;
          case FLOAT32:
            newValue = valueCollection.toArray(new Float[0]);
            break;
          case INT64:
            newValue = valueCollection.toArray(new Long[0]);
            break;
          default:
            break;
        }

        if (newValue != null) {
          statement.setObject(index, newValue, Types.ARRAY);
          return true;
        }
        break;
      }
      default:
        break;
    }
    return super.maybeBindPrimitive(statement, index, schema, value);
  }

  /**
   * Return the transform that produces an assignment expression each with the name of one of the
   * columns and the prepared statement variable. PostgreSQL may require the variable to have a
   * type suffix, such as {@code ?::uuid}.
   *
   * @param defn the table definition; may be null if unknown
   * @return the transform that produces the assignment expression for use within a prepared
   *         statement; never null
   */
  protected Transform<ColumnId> columnNamesWithValueVariables(TableDefinition defn) {
    return (builder, columnId) -> {
      builder.appendColumnName(columnId.name());
      builder.append(" = ?");
      builder.append(valueTypeCast(defn, columnId));
    };
  }

  /**
   * Return the transform that produces a prepared statement variable for each of the columns.
   * PostgreSQL may require the variable to have a type suffix, such as {@code ?::uuid}.
   *
   * @param defn the table definition; may be null if unknown
   * @return the transform that produces the variable expression for each column; never null
   */
  protected Transform<ColumnId> columnValueVariables(TableDefinition defn) {
    return (builder, columnId) -> {
      builder.append("?");
      builder.append(valueTypeCast(defn, columnId));
    };
  }

  /**
   * Return the typecast expression that can be used as a suffix for a value variable of the
   * given column in the defined table.
   *
   * <p>This method returns a blank string except for those column types that require casting
   * when set with literal values. For example, a column of type {@code uuid} must be cast when
   * being bound with with a {@code varchar} literal, since a UUID value cannot be bound directly.
   *
   * @param tableDefn the table definition; may be null if unknown
   * @param columnId  the column within the table; may not be null
   * @return the cast expression, or an empty string; never null
   */
  protected String valueTypeCast(TableDefinition tableDefn, ColumnId columnId) {
    if (tableDefn != null) {
      ColumnDefinition defn = tableDefn.definitionForColumn(columnId.name());
      if (defn != null) {
        String typeName = defn.typeName(); // database-specific
        if (typeName != null) {
          typeName = typeName.toLowerCase();
          if (CAST_TYPES.contains(typeName)) {
            return "::" + typeName;
          }
        }
      }
    }
    return "";
  }

  @Override
  protected int decimalScale(ColumnDefinition defn) {
    if (defn.scale() == NUMERIC_TYPE_SCALE_UNSET) {
      return NUMERIC_TYPE_SCALE_HIGH;
    }

    // Postgres requires DECIMAL/NUMERIC columns to have a precision greater than zero
    // If the precision appears to be zero, it's because the user didn't define a fixed precision
    // for the column.
    if (defn.precision() == 0) {
      // In that case, a scale of zero indicates that there also isn't a fixed scale defined for
      // the column. Instead of treating that column as if its scale is actually zero (which can
      // cause issues since it may contain values that aren't possible with a scale of zero, like
      // 12.12), we fall back on NUMERIC_TYPE_SCALE_HIGH to try to avoid loss of precision
      if (defn.scale() == 0) {
        log.debug(
            "Column {} does not appear to have a fixed scale defined; defaulting to {}",
            defn.id(),
            NUMERIC_TYPE_SCALE_HIGH
        );
        return NUMERIC_TYPE_SCALE_HIGH;
      } else {
        // Should never happen, but if it does may signal an edge case
        // that we need to add new logic for
        log.warn(
            "Column {} has a precision of zero, but a non-zero scale of {}",
            defn.id(),
            defn.scale()
        );
      }
    }

    return defn.scale();
  }

}
