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

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import io.confluent.connect.jdbc.sink.JdbcSinkConfig;
import io.confluent.connect.jdbc.sink.metadata.FieldsMetadata;
import io.confluent.connect.jdbc.sink.metadata.SchemaPair;
import io.confluent.connect.jdbc.sink.metadata.SinkRecordField;
import io.confluent.connect.jdbc.source.ColumnMapping;
import io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig.TransactionIsolationMode;
import io.confluent.connect.jdbc.source.TimestampIncrementingCriteria;
import io.confluent.connect.jdbc.util.ColumnDefinition;
import io.confluent.connect.jdbc.util.ColumnId;
import io.confluent.connect.jdbc.util.ConnectionProvider;
import io.confluent.connect.jdbc.util.ExpressionBuilder;
import io.confluent.connect.jdbc.util.IdentifierRules;
import io.confluent.connect.jdbc.util.TableDefinition;
import io.confluent.connect.jdbc.util.TableId;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;

/**
 * A component that is used to map datatypes and queries for JDBC sources. Every dialect should be
 * thread-safe and all methods should be free of side effects.
 *
 * <h2>Customizing DBMS-specific Behavior</h2> The {@link GenericDatabaseDialect} implementation is
 * intended to be general enough to work with most DBMS sources with common JDBC and SQL types.
 * However, it will likely not support some DBMS-specific features and types.
 *
 * <p>Supporting a particular DBMS more completely may require custom data type mappings and
 * functionality, and <em>dialects</em> are intended to make this customization possible. To provide
 * a custom dialect, implement the {@link DatabaseDialect} interface or extend an existing {@link
 * DatabaseDialect} implementation. A good approach is to subclass {@link GenericDatabaseDialect}
 * and override only the methods necessary to implement the custom behavior and to also delegate to
 * the parent class' method to handle the generic cases. The {@link PostgreSqlDatabaseDialect} is a
 * good example of how to do this.
 *
 * <h3>Mapping Types</h3> This JDBC Source connector creates a {@link Struct} object for each row
 * found in a result set. It uses this Dialect interface to define how database types are mapped to
 * the Connect {@link Schema} types and how database values are converted into the {@link Field}
 * values added to those {@link Struct} objects.
 *
 * <p>When the JDBC Source connector gets a new {@link ResultSet}, it creates a new {@link Schema}
 * object and uses the Dialect's {@link #addFieldToSchema(ColumnDefinition, SchemaBuilder)} method
 * to add to that schema a {@link Field} for each result set column. The connector will then use
 * that same {@link Schema} instance for <i>every</i> row in that result set.
 *
 * <p>At the same time it builds the {@link Schema} for a new result set, the connector will also
 * build for each result set column a {@link ColumnConverter} function that the connector will use
 * to convert the corresponding column value in every row of the result set. These converters are
 * created <i>once</i> and used for all rows in the result set. This approach has several
 * advantages:
 *
 * <ol>
 *
 * <li>The Dialect must use the column metadata to determine <i>how</i> to convert the values to the
 * correct type so that the converted values can be added to the {@link Struct}. The Dialect can do
 * this once for each result set, rather than having to do the logic for every row.</li>
 *
 * <li>Although most dialects will create converter functions that merely use one of the ResultSet
 * methods to get the correct value, implementations can easily create converter functions that have
 * more complex logic.</li>
 *
 * <li>If the Dialect can return a null converter when a field cannot be converted and should be
 * skipped entirely for all rows. This more efficient and explicit than a more conventional approach
 * that calls the dialect to convert each value of the result set.</li>
 *
 * <li>This functional approach makes it very easy for the connector's task to process each row of
 * the result set by calling each of the {@link ColumnConverter} functions that were created
 * once.</li>
 *
 * </ol>
 *
 * <p>This functional approach is less common with Java 7 because it requires create lots of
 * anonymous implementations and is therefore more verbose. However, the same approach is far more
 * common in Java 8 and using lambdas requires very little code. This will make it easy to convert
 * to Java 8 in the near future.
 *
 * <h2>Packaging and Deploying</h2> The dialect framework uses Java's {@link
 * java.util.ServiceLoader} mechanism to find and automatically register all {@link
 * DatabaseDialectProvider} implementations on the classpath. Don't forget to include in your JAR
 * file a {@code META-INF/services/io.confluent.connect.jdbc.dialect.DatabaseDialectProvider} file
 * that contains the fully-qualified name of your implementation class (or one class per line if
 * providing multiple implementations).
 *
 * <p>If the Dialect implementation is included in this project, the class and the service provider
 * file will automatically be included in the JDBC Source connector's JAR file. However, it is also
 * possible to create an implementation outside of this project and to package the dialect(s) and
 * service provider file in a JAR file, and to then include that JAR file inside the JDBC
 * Connector's plugin directory within a Connect installation.
 *
 * <p>This discovery and registration process uses DEBUG messages to report the {@link
 * DatabaseDialectProvider} classes that are found and registered. If you have difficulties getting
 * the connector to find and register your dialect implementation classes, check that your JARs have
 * the service provider file and your JAR is included in the JDBC connector's plugin directory.
 */
public interface DatabaseDialect extends ConnectionProvider {

  /**
   * Return the name of the dialect.
   *
   * @return the dialect's name; never null
   */
  String name();

  /**
   * Create a new prepared statement using the specified database connection.
   *
   * @param connection the database connection; may not be null
   * @param query      the query expression for the prepared statement; may not be null
   * @return a new prepared statement; never null
   * @throws SQLException if there is an error with the database connection
   */
  PreparedStatement createPreparedStatement(
      Connection connection,
      String query
  ) throws SQLException;

  /**
   * Parse the supplied simple name or fully qualified name for a table into a {@link TableId}.
   *
   * @param fqn the fully qualified string representation; may not be null
   * @return the table identifier; never null
   */
  TableId parseTableIdentifier(String fqn);

  /**
   * Get the identifier rules for this database.
   *
   * @return the identifier rules
   */
  IdentifierRules identifierRules();

  /**
   * Get a new {@link ExpressionBuilder} that can be used to build expressions with quoted
   * identifiers.
   *
   * @return the builder; never null
   * @see #identifierRules()
   * @see IdentifierRules#expressionBuilder()
   */
  ExpressionBuilder expressionBuilder();

  /**
   * Return current time at the database
   *
   * @param connection database connection
   * @param cal        calendar
   * @return the current time at the database
   * @throws SQLException if there is an error with the database connection
   */
  Timestamp currentTimeOnDB(
      Connection connection,
      Calendar cal
  ) throws SQLException, ConnectException;

  /**
   * Get a list of identifiers of the non-system tables in the database.
   *
   * @param connection database connection
   * @return a list of tables; never null
   * @throws SQLException if there is an error with the database connection
   */
  List<TableId> tableIds(Connection connection) throws SQLException;

  /**
   * Determine if the specified table exists in the database.
   *
   * @param connection the database connection; may not be null
   * @param tableId    the identifier of the table; may not be null
   * @return true if the table exists, or false otherwise
   * @throws SQLException if there is an error accessing the metadata
   */
  boolean tableExists(Connection connection, TableId tableId) throws SQLException;


  /**
   * Set the isolation mode for the connection.
   * Isolation modes can differ by database so this provides an interface for
   * the mode to be overridden.
   *
   * @param connection the database connection; may not be null
   * @param transactionIsolationMode the transaction isolation config
   */
  void setConnectionIsolationMode(
          Connection connection,
          TransactionIsolationMode transactionIsolationMode
  );

  /**
   * Create the definition for the columns described by the database metadata using the current
   * schema and catalog patterns defined in the configuration.
   *
   * @param connection    the database connection; may not be null
   * @param tablePattern  the pattern for matching the tables; may be null
   * @param columnPattern the pattern for matching the columns; may be null
   * @return the column definitions keyed by their {@link ColumnId}; never null
   * @throws SQLException if there is an error accessing the metadata
   */
  Map<ColumnId, ColumnDefinition> describeColumns(
      Connection connection,
      String tablePattern,
      String columnPattern
  ) throws SQLException;

  /**
   * Create the definition for the columns described by the database metadata.
   *
   * @param connection     the database connection; may not be null
   * @param catalogPattern the pattern for matching the catalog; may be null
   * @param schemaPattern  the pattern for matching the schemas; may be null
   * @param tablePattern   the pattern for matching the tables; may be null
   * @param columnPattern  the pattern for matching the columns; may be null
   * @return the column definitions keyed by their {@link ColumnId}; never null
   * @throws SQLException if there is an error accessing the metadata
   */
  Map<ColumnId, ColumnDefinition> describeColumns(
      Connection connection,
      String catalogPattern,
      String schemaPattern,
      String tablePattern,
      String columnPattern
  ) throws SQLException;

  /**
   * Create the definition for the columns in the result set.
   *
   * @param rsMetadata the result set metadata; may not be null
   * @return the column definitions keyed by their {@link ColumnId} and in the same order as the
   *     result set; never null
   * @throws SQLException if there is an error accessing the result set metadata
   */
  Map<ColumnId, ColumnDefinition> describeColumns(
      ResultSetMetaData rsMetadata
  ) throws SQLException;

  /**
   * Get the definition of the specified table.
   *
   * @param connection the database connection; may not be null
   * @param tableId    the identifier of the table; may not be null
   * @return the table definition; null if the table does not exist
   * @throws SQLException if there is an error accessing the metadata
   */
  TableDefinition describeTable(Connection connection, TableId tableId) throws SQLException;

  /**
   * Create the definition for the columns in the result set returned when querying the table. This
   * may not work if the table is empty.
   *
   * @param connection the database connection; may not be null
   * @param tableId    the name of the table; may be null
   * @return the column definitions keyed by their {@link ColumnId}; never null
   * @throws SQLException if there is an error accessing the result set metadata
   */
  Map<ColumnId, ColumnDefinition> describeColumnsByQuerying(
      Connection connection,
      TableId tableId
  ) throws SQLException;

  /**
   * Create a criteria generator for queries that look for changed data using timestamp and
   * incremented columns.
   *
   * @param incrementingColumn the identifier of the incremented column; may be null if there is
   *                           none
   * @param timestampColumns   the identifiers of the timestamp column; may be null if there is
   *                           none
   * @return the {@link TimestampIncrementingCriteria} implementation; never null
   */
  TimestampIncrementingCriteria criteriaFor(
      ColumnId incrementingColumn,
      List<ColumnId> timestampColumns
  );

  /**
   * Use the supplied {@link SchemaBuilder} to add a field that corresponds to the column with the
   * specified definition.
   *
   * @param column  the definition of the column; may not be null
   * @param builder the schema builder; may not be null
   * @return the name of the field, or null if no field was added
   */
  String addFieldToSchema(ColumnDefinition column, SchemaBuilder builder);

  /**
   * Apply the supplied DDL statements using the given connection. This gives the dialect the
   * opportunity to execute the statements with a different autocommit setting.
   *
   * @param connection the connection to use
   * @param statements the list of DDL statements to execute
   * @throws SQLException if there is an error executing the statements
   */
  void applyDdlStatements(Connection connection, List<String> statements) throws SQLException;

  /**
   * Build the INSERT prepared statement expression for the given table and its columns.
   *
   * <p>This method is only called by the default implementation of
   * {@link #buildInsertStatement(TableId, Collection, Collection, TableDefinition)}, since
   * many dialects implement this variant of the method. However, overriding
   * {@link #buildInsertStatement(TableId, Collection, Collection, TableDefinition)} is suggested.
   *
   * @param table         the identifier of the table; may not be null
   * @param keyColumns    the identifiers of the columns in the primary/unique key; may not be null
   *                      but may be empty
   * @param nonKeyColumns the identifiers of the other columns in the table; may not be null but may
   *                      be empty
   * @return the INSERT statement; may not be null
   * @deprecated use {@link #buildInsertStatement(TableId, Collection, Collection, TableDefinition)}
   */
  @Deprecated
  String buildInsertStatement(
      TableId table,
      Collection<ColumnId> keyColumns,
      Collection<ColumnId> nonKeyColumns
  );

  /**
   * Build the INSERT prepared statement expression for the given table and its columns.
   *
   * <p>By default this method calls
   * {@link #buildInsertStatement(TableId, Collection, Collection)} to maintain backward
   * compatibility with older versions. Subclasses that override this method do not need to
   * override {@link #buildInsertStatement(TableId, Collection, Collection)}.
   *
   * @param table         the identifier of the table; may not be null
   * @param keyColumns    the identifiers of the columns in the primary/unique key; may not be null
   *                      but may be empty
   * @param nonKeyColumns the identifiers of the other columns in the table; may not be null but may
   *                      be empty
   * @param definition    the table definition; may be null if unknown
   * @return the INSERT statement; may not be null
   */
  default String buildInsertStatement(
      TableId table,
      Collection<ColumnId> keyColumns,
      Collection<ColumnId> nonKeyColumns,
      TableDefinition definition
  ) {
    return buildInsertStatement(table, keyColumns, nonKeyColumns);
  }

  /**
   * Build the UPDATE prepared statement expression for the given table and its columns. Variables
   * for each key column should also appear in the WHERE clause of the statement.
   *
   * <p>This method is only called by the default implementation of
   * {@link #buildUpdateStatement(TableId, Collection, Collection, TableDefinition)}, since
   * many dialects implement this variant of the method. However, overriding
   * {@link #buildUpdateStatement(TableId, Collection, Collection, TableDefinition)} is suggested.
   *
   * @param table         the identifier of the table; may not be null
   * @param keyColumns    the identifiers of the columns in the primary/unique key; may not be null
   *                      but may be empty
   * @param nonKeyColumns the identifiers of the other columns in the table; may not be null but may
   *                      be empty
   * @return the UPDATE statement; may not be null
   * @deprecated use {@link #buildUpdateStatement(TableId, Collection, Collection, TableDefinition)}
   */
  @Deprecated
  String buildUpdateStatement(
      TableId table,
      Collection<ColumnId> keyColumns,
      Collection<ColumnId> nonKeyColumns
  );

  /**
   * Build the UPDATE prepared statement expression for the given table and its columns. Variables
   * for each key column should also appear in the WHERE clause of the statement.
   *
   * <p>By default this method calls
   * {@link #buildUpdateStatement(TableId, Collection, Collection)} to maintain backward
   * compatibility with older versions. Subclasses that override this method do not need to
   * override {@link #buildUpdateStatement(TableId, Collection, Collection)}.
   *
   * @param table         the identifier of the table; may not be null
   * @param keyColumns    the identifiers of the columns in the primary/unique key; may not be null
   *                      but may be empty
   * @param nonKeyColumns the identifiers of the other columns in the table; may not be null but may
   *                      be empty
   * @param definition    the table definition; may be null if unknown
   * @return the UPDATE statement; may not be null
   */
  default String buildUpdateStatement(
      TableId table,
      Collection<ColumnId> keyColumns,
      Collection<ColumnId> nonKeyColumns,
      TableDefinition definition
  ) {
    return buildUpdateStatement(table, keyColumns, nonKeyColumns);
  }

  /**
   * Build the UPSERT or MERGE prepared statement expression to either insert a new record into the
   * given table or update an existing record in that table Variables for each key column should
   * also appear in the WHERE clause of the statement.
   *
   * <p>This method is only called by the default implementation of
   * {@link #buildUpsertQueryStatement(TableId, Collection, Collection, TableDefinition)}, since
   * many dialects implement this variant of the method. However, overriding
   * {@link #buildUpsertQueryStatement(TableId, Collection, Collection, TableDefinition)}
   * is suggested.
   *
   * @param table         the identifier of the table; may not be null
   * @param keyColumns    the identifiers of the columns in the primary/unique key; may not be null
   *                      but may be empty
   * @param nonKeyColumns the identifiers of the other columns in the table; may not be null but may
   *                      be empty
   * @return the upsert/merge statement; may not be null
   * @throws UnsupportedOperationException if the dialect does not support upserts
   * @deprecated use {@link #buildUpsertQueryStatement(TableId, Collection, Collection)}
   */
  @Deprecated
  String buildUpsertQueryStatement(
      TableId table,
      Collection<ColumnId> keyColumns,
      Collection<ColumnId> nonKeyColumns
  );

  /**
   * Build the UPSERT or MERGE prepared statement expression to either insert a new record into the
   * given table or update an existing record in that table Variables for each key column should
   * also appear in the WHERE clause of the statement.
   *
   * <p>By default this method calls
   * {@link #buildUpsertQueryStatement(TableId, Collection, Collection)} to maintain backward
   * compatibility with older versions. Subclasses that override this method do not need to
   * override {@link #buildUpsertQueryStatement(TableId, Collection, Collection)}.
   *
   * @param table         the identifier of the table; may not be null
   * @param keyColumns    the identifiers of the columns in the primary/unique key; may not be null
   *                      but may be empty
   * @param nonKeyColumns the identifiers of the other columns in the table; may not be null but may
   *                      be empty
   * @param definition    the table definition; may be null if unknown
   * @return the upsert/merge statement; may not be null
   * @throws UnsupportedOperationException if the dialect does not support upserts
   */
  default String buildUpsertQueryStatement(
      TableId table,
      Collection<ColumnId> keyColumns,
      Collection<ColumnId> nonKeyColumns,
      TableDefinition definition
  ) {
    return buildUpsertQueryStatement(table, keyColumns, nonKeyColumns);
  }

  /**
   * Build the DELETE prepared statement expression for the given table and its columns. Variables
   * for each key column should also appear in the WHERE clause of the statement.
   *
   * @param table         the identifier of the table; may not be null
   * @param keyColumns    the identifiers of the columns in the primary/unique key; may not be null
   *                      but may be empty
   * @return the delete statement; may not be null
   * @throws UnsupportedOperationException if the dialect does not support deletes
   */
  default String buildDeleteStatement(
      TableId table,
      Collection<ColumnId> keyColumns
  ) {
    throw new UnsupportedOperationException();
  }

  /**
   * Build the DROP TABLE statement expression for the given table.
   *
   * @param table   the identifier of the table; may not be null
   * @param options the options; may be null
   * @return the DROP TABLE statement; may not be null
   */
  String buildDropTableStatement(TableId table, DropOptions options);

  /**
   * Build the CREATE TABLE statement expression for the given table and its columns.
   *
   * @param table  the identifier of the table; may not be null
   * @param fields the information about the fields in the sink records; may not be null
   * @return the CREATE TABLE statement; may not be null
   */
  String buildCreateTableStatement(TableId table, Collection<SinkRecordField> fields);

  /**
   * Build the ALTER TABLE statement expression for the given table and its columns.
   *
   * @param table  the identifier of the table; may not be null
   * @param fields the information about the fields in the sink records; may not be null
   * @return the ALTER TABLE statement; may not be null
   */
  List<String> buildAlterTable(TableId table, Collection<SinkRecordField> fields);

  /**
   * Create a component that can bind record values into the supplied prepared statement.
   * @param statement      the prepared statement
   * @param pkMode         the primary key mode; may not be null
   * @param schemaPair     the key and value schemas; may not be null
   * @param fieldsMetadata the field metadata; may not be null
   * @param insertMode     the insert mode; may not be null
   * @return the statement binder; may not be null
   * @see #bindField(PreparedStatement, int, Schema, Object)
   */
  @Deprecated
  StatementBinder statementBinder(
      PreparedStatement statement,
      JdbcSinkConfig.PrimaryKeyMode pkMode,
      SchemaPair schemaPair,
      FieldsMetadata fieldsMetadata,
      JdbcSinkConfig.InsertMode insertMode
  );

  /**
   * Create a component that can bind record values into the supplied prepared statement. By
   * default, the behavior is the same as the other overloaded method with the extra parameter
   * tableDefinition. This overloading method is introduced to deprecate the other overloaded
   * method eventually.
   *
   * @param statement      the prepared statement
   * @param pkMode         the primary key mode; may not be null
   * @param schemaPair     the key and value schemas; may not be null
   * @param fieldsMetadata the field metadata; may not be null
   * @param tableDefinition the table definition; may be null
   * @param insertMode     the insert mode; may not be null
   * @return the statement binder; may not be null
   * @see #bindField(PreparedStatement, int, Schema, Object)
   */
  default StatementBinder statementBinder(
      PreparedStatement statement,
      JdbcSinkConfig.PrimaryKeyMode pkMode,
      SchemaPair schemaPair,
      FieldsMetadata fieldsMetadata,
      TableDefinition tableDefinition,
      JdbcSinkConfig.InsertMode insertMode
  ) {
    return statementBinder(statement, pkMode, schemaPair, fieldsMetadata, insertMode);
  }

  /**
   * Validate if dialect specific column types are compatible with connector.
   * Sometimes JDBC treats some column types in a SQL database the same
   * (eg, MSSQL Server's DATETIME and DATETIME2 are considered {@link java.sql.Time}).
   * This function is used to handle these specifc column types.
   *
   * @param rsMetadata          the result set metadata; may not be null
   * @param columns             columns to check; may not be null
   * @throws ConnectException   if column type not compatible with connector
   *                            or if there is an error accessing the result set metadata
   */
  void validateSpecificColumnTypes(
          ResultSetMetaData rsMetadata,
          List<ColumnId> columns
  ) throws ConnectException;

  /**
   * Method that binds a value with the given schema at the specified variable within a prepared
   * statement.
   * @param statement the prepared statement; may not be null
   * @param index     the 1-based index of the variable within the prepared statement
   * @param schema    the schema for the value; may be null only if the value is null
   * @param value     the value to be bound to the variable; may be null
   * @throws SQLException if there is a problem binding the value into the statement
   * @see #statementBinder
   */
  @Deprecated
  void bindField(
      PreparedStatement statement,
      int index,
      Schema schema,
      Object value
  ) throws SQLException;


  /** Method that binds a value with the given schema at the specified variable within a prepared
   * statement. By default, the behavior is the same as the other overloaded method with the extra
   * parameter colDef. This overloading method is introduced to deprecate the other overloaded
   * method eventually.
   *
   * @param statement the prepared statement; may not be null
   * @param index     the 1-based index of the variable within the prepared statement
   * @param schema    the schema for the value; may be null only if the value is null
   * @param value     the value to be bound to the variable; may be null
   * @param colDef    the Definition of the column to be bound; may be null
   * @throws SQLException if there is a problem binding the value into the statement
   * @see #statementBinder
   */
  default void bindField(
      PreparedStatement statement,
      int index,
      Schema schema,
      Object value,
      ColumnDefinition colDef
  ) throws SQLException {
    bindField(statement, index, schema, value);
  }

  /**
   * A function to bind the values from a sink record into a prepared statement.
   */
  @FunctionalInterface
  interface StatementBinder {

    /**
     * Bind the values in the supplied record.
     *
     * @param record the sink record with values to be bound into the statement; never null
     * @throws SQLException if there is a problem binding values into the statement
     */
    void bindRecord(SinkRecord record) throws SQLException;
  }

  /**
   * Create a function that converts column values for the column defined by the specified mapping.
   *
   * @param mapping the column definition and the corresponding {@link Field}; may not be null
   * @return the column converter function; or null if the column should be ignored
   */
  ColumnConverter createColumnConverter(ColumnMapping mapping);

  /**
   * A function that obtains a column value from the current row of the specified result set.
   */
  @FunctionalInterface
  interface ColumnConverter {

    /**
     * Get the column's value from the row at the current position in the result set, and convert it
     * to a value that should be included in the corresponding {@link Field} in the {@link Struct}
     * for the row.
     *
     * @param resultSet the result set; never null
     * @return the value of the {@link Field} as converted from the column value
     * @throws SQLException if there is an error with the database connection
     * @throws IOException  if there is an error accessing a streaming value from the result set
     */
    Object convert(ResultSet resultSet) throws SQLException, IOException;
  }
}