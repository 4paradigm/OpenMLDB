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

import java.time.ZoneOffset;
import java.util.TimeZone;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.URL;
import java.nio.ByteBuffer;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import io.confluent.connect.jdbc.dialect.DatabaseDialectProvider.FixedScoreProvider;
import io.confluent.connect.jdbc.sink.JdbcSinkConfig;
import io.confluent.connect.jdbc.sink.JdbcSinkConfig.InsertMode;
import io.confluent.connect.jdbc.sink.JdbcSinkConfig.PrimaryKeyMode;
import io.confluent.connect.jdbc.sink.PreparedStatementBinder;
import io.confluent.connect.jdbc.sink.metadata.FieldsMetadata;
import io.confluent.connect.jdbc.sink.metadata.SchemaPair;
import io.confluent.connect.jdbc.sink.metadata.SinkRecordField;
import io.confluent.connect.jdbc.source.ColumnMapping;
import io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig;
import io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig.NumericMapping;
import io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig.TimestampGranularity;
import io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig.TransactionIsolationMode;
import io.confluent.connect.jdbc.source.JdbcSourceTaskConfig;
import io.confluent.connect.jdbc.source.TimestampIncrementingCriteria;
import io.confluent.connect.jdbc.util.ColumnDefinition;
import io.confluent.connect.jdbc.util.ColumnDefinition.Mutability;
import io.confluent.connect.jdbc.util.ColumnDefinition.Nullability;
import io.confluent.connect.jdbc.util.ColumnId;
import io.confluent.connect.jdbc.util.DateTimeUtils;
import io.confluent.connect.jdbc.util.ExpressionBuilder;
import io.confluent.connect.jdbc.util.ExpressionBuilder.Transform;
import io.confluent.connect.jdbc.util.IdentifierRules;
import io.confluent.connect.jdbc.util.JdbcDriverInfo;
import io.confluent.connect.jdbc.util.QuoteMethod;
import io.confluent.connect.jdbc.util.TableDefinition;
import io.confluent.connect.jdbc.util.TableId;
import io.confluent.connect.jdbc.util.TableType;

/**
 * A {@link DatabaseDialect} implementation that provides functionality based upon JDBC and SQL.
 *
 * <p>This class is designed to be extended as required to customize the behavior for a specific
 * DBMS. For example, override the {@link #createColumnConverter(ColumnMapping)} method to customize
 * how a column value is converted to a field value for use in a {@link Struct}. To also change the
 * field's type or schema, also override the {@link #addFieldToSchema} method.
 */
public class GenericDatabaseDialect implements DatabaseDialect {

  protected static final int NUMERIC_TYPE_SCALE_LOW = -84;
  protected static final int NUMERIC_TYPE_SCALE_HIGH = 127;
  protected static final int NUMERIC_TYPE_SCALE_UNSET = -127;

  // The maximum precision that can be achieved in a signed 64-bit integer is 2^63 ~= 9.223372e+18
  private static final int MAX_INTEGER_TYPE_PRECISION = 18;

  private static final String PRECISION_FIELD = "connect.decimal.precision";

  /**
   * The provider for {@link GenericDatabaseDialect}.
   */
  public static class Provider extends FixedScoreProvider {
    public Provider() {
      super(GenericDatabaseDialect.class.getSimpleName(),
            DatabaseDialectProvider.AVERAGE_MATCHING_SCORE
      );
    }

    @Override
    public DatabaseDialect create(AbstractConfig config) {
      return new GenericDatabaseDialect(config);
    }
  }

  private static final Logger glog = LoggerFactory.getLogger(GenericDatabaseDialect.class);

  // This field was originally used by subclasses but resulted in incorrect namespaces in
  // log files. Subclasses are now strongly encouraged to instantiate their own logger, using
  // their own class. This field is only kept now to avoid breaking backwards compatibility for
  // existing dialects that may rely on it.
  @Deprecated
  protected final Logger log = LoggerFactory.getLogger(GenericDatabaseDialect.class);
  protected final AbstractConfig config;

  /**
   * Whether to map {@code NUMERIC} JDBC types by precision.
   */
  protected final NumericMapping mapNumerics;
  protected String catalogPattern;
  protected final String schemaPattern;
  protected final Set<String> tableTypes;
  protected final String jdbcUrl;
  protected final DatabaseDialectProvider.JdbcUrlInfo jdbcUrlInfo;
  private final QuoteMethod quoteSqlIdentifiers;
  private final IdentifierRules defaultIdentifierRules;
  private final AtomicReference<IdentifierRules> identifierRules = new AtomicReference<>();
  private final Queue<Connection> connections = new ConcurrentLinkedQueue<>();
  private volatile JdbcDriverInfo jdbcDriverInfo;
  private final int batchMaxRows;
  private final TimeZone timeZone;
  private final JdbcSourceConnectorConfig.TimestampGranularity tsGranularity;

  /**
   * Create a new dialect instance with the given connector configuration.
   *
   * @param config the connector configuration; may not be null
   */
  public GenericDatabaseDialect(AbstractConfig config) {
    this(config, IdentifierRules.DEFAULT);
  }

  /**
   * Create a new dialect instance with the given connector configuration.
   *
   * @param config                 the connector configuration; may not be null
   * @param defaultIdentifierRules the default rules for identifiers; may be null if the rules are
   *                               to be determined from the database metadata
   */
  protected GenericDatabaseDialect(
      AbstractConfig config,
      IdentifierRules defaultIdentifierRules
  ) {
    this.config = config;
    this.defaultIdentifierRules = defaultIdentifierRules;
    this.jdbcUrl = config.getString(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG);
    this.jdbcUrlInfo = DatabaseDialects.extractJdbcUrlInfo(jdbcUrl);
    if (config instanceof JdbcSinkConfig) {
      JdbcSinkConfig sinkConfig = (JdbcSinkConfig) config;
      catalogPattern = JdbcSourceTaskConfig.CATALOG_PATTERN_DEFAULT;
      schemaPattern = JdbcSourceTaskConfig.SCHEMA_PATTERN_DEFAULT;
      tableTypes = sinkConfig.tableTypeNames();
      quoteSqlIdentifiers = QuoteMethod.get(
          config.getString(JdbcSinkConfig.QUOTE_SQL_IDENTIFIERS_CONFIG)
      );
    } else {
      catalogPattern = config.getString(JdbcSourceTaskConfig.CATALOG_PATTERN_CONFIG);
      schemaPattern = config.getString(JdbcSourceTaskConfig.SCHEMA_PATTERN_CONFIG);
      tableTypes = new HashSet<>(config.getList(JdbcSourceTaskConfig.TABLE_TYPE_CONFIG));
      quoteSqlIdentifiers = QuoteMethod.get(
          config.getString(JdbcSourceConnectorConfig.QUOTE_SQL_IDENTIFIERS_CONFIG)
      );
    }
    if (config instanceof JdbcSourceConnectorConfig) {
      mapNumerics = ((JdbcSourceConnectorConfig)config).numericMapping();
      batchMaxRows = config.getInt(JdbcSourceConnectorConfig.BATCH_MAX_ROWS_CONFIG);
    } else {
      mapNumerics = NumericMapping.NONE;
      batchMaxRows = 0;
    }

    if (config instanceof JdbcSourceConnectorConfig) {
      timeZone = ((JdbcSourceConnectorConfig) config).timeZone();
    } else if (config instanceof JdbcSinkConfig) {
      timeZone = ((JdbcSinkConfig) config).timeZone;
    } else {
      timeZone = TimeZone.getTimeZone(ZoneOffset.UTC);
    }

    if (config instanceof JdbcSourceConnectorConfig) {
      tsGranularity = TimestampGranularity.get((JdbcSourceConnectorConfig) config);
    } else {
      tsGranularity = TimestampGranularity.CONNECT_LOGICAL;
    }
  }

  @Override
  public String name() {
    return getClass().getSimpleName().replace("DatabaseDialect", "");
  }

  protected TimeZone timeZone() {
    return timeZone;
  }

  @Override
  public Connection getConnection() throws SQLException {
    // These config names are the same for both source and sink configs ...
    String username = config.getString(JdbcSourceConnectorConfig.CONNECTION_USER_CONFIG);
    Password dbPassword = config.getPassword(JdbcSourceConnectorConfig.CONNECTION_PASSWORD_CONFIG);
    Properties properties = new Properties();
    if (username != null) {
      properties.setProperty("user", username);
    }
    if (dbPassword != null) {
      properties.setProperty("password", dbPassword.value());
    }
    properties = addConnectionProperties(properties);
    // Timeout is 40 seconds to be as long as possible for customer to have a long connection
    // handshake, while still giving enough time to validate once in the follower worker,
    // and again in the leader worker and still be under 90s REST serving timeout
    DriverManager.setLoginTimeout(40);
    Connection connection = DriverManager.getConnection(jdbcUrl, properties);
    if (jdbcDriverInfo == null) {
      jdbcDriverInfo = createJdbcDriverInfo(connection);
    }
    connections.add(connection);
    return connection;
  }

  @Override
  public void close() {
    Connection conn;
    while ((conn = connections.poll()) != null) {
      try {
        conn.close();
      } catch (Throwable e) {
        glog.warn("Error while closing connection to {}", jdbcDriverInfo, e);
      }
    }
  }

  @Override
  public boolean isConnectionValid(
      Connection connection,
      int timeout
  ) throws SQLException {
    if (jdbcDriverInfo().jdbcMajorVersion() >= 4) {
      return connection.isValid(timeout);
    }
    // issue a test query ...
    String query = checkConnectionQuery();
    if (query != null) {
      try (Statement statement = connection.createStatement()) {
        if (statement.execute(query)) {
          ResultSet rs = null;
          try {
            // do nothing with the result set
            rs = statement.getResultSet();
          } finally {
            if (rs != null) {
              rs.close();
            }
          }
        }
      }
    }
    return true;
  }

  /**
   * Return a query that can be used to check the validity of an existing database connection
   * when the JDBC driver does not support JDBC 4. By default this returns {@code SELECT 1},
   * but subclasses should override this when a different query should be used.
   *
   * @return the check connection query; may be null if the connection should not be queried
   */
  protected String checkConnectionQuery() {
    return "SELECT 1";
  }

  protected JdbcDriverInfo jdbcDriverInfo() {
    if (jdbcDriverInfo == null) {
      try (Connection connection = getConnection()) {
        jdbcDriverInfo = createJdbcDriverInfo(connection);
      } catch (SQLException e) {
        throw new ConnectException("Unable to get JDBC driver information", e);
      }
    }
    return jdbcDriverInfo;
  }

  protected JdbcDriverInfo createJdbcDriverInfo(Connection connection) throws SQLException {
    DatabaseMetaData metadata = connection.getMetaData();
    return new JdbcDriverInfo(
        metadata.getJDBCMajorVersion(),
        metadata.getJDBCMinorVersion(),
        metadata.getDriverName(),
        metadata.getDatabaseProductName(),
        metadata.getDatabaseProductVersion()
    );
  }

  /**
   * Add or modify any connection properties based upon the {@link #config configuration}.
   *
   * <p>By default this method adds any {@code connection.*} properties (except those predefined
   * by the connector's ConfigDef, such as {@code connection.url}, {@code connection.user},
   * {@code connection.password}, {@code connection.attempts}, etc.) only after removing the
   * {@code connection.} prefix. This allows users to add any additional DBMS-specific properties
   * for the database to the connector configuration by prepending the DBMS-specific
   * properties with the {@code connection.} prefix.
   *
   * <p>Subclasses that don't wish to support this behavior can override this method without
   * calling this super method.
   *
   * @param properties the properties that will be passed to the {@link DriverManager}'s {@link
   *                   DriverManager#getConnection(String, Properties) getConnection(...) method};
   *                   never null
   * @return the updated connection properties, or {@code properties} if they are not modified or
   *     should be returned; never null
   */
  protected Properties addConnectionProperties(Properties properties) {
    // Get the set of config keys that are known to the connector
    Set<String> configKeys = config.values().keySet();
    // Add any configuration property that begins with 'connection.` and that is not known
    config.originalsWithPrefix(JdbcSourceConnectorConfig.CONNECTION_PREFIX).forEach((k,v) -> {
      if (!configKeys.contains(JdbcSourceConnectorConfig.CONNECTION_PREFIX + k)) {
        properties.put(k, v);
      }
    });
    return properties;
  }

  @Override
  public PreparedStatement createPreparedStatement(
      Connection db,
      String query
  ) throws SQLException {
    glog.trace("Creating a PreparedStatement '{}'", query);
    PreparedStatement stmt = db.prepareStatement(query);
    initializePreparedStatement(stmt);
    return stmt;
  }

  /**
   * Perform any operations on a {@link PreparedStatement} before it is used. This is called from
   * the {@link #createPreparedStatement(Connection, String)} method after the statement is
   * created but before it is returned/used.
   *
   * <p>By default this method sets the {@link PreparedStatement#setFetchSize(int) fetch size} to
   * the {@link JdbcSourceConnectorConfig#BATCH_MAX_ROWS_CONFIG batch size} of the connector.
   * This will provide a hint to the JDBC driver as to the number of rows to fetch from the database
   * in an attempt to limit memory usage when reading from large tables. Driver implementations
   * often require further configuration to make use of the fetch size.
   *
   * @param stmt the prepared statement; never null
   * @throws SQLException the error that might result from initialization
   */
  protected void initializePreparedStatement(PreparedStatement stmt) throws SQLException {
    if (batchMaxRows > 0) {
      stmt.setFetchSize(batchMaxRows);
    }
  }

  @Override
  public TableId parseTableIdentifier(String fqn) {
    List<String> parts = identifierRules().parseQualifiedIdentifier(fqn);
    if (parts.isEmpty()) {
      throw new IllegalArgumentException("Invalid fully qualified name: '" + fqn + "'");
    }
    if (parts.size() == 1) {
      return new TableId(null, null, parts.get(0));
    }
    if (parts.size() == 3) {
      return new TableId(parts.get(0), parts.get(1), parts.get(2));
    }
    assert parts.size() >= 2;
    if (useCatalog()) {
      return new TableId(parts.get(0), null, parts.get(1));
    }
    return new TableId(null, parts.get(0), parts.get(1));
  }

  /**
   * Return whether the database uses JDBC catalogs.
   *
   * @return true if catalogs are used, or false otherwise
   */
  protected boolean useCatalog() {
    return false;
  }

  @Override
  public List<TableId> tableIds(Connection conn) throws SQLException {
    DatabaseMetaData metadata = conn.getMetaData();
    String[] tableTypes = tableTypes(metadata, this.tableTypes);
    String tableTypeDisplay = displayableTableTypes(tableTypes, ", ");
    glog.debug("Using {} dialect to get {}", this, tableTypeDisplay);

    try (ResultSet rs = metadata.getTables(catalogPattern(), schemaPattern(), "%", tableTypes)) {
      List<TableId> tableIds = new ArrayList<>();
      while (rs.next()) {
        String catalogName = rs.getString(1);
        String schemaName = rs.getString(2);
        String tableName = rs.getString(3);
        TableId tableId = new TableId(catalogName, schemaName, tableName);
        if (includeTable(tableId)) {
          tableIds.add(tableId);
        }
      }
      glog.debug("Used {} dialect to find {} {}", this, tableIds.size(), tableTypeDisplay);
      return tableIds;
    }
  }

  protected String catalogPattern() {
    return catalogPattern;
  }

  protected String schemaPattern() {
    return schemaPattern;
  }

  /**
   * Determine whether the table with the specific name is to be included in the tables.
   *
   * <p>This method can be overridden to exclude certain database tables.
   *
   * @param table the identifier of the table; may be null
   * @return true if the table should be included; false otherwise
   */
  protected boolean includeTable(TableId table) {
    return true;
  }

  /**
   * Find the available table types that are returned by the JDBC driver that case insensitively
   * match the specified types.
   *
   * @param metadata the database metadata; may not be null but may be empty if no table types
   * @param types    the case-independent table types that are desired
   * @return the array of table types take directly from the list of available types returned by the
   *     JDBC driver; never null
   * @throws SQLException if there is an error with the database connection
   */
  protected String[] tableTypes(
      DatabaseMetaData metadata,
      Set<String> types
  ) throws SQLException {
    glog.debug("Using {} dialect to check support for {}", this, types);
    // Compute the uppercase form of the desired types ...
    Set<String> uppercaseTypes = new HashSet<>();
    for (String type : types) {
      if (type != null) {
        uppercaseTypes.add(type.toUpperCase(Locale.ROOT));
      }
    }
    // Now find out the available table types ...
    Set<String> matchingTableTypes = new HashSet<>();
    try (ResultSet rs = metadata.getTableTypes()) {
      while (rs.next()) {
        String tableType = rs.getString(1);
        if (tableType != null && uppercaseTypes.contains(tableType.toUpperCase(Locale.ROOT))) {
          matchingTableTypes.add(tableType);
        }
      }
    }
    String[] result = matchingTableTypes.toArray(new String[matchingTableTypes.size()]);
    glog.debug("Used {} dialect to find table types: {}", this, result);
    return result;
  }

  @Override
  public IdentifierRules identifierRules() {
    if (identifierRules.get() == null) {
      // Otherwise try to get the actual quote string and separator from the database, since
      // many databases allow them to be changed
      try (Connection connection = getConnection()) {
        DatabaseMetaData metaData = connection.getMetaData();
        String leadingQuoteStr = metaData.getIdentifierQuoteString();
        String trailingQuoteStr = leadingQuoteStr; // JDBC does not distinguish
        String separator = metaData.getCatalogSeparator();
        if (leadingQuoteStr == null || leadingQuoteStr.isEmpty()) {
          leadingQuoteStr = defaultIdentifierRules.leadingQuoteString();
          trailingQuoteStr = defaultIdentifierRules.trailingQuoteString();
        }
        if (separator == null || separator.isEmpty()) {
          separator = defaultIdentifierRules.identifierDelimiter();
        }
        identifierRules.set(new IdentifierRules(separator, leadingQuoteStr, trailingQuoteStr));
      } catch (SQLException e) {
        if (defaultIdentifierRules != null) {
          identifierRules.set(defaultIdentifierRules);
          glog.warn("Unable to get identifier metadata; using default rules", e);
        } else {
          throw new ConnectException("Unable to get identifier metadata", e);
        }
      }
    }
    return identifierRules.get();
  }

  @Override
  public ExpressionBuilder expressionBuilder() {
    return identifierRules().expressionBuilder()
                            .setQuoteIdentifiers(quoteSqlIdentifiers);
  }

  /**
   * Return current time at the database
   *
   * @param conn database connection
   * @param cal  calendar
   * @return the current time at the database
   */
  @Override
  public Timestamp currentTimeOnDB(
      Connection conn,
      Calendar cal
  ) throws SQLException, ConnectException {
    String query = currentTimestampDatabaseQuery();
    assert query != null;
    assert !query.isEmpty();
    try (Statement stmt = conn.createStatement()) {
      glog.debug("executing query " + query + " to get current time from database");
      try (ResultSet rs = stmt.executeQuery(query)) {
        if (rs.next()) {
          return rs.getTimestamp(1, cal);
        } else {
          throw new ConnectException(
              "Unable to get current time from DB using " + this + " and query '" + query + "'"
          );
        }
      }
    } catch (SQLException e) {
      glog.error("Failed to get current time from DB using {} and query '{}'", this, query, e);
      throw e;
    }
  }

  /**
   * Get the query string to determine the current timestamp in the database.
   *
   * @return the query string; never null or empty
   */
  protected String currentTimestampDatabaseQuery() {
    return "SELECT CURRENT_TIMESTAMP";
  }

  @Override
  public boolean tableExists(
      Connection connection,
      TableId tableId
  ) throws SQLException {
    DatabaseMetaData metadata = connection.getMetaData();
    String[] tableTypes = tableTypes(metadata, this.tableTypes);
    String tableTypeDisplay = displayableTableTypes(tableTypes, "/");
    glog.info("Checking {} dialect for existence of {} {}", this, tableTypeDisplay, tableId);
    try (ResultSet rs = connection.getMetaData().getTables(
        tableId.catalogName(),
        tableId.schemaName(),
        tableId.tableName(),
        tableTypes
    )) {
      final boolean exists = rs.next();
      glog.info(
          "Using {} dialect {} {} {}",
          this,
          tableTypeDisplay,
          tableId,
          exists ? "present" : "absent"
      );
      return exists;
    }
  }

  public void setConnectionIsolationMode(
          Connection connection,
          TransactionIsolationMode transactionIsolationMode
  ) {
    if (transactionIsolationMode
            == TransactionIsolationMode.DEFAULT) {
      return;
    }
    int isolationMode = TransactionIsolationMode.get(
            transactionIsolationMode
    );
    try {
      DatabaseMetaData metadata = connection.getMetaData();
      if (metadata.supportsTransactionIsolationLevel(isolationMode)) {
        connection.setTransactionIsolation(isolationMode);
      } else {
        throw new ConfigException("Transaction Isolation level not supported by database");
      }
    } catch (SQLException | ConfigException ex) {
      log.warn("Unable to set transaction.isolation.mode: " +  transactionIsolationMode.name()
              +  ". No transaction isolation mode will be set for the queries: " + ex.getMessage());
    }
  }

  protected String displayableTableTypes(String[] types, String delim) {
    return Arrays.stream(types).sorted().collect(Collectors.joining(delim));
  }

  @Override
  public Map<ColumnId, ColumnDefinition> describeColumns(
      Connection connection,
      String tablePattern,
      String columnPattern
  ) throws SQLException {
    //if the table pattern is fqn, then just use the actual table name
    TableId tableId = parseTableIdentifier(tablePattern);
    String catalog = tableId.catalogName() != null ? tableId.catalogName() : catalogPattern;
    String schema = tableId.schemaName() != null ? tableId.schemaName() : schemaPattern;
    return describeColumns(connection, catalog , schema, tableId.tableName(), columnPattern);
  }

  @Override
  public Map<ColumnId, ColumnDefinition> describeColumns(
      Connection connection,
      String catalogPattern,
      String schemaPattern,
      String tablePattern,
      String columnPattern
  ) throws SQLException {
    glog.debug(
        "Querying {} dialect column metadata for catalog:{} schema:{} table:{}",
        this,
        catalogPattern,
        schemaPattern,
        tablePattern
    );

    // Get the primary keys of the table(s) ...
    final Set<ColumnId> pkColumns = primaryKeyColumns(
        connection,
        catalogPattern,
        schemaPattern,
        tablePattern
    );
    Map<ColumnId, ColumnDefinition> results = new HashMap<>();
    try (ResultSet rs = connection.getMetaData().getColumns(
        catalogPattern,
        schemaPattern,
        tablePattern,
        columnPattern
    )) {
      final int rsColumnCount = rs.getMetaData().getColumnCount();
      while (rs.next()) {
        final String catalogName = rs.getString(1);
        final String schemaName = rs.getString(2);
        final String tableName = rs.getString(3);
        final TableId tableId = new TableId(catalogName, schemaName, tableName);
        final String columnName = rs.getString(4);
        final ColumnId columnId = new ColumnId(tableId, columnName, null);
        final int jdbcType = rs.getInt(5);
        final String typeName = rs.getString(6);
        final int precision = rs.getInt(7);
        final int scale = rs.getInt(9);
        final String typeClassName = null;
        Nullability nullability;
        final int nullableValue = rs.getInt(11);
        switch (nullableValue) {
          case DatabaseMetaData.columnNoNulls:
            nullability = Nullability.NOT_NULL;
            break;
          case DatabaseMetaData.columnNullable:
            nullability = Nullability.NULL;
            break;
          case DatabaseMetaData.columnNullableUnknown:
          default:
            nullability = Nullability.UNKNOWN;
            break;
        }
        Boolean autoIncremented = null;
        if (rsColumnCount >= 23) {
          // Not all drivers include all columns ...
          String isAutoIncremented = rs.getString(23);
          if ("yes".equalsIgnoreCase(isAutoIncremented)) {
            autoIncremented = Boolean.TRUE;
          } else if ("no".equalsIgnoreCase(isAutoIncremented)) {
            autoIncremented = Boolean.FALSE;
          }
        }
        Boolean signed = null;
        Boolean caseSensitive = null;
        Boolean searchable = null;
        Boolean currency = null;
        Integer displaySize = null;
        boolean isPrimaryKey = pkColumns.contains(columnId);
        if (isPrimaryKey) {
          // Some DBMSes report pks as null
          nullability = Nullability.NOT_NULL;
        }
        ColumnDefinition defn = columnDefinition(
            rs,
            columnId,
            jdbcType,
            typeName,
            typeClassName,
            nullability,
            Mutability.UNKNOWN,
            precision,
            scale,
            signed,
            displaySize,
            autoIncremented,
            caseSensitive,
            searchable,
            currency,
            isPrimaryKey
        );
        results.put(columnId, defn);
      }
      return results;
    }
  }

  @Override
  public Map<ColumnId, ColumnDefinition> describeColumns(ResultSetMetaData rsMetadata) throws
      SQLException {
    Map<ColumnId, ColumnDefinition> result = new LinkedHashMap<>();
    for (int i = 1; i <= rsMetadata.getColumnCount(); ++i) {
      ColumnDefinition defn = describeColumn(rsMetadata, i);
      result.put(defn.id(), defn);
    }
    return result;
  }

  /**
   * Create a definition for the specified column in the result set.
   *
   * @param rsMetadata the result set metadata; may not be null
   * @param column     the column number, starting at 1 for the first column
   * @return the column definition; never null
   * @throws SQLException if there is an error accessing the result set metadata
   */
  protected ColumnDefinition describeColumn(
      ResultSetMetaData rsMetadata,
      int column
  ) throws SQLException {
    String catalog = rsMetadata.getCatalogName(column);
    String schema = rsMetadata.getSchemaName(column);
    String tableName = rsMetadata.getTableName(column);
    TableId tableId = new TableId(catalog, schema, tableName);
    String name = rsMetadata.getColumnName(column);
    String alias = rsMetadata.getColumnLabel(column);
    ColumnId id = new ColumnId(tableId, name, alias);
    Nullability nullability;
    switch (rsMetadata.isNullable(column)) {
      case ResultSetMetaData.columnNullable:
        nullability = Nullability.NULL;
        break;
      case ResultSetMetaData.columnNoNulls:
        nullability = Nullability.NOT_NULL;
        break;
      case ResultSetMetaData.columnNullableUnknown:
      default:
        nullability = Nullability.UNKNOWN;
        break;
    }
    Mutability mutability = Mutability.MAYBE_WRITABLE;
    if (rsMetadata.isReadOnly(column)) {
      mutability = Mutability.READ_ONLY;
    } else if (rsMetadata.isWritable(column)) {
      mutability = Mutability.MAYBE_WRITABLE;
    } else if (rsMetadata.isDefinitelyWritable(column)) {
      mutability = Mutability.WRITABLE;
    }
    return new ColumnDefinition(
        id,
        rsMetadata.getColumnType(column),
        rsMetadata.getColumnTypeName(column),
        rsMetadata.getColumnClassName(column),
        nullability,
        mutability,
        rsMetadata.getPrecision(column),
        rsMetadata.getScale(column),
        rsMetadata.isSigned(column),
        rsMetadata.getColumnDisplaySize(column),
        rsMetadata.isAutoIncrement(column),
        rsMetadata.isCaseSensitive(column),
        rsMetadata.isSearchable(column),
        rsMetadata.isCurrency(column),
        false
    );
  }

  protected Set<ColumnId> primaryKeyColumns(
      Connection connection,
      String catalogPattern,
      String schemaPattern,
      String tablePattern
  ) throws SQLException {

    // Get the primary keys of the table(s) ...
    final Set<ColumnId> pkColumns = new HashSet<>();
    try (ResultSet rs = connection.getMetaData().getPrimaryKeys(
        catalogPattern, schemaPattern, tablePattern)) {
      while (rs.next()) {
        String catalogName = rs.getString(1);
        String schemaName = rs.getString(2);
        String tableName = rs.getString(3);
        TableId tableId = new TableId(catalogName, schemaName, tableName);
        final String colName = rs.getString(4);
        ColumnId columnId = new ColumnId(tableId, colName);
        pkColumns.add(columnId);
      }
    }
    return pkColumns;
  }

  @Override
  public Map<ColumnId, ColumnDefinition> describeColumnsByQuerying(
      Connection db,
      TableId tableId
  ) throws SQLException {
    String queryStr = "SELECT * FROM {} LIMIT 1";
    String quotedName = expressionBuilder().append(tableId).toString();
    try (PreparedStatement stmt = db.prepareStatement(queryStr)) {
      stmt.setString(1, quotedName);
      try (ResultSet rs = stmt.executeQuery()) {
        ResultSetMetaData rsmd = rs.getMetaData();
        return describeColumns(rsmd);
      }
    }
  }

  @Override
  public TableDefinition describeTable(
      Connection connection,
      TableId tableId
  ) throws SQLException {
    Map<ColumnId, ColumnDefinition> columnDefns = describeColumns(connection, tableId.catalogName(),
                                                                  tableId.schemaName(),
                                                                  tableId.tableName(), null
    );
    if (columnDefns.isEmpty()) {
      return null;
    }
    TableType tableType = tableTypeFor(connection, tableId);
    return new TableDefinition(tableId, columnDefns.values(), tableType);
  }

  protected TableType tableTypeFor(
      Connection connection,
      TableId tableId
  ) throws SQLException {
    DatabaseMetaData metadata = connection.getMetaData();
    String[] tableTypes = tableTypes(metadata, this.tableTypes);
    String tableTypeDisplay = displayableTableTypes(tableTypes, "/");
    glog.info("Checking {} dialect for type of {} {}", this, tableTypeDisplay, tableId);
    try (ResultSet rs = connection.getMetaData().getTables(
        tableId.catalogName(),
        tableId.schemaName(),
        tableId.tableName(),
        tableTypes
    )) {
      if (rs.next()) {
        //final String catalogName = rs.getString(1);
        //final String schemaName = rs.getString(2);
        //final String tableName = rs.getString(3);
        final String tableType = rs.getString(4);
        try {
          return TableType.get(tableType);
        } catch (IllegalArgumentException e) {
          glog.warn(
              "{} dialect found unknown type '{}' for {} {}; using TABLE",
              this,
              tableType,
              tableTypeDisplay,
              tableId
          );
          return TableType.TABLE;
        }
      }
    }
    glog.warn(
        "{} dialect did not find type for {} {}; using TABLE",
        this,
        tableTypeDisplay,
        tableId
    );
    return TableType.TABLE;
  }

  /**
   * Create a ColumnDefinition with supplied values and the result set from the {@link
   * DatabaseMetaData#getColumns(String, String, String, String)} call. By default that method does
   * not describe whether the column is signed, case sensitive, searchable, currency, or the
   * preferred display size.
   *
   * <p>Subclasses can override this method to extract additional non-standard characteristics from
   * the result set, and override the characteristics determined using the standard JDBC metadata
   * columns and supplied as parameters.
   *
   * @param resultSet        the result set
   * @param id               the column identifier
   * @param jdbcType         the JDBC type of the column
   * @param typeName         the name of the column's type
   * @param classNameForType the name of the class used as instances of the value when {@link
   *                         ResultSet#getObject(int)} is called
   * @param nullability      the nullability of the column
   * @param mutability       the mutability of the column
   * @param precision        the precision of the column for numeric values, or the length for
   *                         non-numeric values
   * @param scale            the scale of the column for numeric values; ignored for other values
   * @param signedNumbers    true if the column holds signed numeric values; null if not known
   * @param displaySize      the preferred display size for the column values; null if not known
   * @param autoIncremented  true if the column is auto-incremented; null if not known
   * @param caseSensitive    true if the column values are case-sensitive; null if not known
   * @param searchable       true if the column is searchable; null if no; null if not known known
   * @param currency         true if the column is a currency value
   * @param isPrimaryKey     true if the column is part of the primary key; null if not known known
   * @return the column definition; never null
   */
  protected ColumnDefinition columnDefinition(
      ResultSet resultSet,
      ColumnId id,
      int jdbcType,
      String typeName,
      String classNameForType,
      Nullability nullability,
      Mutability mutability,
      int precision,
      int scale,
      Boolean signedNumbers,
      Integer displaySize,
      Boolean autoIncremented,
      Boolean caseSensitive,
      Boolean searchable,
      Boolean currency,
      Boolean isPrimaryKey
  ) {
    return new ColumnDefinition(
        id,
        jdbcType,
        typeName,
        classNameForType,
        nullability,
        mutability,
        precision,
        scale,
        signedNumbers != null ? signedNumbers.booleanValue() : false,
        displaySize != null ? displaySize.intValue() : 0,
        autoIncremented != null ? autoIncremented.booleanValue() : false,
        caseSensitive != null ? caseSensitive.booleanValue() : false,
        searchable != null ? searchable.booleanValue() : false,
        currency != null ? currency.booleanValue() : false,
        isPrimaryKey != null ? isPrimaryKey.booleanValue() : false
    );
  }

  @Override
  public TimestampIncrementingCriteria criteriaFor(
      ColumnId incrementingColumn,
      List<ColumnId> timestampColumns
  ) {
    return new TimestampIncrementingCriteria(incrementingColumn, timestampColumns, timeZone);
  }

  /**
   * Determine the name of the field. By default this is the column alias or name.
   *
   * @param columnDefinition the column definition; never null
   * @return the field name; never null
   */
  protected String fieldNameFor(ColumnDefinition columnDefinition) {
    return columnDefinition.id().aliasOrName();
  }

  @Override
  public String addFieldToSchema(
      ColumnDefinition columnDefn,
      SchemaBuilder builder
  ) {
    return addFieldToSchema(columnDefn, builder, fieldNameFor(columnDefn), columnDefn.type(),
                            columnDefn.isOptional()
    );
  }

  /**
   * Use the supplied {@link SchemaBuilder} to add a field that corresponds to the column with the
   * specified definition. This is intended to be easily overridden by subclasses.
   *
   * @param columnDefn the definition of the column; may not be null
   * @param builder    the schema builder; may not be null
   * @param fieldName  the name of the field and {@link #fieldNameFor(ColumnDefinition) computed}
   *                   from the column definition; may not be null
   * @param sqlType    the JDBC {@link java.sql.Types type} as obtained from the column definition
   * @param optional   true if the field is to be optional as obtained from the column definition
   * @return the name of the field, or null if no field was added
   */
  @SuppressWarnings("fallthrough")
  protected String addFieldToSchema(
      final ColumnDefinition columnDefn,
      final SchemaBuilder builder,
      final String fieldName,
      final int sqlType,
      final boolean optional
  ) {
    int precision = columnDefn.precision();
    int scale = columnDefn.scale();
    switch (sqlType) {
      case Types.NULL: {
        glog.debug("JDBC type 'NULL' not currently supported for column '{}'", fieldName);
        return null;
      }

      case Types.BOOLEAN: {
        builder.field(fieldName, optional ? Schema.OPTIONAL_BOOLEAN_SCHEMA : Schema.BOOLEAN_SCHEMA);
        break;
      }

      // ints <= 8 bits
      case Types.BIT: {
        builder.field(fieldName, optional ? Schema.OPTIONAL_INT8_SCHEMA : Schema.INT8_SCHEMA);
        break;
      }

      case Types.TINYINT: {
        if (columnDefn.isSignedNumber()) {
          builder.field(fieldName, optional ? Schema.OPTIONAL_INT8_SCHEMA : Schema.INT8_SCHEMA);
        } else {
          builder.field(fieldName, optional ? Schema.OPTIONAL_INT16_SCHEMA : Schema.INT16_SCHEMA);
        }
        break;
      }

      // 16 bit ints
      case Types.SMALLINT: {
        if (columnDefn.isSignedNumber()) {
          builder.field(fieldName, optional ? Schema.OPTIONAL_INT16_SCHEMA : Schema.INT16_SCHEMA);
        } else {
          builder.field(fieldName, optional ? Schema.OPTIONAL_INT32_SCHEMA : Schema.INT32_SCHEMA);
        }
        break;
      }

      // 32 bit ints
      case Types.INTEGER: {
        if (columnDefn.isSignedNumber()) {
          builder.field(fieldName, optional ? Schema.OPTIONAL_INT32_SCHEMA : Schema.INT32_SCHEMA);
        } else {
          builder.field(fieldName, optional ? Schema.OPTIONAL_INT64_SCHEMA : Schema.INT64_SCHEMA);
        }
        break;
      }

      // 64 bit ints
      case Types.BIGINT: {
        builder.field(fieldName, optional ? Schema.OPTIONAL_INT64_SCHEMA : Schema.INT64_SCHEMA);
        break;
      }

      // REAL is a single precision floating point value, i.e. a Java float
      case Types.REAL: {
        builder.field(fieldName, optional ? Schema.OPTIONAL_FLOAT32_SCHEMA : Schema.FLOAT32_SCHEMA);
        break;
      }

      // FLOAT is, confusingly, double precision and effectively the same as DOUBLE. See REAL
      // for single precision
      case Types.FLOAT:
      case Types.DOUBLE: {
        builder.field(fieldName, optional ? Schema.OPTIONAL_FLOAT64_SCHEMA : Schema.FLOAT64_SCHEMA);
        break;
      }

      case Types.NUMERIC:
        if (mapNumerics == NumericMapping.PRECISION_ONLY) {
          glog.debug("NUMERIC with precision: '{}' and scale: '{}'", precision, scale);
          if (scale == 0 && precision <= MAX_INTEGER_TYPE_PRECISION) { // integer
            builder.field(fieldName, integerSchema(optional, precision));
            break;
          }
        } else if (mapNumerics == NumericMapping.BEST_FIT) {
          glog.debug("NUMERIC with precision: '{}' and scale: '{}'", precision, scale);
          if (precision <= MAX_INTEGER_TYPE_PRECISION) { // fits in primitive data types.
            if (scale < 1 && scale >= NUMERIC_TYPE_SCALE_LOW) { // integer
              builder.field(fieldName, integerSchema(optional, precision));
              break;
            } else if (scale > 0) { // floating point - use double in all cases
              Schema schema = (optional) ? Schema.OPTIONAL_FLOAT64_SCHEMA : Schema.FLOAT64_SCHEMA;
              builder.field(fieldName, schema);
              break;
            }
          }
        } else if (mapNumerics == NumericMapping.BEST_FIT_EAGER_DOUBLE) {
          glog.debug("NUMERIC with precision: '{}' and scale: '{}'", precision, scale);
          if (scale < 1 && scale >= NUMERIC_TYPE_SCALE_LOW) { // integer
            if (precision <= MAX_INTEGER_TYPE_PRECISION) { // fits in primitive data types.
              builder.field(fieldName, integerSchema(optional, precision));
              break;
            }
          } else if (scale > 0) { // floating point - use double in all cases
            Schema schema = (optional) ? Schema.OPTIONAL_FLOAT64_SCHEMA : Schema.FLOAT64_SCHEMA;
            builder.field(fieldName, schema);
            break;
          }
        }
        // fallthrough

      case Types.DECIMAL: {
        glog.debug("DECIMAL with precision: '{}' and scale: '{}'", precision, scale);
        scale = decimalScale(columnDefn);
        SchemaBuilder fieldBuilder = Decimal.builder(scale);
        fieldBuilder.parameter(PRECISION_FIELD, Integer.toString(precision));
        if (optional) {
          fieldBuilder.optional();
        }
        builder.field(fieldName, fieldBuilder.build());
        break;
      }

      case Types.CHAR:
      case Types.VARCHAR:
      case Types.LONGVARCHAR:
      case Types.NCHAR:
      case Types.NVARCHAR:
      case Types.LONGNVARCHAR:
      case Types.CLOB:
      case Types.NCLOB:
      case Types.DATALINK:
      case Types.SQLXML: {
        // Some of these types will have fixed size, but we drop this from the schema conversion
        // since only fixed byte arrays can have a fixed size
        builder.field(fieldName, optional ? Schema.OPTIONAL_STRING_SCHEMA : Schema.STRING_SCHEMA);
        break;
      }

      // Binary == fixed bytes
      // BLOB, VARBINARY, LONGVARBINARY == bytes
      case Types.BINARY:
      case Types.BLOB:
      case Types.VARBINARY:
      case Types.LONGVARBINARY: {
        builder.field(fieldName, optional ? Schema.OPTIONAL_BYTES_SCHEMA : Schema.BYTES_SCHEMA);
        break;
      }

      // Date is day + moth + year
      case Types.DATE: {
        SchemaBuilder dateSchemaBuilder = Date.builder();
        if (optional) {
          dateSchemaBuilder.optional();
        }
        builder.field(fieldName, dateSchemaBuilder.build());
        break;
      }

      // Time is a time of day -- hour, minute, seconds, nanoseconds
      case Types.TIME: {
        SchemaBuilder timeSchemaBuilder = Time.builder();
        if (optional) {
          timeSchemaBuilder.optional();
        }
        builder.field(fieldName, timeSchemaBuilder.build());
        break;
      }

      // Timestamp is a date + time
      case Types.TIMESTAMP: {
        builder.field(fieldName, tsGranularity.schemaFunction.apply(optional));
        break;
      }

      case Types.ARRAY:
      case Types.JAVA_OBJECT:
      case Types.OTHER:
      case Types.DISTINCT:
      case Types.STRUCT:
      case Types.REF:
      case Types.ROWID:
      default: {
        glog.warn("JDBC type {} ({}) not currently supported", sqlType, columnDefn.typeName());
        return null;
      }
    }
    return fieldName;
  }

  private Schema integerSchema(boolean optional, int precision) {
    Schema schema;
    if (precision > 9) {
      schema = (optional) ? Schema.OPTIONAL_INT64_SCHEMA : Schema.INT64_SCHEMA;
    } else if (precision > 4) {
      schema = (optional) ? Schema.OPTIONAL_INT32_SCHEMA : Schema.INT32_SCHEMA;
    } else if (precision > 2) {
      schema = (optional) ? Schema.OPTIONAL_INT16_SCHEMA : Schema.INT16_SCHEMA;
    } else {
      schema = (optional) ? Schema.OPTIONAL_INT8_SCHEMA : Schema.INT8_SCHEMA;
    }
    return schema;
  }

  @Override
  public void applyDdlStatements(
      Connection connection,
      List<String> statements
  ) throws SQLException {
    try (Statement statement = connection.createStatement()) {
      for (String ddlStatement : statements) {
        statement.executeUpdate(ddlStatement);
      }
    }
    try {
      connection.commit();
    } catch (Exception e) {
      try {
        connection.rollback();
      } catch (SQLException sqle) {
        e.addSuppressed(sqle);
      } finally {
        throw e;
      }
    }
  }

  @Override
  public ColumnConverter createColumnConverter(
      ColumnMapping mapping
  ) {
    return columnConverterFor(mapping, mapping.columnDefn(), mapping.columnNumber(),
                              jdbcDriverInfo().jdbcVersionAtLeast(4, 0)
    );
  }

  @SuppressWarnings({"deprecation", "fallthrough"})
  protected ColumnConverter columnConverterFor(
      final ColumnMapping mapping,
      final ColumnDefinition defn,
      final int col,
      final boolean isJdbc4
  ) {
    switch (mapping.columnDefn().type()) {

      case Types.BOOLEAN: {
        return rs -> rs.getBoolean(col);
      }

      case Types.BIT: {
        /**
         * BIT should be either 0 or 1.
         * TODO: Postgres handles this differently, returning a string "t" or "f". See the
         * elasticsearch-jdbc plugin for an example of how this is handled
         */
        return rs -> rs.getByte(col);
      }

      // 8 bits int
      case Types.TINYINT: {
        if (defn.isSignedNumber()) {
          return rs -> rs.getByte(col);
        } else {
          return rs -> rs.getShort(col);
        }
      }

      // 16 bits int
      case Types.SMALLINT: {
        if (defn.isSignedNumber()) {
          return rs -> rs.getShort(col);
        } else {
          return rs -> rs.getInt(col);
        }
      }

      // 32 bits int
      case Types.INTEGER: {
        if (defn.isSignedNumber()) {
          return rs -> rs.getInt(col);
        } else {
          return rs -> rs.getLong(col);
        }
      }

      // 64 bits int
      case Types.BIGINT: {
        return rs -> rs.getLong(col);
      }

      // REAL is a single precision floating point value, i.e. a Java float
      case Types.REAL: {
        return rs -> rs.getFloat(col);
      }

      // FLOAT is, confusingly, double precision and effectively the same as DOUBLE. See REAL
      // for single precision
      case Types.FLOAT:
      case Types.DOUBLE: {
        return rs -> rs.getDouble(col);
      }

      case Types.NUMERIC:
        if (mapNumerics == NumericMapping.PRECISION_ONLY) {
          int precision = defn.precision();
          int scale = defn.scale();
          glog.trace("NUMERIC with precision: '{}' and scale: '{}'", precision, scale);
          if (scale == 0 && precision <= MAX_INTEGER_TYPE_PRECISION) { // integer
            if (precision > 9) {
              return rs -> rs.getLong(col);
            } else if (precision > 4) {
              return rs -> rs.getInt(col);
            } else if (precision > 2) {
              return rs -> rs.getShort(col);
            } else {
              return rs -> rs.getByte(col);
            }
          }
        } else if (mapNumerics == NumericMapping.BEST_FIT) {
          int precision = defn.precision();
          int scale = defn.scale();
          glog.trace("NUMERIC with precision: '{}' and scale: '{}'", precision, scale);
          if (precision <= MAX_INTEGER_TYPE_PRECISION) { // fits in primitive data types.
            if (scale < 1 && scale >= NUMERIC_TYPE_SCALE_LOW) { // integer
              if (precision > 9) {
                return rs -> rs.getLong(col);
              } else if (precision > 4) {
                return rs -> rs.getInt(col);
              } else if (precision > 2) {
                return rs -> rs.getShort(col);
              } else {
                return rs -> rs.getByte(col);
              }
            } else if (scale > 0) { // floating point - use double in all cases
              return rs -> rs.getDouble(col);
            }
          }
        } else if (mapNumerics == NumericMapping.BEST_FIT_EAGER_DOUBLE) {
          int precision = defn.precision();
          int scale = defn.scale();
          glog.trace("NUMERIC with precision: '{}' and scale: '{}'", precision, scale);
          if (scale < 1 && scale >= NUMERIC_TYPE_SCALE_LOW) { // integer
            if (precision <= MAX_INTEGER_TYPE_PRECISION) { // fits in primitive data types.
              if (precision > 9) {
                return rs -> rs.getLong(col);
              } else if (precision > 4) {
                return rs -> rs.getInt(col);
              } else if (precision > 2) {
                return rs -> rs.getShort(col);
              } else {
                return rs -> rs.getByte(col);
              }
            }
          } else if (scale > 0) { // floating point - use double in all cases
            return rs -> rs.getDouble(col);
          }
        }
        // fallthrough

      case Types.DECIMAL: {
        final int precision = defn.precision();
        glog.debug("DECIMAL with precision: '{}' and scale: '{}'", precision, defn.scale());
        final int scale = decimalScale(defn);
        return rs -> rs.getBigDecimal(col, scale);
      }

      case Types.CHAR:
      case Types.VARCHAR:
      case Types.LONGVARCHAR: {
        return rs -> rs.getString(col);
      }

      case Types.NCHAR:
      case Types.NVARCHAR:
      case Types.LONGNVARCHAR: {
        return rs -> rs.getNString(col);
      }

      // Binary == fixed, VARBINARY and LONGVARBINARY == bytes
      case Types.BINARY:
      case Types.VARBINARY:
      case Types.LONGVARBINARY: {
        return rs -> rs.getBytes(col);
      }

      // Date is day + month + year
      case Types.DATE: {
        return rs -> rs.getDate(col,
            DateTimeUtils.getTimeZoneCalendar(TimeZone.getTimeZone(ZoneOffset.UTC)));
      }

      // Time is a time of day -- hour, minute, seconds, nanoseconds
      case Types.TIME: {
        return rs -> rs.getTime(col, DateTimeUtils.getTimeZoneCalendar(timeZone));
      }

      // Timestamp is a date + time
      case Types.TIMESTAMP: {
        return rs -> {
          Timestamp timestamp = rs.getTimestamp(col, DateTimeUtils.getTimeZoneCalendar(timeZone));
          return tsGranularity.fromTimestamp.apply(timestamp, timeZone);
        };
      }

      // Datalink is basically a URL -> string
      case Types.DATALINK: {
        return rs -> {
          URL url = rs.getURL(col);
          return (url != null ? url.toString() : null);
        };
      }

      // BLOB == fixed
      case Types.BLOB: {
        return rs -> {
          Blob blob = rs.getBlob(col);
          if (blob == null) {
            return null;
          } else {
            try {
              if (blob.length() > Integer.MAX_VALUE) {
                throw new IOException("Can't process BLOBs longer than " + Integer.MAX_VALUE);
              }
              return blob.getBytes(1, (int) blob.length());
            } finally {
              if (isJdbc4) {
                free(blob);
              }
            }
          }
        };
      }
      case Types.CLOB:
        return rs -> {
          Clob clob = rs.getClob(col);
          if (clob == null) {
            return null;
          } else {
            try {
              if (clob.length() > Integer.MAX_VALUE) {
                throw new IOException("Can't process CLOBs longer than " + Integer.MAX_VALUE);
              }
              return clob.getSubString(1, (int) clob.length());
            } finally {
              if (isJdbc4) {
                free(clob);
              }
            }
          }
        };
      case Types.NCLOB: {
        return rs -> {
          Clob clob = rs.getNClob(col);
          if (clob == null) {
            return null;
          } else {
            try {
              if (clob.length() > Integer.MAX_VALUE) {
                throw new IOException("Can't process NCLOBs longer than " + Integer.MAX_VALUE);
              }
              return clob.getSubString(1, (int) clob.length());
            } finally {
              if (isJdbc4) {
                free(clob);
              }
            }
          }
        };
      }

      // XML -> string
      case Types.SQLXML: {
        return rs -> {
          SQLXML xml = rs.getSQLXML(col);
          return xml != null ? xml.getString() : null;
        };
      }

      case Types.NULL:
      case Types.ARRAY:
      case Types.JAVA_OBJECT:
      case Types.OTHER:
      case Types.DISTINCT:
      case Types.STRUCT:
      case Types.REF:
      case Types.ROWID:
      default: {
        // These are not currently supported, but we don't want to log something for every single
        // record we translate. There will already be errors logged for the schema translation
        break;
      }
    }
    return null;
  }

  protected int decimalScale(ColumnDefinition defn) {
    return defn.scale() == NUMERIC_TYPE_SCALE_UNSET ? NUMERIC_TYPE_SCALE_HIGH : defn.scale();
  }

  /**
   * Called when the object has been fully read and {@link Blob#free()} should be called.
   *
   * @param blob the Blob; never null
   * @throws SQLException if there is a problem calling free()
   */
  protected void free(Blob blob) throws SQLException {
    blob.free();
  }

  /**
   * Called when the object has been fully read and {@link Clob#free()} should be called.
   *
   * @param clob the Clob; never null
   * @throws SQLException if there is a problem calling free()
   */
  protected void free(Clob clob) throws SQLException {
    clob.free();
  }

  @Override
  @SuppressWarnings("deprecation")
  public String buildInsertStatement(
      TableId table,
      Collection<ColumnId> keyColumns,
      Collection<ColumnId> nonKeyColumns
  ) {
    ExpressionBuilder builder = expressionBuilder();
    builder.append("INSERT INTO ");
    builder.append(table);
    builder.append("(");
    builder.appendList()
           .delimitedBy(",")
           .transformedBy(ExpressionBuilder.columnNames())
           .of(keyColumns, nonKeyColumns);
    builder.append(") VALUES(");
    builder.appendMultiple(",", "?", keyColumns.size() + nonKeyColumns.size());
    builder.append(")");
    return builder.toString();
  }

  @Override
  @SuppressWarnings("deprecation")
  public String buildUpdateStatement(
      TableId table,
      Collection<ColumnId> keyColumns,
      Collection<ColumnId> nonKeyColumns
  ) {
    ExpressionBuilder builder = expressionBuilder();
    builder.append("UPDATE ");
    builder.append(table);
    builder.append(" SET ");
    builder.appendList()
           .delimitedBy(", ")
           .transformedBy(ExpressionBuilder.columnNamesWith(" = ?"))
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
  @SuppressWarnings("deprecation")
  public String buildUpsertQueryStatement(
      TableId table,
      Collection<ColumnId> keyColumns,
      Collection<ColumnId> nonKeyColumns
  ) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String buildDeleteStatement(
          TableId table,
          Collection<ColumnId> keyColumns
  ) {
    ExpressionBuilder builder = expressionBuilder();
    builder.append("DELETE FROM ");
    builder.append(table);
    if (!keyColumns.isEmpty()) {
      builder.append(" WHERE ");
      builder.appendList()
          .delimitedBy(" AND ")
          .transformedBy(ExpressionBuilder.columnNamesWith(" = ?"))
          .of(keyColumns);
    }
    return builder.toString();
  }

  @SuppressWarnings("deprecation")
  @Override
  public StatementBinder statementBinder(
      PreparedStatement statement,
      PrimaryKeyMode pkMode,
      SchemaPair schemaPair,
      FieldsMetadata fieldsMetadata,
      InsertMode insertMode
  ) {
    return new PreparedStatementBinder(
        this,
        statement,
        pkMode,
        schemaPair,
        fieldsMetadata,
        insertMode
    );
  }

  @SuppressWarnings("deprecation")
  @Override
  public void bindField(
      PreparedStatement statement,
      int index,
      Schema schema,
      Object value
  ) throws SQLException {
    if (value == null) {
      Integer type = getSqlTypeForSchema(schema);
      if (type != null) {
        statement.setNull(index, type);
      } else {
        statement.setObject(index, null);
      }
    } else {
      boolean bound = maybeBindLogical(statement, index, schema, value);
      if (!bound) {
        bound = maybeBindPrimitive(statement, index, schema, value);
      }
      if (!bound) {
        throw new ConnectException("Unsupported source data type: " + schema.type());
      }
    }
  }

  /**
   * Dialects not supporting `setObject(index, null)` can override this method
   * to provide a specific sqlType, as per the JDBC documentation
   * https://docs.oracle.com/javase/7/docs/api/java/sql/PreparedStatement.html
   *
   * @param schema the schema
   * @return the SQL type
   */
  protected Integer getSqlTypeForSchema(Schema schema) {
    return null;
  }

  protected boolean maybeBindPrimitive(
      PreparedStatement statement,
      int index,
      Schema schema,
      Object value
  ) throws SQLException {
    switch (schema.type()) {
      case INT8:
        statement.setByte(index, (Byte) value);
        break;
      case INT16:
        statement.setShort(index, (Short) value);
        break;
      case INT32:
        statement.setInt(index, (Integer) value);
        break;
      case INT64:
        statement.setLong(index, (Long) value);
        break;
      case FLOAT32:
        statement.setFloat(index, (Float) value);
        break;
      case FLOAT64:
        statement.setDouble(index, (Double) value);
        break;
      case BOOLEAN:
        statement.setBoolean(index, (Boolean) value);
        break;
      case STRING:
        statement.setString(index, (String) value);
        break;
      case BYTES:
        final byte[] bytes;
        if (value instanceof ByteBuffer) {
          final ByteBuffer buffer = ((ByteBuffer) value).slice();
          bytes = new byte[buffer.remaining()];
          buffer.get(bytes);
        } else {
          bytes = (byte[]) value;
        }
        statement.setBytes(index, bytes);
        break;
      default:
        return false;
    }
    return true;
  }

  protected boolean maybeBindLogical(
      PreparedStatement statement,
      int index,
      Schema schema,
      Object value
  ) throws SQLException {
    if (schema.name() != null) {
      switch (schema.name()) {
        case Date.LOGICAL_NAME:
          statement.setDate(
              index,
              new java.sql.Date(((java.util.Date) value).getTime()),
              DateTimeUtils.getTimeZoneCalendar(timeZone)
          );
          return true;
        case Decimal.LOGICAL_NAME:
          statement.setBigDecimal(index, (BigDecimal) value);
          return true;
        case Time.LOGICAL_NAME:
          statement.setTime(
              index,
              new java.sql.Time(((java.util.Date) value).getTime()),
              DateTimeUtils.getTimeZoneCalendar(timeZone)
          );
          return true;
        case org.apache.kafka.connect.data.Timestamp.LOGICAL_NAME:
          statement.setTimestamp(
              index,
              new java.sql.Timestamp(((java.util.Date) value).getTime()),
              DateTimeUtils.getTimeZoneCalendar(timeZone)
          );
          return true;
        default:
          return false;
      }
    }
    return false;
  }

  @Override
  public String buildCreateTableStatement(
      TableId table,
      Collection<SinkRecordField> fields
  ) {
    ExpressionBuilder builder = expressionBuilder();

    final List<String> pkFieldNames = extractPrimaryKeyFieldNames(fields);
    builder.append("CREATE TABLE ");
    builder.append(table);
    builder.append(" (");
    writeColumnsSpec(builder, fields);
    if (!pkFieldNames.isEmpty()) {
      builder.append(",");
      builder.append(System.lineSeparator());
      builder.append("PRIMARY KEY(");
      builder.appendList()
             .delimitedBy(",")
             .transformedBy(ExpressionBuilder.quote())
             .of(pkFieldNames);
      builder.append(")");
    }
    builder.append(")");
    return builder.toString();
  }

  @Override
  public String buildDropTableStatement(
      TableId table,
      DropOptions options
  ) {
    ExpressionBuilder builder = expressionBuilder();

    builder.append("DROP TABLE ");
    builder.append(table);
    if (options.ifExists()) {
      builder.append(" IF EXISTS");
    }
    if (options.cascade()) {
      builder.append(" CASCADE");
    }
    return builder.toString();
  }

  @Override
  public List<String> buildAlterTable(
      TableId table,
      Collection<SinkRecordField> fields
  ) {
    final boolean newlines = fields.size() > 1;

    final Transform<SinkRecordField> transform = (builder, field) -> {
      if (newlines) {
        builder.appendNewLine();
      }
      builder.append("ADD ");
      writeColumnSpec(builder, field);
    };

    ExpressionBuilder builder = expressionBuilder();
    builder.append("ALTER TABLE ");
    builder.append(table);
    builder.append(" ");
    builder.appendList()
           .delimitedBy(",")
           .transformedBy(transform)
           .of(fields);
    return Collections.singletonList(builder.toString());
  }

  @Override
  public void validateSpecificColumnTypes(
          ResultSetMetaData rsMetadata,
          List<ColumnId> columns
  ) throws ConnectException { }

  protected List<String> extractPrimaryKeyFieldNames(Collection<SinkRecordField> fields) {
    final List<String> pks = new ArrayList<>();
    for (SinkRecordField f : fields) {
      if (f.isPrimaryKey()) {
        pks.add(f.name());
      }
    }
    return pks;
  }

  protected void writeColumnsSpec(
      ExpressionBuilder builder,
      Collection<SinkRecordField> fields
  ) {
    Transform<SinkRecordField> transform = (b, field) -> {
      b.append(System.lineSeparator());
      writeColumnSpec(b, field);
    };
    builder.appendList().delimitedBy(",").transformedBy(transform).of(fields);
  }

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
    } else if (isColumnOptional(f)) {
      builder.append(" NULL");
    } else {
      builder.append(" NOT NULL");
    }
  }

  protected boolean isColumnOptional(SinkRecordField field) {
    return field.isOptional();
  }

  protected void formatColumnValue(
      ExpressionBuilder builder,
      String schemaName,
      Map<String, String> schemaParameters,
      Schema.Type type,
      Object value
  ) {
    if (schemaName != null) {
      switch (schemaName) {
        case Decimal.LOGICAL_NAME:
          builder.append(value);
          return;
        case Date.LOGICAL_NAME:
          builder.appendStringQuoted(DateTimeUtils.formatDate((java.util.Date) value, timeZone));
          return;
        case Time.LOGICAL_NAME:
          builder.appendStringQuoted(DateTimeUtils.formatTime((java.util.Date) value, timeZone));
          return;
        case org.apache.kafka.connect.data.Timestamp.LOGICAL_NAME:
          builder.appendStringQuoted(
              DateTimeUtils.formatTimestamp((java.util.Date) value, timeZone)
          );
          return;
        default:
          // fall through to regular types
          break;
      }
    }
    switch (type) {
      case INT8:
      case INT16:
      case INT32:
      case INT64:
      case FLOAT32:
      case FLOAT64:
        // no escaping required
        builder.append(value);
        break;
      case BOOLEAN:
        // 1 & 0 for boolean is more portable rather than TRUE/FALSE
        builder.append((Boolean) value ? '1' : '0');
        break;
      case STRING:
        builder.appendStringQuoted(value);
        break;
      case BYTES:
        final byte[] bytes;
        if (value instanceof ByteBuffer) {
          final ByteBuffer buffer = ((ByteBuffer) value).slice();
          bytes = new byte[buffer.remaining()];
          buffer.get(bytes);
        } else {
          bytes = (byte[]) value;
        }
        builder.appendBinaryLiteral(bytes);
        break;
      default:
        throw new ConnectException("Unsupported type for column value: " + type);
    }
  }

  protected String getSqlType(SinkRecordField f) {
    throw new ConnectException(String.format(
        "%s (%s) type doesn't have a mapping to the SQL database column type", f.schemaName(),
        f.schemaType()
    ));
  }

  /**
   * Return the sanitized form of the supplied JDBC URL, which masks any secrets or credentials.
   *
   * <p>This implementation replaces the value of all properties that contain {@code password}.
   *
   * @param url the JDBC URL; may not be null
   * @return the sanitized URL; never null
   */
  protected String sanitizedUrl(String url) {
    // Only replace standard URL-type properties ...
    return url.replaceAll("(?i)([?&]([^=&]*)password([^=&]*)=)[^&]*", "$1****");
  }

  @Override
  public String identifier() {
    return name() + " database " + sanitizedUrl(jdbcUrl);
  }

  @Override
  public String toString() {
    return name();
  }
}
