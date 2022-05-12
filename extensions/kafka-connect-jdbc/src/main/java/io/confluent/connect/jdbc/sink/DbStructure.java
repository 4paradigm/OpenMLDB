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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.sink.metadata.FieldsMetadata;
import io.confluent.connect.jdbc.sink.metadata.SinkRecordField;
import io.confluent.connect.jdbc.util.TableDefinition;
import io.confluent.connect.jdbc.util.TableDefinitions;
import io.confluent.connect.jdbc.util.TableId;
import io.confluent.connect.jdbc.util.TableType;

public class DbStructure {
  private static final Logger log = LoggerFactory.getLogger(DbStructure.class);

  private final DatabaseDialect dbDialect;
  private final TableDefinitions tableDefns;

  public DbStructure(DatabaseDialect dbDialect) {
    this.dbDialect = dbDialect;
    this.tableDefns = new TableDefinitions(dbDialect);
  }

  /**
   * Create or amend table.
   *
   * @param config the connector configuration
   * @param connection the database connection handle
   * @param tableId the table ID
   * @param fieldsMetadata the fields metadata
   * @return whether a DDL operation was performed
   * @throws SQLException if a DDL operation was deemed necessary but failed
   */
  public boolean createOrAmendIfNecessary(
      final JdbcSinkConfig config,
      final Connection connection,
      final TableId tableId,
      final FieldsMetadata fieldsMetadata
  ) throws SQLException, TableAlterOrCreateException {
    if (tableDefns.get(connection, tableId) == null) {
      // Table does not yet exist, so attempt to create it ...
      try {
        create(config, connection, tableId, fieldsMetadata);
      } catch (SQLException sqle) {
        log.warn("Create failed, will attempt amend if table already exists", sqle);
        try {
          TableDefinition newDefn = tableDefns.refresh(connection, tableId);
          if (newDefn == null) {
            throw sqle;
          }
        } catch (SQLException e) {
          throw sqle;
        } catch (TableAlterOrCreateException te) {
          log.warn(te.getMessage());
          throw te;
        }
      }
    }
    return amendIfNecessary(config, connection, tableId, fieldsMetadata, config.maxRetries);
  }

  /**
   * Get the definition for the table with the given ID. This returns a cached definition if
   * there is one; otherwise, it reads the definition from the database
   *
   * @param connection the connection that may be used to fetch the table definition if not
   *                   already known; may not be null
   * @param tableId    the ID of the table; may not be null
   * @return the table definition; or null if the table does not exist
   * @throws SQLException if there is an error getting the definition from the database
   */
  public TableDefinition tableDefinition(
      Connection connection,
      TableId tableId
  ) throws SQLException {
    TableDefinition defn = tableDefns.get(connection, tableId);
    if (defn != null) {
      return defn;
    }
    return tableDefns.refresh(connection, tableId);
  }

  /**
   * @throws SQLException if CREATE failed
   */
  void create(
      final JdbcSinkConfig config,
      final Connection connection,
      final TableId tableId,
      final FieldsMetadata fieldsMetadata
  ) throws SQLException, TableAlterOrCreateException {
    if (!config.autoCreate) {
      throw new TableAlterOrCreateException(
          String.format("Table %s is missing and auto-creation is disabled", tableId)
      );
    }
    String sql = dbDialect.buildCreateTableStatement(tableId, fieldsMetadata.allFields.values());
    log.info("Creating table with sql: {}", sql);
    dbDialect.applyDdlStatements(connection, Collections.singletonList(sql));
  }

  /**
   * @return whether an ALTER was successfully performed
   * @throws SQLException if ALTER was deemed necessary but failed
   */
  boolean amendIfNecessary(
      final JdbcSinkConfig config,
      final Connection connection,
      final TableId tableId,
      final FieldsMetadata fieldsMetadata,
      final int maxRetries
  ) throws SQLException, TableAlterOrCreateException {
    // NOTE:
    //   The table might have extra columns defined (hopefully with default values), which is not
    //   a case we check for here.
    //   We also don't check if the data types for columns that do line-up are compatible.

    final TableDefinition tableDefn = tableDefns.get(connection, tableId);

    // FIXME: SQLite JDBC driver seems to not always return the PK column names?
    //    if (!tableMetadata.getPrimaryKeyColumnNames().equals(fieldsMetadata.keyFieldNames)) {
    //      throw new ConnectException(String.format(
    //          "Table %s has different primary key columns - database (%s), desired (%s)",
    //          tableName, tableMetadata.getPrimaryKeyColumnNames(), fieldsMetadata.keyFieldNames
    //      ));
    //    }

    final Set<SinkRecordField> missingFields = missingFields(
        fieldsMetadata.allFields.values(),
        tableDefn.columnNames()
    );

    if (missingFields.isEmpty()) {
      return false;
    }

    // At this point there are missing fields
    TableType type = tableDefn.type();
    switch (type) {
      case TABLE:
        // Rather than embed the logic and change lots of lines, just break out
        break;
      case VIEW:
      default:
        throw new TableAlterOrCreateException(
            String.format(
                "%s %s is missing fields (%s) and ALTER %s is unsupported",
                type.capitalized(),
                tableId,
                missingFields,
                type.jdbcName()
            )
        );
    }

    for (SinkRecordField missingField: missingFields) {
      if (!missingField.isOptional() && missingField.defaultValue() == null) {
        throw new TableAlterOrCreateException(String.format(
            "Cannot ALTER %s %s to add missing field %s, as the field is not optional and does "
            + "not have a default value",
            type.jdbcName(),
            tableId,
            missingField
        ));
      }
    }

    if (!config.autoEvolve) {
      throw new TableAlterOrCreateException(String.format(
          "%s %s is missing fields (%s) and auto-evolution is disabled",
          type.capitalized(),
          tableId,
          missingFields
      ));
    }

    final List<String> amendTableQueries = dbDialect.buildAlterTable(tableId, missingFields);
    log.info(
        "Amending {} to add missing fields:{} maxRetries:{} with SQL: {}",
        type,
        missingFields,
        maxRetries,
        amendTableQueries
    );
    try {
      dbDialect.applyDdlStatements(connection, amendTableQueries);
    } catch (SQLException sqle) {
      if (maxRetries <= 0) {
        throw new TableAlterOrCreateException(
            String.format(
                "Failed to amend %s '%s' to add missing fields: %s",
                type,
                tableId,
                missingFields
            ),
            sqle
        );
      }
      log.warn("Amend failed, re-attempting", sqle);
      tableDefns.refresh(connection, tableId);
      // Perhaps there was a race with other tasks to add the columns
      return amendIfNecessary(
          config,
          connection,
          tableId,
          fieldsMetadata,
          maxRetries - 1
      );
    }

    tableDefns.refresh(connection, tableId);
    return true;
  }

  Set<SinkRecordField> missingFields(
      Collection<SinkRecordField> fields,
      Set<String> dbColumnNames
  ) {
    final Set<SinkRecordField> missingFields = new HashSet<>();
    for (SinkRecordField field : fields) {
      if (!dbColumnNames.contains(field.name())) {
        log.debug("Found missing field: {}", field);
        missingFields.add(field);
      }
    }

    if (missingFields.isEmpty()) {
      return missingFields;
    }

    // check if the missing fields can be located by ignoring case
    Set<String> columnNamesLowerCase = new HashSet<>();
    for (String columnName: dbColumnNames) {
      columnNamesLowerCase.add(columnName.toLowerCase());
    }

    if (columnNamesLowerCase.size() != dbColumnNames.size()) {
      log.warn(
          "Table has column names that differ only by case. Original columns={}",
          dbColumnNames
      );
    }

    final Set<SinkRecordField> missingFieldsIgnoreCase = new HashSet<>();
    for (SinkRecordField missing: missingFields) {
      if (!columnNamesLowerCase.contains(missing.name().toLowerCase())) {
        missingFieldsIgnoreCase.add(missing);
      }
    }

    if (missingFieldsIgnoreCase.size() > 0) {
      log.info(
          "Unable to find fields {} among column names {}",
          missingFieldsIgnoreCase,
          dbColumnNames
      );
    }

    return missingFieldsIgnoreCase;
  }
}
