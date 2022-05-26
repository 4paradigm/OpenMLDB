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

package io.confluent.connect.jdbc.source;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.source.SchemaMapping.FieldSetter;
import io.confluent.connect.jdbc.util.ExpressionBuilder;

/**
 * BulkTableQuerier always returns the entire table.
 */
public class BulkTableQuerier extends TableQuerier {
  private static final Logger log = LoggerFactory.getLogger(BulkTableQuerier.class);

  public BulkTableQuerier(
      DatabaseDialect dialect,
      QueryMode mode,
      String name,
      String topicPrefix,
      String suffix
  ) {
    super(dialect, mode, name, topicPrefix, suffix);
  }

  @Override
  protected void createPreparedStatement(Connection db) throws SQLException {
    ExpressionBuilder builder = dialect.expressionBuilder();  
    switch (mode) {
      case TABLE:
        builder.append("SELECT * FROM ").append(tableId);

        break;
      case QUERY:
        builder.append(query);  
        
        break;
      default:
        throw new ConnectException("Unknown mode: " + mode);
    }

    addSuffixIfPresent(builder);
    
    String queryStr = builder.toString();

    recordQuery(queryStr);
    log.trace("{} prepared SQL query: {}", this, queryStr);
    stmt = dialect.createPreparedStatement(db, queryStr);
  }

  @Override
  protected ResultSet executeQuery() throws SQLException {
    return stmt.executeQuery();
  }

  @Override
  public SourceRecord extractRecord() throws SQLException {
    Struct record = new Struct(schemaMapping.schema());
    for (FieldSetter setter : schemaMapping.fieldSetters()) {
      try {
        setter.setField(record, resultSet);
      } catch (IOException e) {
        log.warn("Error mapping fields into Connect record", e);
        throw new ConnectException(e);
      } catch (SQLException e) {
        log.warn("SQL error mapping fields into Connect record", e);
        throw new DataException(e);
      }
    }
    // TODO: key from primary key? partition?
    final String topic;
    final Map<String, String> partition;
    switch (mode) {
      case TABLE:
        String name = tableId.tableName(); // backwards compatible
        partition = Collections.singletonMap(JdbcSourceConnectorConstants.TABLE_NAME_KEY, name);
        topic = topicPrefix + name;
        break;
      case QUERY:
        partition = Collections.singletonMap(JdbcSourceConnectorConstants.QUERY_NAME_KEY,
                                             JdbcSourceConnectorConstants.QUERY_NAME_VALUE
        );
        topic = topicPrefix;
        break;
      default:
        throw new ConnectException("Unexpected query mode: " + mode);
    }
    return new SourceRecord(partition, null, topic, record.schema(), record);
  }

  @Override
  public String toString() {
    return "BulkTableQuerier{" + "table='" + tableId + '\'' + ", query='" + query + '\''
           + ", topicPrefix='" + topicPrefix + '\'' + '}';
  }

}
