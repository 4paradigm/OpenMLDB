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

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.source.TableQuerier.QueryMode;
import io.confluent.connect.jdbc.util.ColumnId;
import io.confluent.connect.jdbc.util.ExpressionBuilder;
import io.confluent.connect.jdbc.util.TableId;

import java.sql.Connection;
import java.sql.SQLException;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TableQuerierTest {  
  private static final String TABLE_NAME = "name";
  private static final String INCREMENTING_COLUMN_NAME = "column";
  private static final String SUFFIX = "/* SUFFIX */";   
  private static final Long TIMESTAMP_DELAY = 0l;
  private static final String QUERY = "SELECT * FROM name";

  DatabaseDialect databaseDialectMock;
  
  Connection connectionMock;
  
  @Before
  public void init()
  {
    databaseDialectMock = mock(DatabaseDialect.class);
    when(databaseDialectMock.parseTableIdentifier(Matchers.anyString()))
      .thenReturn(new TableId(null,null,TABLE_NAME));	  
    when(databaseDialectMock.expressionBuilder())
      .thenReturn(ExpressionBuilder.create());
    when(databaseDialectMock.criteriaFor(Matchers.any(ColumnId.class), Matchers.anyListOf(ColumnId.class)))
      .thenReturn(new TimestampIncrementingCriteria(new ColumnId(new TableId(null,null,TABLE_NAME),INCREMENTING_COLUMN_NAME), null,null));
	    
    connectionMock = mock(Connection.class);	  
  }
  
  @Test
  public void testTimestampIncrementingTableQuerierInTableModeWithSuffix() throws SQLException {
    TimestampIncrementingTableQuerier querier = new TimestampIncrementingTableQuerier(
                                                    databaseDialectMock, 
                                                    QueryMode.TABLE, 
                                                    TABLE_NAME, 
                                                    null, 
                                                    null,
                                                    INCREMENTING_COLUMN_NAME, 
                                                    null,
                                                    TIMESTAMP_DELAY,
                                                    null,
                                                    SUFFIX,
                                                    JdbcSourceConnectorConfig.TimestampGranularity.CONNECT_LOGICAL
                                                );
      
    querier.createPreparedStatement(connectionMock);

    verify(databaseDialectMock, times(1)).createPreparedStatement(Matchers.any(),Matchers.eq("SELECT * FROM \"name\" WHERE \"name\".\"column\" > ? ORDER BY \"name\".\"column\" ASC /* SUFFIX */"));
  }

  @Test
  public void testTimestampIncrementingTableQuerierInQueryModeWithSuffix() throws SQLException {	    
    TimestampIncrementingTableQuerier querier = new TimestampIncrementingTableQuerier(
                                                    databaseDialectMock, 
                                                    QueryMode.QUERY, 
                                                    QUERY, 
                                                    null, 
                                                    null, 
                                                    INCREMENTING_COLUMN_NAME, 
                                                    null, 
                                                    TIMESTAMP_DELAY, 
                                                    null, 
                                                    SUFFIX,
                                                    JdbcSourceConnectorConfig.TimestampGranularity.CONNECT_LOGICAL
                                                );
      
    querier.createPreparedStatement(connectionMock);

    verify(databaseDialectMock, times(1)).createPreparedStatement(Matchers.any(),Matchers.eq("SELECT * FROM name WHERE \"name\".\"column\" > ? ORDER BY \"name\".\"column\" ASC /* SUFFIX */"));
  }
  
  @Test
  public void testBulkTableQuerierInTableModeWithSuffix() throws SQLException {	    
    BulkTableQuerier querier = new BulkTableQuerier(
                                   databaseDialectMock,
                                   QueryMode.TABLE, 
                                   TABLE_NAME, 
                                   null, 
                                   SUFFIX
                               );
      
    querier.createPreparedStatement(connectionMock);

    verify(databaseDialectMock, times(1)).createPreparedStatement(Matchers.any(),Matchers.eq("SELECT * FROM \"name\" /* SUFFIX */"));
  }

  @Test
  public void testBulkTableQuerierInQueryModeWithSuffix() throws SQLException {
	BulkTableQuerier querier = new BulkTableQuerier(
                                   databaseDialectMock, 
                                   QueryMode.QUERY,
                                   QUERY, 
                                   null, 
                                   SUFFIX
                               );
      
    querier.createPreparedStatement(connectionMock);

    verify(databaseDialectMock, times(1)).createPreparedStatement(Matchers.any(),Matchers.eq("SELECT * FROM name /* SUFFIX */"));
  }

  @Test
  public void testBulkTableQuerierInQueryModeWithoutSuffix() throws SQLException {
    BulkTableQuerier querier = new BulkTableQuerier(
                                   databaseDialectMock, 
                                   QueryMode.QUERY, 
                                   QUERY, 
                                   null, 
                                   "" /* default value */
                               );
      
    querier.createPreparedStatement(connectionMock);

    verify(databaseDialectMock, times(1)).createPreparedStatement(Matchers.any(),Matchers.eq("SELECT * FROM name"));
  }  
}
