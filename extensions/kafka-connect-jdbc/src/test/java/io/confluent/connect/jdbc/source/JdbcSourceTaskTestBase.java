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

import io.confluent.connect.jdbc.util.TableId;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.powermock.api.easymock.annotation.Mock;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

import static io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig.NumericMapping;

public class JdbcSourceTaskTestBase {

  protected static String SINGLE_TABLE_NAME = "test";
  protected static TableId SINGLE_TABLE_ID = new TableId(null, null, SINGLE_TABLE_NAME);
  protected static Map<String, String> SINGLE_TABLE_PARTITION =
      OffsetProtocols.sourcePartitionForProtocolV0(SINGLE_TABLE_ID);
  protected static Map<String, String> SINGLE_TABLE_PARTITION_WITH_VERSION =
      OffsetProtocols.sourcePartitionForProtocolV1(SINGLE_TABLE_ID);

  protected static EmbeddedDerby.TableName SINGLE_TABLE
      = new EmbeddedDerby.TableName(SINGLE_TABLE_NAME);

  protected static String SECOND_TABLE_NAME = "test2";
  protected static Map<String, Object> SECOND_TABLE_PARTITION = new HashMap<>();

  static {
    SECOND_TABLE_PARTITION.put(JdbcSourceConnectorConstants.TABLE_NAME_KEY, SECOND_TABLE_NAME);
  }

  protected static EmbeddedDerby.TableName SECOND_TABLE
      = new EmbeddedDerby.TableName(SECOND_TABLE_NAME);

  protected static String JOIN_TABLE_NAME = "users";
  protected static Map<String, Object> JOIN_QUERY_PARTITION = new HashMap<>();

  static {
    JOIN_QUERY_PARTITION.put(JdbcSourceConnectorConstants.QUERY_NAME_KEY,
                             JdbcSourceConnectorConstants.QUERY_NAME_VALUE);
  }
  protected static EmbeddedDerby.TableName JOIN_TABLE
      = new EmbeddedDerby.TableName(JOIN_TABLE_NAME);

  protected static final String TOPIC_PREFIX = "test-";

  protected Time time;
  @Mock
  protected SourceTaskContext taskContext;
  protected JdbcSourceTask task;
  protected EmbeddedDerby db;
  @Mock
  private OffsetStorageReader reader;

  @Before
  public void setup() throws Exception {
    time = new MockTime();
    task = new JdbcSourceTask(time);
    db = new EmbeddedDerby();
  }

  @After
  public void tearDown() throws Exception {
    db.close();
    db.dropDatabase();
  }

  protected Map<String, String> singleTableConfig() {
    return singleTableConfig(false);
  }

  protected Map<String, String> singleTableConfig(boolean completeMapping) {
    Map<String, String> props = new HashMap<>();
    props.put(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, db.getUrl());
    props.put(JdbcSourceTaskConfig.TABLES_CONFIG, SINGLE_TABLE_NAME);
    props.put(JdbcSourceConnectorConfig.MODE_CONFIG, JdbcSourceConnectorConfig.MODE_BULK);
    props.put(JdbcSourceTaskConfig.TOPIC_PREFIX_CONFIG, TOPIC_PREFIX);
    if (completeMapping) {
      props.put(JdbcSourceTaskConfig.NUMERIC_MAPPING_CONFIG, NumericMapping.BEST_FIT.toString());
    } else {
      props.put(JdbcSourceTaskConfig.NUMERIC_PRECISION_MAPPING_CONFIG, "true");
    }
    return props;
  }

  protected Map<String, String> singleTableWithTimezoneConfig(
      boolean completeMapping,
      TimeZone tz) {
    Map<String, String> props = singleTableConfig(completeMapping);
    props.put(JdbcSourceTaskConfig.DB_TIMEZONE_CONFIG, tz.getID());
    return props;
  }

  protected Map<String, String> twoTableConfig() {
    Map<String, String> props = new HashMap<>();
    props.put(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, db.getUrl());
    props.put(JdbcSourceTaskConfig.TABLES_CONFIG, SINGLE_TABLE_NAME + "," + SECOND_TABLE_NAME);
    props.put(JdbcSourceConnectorConfig.MODE_CONFIG, JdbcSourceConnectorConfig.MODE_BULK);
    props.put(JdbcSourceTaskConfig.TOPIC_PREFIX_CONFIG, TOPIC_PREFIX);
    return props;
  }

  protected <T> void expectInitialize(Collection<Map<String, T>> partitions, Map<Map<String, T>, Map<String, Object>> offsets) {
    EasyMock.expect(taskContext.offsetStorageReader()).andReturn(reader);
    EasyMock.expect(reader.offsets(EasyMock.eq(partitions))).andReturn(offsets);
  }

  protected <T> void expectInitializeNoOffsets(Collection<Map<String, T>> partitions) {
    Map<Map<String, T>, Map<String, Object>> offsets = new HashMap<>();

    for(Map<String, T> partition : partitions) {
      offsets.put(partition, null);
    }
    expectInitialize(partitions, offsets);
  }

  protected void initializeTask() {
    task.initialize(taskContext);
  }

}
