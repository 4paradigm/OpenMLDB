/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.jdbc.sink;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

import io.confluent.connect.jdbc.util.TableType;

import org.apache.kafka.common.config.ConfigException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class JdbcSinkConfigTest {

  private Map<String, String> props = new HashMap<>();
  private JdbcSinkConfig config;

  @Before
  public void beforeEach() {
    // add the minimum settings only
    props.put("connection.url", "jdbc:mysql://something"); // we won't connect
  }

  @After
  public void afterEach() {
    props.clear();
    config = null;
  }

  @Test(expected = ConfigException.class)
  public void shouldFailToCreateConfigWithoutConnectionUrl() {
    props.remove(JdbcSinkConfig.CONNECTION_URL);
    createConfig();
  }

  @Test(expected = ConfigException.class)
  public void shouldFailToCreateConfigWithEmptyTableNameFormat() {
    props.put(JdbcSinkConfig.TABLE_NAME_FORMAT, "");
    createConfig();
  }

  @Test
  public void shouldCreateConfigWithMinimalConfigs() {
    createConfig();
    assertTableTypes(TableType.TABLE);
  }

  @Test
  public void shouldCreateConfigWithAdditionalConfigs() {
    props.put("auto.create", "true");
    props.put("pk.mode", "kafka");
    props.put("pk.fields", "kafka_topic,kafka_partition,kafka_offset");
    createConfig();
    assertTableTypes(TableType.TABLE);
  }

  @Test
  public void shouldCreateConfigWithViewOnly() {
    props.put("table.types", "view");
    createConfig();
    assertTableTypes(TableType.VIEW);
  }

  @Test
  public void shouldCreateConfigWithTableOnly() {
    props.put("table.types", "table");
    createConfig();
    assertTableTypes(TableType.TABLE);
  }

  @Test
  public void shouldCreateConfigWithViewAndTable() {
    props.put("table.types", "view,table");
    createConfig();
    assertTableTypes(TableType.TABLE, TableType.VIEW);
    props.put("table.types", "table,view");
    createConfig();
    assertTableTypes(TableType.TABLE, TableType.VIEW);
    props.put("table.types", "table , view");
    createConfig();
    assertTableTypes(TableType.TABLE, TableType.VIEW);
  }

  @Test
  public void shouldCreateConfigWithLeadingWhitespaceInTableTypes() {
    props.put("table.types", " \t\n  view");
    createConfig();
    assertTableTypes(TableType.VIEW);
  }

  @Test
  public void shouldCreateConfigWithTrailingWhitespaceInTableTypes() {
    props.put("table.types", "table \t \n");
    createConfig();
    assertTableTypes(TableType.TABLE);
  }

  protected void createConfig() {
    config = new JdbcSinkConfig(props);
  }

  protected void assertTableTypes(TableType...types) {
    EnumSet<TableType> expected = EnumSet.copyOf(Arrays.asList(types));
    EnumSet<TableType> tableTypes = config.tableTypes;
    assertEquals(expected, tableTypes);
  }

}