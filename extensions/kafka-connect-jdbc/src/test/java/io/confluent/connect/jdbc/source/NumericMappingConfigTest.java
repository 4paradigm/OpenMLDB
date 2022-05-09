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

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig.NumericMapping;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class NumericMappingConfigTest {
  private Map<String, String> props;

  @Parameterized.Parameters
  public static Iterable<Object[]> mapping() {
    return Arrays.asList(
        new Object[][] {
            {NumericMapping.NONE, false, null},
            {NumericMapping.NONE, false, "none"},
            {NumericMapping.NONE, false, "NONE"},
            {NumericMapping.PRECISION_ONLY, false, "precision_only"},
            {NumericMapping.PRECISION_ONLY, false, "PRECISION_ONLY"},
            {NumericMapping.BEST_FIT, false, "best_fit"},
            {NumericMapping.BEST_FIT, false, "BEST_FIT"},
            {NumericMapping.PRECISION_ONLY, true, null},
            {NumericMapping.NONE, true, "none"},
            {NumericMapping.NONE, true, "NONE"},
            {NumericMapping.PRECISION_ONLY, true, "precision_only"},
            {NumericMapping.PRECISION_ONLY, true, "PRECISION_ONLY"},
            {NumericMapping.BEST_FIT, true, "best_fit"},
            {NumericMapping.BEST_FIT, true, "BEST_FIT"}
        }
    );
  }

  @Parameterized.Parameter(0)
  public NumericMapping expected;

  @Parameterized.Parameter(1)
  public boolean precisionMapping;

  @Parameterized.Parameter(2)
  public String extendedMapping;

  @Before
  public void setup() throws Exception {
    props = new HashMap<>();
  }

  @Test
  public void testNumericMapping() throws Exception {
    props.put(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, "jdbc:foo:bar");
    props.put(JdbcSourceConnectorConfig.MODE_CONFIG, JdbcSourceConnectorConfig.MODE_BULK);
    props.put(JdbcSourceConnectorConfig.TOPIC_PREFIX_CONFIG, "test-");
    props.put(
        JdbcSourceConnectorConfig.NUMERIC_PRECISION_MAPPING_CONFIG,
        String.valueOf(precisionMapping)
    );
    if (extendedMapping != null) {
      props.put(JdbcSourceConnectorConfig.NUMERIC_MAPPING_CONFIG, extendedMapping);
    }
    JdbcSourceConnectorConfig config = new JdbcSourceConnectorConfig(props);
    assertEquals(expected, NumericMapping.get(config));
  }
}
