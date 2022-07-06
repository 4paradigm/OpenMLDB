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

package io.confluent.connect.jdbc;


import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.common.config.Config;

import static io.confluent.connect.jdbc.sink.JdbcSinkConfig.PK_MODE;
import static java.util.Collections.EMPTY_LIST;
import static java.util.Collections.singletonList;
import static org.junit.Assert.*;

import org.junit.Test;

public class JdbcSinkConnectorTest {

  @Test
  public void testValidationWhenDeleteEnabled() {

    JdbcSinkConnector connector = new JdbcSinkConnector();

    Map<String, String> connConfig = new HashMap<>();
    connConfig.put("connector.class", "io.confluent.connect.jdbc.JdbcSinkConnector");
    connConfig.put("delete.enabled", "true");

    connConfig.put("pk.mode", "record_key");
    assertEquals("'pk.mode must be 'RECORD_KEY/record_key' when 'delete.enabled' == true",
        EMPTY_LIST, configErrors(connector.validate(connConfig), PK_MODE));

    connConfig.put("pk.mode", "RECORD_KEY");
    assertEquals("pk.mode must be 'RECORD_KEY/record_key' when 'delete.enabled' == true",
        EMPTY_LIST, configErrors(connector.validate(connConfig), PK_MODE));

    connConfig.put("pk.mode", "none");

    final String conflictMsg = "Deletes are only supported for pk.mode record_key";

    assertEquals("'record_key' is the only valid mode when 'delete.enabled' == true",
        singletonList(conflictMsg),
        configErrors(connector.validate(connConfig), PK_MODE));
  }

  @Test
  public void testValidationWhenDeleteNotEnabled() {

    JdbcSinkConnector connector = new JdbcSinkConnector();

    Map<String, String> connConfig = new HashMap<>();
    connConfig.put("connector.class", "io.confluent.connect.jdbc.JdbcSinkConnector");
    connConfig.put("delete.enabled", "false");

    connConfig.put("pk.mode", "none");
    assertEquals("any defined mode is valid when 'delete.enabled' == false",
        EMPTY_LIST, configErrors(connector.validate(connConfig), PK_MODE));
  }

  @Test
  public void testValidationWhenPKModeInvalid() {

    JdbcSinkConnector connector = new JdbcSinkConnector();

    Map<String, String> connConfig = new HashMap<>();
    connConfig.put("connector.class", "io.confluent.connect.jdbc.JdbcSinkConnector");
    connConfig.put("delete.enabled", "false");
    connConfig.put("pk.mode", "gibberish");

    assertEquals("no double reporting for unknown pk.mode",
        1, configErrors(connector.validate(connConfig), PK_MODE).size());
  }


  private List<String> configErrors(Config config, String propertyName) {
    return config.configValues()
        .stream()
        .flatMap(cfg -> propertyName.equals(cfg.name()) ?
            cfg.errorMessages().stream() : Stream.empty())
        .collect(Collectors.toList());
  }
}
