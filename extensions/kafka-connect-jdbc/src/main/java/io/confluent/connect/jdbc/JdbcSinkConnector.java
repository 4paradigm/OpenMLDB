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

import static io.confluent.connect.jdbc.sink.JdbcSinkConfig.DELETE_ENABLED;
import static io.confluent.connect.jdbc.sink.JdbcSinkConfig.PK_MODE;
import static io.confluent.connect.jdbc.sink.JdbcSinkConfig.PrimaryKeyMode.RECORD_KEY;

import java.util.Locale;
import java.util.Optional;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import io.confluent.connect.jdbc.sink.JdbcSinkConfig;
import io.confluent.connect.jdbc.sink.JdbcSinkTask;
import io.confluent.connect.jdbc.util.Version;

public class JdbcSinkConnector extends SinkConnector {
  private static final Logger log = LoggerFactory.getLogger(JdbcSinkConnector.class);

  private Map<String, String> configProps;

  public Class<? extends Task> taskClass() {
    return JdbcSinkTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    log.info("Setting task configurations for {} workers.", maxTasks);
    final List<Map<String, String>> configs = new ArrayList<>(maxTasks);
    for (int i = 0; i < maxTasks; ++i) {
      configs.add(configProps);
    }
    return configs;
  }

  @Override
  public void start(Map<String, String> props) {
    configProps = props;
  }

  @Override
  public void stop() {
  }

  @Override
  public ConfigDef config() {
    return JdbcSinkConfig.CONFIG_DEF;
  }

  @Override
  public Config validate(Map<String, String> connectorConfigs) {
    /** get configuration parsed and validated individually */
    Config config = super.validate(connectorConfigs);

    return validateDeleteEnabledPkMode(config);
  }

  private Config validateDeleteEnabledPkMode(Config config) {
    configValue(config, DELETE_ENABLED)
        .filter(deleteEnabled -> Boolean.TRUE.equals(deleteEnabled.value()))
        .ifPresent(deleteEnabled -> configValue(config, PK_MODE)
            .ifPresent(pkMode -> {
              if (!RECORD_KEY.name().toLowerCase(Locale.ROOT).equals(pkMode.value())
                  && !RECORD_KEY.name().toUpperCase(Locale.ROOT).equals(pkMode.value())) {
                String conflictMsg = "Deletes are only supported for pk.mode record_key";
                pkMode.addErrorMessage(conflictMsg);
                deleteEnabled.addErrorMessage(conflictMsg);
              }
            }));
    return config;
  }

  /** only if individual validation passed. */
  private Optional<ConfigValue> configValue(Config config, String name) {
    return config.configValues()
        .stream()
        .filter(cfg -> name.equals(cfg.name())
            && cfg.errorMessages().isEmpty())
        .findFirst();
  }

  @Override
  public String version() {
    return Version.getVersion();
  }
}
