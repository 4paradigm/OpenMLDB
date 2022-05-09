/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.connect.jdbc.util;

import io.confluent.connect.jdbc.sink.JdbcSinkConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.confluent.connect.jdbc.sink.JdbcSinkConfig.DELETE_ENABLED;

public class PrimaryKeyModeRecommender implements ConfigDef.Recommender {

  public static final PrimaryKeyModeRecommender INSTANCE = new PrimaryKeyModeRecommender();

  private static final List<Object> ALL_VALUES =
      Arrays.stream(JdbcSinkConfig.PrimaryKeyMode.values())
          .map(mode -> mode.name().toLowerCase())
          .collect(Collectors.toList());
  private static final List<Object> RECORD_KEY_ONLY =
      Collections.singletonList(JdbcSinkConfig.PrimaryKeyMode.RECORD_KEY.name().toLowerCase());

  @Override
  public List<Object> validValues(final String name, final Map<String, Object> parsedConfig) {
    final boolean deleteEnabled = (Boolean) parsedConfig.getOrDefault(DELETE_ENABLED, false);
    return deleteEnabled ? RECORD_KEY_ONLY : ALL_VALUES;
  }

  @Override
  public boolean visible(final String name, final Map<String, Object> parsedConfig) {
    return true;
  }

}
