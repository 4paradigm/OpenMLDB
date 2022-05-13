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

package io.confluent.connect.jdbc.util;

import java.util.Arrays;
import java.util.TimeZone;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

public class TimeZoneValidator implements ConfigDef.Validator {

  public static final TimeZoneValidator INSTANCE = new TimeZoneValidator();

  @Override
  public void ensureValid(String name, Object value) {
    if (value != null) {
      if (!Arrays.asList(TimeZone.getAvailableIDs()).contains(value.toString())) {
        throw new ConfigException(name, value, "Invalid time zone identifier");
      }
    }
  }

  @Override
  public String toString() {
    return "Any valid JDK time zone";
  }
}
