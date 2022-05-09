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

package io.confluent.connect.jdbc.util;

import java.util.Collection;
import java.util.EnumSet;
import java.util.Set;
import java.util.stream.Collectors;

public enum TableType {

  TABLE("TABLE", "Table"), VIEW("VIEW", "View");

  private final String value;
  private final String capitalCase;

  TableType(String value, String capitalCase) {
    this.value = value.toUpperCase();
    this.capitalCase = capitalCase;
  }

  public String capitalized() {
    return capitalCase;
  }

  public String jdbcName() {
    return value;
  }

  @Override
  public String toString() {
    return value;
  }

  public static TableType get(String name) {
    if (name != null) {
      name = name.trim();
    }
    for (TableType method : values()) {
      if (method.toString().equalsIgnoreCase(name)) {
        return method;
      }
    }
    throw new IllegalArgumentException("No matching QuoteMethod found for '" + name + "'");
  }

  public static EnumSet<TableType> parse(Collection<String> values) {
    Set<TableType> types = values.stream().map(TableType::get).collect(Collectors.toSet());
    return EnumSet.copyOf(types);
  }

  public static String asJdbcTableTypeNames(EnumSet<TableType> types, String delim) {
    return types.stream()
                .map(TableType::jdbcName)
                .sorted()
                .collect(Collectors.joining(delim));
  }

  public static String[] asJdbcTableTypeArray(EnumSet<TableType> types) {
    return types.stream()
                .map(TableType::jdbcName)
                .sorted()
                .collect(Collectors.toList())
                .toArray(new String[types.size()]);
  }

}