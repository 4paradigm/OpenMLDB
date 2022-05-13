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

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class TableDefinitionBuilder {

  private TableId tableId;
  private Map<String, ColumnDefinitionBuilder> columnBuilders = new HashMap<>();

  public TableDefinitionBuilder withTable(String tableName) {
    tableId = new TableId(null, null, tableName);
    return this;
  }

  public ColumnDefinitionBuilder withColumn(String columnName) {
    return columnBuilders.computeIfAbsent(columnName, ColumnDefinitionBuilder::new);
  }

  public TableDefinition build() {
    return new TableDefinition(
        tableId,
        columnBuilders.values().stream().map(b -> b.buildFor(tableId)).collect(Collectors.toList())
    );
  }

}
