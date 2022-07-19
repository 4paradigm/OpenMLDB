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

import java.sql.JDBCType;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class ColumnDefinitionBuilder {

  private String columnName;
  private String typeName;
  private int jdbcType = JDBCType.INTEGER.ordinal();
  private int displaySize;
  private int precision = 10;
  private int scale = 0;
  private boolean autoIncremented = false;
  private boolean caseSensitive = false;
  private boolean searchable = true;
  private boolean currency = false;
  private boolean signedNumbers = false;
  private boolean isPrimaryKey = false;
  private ColumnDefinition.Nullability nullability = ColumnDefinition.Nullability.NULL;
  private ColumnDefinition.Mutability mutability = ColumnDefinition.Mutability.WRITABLE;
  private String classNameForType;

  public ColumnDefinitionBuilder(String name) {
    this.columnName = name;
  }

  public ColumnDefinitionBuilder name(String columnName) {
    this.columnName = columnName;
    return this;
  }

  public ColumnDefinitionBuilder type(String typeName, JDBCType jdbcType, Class<?> clazz) {
    typeName(typeName);
    jdbcType(jdbcType);
    classNameForType(clazz != null ? clazz.getName() : null);
    return this;
  }

  public ColumnDefinitionBuilder typeName(String typeName) {
    this.typeName = typeName;
    return this;
  }

  public ColumnDefinitionBuilder jdbcType(JDBCType type) {
    this.jdbcType = type.ordinal();
    return this;
  }

  public ColumnDefinitionBuilder classNameForType(String classNameForType) {
    this.classNameForType = classNameForType;
    return this;
  }

  public ColumnDefinitionBuilder displaySize(int size) {
    this.displaySize = size;
    return this;
  }

  public ColumnDefinitionBuilder precision(int precision) {
    this.precision = precision;
    return this;
  }

  public ColumnDefinitionBuilder scale(int scale) {
    this.scale = scale;
    return this;
  }

  public ColumnDefinitionBuilder autoIncremented(boolean autoIncremented) {
    this.autoIncremented = autoIncremented;
    return this;
  }

  public ColumnDefinitionBuilder caseSensitive(boolean caseSensitive) {
    this.caseSensitive = caseSensitive;
    return this;
  }

  public ColumnDefinitionBuilder searchable(boolean searchable) {
    this.searchable = searchable;
    return this;
  }

  public ColumnDefinitionBuilder currency(boolean currency) {
    this.currency = currency;
    return this;
  }

  public ColumnDefinitionBuilder signedNumbers(boolean signedNumbers) {
    this.signedNumbers = signedNumbers;
    return this;
  }

  public ColumnDefinitionBuilder primaryKey(boolean isPrimaryKey) {
    this.isPrimaryKey = isPrimaryKey;
    return this;
  }

  public ColumnDefinitionBuilder nullable(boolean nullable) {
    return nullability(
        nullable ? ColumnDefinition.Nullability.NULL : ColumnDefinition.Nullability.NOT_NULL
    );
  }

  public ColumnDefinitionBuilder nullability(ColumnDefinition.Nullability nullability) {
    this.nullability = nullability;
    return this;
  }

  public ColumnDefinitionBuilder mutability(ColumnDefinition.Mutability mutability) {
    this.mutability = mutability;
    return this;
  }

  public ColumnDefinition buildFor(TableId tableId) {
    return new ColumnDefinition(
        new ColumnId(tableId, columnName),
        jdbcType,
        typeName,
        classNameForType,
        nullability,
        mutability,
        precision,
        scale,
        signedNumbers,
        displaySize,
        autoIncremented,
        caseSensitive,
        searchable,
        currency,
        isPrimaryKey
    );
  }

}
