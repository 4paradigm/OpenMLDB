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

import java.util.Objects;

import io.confluent.connect.jdbc.util.ExpressionBuilder.Expressable;

public class TableId implements Comparable<TableId>, Expressable {

  private final String catalogName;
  private final String schemaName;
  private final String tableName;
  private final int hash;

  public TableId(
      String catalogName,
      String schemaName,
      String tableName
  ) {
    this.catalogName = catalogName == null || catalogName.isEmpty() ? null : catalogName;
    this.schemaName = schemaName == null || schemaName.isEmpty() ? null : schemaName;
    this.tableName = tableName;
    this.hash = Objects.hash(catalogName, schemaName, tableName);
  }

  public String catalogName() {
    return catalogName;
  }

  public String schemaName() {
    return schemaName;
  }

  public String tableName() {
    return tableName;
  }

  @Override
  public void appendTo(ExpressionBuilder builder, boolean useQuotes) {
    appendTo(builder, useQuotes ? QuoteMethod.ALWAYS : QuoteMethod.NEVER);
  }

  @Override
  public void appendTo(
      ExpressionBuilder builder,
      QuoteMethod useQuotes
  ) {
    if (catalogName != null) {
      builder.appendIdentifier(catalogName, useQuotes);
      builder.appendIdentifierDelimiter();
    }
    if (schemaName != null) {
      builder.appendIdentifier(schemaName, useQuotes);
      builder.appendIdentifierDelimiter();
    }
    builder.appendTableName(tableName, useQuotes);
  }

  @Override
  public int hashCode() {
    return hash;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (obj instanceof TableId) {
      TableId that = (TableId) obj;
      return Objects.equals(this.catalogName, that.catalogName)
             && Objects.equals(this.schemaName, that.schemaName)
             && Objects.equals(this.tableName, that.tableName);
    }
    return false;
  }

  @Override
  public int compareTo(TableId that) {
    if (that == this) {
      return 0;
    }
    int diff = this.tableName.compareTo(that.tableName);
    if (diff != 0) {
      return diff;
    }
    if (this.schemaName == null) {
      if (that.schemaName != null) {
        return -1;
      }
    } else {
      if (that.schemaName == null) {
        return 1;
      }
      diff = this.schemaName.compareTo(that.schemaName);
      if (diff != 0) {
        return diff;
      }
    }
    if (this.catalogName == null) {
      if (that.catalogName != null) {
        return -1;
      }
    } else {
      if (that.catalogName == null) {
        return 1;
      }
      diff = this.catalogName.compareTo(that.catalogName);
      if (diff != 0) {
        return diff;
      }
    }
    return 0;
  }

  @Override
  public String toString() {
    return ExpressionBuilder.create().append(this).toString();
  }
}
