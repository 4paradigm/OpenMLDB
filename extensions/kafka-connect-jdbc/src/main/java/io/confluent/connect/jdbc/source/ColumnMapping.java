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

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

import java.sql.ResultSet;
import java.util.Objects;

import io.confluent.connect.jdbc.util.ColumnDefinition;

/**
 * A mapping of a {@link ColumnDefinition result set column definition} and a {@link Field} within a
 * {@link Schema}.
 */
public class ColumnMapping {

  private final Field field;
  private final ColumnDefinition columnDefn;
  private final int columnNumber;
  private final int hash;

  /**
   * Create the column mapping.
   *
   * @param columnDefn   the definition of the column; may not be null
   * @param columnNumber the 1-based number of the column within the result set; must be positive
   * @param field        the corresponding {@link Field} within the {@link Schema}; may not be null
   */
  public ColumnMapping(
      ColumnDefinition columnDefn,
      int columnNumber,
      Field field
  ) {
    assert columnDefn != null;
    assert field != null;
    assert columnNumber > 0;
    this.columnDefn = columnDefn;
    this.field = field;
    this.columnNumber = columnNumber;
    this.hash = Objects.hash(this.columnNumber, this.columnDefn, this.field);
  }

  /**
   * Get this mapping's {@link Field}.
   *
   * @return the field; never null
   */
  public Field field() {
    return field;
  }

  /**
   * Get this mapping's {@link ColumnDefinition result set column definition}.
   *
   * @return the column definition; never null
   */
  public ColumnDefinition columnDefn() {
    return columnDefn;
  }

  /**
   * Get the 1-based number of the column within the result set. This can be used to access the
   * corresponding value from the {@link ResultSet}.
   *
   * @return the column number within the {@link ResultSet}; always positive
   */
  public int columnNumber() {
    return columnNumber;
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
    if (obj instanceof ColumnMapping) {
      ColumnMapping that = (ColumnMapping) obj;
      return this.columnNumber == that.columnNumber && Objects.equals(
          this.columnDefn, that.columnDefn) && Objects.equals(this.field, that.field);
    }
    return false;
  }

  @Override
  public String toString() {
    return field.name() + " (col=" + columnNumber + ", " + columnDefn + ")";
  }
}
