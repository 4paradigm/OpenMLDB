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

import java.sql.Types;
import java.util.Objects;

public class ColumnDefinition {

  /**
   * The nullability of a column.
   */
  public enum Nullability {
    NULL, NOT_NULL, UNKNOWN
  }

  /**
   * The mutability of a column.
   */
  public enum Mutability {
    READ_ONLY, MAYBE_WRITABLE, WRITABLE, UNKNOWN
  }

  private final ColumnId id;
  private final String typeName;
  private final int jdbcType;
  private final int displaySize;
  private final int precision;
  private final int scale;
  private final boolean autoIncremented;
  private final boolean caseSensitive;
  private final boolean searchable;
  private final boolean currency;
  private final boolean signedNumbers;
  private final boolean isPrimaryKey;
  private final Nullability nullability;
  private final Mutability mutability;
  private final String classNameForType;

  public ColumnDefinition(
      ColumnId id,
      int jdbcType,
      String typeName,
      String classNameForType,
      Nullability nullability,
      Mutability mutability,
      int precision,
      int scale,
      boolean signedNumbers,
      int displaySize,
      boolean autoIncremented,
      boolean caseSensitive,
      boolean searchable,
      boolean currency,
      boolean isPrimaryKey
  ) {
    this.id = id;
    this.typeName = typeName;
    this.jdbcType = jdbcType;
    this.displaySize = displaySize;
    this.precision = precision;
    this.scale = scale;
    this.autoIncremented = autoIncremented;
    this.caseSensitive = caseSensitive;
    this.searchable = searchable;
    this.currency = currency;
    this.signedNumbers = signedNumbers;
    this.nullability = nullability != null ? nullability : Nullability.UNKNOWN;
    this.mutability = mutability != null ? mutability : Mutability.MAYBE_WRITABLE;
    this.classNameForType = classNameForType;
    this.isPrimaryKey = isPrimaryKey;
  }


  /**
   * Indicates whether the column is automatically numbered.
   *
   * @return <code>true</code> if so; <code>false</code> otherwise
   */
  public boolean isAutoIncrement() {
    return autoIncremented;
  }

  /**
   * Indicates whether the column's case matters.
   *
   * @return <code>true</code> if so; <code>false</code> otherwise
   */
  public boolean isCaseSensitive() {
    return caseSensitive;
  }

  /**
   * Indicates whether the column can be used in a where clause.
   *
   * @return <code>true</code> if so; <code>false</code> otherwise
   */
  public boolean isSearchable() {
    return searchable;
  }

  /**
   * Indicates whether the column is a cash value.
   *
   * @return <code>true</code> if so; <code>false</code> otherwise
   */
  public boolean isCurrency() {
    return currency;
  }

  /**
   * Indicates whether the column is part of the table's primary key.
   *
   * @return <code>true</code> if so; <code>false</code> otherwise
   */
  public boolean isPrimaryKey() {
    return isPrimaryKey;
  }

  /**
   * Indicates the nullability of values in the column.
   *
   * @return the nullability status of the given column; never null
   */
  public Nullability nullability() {
    return nullability;
  }

  /**
   * Indicates whether values in the column are optional. This is equivalent to calling:
   * <pre>
   *   nullability() == Nullability.NULL || nullability() == Nullability.UNKNOWN
   * </pre>
   *
   * @return <code>true</code> if so; <code>false</code> otherwise
   */
  public boolean isOptional() {
    return nullability == Nullability.NULL || nullability == Nullability.UNKNOWN;
  }

  /**
   * Indicates whether values in the column are signed numbers.
   *
   * @return <code>true</code> if so; <code>false</code> otherwise
   */
  public boolean isSignedNumber() {
    return signedNumbers;
  }

  /**
   * Indicates the column's normal maximum width in characters.
   *
   * @return the normal maximum number of characters allowed as the width of the designated column
   */
  public int displaySize() {
    return displaySize;
  }

  /**
   * Get the column's identifier.
   *
   * @return column identifier; never null
   */
  public ColumnId id() {
    return id;
  }

  /**
   * Get the column's table identifier.
   *
   * @return the table identifier; never null
   */
  public TableId tableId() {
    return id.tableId();
  }

  /**
   * Get the column's specified column size. For numeric data, this is the maximum precision.  For
   * character data, this is the length in characters. For datetime datatypes, this is the length in
   * characters of the String representation (assuming the maximum allowed precision of the
   * fractional seconds component). For binary data, this is the length in bytes. For the ROWID
   * datatype, this is the length in bytes. 0 is returned for data types where the column size is
   * not applicable.
   *
   * @return precision
   */
  public int precision() {
    return precision;
  }

  /**
   * Gets the column's number of digits to right of the decimal point. 0 is returned for data types
   * where the scale is not applicable.
   *
   * @return scale
   */
  public int scale() {
    return scale;
  }

  /**
   * Retrieves the column's JDBC type.
   *
   * @return SQL type from java.sql.Types
   * @see Types
   */
  public int type() {
    return jdbcType;
  }

  /**
   * Retrieves the designated column's database-specific type name.
   *
   * @return type name used by the database. If the column type is a user-defined type, then a
   *     fully-qualified type name is returned.
   */
  public String typeName() {
    return typeName;
  }

  /**
   * Indicates whether the designated column is mutable.
   *
   * @return the mutability; never null
   */
  public Mutability mutability() {
    return mutability;
  }

  /**
   * Returns the fully-qualified name of the Java class whose instances are manufactured if the
   * method {@link java.sql.ResultSet#getObject(int)} is called to retrieve a value from the column.
   * {@link java.sql.ResultSet#getObject(int)} may return a subclass of the class returned by this
   * method.
   *
   * @return the fully-qualified name of the class in the Java programming language that would be
   *     used by the method <code>ResultSet.getObject</code> to retrieve the value in the specified
   *     column. This is the class name used for custom mapping.
   */
  public String classNameForType() {
    return classNameForType;
  }


  @Override
  public int hashCode() {
    return id.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (obj instanceof ColumnDefinition) {
      ColumnDefinition that = (ColumnDefinition) obj;
      return Objects.equals(this.id, that.id)
             && this.jdbcType == that.jdbcType
             && this.displaySize == that.displaySize
             && this.precision == that.precision
             && this.scale == that.scale
             && this.autoIncremented == that.autoIncremented
             && this.caseSensitive == that.caseSensitive
             && this.searchable == that.searchable
             && this.currency == that.currency
             && this.signedNumbers == that.signedNumbers
             && this.nullability == that.nullability
             && Objects.equals(this.typeName, that.typeName)
             && Objects.equals(this.classNameForType, that.classNameForType)
             && Objects.equals(this.isPrimaryKey, that.isPrimaryKey);
    }
    return false;
  }

  @Override
  public String toString() {
    return "Column{'" + id.name() + '\'' + ", isPrimaryKey=" + isPrimaryKey + ", allowsNull="
           + isOptional() + ", sqlType=" + typeName + '}';
  }

  /**
   * Obtain a {@link ColumnDefinition} that has all the same characteristics as this column except
   * that belongs to the specified table.
   *
   * @param tableId the new table ID; may not be null
   * @return this definition if the specified table ID matches this definition's {@link #tableId()},
   *     or a new definition that is a copy of this definition except with an {@link #id() ID} that
   *     uses the specified table; never null
   */
  public ColumnDefinition forTable(TableId tableId) {
    if (tableId().equals(tableId)) {
      return this;
    }
    ColumnId newId = new ColumnId(tableId, this.id().name());
    return new ColumnDefinition(newId, jdbcType, typeName, classNameForType, nullability,
                                mutability, precision, scale, signedNumbers, displaySize,
                                autoIncremented, caseSensitive, searchable, currency, isPrimaryKey
    );
  }

  /**
   * Obtain a {@link ColumnDefinition} that has all the same characteristics as this column except
   * that it does or does not belong to the table's primary key
   *
   * @param isPrimaryKey true if the resulting column definition should be part of the table's
   *                     primary key, or false otherwise
   * @return a definition that is the same as this definition except it is or is not part of the
   *     tables primary key, or may be this object if {@link #isPrimaryKey()} already matches the
   *     supplied parameter; never null
   */
  public ColumnDefinition asPartOfPrimaryKey(boolean isPrimaryKey) {
    if (isPrimaryKey == isPrimaryKey()) {
      return this;
    }
    return new ColumnDefinition(id, jdbcType, typeName, classNameForType, nullability, mutability,
                                precision, scale, signedNumbers, displaySize, autoIncremented,
                                caseSensitive, searchable, currency, isPrimaryKey
    );
  }
}
