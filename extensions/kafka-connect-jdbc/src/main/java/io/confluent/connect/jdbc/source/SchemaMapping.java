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
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.dialect.DatabaseDialect.ColumnConverter;
import io.confluent.connect.jdbc.util.ColumnDefinition;
import io.confluent.connect.jdbc.util.ColumnId;

/**
 * A mapping from a result set into a {@link Schema}. This mapping contains an array of {@link
 * FieldSetter} functions (one for each column in the result set), and the caller should iterate
 * over these and call the function with the result set.
 *
 * <p>This mapping contains the {@link ColumnConverter} functions that should be called for each row
 * in the result set. and these are exposed to users of this class via the {@link FieldSetter}
 * function.
 */
public final class SchemaMapping {

  /**
   * Convert the result set into a {@link Schema}.
   *
   * @param schemaName the name of the schema; may be null
   * @param metadata   the result set metadata; never null
   * @param dialect    the dialect for the source database; never null
   * @return the schema mapping; never null
   * @throws SQLException if there is a problem accessing the result set metadata
   */
  public static SchemaMapping create(
      String schemaName,
      ResultSetMetaData metadata,
      DatabaseDialect dialect
  ) throws SQLException {
    Map<ColumnId, ColumnDefinition> colDefns = dialect.describeColumns(metadata);
    Map<String, ColumnConverter> colConvertersByFieldName = new LinkedHashMap<>();
    SchemaBuilder builder = SchemaBuilder.struct().name(schemaName);
    int columnNumber = 0;
    for (ColumnDefinition colDefn : colDefns.values()) {
      ++columnNumber;
      String fieldName = dialect.addFieldToSchema(colDefn, builder);
      if (fieldName == null) {
        continue;
      }
      Field field = builder.field(fieldName);
      ColumnMapping mapping = new ColumnMapping(colDefn, columnNumber, field);
      ColumnConverter converter = dialect.createColumnConverter(mapping);
      colConvertersByFieldName.put(fieldName, converter);
    }
    Schema schema = builder.build();
    return new SchemaMapping(schema, colConvertersByFieldName);
  }

  private final Schema schema;
  private final List<FieldSetter> fieldSetters;

  private SchemaMapping(
      Schema schema,
      Map<String, ColumnConverter> convertersByFieldName
  ) {
    assert schema != null;
    assert convertersByFieldName != null;
    assert !convertersByFieldName.isEmpty();
    this.schema = schema;
    List<FieldSetter> fieldSetters = new ArrayList<>(convertersByFieldName.size());
    for (Map.Entry<String, ColumnConverter> entry : convertersByFieldName.entrySet()) {
      ColumnConverter converter = entry.getValue();
      Field field = schema.field(entry.getKey());
      assert field != null;
      fieldSetters.add(new FieldSetter(converter, field));
    }
    this.fieldSetters = Collections.unmodifiableList(fieldSetters);
  }

  public Schema schema() {
    return schema;
  }

  /**
   * Get the {@link FieldSetter} functions, which contain one for each result set column whose
   * values are to be mapped/converted and then set on the corresponding {@link Field} in supplied
   * {@link Struct} objects.
   *
   * @return the array of {@link FieldSetter} instances; never null and never empty
   */
  List<FieldSetter> fieldSetters() {
    return fieldSetters;
  }

  @Override
  public String toString() {
    return "Mapping for " + schema.name();
  }

  public static final class FieldSetter {

    private final ColumnConverter converter;
    private final Field field;

    private FieldSetter(
        ColumnConverter converter,
        Field field
    ) {
      this.converter = converter;
      this.field = field;
    }

    /**
     * Get the {@link Field} that this setter function sets.
     *
     * @return the field; never null
     */
    public Field field() {
      return field;
    }

    /**
     * Call the {@link ColumnConverter converter} on the supplied {@link ResultSet} and set the
     * corresponding {@link #field() field} on the supplied {@link Struct}.
     *
     * @param struct    the struct whose field is to be set with the converted value from the result
     *                  set; may not be null
     * @param resultSet the result set positioned at the row to be processed; may not be null
     * @throws SQLException if there is an error accessing the result set
     * @throws IOException  if there is an error accessing a streaming value from the result set
     */
    void setField(
        Struct struct,
        ResultSet resultSet
    ) throws SQLException, IOException {
      Object value = this.converter.convert(resultSet);
      if (resultSet.wasNull()) {
        struct.put(field, null);
      } else {
        struct.put(field, value);
      }
    }

    @Override
    public String toString() {
      return field.name();
    }
  }
}
