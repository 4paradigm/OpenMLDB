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

package io.confluent.connect.jdbc.dialect;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.confluent.connect.jdbc.source.ColumnMapping;
import io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig;
import io.confluent.connect.jdbc.util.ColumnDefinition;
import io.confluent.connect.jdbc.util.ColumnId;
import io.confluent.connect.jdbc.util.TableId;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;
import org.mockito.Mock;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public abstract class BaseDialectTypeTest<T extends GenericDatabaseDialect> {

  public static final boolean NULLABLE = true;
  public static final boolean NOT_NULLABLE = false;

  public static final TableId TABLE_ID = new TableId(null, null, "MyTable");
  public static final ColumnId COLUMN_ID = new ColumnId(TABLE_ID, "columnA", "aliasA");

  public static final BigDecimal BIG_DECIMAL = new BigDecimal(9.9);
  public static final long LONG = Long.MAX_VALUE;
  public static final int INT = Integer.MAX_VALUE;
  public static final short SHORT = Short.MAX_VALUE;
  public static final byte BYTE = Byte.MAX_VALUE;
  public static final double DOUBLE = Double.MAX_VALUE;

  @Parameterized.Parameter(0)
  public Schema.Type expectedType;

  @Parameterized.Parameter(1)
  public Object expectedValue;

  @Parameterized.Parameter(2)
  public JdbcSourceConnectorConfig.NumericMapping numMapping;

  @Parameterized.Parameter(3)
  public boolean optional;

  @Parameterized.Parameter(4)
  public int columnType;

  @Parameterized.Parameter(5)
  public int precision;

  @Parameterized.Parameter(6)
  public int scale;

  @Mock
  ResultSet resultSet = mock(ResultSet.class);

  @Mock
  ColumnDefinition columnDefn = mock(ColumnDefinition.class);

  protected boolean signed = true;
  protected T dialect;
  protected SchemaBuilder schemaBuilder;
  protected DatabaseDialect.ColumnConverter converter;

  @Before
  public void setup() throws Exception {
    dialect = createDialect();
  }

  @SuppressWarnings("deprecation")
  @Test
  public void testValueConversion() throws Exception {
    when(columnDefn.precision()).thenReturn(precision);
    when(columnDefn.scale()).thenReturn(scale);
    when(columnDefn.type()).thenReturn(columnType);
    when(columnDefn.isOptional()).thenReturn(optional);
    when(columnDefn.id()).thenReturn(COLUMN_ID);
    when(columnDefn.isSignedNumber()).thenReturn(signed);
    when(columnDefn.typeName()).thenReturn("parameterizedType");

    dialect = createDialect();
    schemaBuilder = SchemaBuilder.struct();

    // Check the schema field is created with the right type
    dialect.addFieldToSchema(columnDefn, schemaBuilder);
    Schema schema = schemaBuilder.build();
    List<Field> fields = schema.fields();
    assertEquals(1, fields.size());
    Field field = fields.get(0);
    assertEquals(expectedType, field.schema().type());

    // Set up the ResultSet
    when(resultSet.getBigDecimal(1, scale)).thenReturn(BIG_DECIMAL);
    when(resultSet.getBigDecimal(1, -scale)).thenReturn(BIG_DECIMAL);
    when(resultSet.getBigDecimal(1)).thenReturn(BIG_DECIMAL);
    when(resultSet.getLong(1)).thenReturn(LONG);
    when(resultSet.getInt(1)).thenReturn(INT);
    when(resultSet.getShort(1)).thenReturn(SHORT);
    when(resultSet.getByte(1)).thenReturn(BYTE);
    when(resultSet.getDouble(1)).thenReturn(DOUBLE);

    if (expectedValue instanceof String) {
      when(resultSet.getString(1)).thenReturn((String)expectedValue);
    }

    // For BigDecimal, ensure we're adding precision and scale as schema parameters
    // Special case: scale of -127 (NUMERIC_TYPE_SCALE_UNSET) indicates scale is unset, which is overridden to 127
    if (expectedValue instanceof BigDecimal) {
      assertEquals(precision,
          Integer.parseInt(field.schema().parameters().get("connect.decimal.precision")));

      if (scale != -127) {
        assertEquals(scale,
            Integer.parseInt(field.schema().parameters().get("scale")));
      } else {
        assertEquals(127,
            Integer.parseInt(field.schema().parameters().get("scale")));
      }
    }

    // Check the converter operates correctly
    ColumnMapping mapping = new ColumnMapping(columnDefn, 1, field);
    converter = dialect.columnConverterFor(
        mapping,
        mapping.columnDefn(),
        mapping.columnNumber(),
        true
    );
    Object value = converter.convert(resultSet);

    if (value instanceof Number && expectedValue instanceof Number) {
      assertEquals(((Number) expectedValue).floatValue(), ((Number) value).floatValue(), 0.01d);
    } else {
      assertEquals(expectedValue, value);
    }
  }

  /**
   * Create an instance of the dialect to be tested.
   *
   * @return the dialect; may not be null
   */
  protected abstract T createDialect();

  /**
   * Create a {@link JdbcSourceConnectorConfig} with the specified URL and optional config props.
   *
   * @param url           the database URL; may not be null
   * @param propertyPairs optional set of config name-value pairs; must be an even number
   * @return the config; never null
   */
  protected JdbcSourceConnectorConfig sourceConfigWithUrl(
      String url,
      String... propertyPairs
  ) {
    Map<String, String> connProps = new HashMap<>();
    connProps.put(JdbcSourceConnectorConfig.MODE_CONFIG, JdbcSourceConnectorConfig.MODE_BULK);
    connProps.put(JdbcSourceConnectorConfig.TOPIC_PREFIX_CONFIG, "test-");
    connProps.putAll(propertiesFromPairs(propertyPairs));
    connProps.put(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, url);
    connProps.put(JdbcSourceConnectorConfig.NUMERIC_MAPPING_CONFIG, numMapping.toString());
    return new JdbcSourceConnectorConfig(connProps);
  }

  protected Map<String, String> propertiesFromPairs(String... pairs) {
    Map<String, String> props = new HashMap<>();
    assertEquals("Expecting even number of properties but found " + pairs.length, 0,
                 pairs.length % 2);
    for (int i = 0; i != pairs.length; ++i) {
      String key = pairs[i];
      String value = pairs[++i];
      props.put(key, value);
    }
    return props;
  }
}
