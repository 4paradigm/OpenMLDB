/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 **/

package io.confluent.connect.jdbc.dialect;

import io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig;
import org.apache.kafka.connect.data.Schema;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.Types;
import java.util.Arrays;
import java.util.UUID;

import static org.mockito.Mockito.when;

@RunWith(Parameterized.class)
public class PostgreSqlDatabaseDialectTypeTest extends BaseDialectTypeTest<PostgreSqlDatabaseDialect> {

  public static final String UUID_VALUE = "8A52DFE1-CFB9-4C55-B74F-E3D56BBED827";

  @Parameterized.Parameter(7)
  public String classNameForType;

  @Parameterized.Parameters
  public static Iterable<Object[]> mapping() {
    return Arrays.asList(
        new Object[][] {
            // UUID - non optional
            {Schema.Type.STRING, UUID_VALUE, JdbcSourceConnectorConfig.NumericMapping.NONE, NOT_NULLABLE, Types.OTHER, 0, 0, UUID.class.getName() },

            // UUID - optional
            {Schema.Type.STRING, UUID_VALUE, JdbcSourceConnectorConfig.NumericMapping.NONE, NULLABLE, Types.OTHER, 0, 0, UUID.class.getName() },
        }
    );
  }

  @Override
  protected PostgreSqlDatabaseDialect createDialect() {
    return new PostgreSqlDatabaseDialect(sourceConfigWithUrl("jdbc:some:db"));
  }

  @Override
  public void testValueConversion() throws Exception {
    when(columnDefn.classNameForType()).thenReturn(classNameForType);

    super.testValueConversion();
  }
}