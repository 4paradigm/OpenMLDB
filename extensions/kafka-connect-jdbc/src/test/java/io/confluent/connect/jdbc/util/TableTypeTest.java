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

import java.util.Arrays;
import java.util.EnumSet;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertArrayEquals;

public class TableTypeTest {

  private static EnumSet<TableType> TABLE_ONLY = types(TableType.TABLE);
  private static EnumSet<TableType> VIEW_ONLY = types(TableType.VIEW);
  private static EnumSet<TableType> TABLE_AND_VIEW = types(TableType.TABLE, TableType.VIEW);

  @Test
  public void shouldParseLowercaseTypes() {
    assertEquals(TableType.TABLE, TableType.get("table"));
    assertEquals(TableType.VIEW, TableType.get("view"));
  }

  @Test
  public void shouldParseUppercaseTypes() {
    assertEquals(TableType.TABLE, TableType.get("TABLE"));
    assertEquals(TableType.VIEW, TableType.get("VIEW"));
  }

  @Test
  public void shouldParseMixedcaseTypes() {
    assertEquals(TableType.TABLE, TableType.get("Table"));
    assertEquals(TableType.VIEW, TableType.get("vIeW"));
  }

  @Test
  public void shouldParseTypeStringWithWhitespace() {
    assertEquals(TableType.TABLE, TableType.get(" table \t"));
    assertEquals(TableType.VIEW, TableType.get("VIEW \t\n "));
  }

  @Test
  public void shouldComputeJdbcTypeArray() {
    assertArrayEquals(array("TABLE"), TableType.asJdbcTableTypeArray(TABLE_ONLY));
    assertArrayEquals(array("VIEW"), TableType.asJdbcTableTypeArray(VIEW_ONLY));
    assertArrayEquals(array("TABLE", "VIEW"), TableType.asJdbcTableTypeArray(TABLE_AND_VIEW));
  }

  @Test
  public void shouldComputeJdbcTypeNames() {
    assertEquals("TABLE", TableType.asJdbcTableTypeNames(TABLE_ONLY, "/"));
    assertEquals("VIEW", TableType.asJdbcTableTypeNames(VIEW_ONLY, "/"));
    assertEquals("TABLE/VIEW", TableType.asJdbcTableTypeNames(TABLE_AND_VIEW, "/"));
  }

  @Test
  public void shouldHaveUpperCaseToString() {
    assertEquals("TABLE", TableType.TABLE.toString());
    assertEquals("VIEW", TableType.VIEW.toString());
  }

  protected static EnumSet<TableType> types(TableType...types) {
    return EnumSet.copyOf(Arrays.asList(types));
  }

  protected static String[] array(String...strs) {
    return strs;
  }

}