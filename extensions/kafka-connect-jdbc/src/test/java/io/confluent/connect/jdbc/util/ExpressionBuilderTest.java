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

import java.util.function.Consumer;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class ExpressionBuilderTest {

  protected static final TableId TABLE_ID_T1 = new TableId("c", "s", "t1");
  protected static final ColumnId COLUMN_ID_T1_A = new ColumnId(TABLE_ID_T1, "c1");

  protected static final TableId TABLE_ID_T2 = new TableId(null, null, "t2");
  protected static final ColumnId COLUMN_ID_T2_A = new ColumnId(TABLE_ID_T2, "c2");

  private IdentifierRules rules;
  private QuoteMethod quoteIdentifiers;

  @Before
  public void setup() {
    rules = null;
    quoteIdentifiers = QuoteMethod.ALWAYS;
  }

  @Test
  public void shouldQuoteTableNamesByDefault() {
    assertExpression("\"c\".\"s\".\"t1\"", b-> b.append(TABLE_ID_T1));
    assertExpression("\"t1\"", b-> b.appendTableName(TABLE_ID_T1.tableName()));

    assertExpression("\"t2\"", b-> b.append(TABLE_ID_T2));
    assertExpression("\"t2\"", b-> b.appendTableName(TABLE_ID_T2.tableName()));
  }

  @Test
  public void shouldQuoteTableNamesOnlyWhenSet() {
    quoteIdentifiers = QuoteMethod.ALWAYS;
    assertExpression("\"c\".\"s\".\"t1\"", b-> b.append(TABLE_ID_T1));
    assertExpression("\"t1\"", b-> b.appendTableName(TABLE_ID_T1.tableName()));

    assertExpression("\"t2\"", b-> b.append(TABLE_ID_T2));
    assertExpression("\"t2\"", b-> b.appendTableName(TABLE_ID_T2.tableName()));

    quoteIdentifiers = QuoteMethod.NEVER;
    assertExpression("c.s.t1", b-> b.append(TABLE_ID_T1));
    assertExpression("t1", b-> b.appendTableName(TABLE_ID_T1.tableName()));

    assertExpression("t2", b-> b.append(TABLE_ID_T2));
    assertExpression("t2", b-> b.appendTableName(TABLE_ID_T2.tableName()));
  }

  @Test
  public void shouldQuoteTableNamesAndColumnNamesByDefault() {
    assertExpression("\"c\".\"s\".\"t1\".\"c1\"", b-> b.append(COLUMN_ID_T1_A));
    assertExpression("\"t2\".\"c2\"", b-> b.append(COLUMN_ID_T2_A));
  }

  @Test
  public void shouldQuoteTableNamesAndColumnNamesOnlyWhenSet() {
    quoteIdentifiers = QuoteMethod.ALWAYS;
    assertExpression("\"c\".\"s\".\"t1\".\"c1\"", b-> b.append(COLUMN_ID_T1_A));
    assertExpression("\"t2\".\"c2\"", b-> b.append(COLUMN_ID_T2_A));

    quoteIdentifiers = QuoteMethod.NEVER;
    assertExpression("c.s.t1.c1", b-> b.append(COLUMN_ID_T1_A));
    assertExpression("t2.c2", b-> b.append(COLUMN_ID_T2_A));
  }

  @Test
  public void shouldQuoteColumnNamesWhenSet() {
    quoteIdentifiers = QuoteMethod.ALWAYS;
    assertExpression("\"c1\"", b-> b.appendColumnName(COLUMN_ID_T1_A.name()));
    assertExpression("\"c2\"", b-> b.appendColumnName(COLUMN_ID_T2_A.name()));

    quoteIdentifiers = QuoteMethod.NEVER;
    assertExpression("c1", b-> b.appendColumnName(COLUMN_ID_T1_A.name()));
    assertExpression("c2", b-> b.appendColumnName(COLUMN_ID_T2_A.name()));
  }

  protected void assertExpression(String expected, Consumer<ExpressionBuilder> builderFunction) {
    ExpressionBuilder builder = builderWith(rules);
    builderFunction.accept(builder);
    String actual = builder.toString();
    assertEquals(expected, actual);
  }

  protected ExpressionBuilder builderWith(IdentifierRules rules) {
    ExpressionBuilder result = new ExpressionBuilder(rules);
    result.setQuoteIdentifiers(quoteIdentifiers);
    return result;
  }

}