/*
 * PercentileApprox.scala
 * Copyright 2021 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com._4paradigm.fesql.spark.udf

import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.aggregate.ApproximatePercentile


object PercentileApprox {
  def percentile_approx(col: Column, percentage: Column, accuracy: Column): Column = {
    val expr = new ApproximatePercentile(
      col.expr,  percentage.expr, accuracy.expr
    ).toAggregateExpression
    new Column(expr)
  }
  def percentile_approx(col: Column, percentage: Column): Column = percentile_approx(
    col, percentage, lit(ApproximatePercentile.DEFAULT_PERCENTILE_ACCURACY)
  )

  /**
   *
   * @param col
   * @param percentage
   * @param accu
   * @return
   */
  def percentile_approx(col: Column, percentage: Column, accu: Int): Column = {
    if (accu > 0) {
      percentile_approx(col, percentage, lit(accu))
    } else {
      percentile_approx(col, percentage, lit(ApproximatePercentile.DEFAULT_PERCENTILE_ACCURACY))
    }
  }
}
