/*
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

package com._4paradigm.openmldb.batch.udf

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.aggregate.ApproximatePercentile
import org.apache.spark.sql.functions.lit



object PercentileApprox {
  def percentileApprox(col: Column, percentage: Column, accuracy: Column): Column = {
    val expr = new ApproximatePercentile(
      col.expr,  percentage.expr, accuracy.expr
    ).toAggregateExpression
    new Column(expr)
  }
  def percentileApprox(col: Column, percentage: Column): Column = percentileApprox(
    col, percentage, lit(ApproximatePercentile.DEFAULT_PERCENTILE_ACCURACY)
  )

  /**
   *
   * @param col
   * @param percentage
   * @param accu
   * @return
   */
  def percentileApprox(col: Column, percentage: Column, accu: Int): Column = {
    if (accu > 0) {
      percentileApprox(col, percentage, lit(accu))
    } else {
      percentileApprox(col, percentage, lit(ApproximatePercentile.DEFAULT_PERCENTILE_ACCURACY))
    }
  }
}
