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
