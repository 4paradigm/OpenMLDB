package com._4paradigm.fesql.offline.utils

import com._4paradigm.fesql.node.{ColumnRefNode, ExprNode, ExprType}
import com._4paradigm.fesql.offline.{FeSQLException, PlanContext}
import org.apache.spark.sql.{Column, DataFrame, Row}


object SparkColumnUtil {

  def resolve(expr: ExprNode, dataFrame: DataFrame, ctx: PlanContext): Column = {
    expr.GetExprType match {
      case ExprType.kExprColumnRef =>
        val colName = ColumnRefNode.CastFrom(expr).GetColumnName()
        dataFrame.col(colName)

      case _ => throw new FeSQLException(
        s"Unknown expression: ${expr.GetExprString()}")
    }
  }

  def resolveColName(expr: ExprNode, dataFrame: DataFrame, ctx: PlanContext): String = {
    expr.GetExprType match {
      case ExprType.kExprColumnRef =>
        ColumnRefNode.CastFrom(expr).GetColumnName()

      case _ => throw new FeSQLException(
        s"Unknown expression: ${expr.GetExprString()}")
    }
  }

  def getCol(dataFrame: DataFrame, index: Int): Column = {
    new Column(dataFrame.queryExecution.analyzed.output(index))
  }
}
