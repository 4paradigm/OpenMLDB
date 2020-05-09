package com._4paradigm.fesql.offline.utils

import com._4paradigm.fesql.node.{ColumnRefNode, ExprNode, ExprType}
import com._4paradigm.fesql.offline.{FeSQLException, PlanContext}
import org.apache.spark.sql.{Column, DataFrame, Row}


object SparkColumnUtil {

  def resolveColumn(expr: ExprNode,
                    dataFrame: DataFrame,
                    ctx: PlanContext
                   ): (String, Int, Column) = {
    expr.GetExprType match {
      case ExprType.kExprColumnRef =>
        val colNode = ColumnRefNode.CastFrom(expr)
        val colName = colNode.GetColumnName()
        val tblName = colNode.GetRelationName()

        val fullName = tblName + "." + colName

        val cols = dataFrame.columns
        if (cols.contains(colName)) {
          (colName, cols.indexOf(colName), dataFrame.col(colName))

        } else if (cols.contains(fullName)) {
          (fullName, cols.indexOf(fullName), dataFrame.col("`" + fullName + "`"))

        } else {
          throw new FeSQLException(s"Unknown column: $colName")
        }

      case _ => throw new FeSQLException(
        s"Unknown expression: ${expr.GetExprString()}")
    }
  }

  def getCol(dataFrame: DataFrame, index: Int): Column = {
    new Column(dataFrame.queryExecution.analyzed.output(index))
  }
}
