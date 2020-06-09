package com._4paradigm.fesql.offline.utils

import com._4paradigm.fesql.node.{ColumnRefNode, ExprNode, ExprType}
import com._4paradigm.fesql.offline.{FeSQLException, PlanContext}
import com._4paradigm.fesql.vm.{CoreAPI, PhysicalJoinNode, PhysicalOpNode}
import org.apache.spark.sql.{Column, DataFrame}


object SparkColumnUtil {

  def resolveLeftColumn(expr: ExprNode,
                        planNode: PhysicalJoinNode,
                        left: DataFrame,
                        ctx: PlanContext): Column = {
    expr.getExpr_type_ match {
      case ExprType.kExprColumnRef =>
        val index = CoreAPI.ResolveColumnIndex(planNode, ColumnRefNode.CastFrom(expr))
        if (index < 0) {
          throw new FeSQLException(s"Can not resolve column of left table: ${expr.GetExprString()}")
        }
        getCol(left, index)

      case _ => throw new FeSQLException(
        s"Expr ${expr.GetExprString()} not supported")
    }
  }

  def resolveRightColumn(expr: ExprNode,
                         planNode: PhysicalJoinNode,
                         right: DataFrame,
                         ctx: PlanContext): Column = {
    expr.getExpr_type_ match {
      case ExprType.kExprColumnRef =>
        val leftSize = planNode.GetProducer(0).GetOutputSchema().size()
        val index = CoreAPI.ResolveColumnIndex(planNode, ColumnRefNode.CastFrom(expr))
        if (index < leftSize) {
          throw new FeSQLException("Can not resolve column of left table")
        }
        getCol(right, index - leftSize)

      case _ => throw new FeSQLException(
        s"Expr ${expr.GetExprString()} not supported")
    }
  }

  def resolveColumnIndex(expr: ExprNode, planNode: PhysicalOpNode): Int = {
    expr.getExpr_type_ match {
      case ExprType.kExprColumnRef =>
        val index = CoreAPI.ResolveColumnIndex(planNode, ColumnRefNode.CastFrom(expr))
        if (index < 0) {
          throw new FeSQLException(s"Fail to resolve ${expr.GetExprString()}")
        } else if (index >= planNode.GetOutputSchema().size()) {
          throw new FeSQLException(s"Column index out of bounds: $index")
        }
        index

      case _ => throw new FeSQLException(
        s"Expr ${expr.GetExprString()} not supported")
    }
  }

  def resolveColumnIndex(schema_idx: Int, column_idx: Int, planNode: PhysicalOpNode): Int = {
    val index = CoreAPI.ResolveColumnIndex(planNode, schema_idx, column_idx)
    if (index < 0) {
      throw new FeSQLException(s"Fail to resolve schema_idx: $schema_idx, column_idx: $column_idx")
    } else if (index >= planNode.GetOutputSchema().size()) {
      throw new FeSQLException(s"Column index out of bounds: $index")
    }
    index
  }

  def getCol(dataFrame: DataFrame, index: Int): Column = {
    new Column(dataFrame.queryExecution.analyzed.output(index))
  }
}
