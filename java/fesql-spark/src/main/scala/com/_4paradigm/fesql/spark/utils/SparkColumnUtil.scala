package com._4paradigm.fesql.spark.utils

import com._4paradigm.fesql.common.{FesqlException, UnsupportedFesqlException}
import com._4paradigm.fesql.node.{ColumnRefNode, ConstNode, ExprNode, ExprType, DataType => FesqlDataType}
import com._4paradigm.fesql.spark.PlanContext
import com._4paradigm.fesql.vm.{CoreAPI, PhysicalJoinNode, PhysicalOpNode}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame}

import scala.collection.mutable


object SparkColumnUtil {

  def resolveLeftColumn(expr: ExprNode,
                        planNode: PhysicalJoinNode,
                        left: DataFrame,
                        ctx: PlanContext): Column = {
    expr.getExpr_type_ match {
      case ExprType.kExprColumnRef | ExprType.kExprColumnId =>
        val index = CoreAPI.ResolveColumnIndex(planNode, expr)
        if (index < 0) {
          throw new FesqlException(s"Can not resolve column of left table: ${expr.GetExprString()}")
        }
        getColumnFromIndex(left, index)

      case _ => throw new FesqlException(
        s"Expr ${expr.GetExprString()} not supported")
    }
  }

  def resolveRightColumn(expr: ExprNode,
                         planNode: PhysicalJoinNode,
                         right: DataFrame,
                         ctx: PlanContext): Column = {
    expr.getExpr_type_ match {
      case ExprType.kExprColumnRef | ExprType.kExprColumnId =>
        val leftSize = planNode.GetProducer(0).GetOutputSchema().size()
        val index = CoreAPI.ResolveColumnIndex(planNode, expr)
        if (index < leftSize) {
          throw new FesqlException("Can not resolve column of left table")
        }
        getColumnFromIndex(right, index - leftSize)

      case _ => throw new FesqlException(
        s"Expr ${expr.GetExprString()} not supported")
    }
  }

  // Resolve FESQL column reference expression to get column index
  def resolveColumnIndex(expr: ExprNode, planNode: PhysicalOpNode): Int = {
    expr.getExpr_type_ match {
      case ExprType.kExprColumnRef | ExprType.kExprColumnId =>
        val index = CoreAPI.ResolveColumnIndex(planNode, expr)
        if (index < 0) {
          throw new FesqlException(s"Fail to resolve ${expr.GetExprString()}, get index: ${index}")
        } else if (index >= planNode.GetOutputSchema().size()) {
          throw new FesqlException(s"Column index out of bounds: $index")
        }
        index

      case _ => throw new FesqlException(
        s"Expr ${expr.GetExprString()} not supported")
    }
  }

  // Resolve FESQL expr node to get Spark column
  def resolveExprNodeToColumn(expr: ExprNode, planNode: PhysicalOpNode, inputDf: DataFrame): Column = {
    expr.getExpr_type_ match {
      case ExprType.kExprColumnRef | ExprType.kExprColumnId=>
        val index = CoreAPI.ResolveColumnIndex(planNode, expr)
        if (index < 0) {
          throw new FesqlException(s"Fail to resolve ${expr.GetExprString()}")
        } else if (index >= planNode.GetOutputSchema().size()) {
          throw new FesqlException(s"Column index out of bounds: $index")
        }
        getColumnFromIndex(inputDf, index)

      case ExprType.kExprPrimary =>
        val constNode = ConstNode.CastFrom(expr)
        constNode.GetDataType() match {
          case FesqlDataType.kInt16 | FesqlDataType.kInt32 | FesqlDataType.kInt64 => lit(constNode.GetAsInt64())
          case FesqlDataType.kFloat | FesqlDataType.kDouble => lit(constNode.GetAsDouble())
          case FesqlDataType.kVarchar => lit(constNode.GetAsString())
          case _ => throw new UnsupportedFesqlException(s"Fail to support const node ${constNode.GetExprString()}")
        }

      case _ => throw new UnsupportedFesqlException(
        s"Fail to resolve expr type: ${expr.getExpr_type_}, expr node: ${expr.GetExprString()}")
    }

  }

  def getColumnFromIndex(df: DataFrame, index: Int): Column = {
    new Column(df.queryExecution.analyzed.output(index))
  }

  def getColumnsFromDataFrame(df: DataFrame): mutable.ArrayBuffer[Column] = {
    val columnList = new mutable.ArrayBuffer[Column]()
    for (i <- df.schema.indices) {
      columnList += getColumnFromIndex(df, i)
    }
    columnList
  }

  // Set the nullable property of the dataframe
  def setDataframeNullable(df: DataFrame, nullable: Boolean) : DataFrame = {
    val newSchema = StructType(df.schema.map {
      case StructField(c, t, _, m) => StructField(c, t, nullable = nullable, m)
    })
    df.sqlContext.createDataFrame(df.rdd, newSchema)
  }

}
