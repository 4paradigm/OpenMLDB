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

package com._4paradigm.hybridsql.spark.utils

import com._4paradigm.hybridse.common.{HybridSEException, UnsupportedHybridSEException}
import com._4paradigm.hybridse.node.{ConstNode, ExprNode, ExprType, DataType => HybridseDataType}
import com._4paradigm.hybridse.vm.{CoreAPI, PhysicalJoinNode, PhysicalOpNode}
import com._4paradigm.hybridsql.spark.PlanContext
import org.apache.spark.sql.functions.lit
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
          throw new HybridSEException(s"Can not resolve column of left table: ${expr.GetExprString()}")
        }
        getColumnFromIndex(left, index)

      case _ => throw new HybridSEException(
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
          throw new HybridSEException("Can not resolve column of left table")
        }
        getColumnFromIndex(right, index - leftSize)

      case _ => throw new HybridSEException(
        s"Expr ${expr.GetExprString()} not supported")
    }
  }

  // Resolve HybridSE column reference expression to get column index
  def resolveColumnIndex(expr: ExprNode, planNode: PhysicalOpNode): Int = {
    expr.getExpr_type_ match {
      case ExprType.kExprColumnRef | ExprType.kExprColumnId =>
        val index = CoreAPI.ResolveColumnIndex(planNode, expr)
        if (index < 0) {
          throw new HybridSEException(s"Fail to resolve ${expr.GetExprString()}, get index: ${index}")
        } else if (index >= planNode.GetOutputSchema().size()) {
          throw new HybridSEException(s"Column index out of bounds: $index")
        }
        index

      case _ => throw new HybridSEException(
        s"Expr ${expr.GetExprString()} not supported")
    }
  }

  // Resolve HybridSE expr node to get Spark column
  def resolveExprNodeToColumn(expr: ExprNode, planNode: PhysicalOpNode, inputDf: DataFrame): Column = {
    expr.getExpr_type_ match {
      case ExprType.kExprColumnRef | ExprType.kExprColumnId=>
        val index = CoreAPI.ResolveColumnIndex(planNode, expr)
        if (index < 0) {
          throw new HybridSEException(s"Fail to resolve ${expr.GetExprString()}")
        } else if (index >= planNode.GetOutputSchema().size()) {
          throw new HybridSEException(s"Column index out of bounds: $index")
        }
        getColumnFromIndex(inputDf, index)

      case ExprType.kExprPrimary =>
        val constNode = ConstNode.CastFrom(expr)
        constNode.GetDataType() match {
          case HybridseDataType.kInt16 | HybridseDataType.kInt32 | HybridseDataType.kInt64 =>
            lit(constNode.GetAsInt64())
          case HybridseDataType.kFloat | HybridseDataType.kDouble => lit(constNode.GetAsDouble())
          case HybridseDataType.kVarchar => lit(constNode.GetAsString())
          case _ => throw new UnsupportedHybridSEException(s"Fail to support const node ${constNode.GetExprString()}")
        }

      case _ => throw new UnsupportedHybridSEException(
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

}
