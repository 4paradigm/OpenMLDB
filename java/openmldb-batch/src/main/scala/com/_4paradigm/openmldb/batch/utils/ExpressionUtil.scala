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

package com._4paradigm.openmldb.batch.utils

import com._4paradigm.hybridse.node.{BinaryExpr, ConstNode, ExprNode, ExprType, FnOperator,
  DataType => HybridseDataType}
import com._4paradigm.hybridse.sdk.UnsupportedHybridSeException
import com._4paradigm.hybridse.vm.{CoreAPI, PhysicalOpNode}
import org.apache.spark.sql.functions.{lit, typedLit}
import org.apache.spark.sql.{Column, DataFrame}

object ExpressionUtil {

  /**
   * Convert const expression to Spark Column object.
   *
   * @param constNode
   * @return
   */
  def constExprToSparkColumn(constNode: ConstNode): Column = {
    constNode.GetDataType() match {
      case HybridseDataType.kNull => lit(null)

      case HybridseDataType.kInt16 =>
        typedLit[Short](constNode.GetAsInt16())

      case HybridseDataType.kInt32 =>
        typedLit[Int](constNode.GetAsInt32())

      case HybridseDataType.kInt64 =>
        typedLit[Long](constNode.GetAsInt64())

      case HybridseDataType.kFloat =>
        typedLit[Float](constNode.GetAsFloat())

      case HybridseDataType.kDouble =>
        typedLit[Double](constNode.GetAsDouble())

      case HybridseDataType.kVarchar =>
        typedLit[String](constNode.GetAsString())

      case _ => throw new UnsupportedHybridSeException(
        s"Const value for HybridSE type ${constNode.GetDataType()} not supported")
    }
  }

  /**
   * Convert expr object to Spark Column object.
   * Notice that this only works for some non-computing expressions.
   *
   * @param expr
   * @param leftDf
   * @param rightDf
   * @param physicalNode
   * @return
   */
  def exprToSparkColumn(expr: ExprNode,
                        leftDf: DataFrame,
                        rightDf: DataFrame,
                        physicalNode: PhysicalOpNode,
                        hasIndexColumn: Boolean): Column = {
    expr.GetExprType() match {
      case ExprType.kExprColumnRef | ExprType.kExprColumnId =>
        val inputNode = physicalNode
        val colIndex = CoreAPI.ResolveColumnIndex(inputNode, expr)
        if (colIndex < 0 || colIndex >= inputNode.GetOutputSchemaSize()) {
          inputNode.Print()
          inputNode.PrintSchema()
          throw new IndexOutOfBoundsException(
            s"${expr.GetExprString()} resolved index out of bound: $colIndex")
        }

        if (hasIndexColumn) {
          if (colIndex < leftDf.schema.size - 1) {
            // Get from left df
            SparkColumnUtil.getColumnFromIndex(leftDf, colIndex)
          } else {
            // Get from right df
            val rightColIndex = colIndex - (leftDf.schema.size - 1)
            SparkColumnUtil.getColumnFromIndex(rightDf, rightColIndex)
          }
        } else {
          if (colIndex < leftDf.schema.size) {
            // Get from left df
            SparkColumnUtil.getColumnFromIndex(leftDf, colIndex)
          } else {
            // Get from right df
            val rightColIndex = colIndex - leftDf.schema.size
            SparkColumnUtil.getColumnFromIndex(rightDf, rightColIndex)
          }
        }

      case ExprType.kExprPrimary =>
        val const = ConstNode.CastFrom(expr)
        ExpressionUtil.constExprToSparkColumn(const)

      case _ => throw new UnsupportedHybridSeException(
        s"Do not support converting expression to Spark Column for expression type ${expr.GetExprType}")
    }
  }

  /**
   * Convert binary expression to two Spark Column objects.
   *
   * @param binaryExpr
   * @param physicalNode
   * @param leftDf
   * @param rightDf
   * @return
   */
  def binaryExprToSparkColumns(binaryExpr: BinaryExpr, physicalNode: PhysicalOpNode, leftDf: DataFrame,
                               rightDf: DataFrame, hasIndexColumn: Boolean): (Column, Column) = {
    val leftExpr = binaryExpr.GetChild(0)
    val rightExpr = binaryExpr.GetChild(1)
    val leftSparkColumn = ExpressionUtil.exprToSparkColumn(leftExpr, leftDf, rightDf, physicalNode, hasIndexColumn)
    val rightSparkColumn = ExpressionUtil.exprToSparkColumn(rightExpr, leftDf, rightDf, physicalNode, hasIndexColumn)
    leftSparkColumn -> rightSparkColumn
  }


  def recusiveGetSparkColumnFromExpr(expr: ExprNode, node: PhysicalOpNode, leftDf: DataFrame,
                                           rightDf: DataFrame, hasIndexColumn: Boolean): Column = {
    expr.GetExprType() match {
      case ExprType.kExprBinary =>
        val binaryExpr = BinaryExpr.CastFrom(expr)
        val op = binaryExpr.GetOp()
        op match {
          case FnOperator.kFnOpAnd =>
            // TODO(tobe): Only support for binary sub expressions
            val leftExpr = BinaryExpr.CastFrom(binaryExpr.GetChild(0))
            val rightExpr = BinaryExpr.CastFrom(binaryExpr.GetChild(1))
            val leftColumn = recusiveGetSparkColumnFromExpr(leftExpr, node, leftDf, rightDf, hasIndexColumn)
            val rightColumn = recusiveGetSparkColumnFromExpr(rightExpr, node, leftDf, rightDf, hasIndexColumn)
            leftColumn.and(rightColumn)
          case FnOperator.kFnOpOr =>
            val leftExpr = BinaryExpr.CastFrom(binaryExpr.GetChild(0))
            val rightExpr = BinaryExpr.CastFrom(binaryExpr.GetChild(1))
            val leftColumn = recusiveGetSparkColumnFromExpr(leftExpr, node, leftDf, rightDf, hasIndexColumn)
            val rightColumn = recusiveGetSparkColumnFromExpr(rightExpr, node, leftDf, rightDf, hasIndexColumn)
            leftColumn.or(rightColumn)
          case FnOperator.kFnOpNot =>
            !recusiveGetSparkColumnFromExpr(expr, node, leftDf, rightDf, hasIndexColumn)
          case FnOperator.kFnOpEq => // TODO(todo): Support null-safe equal in the future
            // Notice that it may be handled by physical plan's left_key() and right_key()
            val (left, right) = ExpressionUtil.binaryExprToSparkColumns(binaryExpr, node, leftDf, rightDf,
              hasIndexColumn)
            left.equalTo(right)
          case FnOperator.kFnOpNeq =>
            val (left, right) = ExpressionUtil.binaryExprToSparkColumns(binaryExpr, node, leftDf, rightDf,
              hasIndexColumn)
            left.notEqual(right)
          case FnOperator.kFnOpLt =>
            val (left, right) = ExpressionUtil.binaryExprToSparkColumns(binaryExpr, node, leftDf, rightDf,
              hasIndexColumn)
            left < right
          case FnOperator.kFnOpLe =>
            val (left, right) = ExpressionUtil.binaryExprToSparkColumns(binaryExpr, node, leftDf, rightDf,
              hasIndexColumn)
            left <= right
          case FnOperator.kFnOpGt =>
            val (left, right) = ExpressionUtil.binaryExprToSparkColumns(binaryExpr, node, leftDf, rightDf,
              hasIndexColumn)
            left > right
          case FnOperator.kFnOpGe =>
            val (left, right) = ExpressionUtil.binaryExprToSparkColumns(binaryExpr, node, leftDf, rightDf,
              hasIndexColumn)
            left >= right
        }
      case _ => throw new UnsupportedHybridSeException(
        s"Does not support convert expression type ${expr.GetExprType} to Spark Column")
    }

  }

}
