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

package com._4paradigm.openmldb.batch.nodes

import com._4paradigm.hybridse.sdk.UnsupportedHybridSeException
import com._4paradigm.hybridse.node.{CastExprNode, ConstNode, ExprNode, ExprType, DataType => HybridseDataType}
import com._4paradigm.hybridse.vm.{CoreAPI, PhysicalSimpleProjectNode}
import com._4paradigm.openmldb.batch.utils.{DataTypeUtil, ExpressionUtil, HybridseUtil, SparkColumnUtil}
import com._4paradigm.openmldb.batch.{PlanContext, SparkInstance}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.mutable


object SimpleProjectPlan {

  private val logger = LoggerFactory.getLogger(this.getClass)

  def gen(ctx: PlanContext, node: PhysicalSimpleProjectNode, inputs: Seq[SparkInstance]): SparkInstance = {
    val inputInstance = inputs.head

    val inputDf = inputInstance.getDfConsideringIndex(ctx, node.GetNodeId())

    // Check if we should keep the index column
    val keepIndexColumn = SparkInstance.keepIndexColumn(ctx, node.GetNodeId())

    val outputSchema = node.GetOutputSchema()

    // Get the output column names from output schema
    val outputColNameList = outputSchema.asScala.map(col =>
      col.getName
    ).toList

    val outputColTypeList = outputSchema.asScala.map(col =>
      DataTypeUtil.hybridseProtoTypeToOpenmldbType(col.getType)
    ).toList

    val selectColList = mutable.ArrayBuffer[Column]()

    for (i <- 0 until node.project().size.toInt) {
      val expr = node.project().GetExpr(i)
      val (col, innerType) = createSparkColumn(ctx.getSparkSession, inputDf, node, expr)
      val castOutputCol = ConstProjectPlan.castSparkOutputCol(
        ctx.getSparkSession, col, outputColTypeList(i), innerType)
      castOutputCol.alias(outputColNameList(i))

      selectColList.append(castOutputCol.alias(outputColNameList(i)))
    }

    if (keepIndexColumn) {
      selectColList.append(inputDf(ctx.getIndexInfo(node.GetNodeId()).indexColumnName))
    }

    // Use Spark DataFrame to select columns
    val result = inputDf.select(selectColList: _*)

    SparkInstance.createConsideringIndex(ctx, node.GetNodeId(), result)
  }

  /**
    * @param inputDf Input dataframe
    * @param node    Current plan node to explain project expression
    * @param expr    Simple project expression
    * @return   Spark column instance compatible with inner expression
    */
  def createSparkColumn(spark: SparkSession,
                        inputDf: DataFrame,
                        node: PhysicalSimpleProjectNode,
                        expr: ExprNode): (Column, HybridseDataType) = {
    expr.GetExprType() match {
      case ExprType.kExprColumnRef | ExprType.kExprColumnId =>
        val inputNode = node.GetProducer(0)
        val colIndex = CoreAPI.ResolveColumnIndex(inputNode, expr)
        if (colIndex < 0 || colIndex >= inputNode.GetOutputSchemaSize()) {
          inputNode.Print()
          inputNode.PrintSchema()
          throw new IndexOutOfBoundsException(
            s"${expr.GetExprString()} resolved index out of bound: $colIndex")
        }
        val sparkCol = SparkColumnUtil.getColumnFromIndex(inputDf, colIndex)
        val sparkType = inputDf.schema(colIndex).dataType
        val schemaType = DataTypeUtil.sparkTypeToHybridseProtoType(sparkType)
        if (!schemaType.hasBaseType()) {
          throw new UnsupportedHybridSeException(
            s"expression output type does not expect to be ${schemaType} for simple project")
        }
        val innerType = DataTypeUtil.hybridseProtoTypeToOpenmldbType(schemaType.getBaseType())
        sparkCol -> innerType

      case ExprType.kExprPrimary =>
        val const = ConstNode.CastFrom(expr)
        ExpressionUtil.constExprToSparkColumn(const) -> const.GetDataType

      case ExprType.kExprCast =>
        val cast = CastExprNode.CastFrom(expr)
        val castType = cast.base_cast_type
        val (childCol, childType) =
          createSparkColumn(spark, inputDf, node, cast.GetChild(0))
        val castColumn = ConstProjectPlan.castSparkOutputCol(
          spark, childCol, childType, castType)
        castColumn -> castType

      case _ => throw new UnsupportedHybridSeException(
        s"Simple project do not support expression type ${expr.GetExprType}")
    }
  }
}
