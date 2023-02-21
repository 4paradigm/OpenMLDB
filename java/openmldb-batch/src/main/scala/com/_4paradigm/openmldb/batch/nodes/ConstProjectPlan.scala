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
import com._4paradigm.hybridse.node.{ConstNode, ExprType, DataType => HybridseDataType}
import com._4paradigm.hybridse.vm.PhysicalConstProjectNode
import com._4paradigm.openmldb.batch.{PlanContext, SparkInstance}
import com._4paradigm.openmldb.batch.utils.{DataTypeUtil, ExpressionUtil}
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions.{to_date, when}
import org.apache.spark.sql.types.{BooleanType, DateType, DoubleType, FloatType, IntegerType, LongType, ShortType,
  StringType, TimestampType}
import java.sql.Timestamp
import scala.collection.JavaConverters.asScalaBufferConverter

object ConstProjectPlan {

  def gen(ctx: PlanContext, node: PhysicalConstProjectNode): SparkInstance = {

    // Get the output column names from output schema
    val outputColNameList = node.GetOutputSchema().asScala.map(col =>
      col.getName
    ).toList

    val outputColTypeList = node.GetOutputSchema().asScala.map(col =>
      DataTypeUtil.hybridseProtoTypeToOpenmldbType(col.getType)
    ).toList

    // Get the select columns
    val selectColList = (0 until node.project().size.toInt).map(i => {
      val expr = node.project().GetExpr(i)
      expr.GetExprType() match {
        case ExprType.kExprPrimary =>
          val constNode = ConstNode.CastFrom(expr)
          val outputColName = outputColNameList(i)

          // Create simple literal Spark column
          val column = ExpressionUtil.constExprToSparkColumn(constNode)

          // Match column type for output type
          castSparkOutputCol(ctx.getSparkSession, column, constNode.GetDataType(), outputColTypeList(i))
            .alias(outputColName)

        case _ => throw new UnsupportedHybridSeException(
          s"Should not handle non-const column for const project node")
      }
    })

    // Use Spark DataFrame to select columns
    val result = ctx.getSparkSession.emptyDataFrame.select(selectColList: _*)

    SparkInstance.createConsideringIndex(ctx, node.GetNodeId(), result)
  }

  def stringToTimestamp: String => Timestamp = (input: String) => {
    if (input == null) {
      null.asInstanceOf[Timestamp]
    } else {
      val stringPattern = input.length match {
        case 19 => "yyyy-MM-dd HH:mm:ss"
        case 10 => "yyyy-MM-dd"
        case 8 => "yyyyMMdd"
        case _ => throw new Exception(s"Unsupported timestamp format for string $input")
      }

      val format = new java.text.SimpleDateFormat(stringPattern)
      new Timestamp(format.parse(input).getTime)
    }
  }

  def castSparkOutputCol(spark: SparkSession,
                         inputCol: Column,
                         fromType: HybridseDataType,
                         targetType: HybridseDataType): Column = {
    if (fromType == targetType) {
      return inputCol
    }
    targetType match {
      case HybridseDataType.kInt16 =>
        fromType match {
          case HybridseDataType.kNull => inputCol.cast(ShortType)
          case (HybridseDataType.kInt16 | HybridseDataType.kInt32 | HybridseDataType.kInt64 | HybridseDataType.kFloat
               | HybridseDataType.kDouble | HybridseDataType.kVarchar) =>
            inputCol.cast(ShortType)
          case HybridseDataType.kBool => inputCol.cast(ShortType)
          case HybridseDataType.kTimestamp => inputCol.cast(ShortType).multiply(1000).cast(ShortType)
          case HybridseDataType.kDate => inputCol.cast(ShortType)
          case _ => throw new UnsupportedHybridSeException(
            s"HybridSE type from $fromType to $targetType is not supported")
        }

      case HybridseDataType.kInt32 =>
        fromType match {
          case HybridseDataType.kNull => inputCol.cast(IntegerType)
          case (HybridseDataType.kInt16 | HybridseDataType.kInt32 | HybridseDataType.kInt64 | HybridseDataType.kFloat
               | HybridseDataType.kDouble | HybridseDataType.kVarchar) =>
            inputCol.cast(IntegerType)
          case HybridseDataType.kBool => inputCol.cast(IntegerType)
          // Spark timestamp to long returns seconds, which need to multiply 1000 to be millis seconds
          case HybridseDataType.kTimestamp => inputCol.cast(IntegerType).multiply(1000)
          case HybridseDataType.kDate => inputCol.cast(IntegerType)
          case _ => throw new UnsupportedHybridSeException(
            s"HybridSE type from $fromType to $targetType is not supported")
        }

      case HybridseDataType.kInt64 =>
        fromType match {
          case HybridseDataType.kNull => inputCol.cast(LongType)
          case (HybridseDataType.kInt16 | HybridseDataType.kInt32 | HybridseDataType.kInt64 | HybridseDataType.kFloat
               | HybridseDataType.kDouble | HybridseDataType.kVarchar) =>
            inputCol.cast(LongType)
          case HybridseDataType.kBool => inputCol.cast(LongType)
          case HybridseDataType.kTimestamp => inputCol.cast(LongType).multiply(1000)
          case HybridseDataType.kDate => inputCol.cast(LongType)
          case _ => throw new UnsupportedHybridSeException(
            s"HybridSE type from $fromType to $targetType is not supported")
        }

      case HybridseDataType.kFloat =>
        fromType match {
          case HybridseDataType.kNull => inputCol.cast(FloatType)
          case (HybridseDataType.kInt16 | HybridseDataType.kInt32 | HybridseDataType.kInt64 | HybridseDataType.kFloat
               | HybridseDataType.kDouble | HybridseDataType.kVarchar) =>
            inputCol.cast(FloatType)
          case HybridseDataType.kBool => inputCol.cast(FloatType)
          case HybridseDataType.kTimestamp => inputCol.cast(FloatType).multiply(1000)
          case HybridseDataType.kDate => inputCol.cast(FloatType)
          case _ => throw new UnsupportedHybridSeException(
            s"HybridSE type from $fromType to $targetType is not supported")
        }

      case HybridseDataType.kDouble =>
        fromType match {
          case HybridseDataType.kNull => inputCol.cast(DoubleType)
          case (HybridseDataType.kInt16 | HybridseDataType.kInt32 | HybridseDataType.kInt64 | HybridseDataType.kFloat
               | HybridseDataType.kDouble | HybridseDataType.kVarchar) =>
            inputCol.cast(DoubleType)
          case HybridseDataType.kBool => inputCol.cast(DoubleType)
          case HybridseDataType.kTimestamp => inputCol.cast(DoubleType).multiply(1000)
          case HybridseDataType.kDate => inputCol.cast(DoubleType)
          case _ => throw new UnsupportedHybridSeException(
            s"HybridSE type from $fromType to $targetType is not supported")
        }

      case HybridseDataType.kBool =>
        fromType match {
          case HybridseDataType.kNull =>
            inputCol.cast(BooleanType)
          case HybridseDataType.kInt16 | HybridseDataType.kInt32 | HybridseDataType.kInt64 =>
            inputCol.cast(BooleanType)
          case HybridseDataType.kFloat | HybridseDataType.kDouble =>
            inputCol.cast(BooleanType)
          case HybridseDataType.kTimestamp => inputCol.cast(BooleanType)
          case HybridseDataType.kDate => inputCol.cast(BooleanType)
          // TODO: may catch exception if it fails to convert to string
          case HybridseDataType.kVarchar =>
            inputCol.cast(BooleanType)

          case _ => throw new UnsupportedHybridSeException(
            s"HybridSE type from $fromType to $targetType is not supported")
        }

      case HybridseDataType.kDate =>
        fromType match {
          case HybridseDataType.kNull => inputCol.cast(DateType)
          case (HybridseDataType.kInt16 | HybridseDataType.kInt32 | HybridseDataType.kInt64 | HybridseDataType.kFloat
               | HybridseDataType.kDouble) =>
            inputCol.cast(TimestampType).cast(DateType)
          case HybridseDataType.kBool => inputCol.cast(TimestampType).cast(DateType)
          case HybridseDataType.kTimestamp => inputCol.cast(DateType)
          case HybridseDataType.kVarchar =>
            to_date(inputCol, "yyyy-MM-dd")
          case _ => throw new UnsupportedHybridSeException(
            s"HybridSE type from $fromType to $targetType is not supported")
        }

      case HybridseDataType.kTimestamp => // TODO: May set timezone if it is different from database
        fromType match {
          case HybridseDataType.kNull =>
            inputCol.cast(TimestampType)
          case (HybridseDataType.kInt16 | HybridseDataType.kInt32 | HybridseDataType.kInt64 | HybridseDataType.kFloat
               | HybridseDataType.kDouble) =>
            when(inputCol >= 0, inputCol.divide(1000)).otherwise(null).cast(TimestampType)
          case HybridseDataType.kBool =>
            inputCol.cast(LongType).divide(1000).cast(TimestampType)
          case HybridseDataType.kDate => inputCol.cast(TimestampType)
          case HybridseDataType.kVarchar =>
            val stringToTimestampUdf = spark.udf.register("timestamp", stringToTimestamp)
            stringToTimestampUdf(inputCol)

          case _ => throw new UnsupportedHybridSeException(
            s"HybridSE type from $fromType to $targetType is not supported")
        }

      case HybridseDataType.kVarchar =>
        fromType match {
          case HybridseDataType.kNull => inputCol.cast(StringType)
          case (HybridseDataType.kInt16 | HybridseDataType.kInt32 | HybridseDataType.kInt64 | HybridseDataType.kFloat
               | HybridseDataType.kDouble) =>
            inputCol.cast(StringType)
          case HybridseDataType.kBool => inputCol.cast(StringType)
          case HybridseDataType.kTimestamp => inputCol.cast(StringType)
          case HybridseDataType.kDate => inputCol.cast(StringType)
          case _ => throw new UnsupportedHybridSeException(
            s"HybridSE type from $fromType to $targetType is not supported")
        }

      case _ => throw new UnsupportedHybridSeException(
        s"HybridSE schema type $targetType not supported")
    }
  }
}
