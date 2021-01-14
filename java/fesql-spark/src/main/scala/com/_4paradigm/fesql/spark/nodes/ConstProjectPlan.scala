package com._4paradigm.fesql.spark.nodes

import com._4paradigm.fesql.common.UnsupportedFesqlException
import com._4paradigm.fesql.spark._
import com._4paradigm.fesql.spark.utils.{FesqlUtil, SparkColumnUtil}
import com._4paradigm.fesql.vm.PhysicalConstProjectNode
import org.apache.spark.sql.functions.{lit, to_date, to_timestamp, typedLit, when}
import com._4paradigm.fesql.node.{ConstNode, ExprType, DataType => FesqlDataType}
import org.apache.spark.sql.Column
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._


object ConstProjectPlan {

  def gen(ctx: PlanContext, node: PhysicalConstProjectNode): SparkInstance = {

    // Get the output column names from output schema
    val outputColNameList = node.GetOutputSchema().asScala.map(col =>
      col.getName
    ).toList

    val outputColTypeList = node.GetOutputSchema().asScala.map(col =>
      FesqlUtil.getInnerTypeFromSchemaType(col.getType)
    ).toList

    // Get the select columns
    val selectColList = (0 until node.project().size.toInt).map(i => {
      val expr = node.project().GetExpr(i)
      expr.GetExprType() match {
        case ExprType.kExprPrimary =>
          val constNode = ConstNode.CastFrom(expr)
          val outputColName = outputColNameList(i)

          // Create simple literal spark column
          val column = getConstCol(constNode)

          // Match column type for output type
          castSparkOutputCol(column, constNode.GetDataType(), outputColTypeList(i))
            .alias(outputColName)

        case _ => throw new UnsupportedFesqlException(
          s"Should not handle non-const column for const project node")
      }
    })

    // Use Spark DataFrame to select columns
    val emptyDf = ctx.getSparkSession.emptyDataFrame
    val result = SparkColumnUtil.setDataframeNullable(emptyDf.select(selectColList:_*), nullable=true)
    SparkInstance.fromDataFrame(result)
  }

  // Generate Spark column from const value
  def getConstCol(constNode: ConstNode): Column = {
    constNode.GetDataType() match {
      case FesqlDataType.kNull => lit(null)

      case FesqlDataType.kInt16 =>
        typedLit[Short](constNode.GetAsInt16())

      case FesqlDataType.kInt32 =>
        typedLit[Int](constNode.GetAsInt32())

      case FesqlDataType.kInt64 =>
        typedLit[Long](constNode.GetAsInt64())

      case FesqlDataType.kFloat =>
        typedLit[Float](constNode.GetAsFloat())

      case FesqlDataType.kDouble =>
        typedLit[Double](constNode.GetAsDouble())

      case FesqlDataType.kVarchar =>
        typedLit[String](constNode.GetAsString())

      case _ => throw new UnsupportedFesqlException(
        s"Const value for FESQL type ${constNode.GetDataType()} not supported")
    }
  }

  def castSparkOutputCol(inputCol: Column,
                         fromType: FesqlDataType,
                         targetType: FesqlDataType): Column = {
    if (fromType == targetType) {
      return inputCol
    }
    targetType match {
      case FesqlDataType.kInt16 =>
        fromType match {
          case FesqlDataType.kNull => inputCol.cast(ShortType)
          case FesqlDataType.kInt16 | FesqlDataType.kInt32 | FesqlDataType.kInt64 | FesqlDataType.kFloat | FesqlDataType.kDouble | FesqlDataType.kVarchar =>
            inputCol.cast(ShortType)
          case FesqlDataType.kBool => inputCol.cast(ShortType)
          case FesqlDataType.kTimestamp => inputCol.cast(ShortType).multiply(1000).cast(ShortType)
          case FesqlDataType.kDate => inputCol.cast(DateType)
          case _ => throw new UnsupportedFesqlException(
            s"FESQL type from $fromType to $targetType is not supported")
        }

      case FesqlDataType.kInt32 =>
        fromType match {
          case FesqlDataType.kNull => inputCol.cast(IntegerType)
          case FesqlDataType.kInt16 | FesqlDataType.kInt32 | FesqlDataType.kInt64 | FesqlDataType.kFloat | FesqlDataType.kDouble | FesqlDataType.kVarchar =>
            inputCol.cast(IntegerType)
          case FesqlDataType.kBool => inputCol.cast(IntegerType)
          // Spark timestamp to long returns seconds, which need to multiply 1000 to be millis seconds
          case FesqlDataType.kTimestamp => inputCol.cast(IntegerType).multiply(1000)
          case FesqlDataType.kDate => inputCol.cast(DateType)
          case _ => throw new UnsupportedFesqlException(
            s"FESQL type from $fromType to $targetType is not supported")
        }

      case FesqlDataType.kInt64 =>
        fromType match {
          case FesqlDataType.kNull => inputCol.cast(LongType)
          case FesqlDataType.kInt16 | FesqlDataType.kInt32 | FesqlDataType.kInt64 | FesqlDataType.kFloat | FesqlDataType.kDouble | FesqlDataType.kVarchar =>
            inputCol.cast(LongType)
          case FesqlDataType.kBool => inputCol.cast(LongType)
          case FesqlDataType.kTimestamp => inputCol.cast(LongType).multiply(1000)
          case FesqlDataType.kDate => inputCol.cast(DateType)
          case _ => throw new UnsupportedFesqlException(
            s"FESQL type from $fromType to $targetType is not supported")
        }

      case FesqlDataType.kFloat =>
        fromType match {
          case FesqlDataType.kNull => inputCol.cast(FloatType)
          case FesqlDataType.kInt16 | FesqlDataType.kInt32 | FesqlDataType.kInt64 | FesqlDataType.kFloat | FesqlDataType.kDouble | FesqlDataType.kVarchar =>
            inputCol.cast(FloatType)
          case FesqlDataType.kBool => inputCol.cast(FloatType)
          case FesqlDataType.kTimestamp => inputCol.cast(FloatType).multiply(1000)
          case FesqlDataType.kDate => inputCol.cast(DateType)
          case _ => throw new UnsupportedFesqlException(
            s"FESQL type from $fromType to $targetType is not supported")
        }

      case FesqlDataType.kDouble =>
        fromType match {
          case FesqlDataType.kNull => inputCol.cast(DoubleType)
          case FesqlDataType.kInt16 | FesqlDataType.kInt32 | FesqlDataType.kInt64 | FesqlDataType.kFloat | FesqlDataType.kDouble | FesqlDataType.kVarchar =>
            inputCol.cast(DoubleType)
          case FesqlDataType.kBool => inputCol.cast(DoubleType)
          case FesqlDataType.kTimestamp => inputCol.cast(DoubleType).multiply(1000)
          case FesqlDataType.kDate => inputCol.cast(DateType)
          case _ => throw new UnsupportedFesqlException(
            s"FESQL type from $fromType to $targetType is not supported")
        }

      case FesqlDataType.kBool =>
        fromType match {
          case FesqlDataType.kNull =>
            inputCol.cast(BooleanType)
          case FesqlDataType.kInt16 | FesqlDataType.kInt32 | FesqlDataType.kInt64 =>
            inputCol.cast(BooleanType)
          case FesqlDataType.kFloat | FesqlDataType.kDouble =>
            inputCol.cast(BooleanType)
          case FesqlDataType.kTimestamp => inputCol.cast(BooleanType)
          case FesqlDataType.kDate => inputCol.cast(BooleanType)
          // TODO: may catch exception if it fails to convert to string
          case FesqlDataType.kVarchar =>
            inputCol.cast(BooleanType)

          case _ => throw new UnsupportedFesqlException(
            s"FESQL type from $fromType to $targetType is not supported")
        }

      case FesqlDataType.kDate =>
        fromType match {
          case FesqlDataType.kNull => inputCol.cast(DateType)
          case FesqlDataType.kInt16 | FesqlDataType.kInt32 | FesqlDataType.kInt64 | FesqlDataType.kFloat | FesqlDataType.kDouble =>
            inputCol.cast(TimestampType).cast(DateType)
          case FesqlDataType.kBool => inputCol.cast(TimestampType).cast(DateType)
          case FesqlDataType.kTimestamp => inputCol.cast(DateType)
          case FesqlDataType.kVarchar =>
            to_date(inputCol, "yyyy-MM-dd")
          case _ => throw new UnsupportedFesqlException(
            s"FESQL type from $fromType to $targetType is not supported")
        }

      case FesqlDataType.kTimestamp =>  // TODO: May set timezone if it is different from database
        fromType match {
          case FesqlDataType.kNull =>
            inputCol.cast(TimestampType)
          case FesqlDataType.kInt16 | FesqlDataType.kInt32 | FesqlDataType.kInt64 | FesqlDataType.kFloat | FesqlDataType.kDouble =>
            when(inputCol >= 0, inputCol.divide(1000)).otherwise(null).cast(TimestampType)
          case FesqlDataType.kBool =>
            inputCol.cast(LongType).divide(1000).cast(TimestampType)
          case FesqlDataType.kDate => inputCol.cast(TimestampType)
          case FesqlDataType.kVarchar =>
            to_timestamp(inputCol)
          case _ => throw new UnsupportedFesqlException(
            s"FESQL type from $fromType to $targetType is not supported")
        }

      case FesqlDataType.kVarchar =>
        fromType match {
          case FesqlDataType.kNull => inputCol.cast(StringType)
          case FesqlDataType.kInt16 | FesqlDataType.kInt32 | FesqlDataType.kInt64 | FesqlDataType.kFloat | FesqlDataType.kDouble =>
            inputCol.cast(StringType)
          case FesqlDataType.kBool => inputCol.cast(StringType)
          case FesqlDataType.kTimestamp => inputCol.cast(StringType)
          case FesqlDataType.kDate => inputCol.cast(StringType)
          case _ => throw new UnsupportedFesqlException(
            s"FESQL type from $fromType to $targetType is not supported")
        }

      case _ => throw new UnsupportedFesqlException(
        s"FESQL schema type $targetType not supported")
    }
  }
}
