package com._4paradigm.fesql.spark.nodes

import com._4paradigm.fesql.`type`.TypeOuterClass.Type
import com._4paradigm.fesql.common.FesqlException
import com._4paradigm.fesql.spark._
import com._4paradigm.fesql.spark.utils.SparkColumnUtil
import com._4paradigm.fesql.vm.{PhysicalConstProjectNode, SourceType}
import org.apache.spark.sql.functions.{lit, to_date, to_timestamp}
import com._4paradigm.fesql.node.{ConstNode, DataType => FesqlDataType}
import org.apache.spark.sql.{Column, Row}
import org.apache.spark.sql.types.{BooleanType, DateType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, StructField, StructType, TimestampType}

import scala.collection.JavaConverters._


object ConstProjectPlan {

  def gen(ctx: PlanContext, node: PhysicalConstProjectNode): SparkInstance = {

    // Get the output column names from output schema
    val outputColNameList = node.GetOutputSchema().asScala.map(col =>
      col.getName
    ).toList

    val outputColTypeList = node.GetOutputSchema().asScala.map(col =>
      col.getType
    ).toList

    // Get the select column indexes from node
    val columnSourceList = node.getSources_

    val selectColList = (0 until columnSourceList.size()).map(i => {

      val columnSource = columnSourceList.get(i);

      columnSource.`type`() match {
        case SourceType.kSourceColumn => {
          throw new FesqlException(s"Should not handle source column for const proejct node")
        }
        case SourceType.kSourceConst => {
          val const_value = columnSource.const_value()
          val outputColName = outputColNameList(i)
          getConstCol(outputColTypeList(i), const_value, outputColName)
        }
      }

    })

    // Use Spark DataFrame to select columns
    val emptyDf = ctx.getSparkSession.createDataFrame(ctx.getSparkSession.sparkContext.makeRDD(Seq(Row("A"))), StructType(List(StructField("name", StringType))))
    val result = SparkColumnUtil.setDataframeNullable(emptyDf.select(selectColList:_*), true)
    SparkInstance.fromDataFrame(result)

  }

  // Generate Spark column from const value
  def getConstCol(outputType: Type, const_value: ConstNode, outputColName: String): Column = {
    // TODO: Do not handle multiple cast for const value, require core to optimize and generate the final const node

    // Get constant value and case type and rename
    outputType match {
      // TODO: Support cast other type to bool
      case Type.kInt16 => {
        const_value.GetDataType() match {
          case FesqlDataType.kInt16 | FesqlDataType.kInt32 | FesqlDataType.kInt64 | FesqlDataType.kFloat | FesqlDataType.kDouble | FesqlDataType.kVarchar => lit(const_value.GetAsInt16()).cast(ShortType).alias(outputColName)
          case FesqlDataType.kNull => lit(null).cast(ShortType).alias(outputColName)
          case _ => throw new FesqlException(s"FESQL type from ${const_value.GetDataType()} to ${outputType} is not supported")
        }
      }
      case Type.kInt32 => {
        const_value.GetDataType() match {
          case FesqlDataType.kInt16 | FesqlDataType.kInt32 | FesqlDataType.kInt64 | FesqlDataType.kFloat | FesqlDataType.kDouble | FesqlDataType.kVarchar => lit(const_value.GetAsInt32()).cast(IntegerType).alias(outputColName)
          case FesqlDataType.kNull => lit(null).cast(IntegerType).alias(outputColName)
          case _ => throw new FesqlException(s"FESQL type from ${const_value.GetDataType()} to ${outputType} is not supported")
        }
      }
      case Type.kInt64 => {
        const_value.GetDataType() match {
          case FesqlDataType.kInt16 | FesqlDataType.kInt32 | FesqlDataType.kInt64 | FesqlDataType.kFloat | FesqlDataType.kDouble | FesqlDataType.kVarchar => lit(const_value.GetAsInt64()).cast(LongType).alias(outputColName)
          case FesqlDataType.kNull => lit(null).cast(LongType).alias(outputColName)
          case _ => throw new FesqlException(s"FESQL type from ${const_value.GetDataType()} to ${outputType} is not supported")
        }
      }
      case Type.kFloat => {
        const_value.GetDataType() match {
          case FesqlDataType.kInt16 | FesqlDataType.kInt32 | FesqlDataType.kInt64 | FesqlDataType.kFloat | FesqlDataType.kDouble | FesqlDataType.kVarchar => lit(const_value.GetAsFloat()).cast(FloatType).alias(outputColName)
          case FesqlDataType.kNull => lit(null).cast(FloatType).alias(outputColName)
          case _ => throw new FesqlException(s"FESQL type from ${const_value.GetDataType()} to ${outputType} is not supported")
        }
      }
      case Type.kDouble => {
        const_value.GetDataType() match {
          case FesqlDataType.kInt16 | FesqlDataType.kInt32 | FesqlDataType.kInt64 | FesqlDataType.kFloat | FesqlDataType.kDouble | FesqlDataType.kVarchar => lit(const_value.GetAsDouble()).cast(DoubleType).alias(outputColName)
          case FesqlDataType.kNull => lit(null).cast(DoubleType).alias(outputColName)
          case _ => throw new FesqlException(s"FESQL type from ${const_value.GetDataType()} to ${outputType} is not supported")
        }
      }
      case Type.kBool => {
        const_value.GetDataType() match {
          case FesqlDataType.kInt16 | FesqlDataType.kInt32 | FesqlDataType.kInt64 => lit(const_value.GetAsInt64()).cast(BooleanType).alias(outputColName)
          case FesqlDataType.kFloat | FesqlDataType.kDouble => lit(const_value.GetAsDouble()).cast(BooleanType).alias(outputColName)
          // TODO: may catch exception if it fails to convert to string
          case FesqlDataType.kVarchar => lit(const_value.GetAsString()).cast(BooleanType).alias(outputColName)
          case FesqlDataType.kNull => lit(null).cast(BooleanType).alias(outputColName)
          case _ => throw new FesqlException(s"FESQL type from ${const_value.GetDataType()} to ${outputType} is not supported")
        }
      }
      case Type.kDate => {
        const_value.GetDataType() match {
          case FesqlDataType.kInt16 | FesqlDataType.kInt32 | FesqlDataType.kInt64 => lit(const_value.GetAsInt64()).cast(DateType).alias(outputColName)
          case FesqlDataType.kFloat | FesqlDataType.kDouble => lit(const_value.GetAsDouble()).cast(DateType).alias(outputColName)
          case FesqlDataType.kVarchar => to_date(lit(const_value.GetAsString()), "yyyy-MM-dd").alias(outputColName)
          case FesqlDataType.kNull => lit(null).cast(DateType).alias(outputColName)
          case _ => throw new FesqlException(s"FESQL type from ${const_value.GetDataType()} to ${outputType} is not supported")
        }
      }
      case Type.kTimestamp => { // TODO: May set timezone if it is different from database
        const_value.GetDataType() match {
          case FesqlDataType.kInt16 | FesqlDataType.kInt32 | FesqlDataType.kInt64 => lit(const_value.GetAsInt64()).divide(1000).cast(TimestampType).alias(outputColName)
          case FesqlDataType.kFloat | FesqlDataType.kDouble => lit(const_value.GetAsInt64()).divide(1000).cast(TimestampType).alias(outputColName)
          case FesqlDataType.kVarchar => to_timestamp(lit(const_value.GetAsString())).alias(outputColName)
          case FesqlDataType.kNull => lit(null).cast(TimestampType).alias(outputColName)
          case _ => throw new FesqlException(s"FESQL type from ${const_value.GetDataType()} to ${outputType} is not supported")
        }
      }
      case Type.kVarchar => {
        const_value.GetDataType() match {
          case FesqlDataType.kInt16 | FesqlDataType.kInt32 | FesqlDataType.kInt64 | FesqlDataType.kFloat | FesqlDataType.kDouble | FesqlDataType.kVarchar => lit(const_value.GetAsString()).cast(StringType).alias(outputColName)
          case FesqlDataType.kNull => lit(null).cast(StringType).alias(outputColName)
          case _ => throw new FesqlException(s"FESQL type from ${const_value.GetDataType()} to ${outputType} is not supported")
        }
      }
      case _ => throw new FesqlException(s"FESQL type ${outputType} not supported")
    }
  }

}
