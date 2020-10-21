package com._4paradigm.fesql.spark.nodes

import com._4paradigm.fesql.`type`.TypeOuterClass.Type
import com._4paradigm.fesql.spark._
import com._4paradigm.fesql.spark.utils.SparkColumnUtil
import com._4paradigm.fesql.vm.{PhysicalSimpleProjectNode, SourceType}
import org.slf4j.LoggerFactory
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{BooleanType, DateType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, TimestampType}
import com._4paradigm.fesql.node.{DataType => FesqlDataType}
import org.apache.spark.sql.functions.to_date

import scala.collection.JavaConverters._

object SimpleProjectPlan {

  private val logger = LoggerFactory.getLogger(this.getClass)

  /**
   * @param ctx
   * @param node
   * @param inputs
   * @return
   */
  def gen(ctx: PlanContext, node: PhysicalSimpleProjectNode, inputs: Seq[SparkInstance]): SparkInstance = {
    val inputInstance = inputs.head
    val inputDf = inputInstance.getDf(ctx.getSparkSession)

    // Get the output column names from output schema
    val outputColNameList = node.GetOutputSchema().asScala.map(col =>
      col.getName
    ).toList

    val outputColTypeList = node.GetOutputSchema().asScala.map(col =>
      col.getType
    ).toList

    // Get the select column indexes from node
    val columnSourceList = node.getProject_().getColumn_sources_()

    val selectColList = (0 until columnSourceList.size()).map(i => {

      val columnSource = columnSourceList.get(i);

      columnSource.`type`() match {
        case SourceType.kSourceColumn => {
          // Resolved the column index to get column and rename
          val colIndex = SparkColumnUtil.resolveColumnIndex(columnSource.schema_idx(), columnSource.column_idx(), node.GetProducer(0))
          SparkColumnUtil.getCol(inputDf, colIndex).alias(outputColNameList(i));
        }
        case SourceType.kSourceConst => {
          val const_value = columnSource.const_value()
          val outputColName = outputColNameList(i)

          // Get constant value and case type and rename
          outputColTypeList(i) match {
            // TODO: Support cast other type to bool
            case Type.kInt16 => lit(const_value.GetAsInt16()).cast(ShortType).alias(outputColName)
            case Type.kInt32 => lit(const_value.GetAsInt32()).cast(IntegerType).alias(outputColName)
            case Type.kInt64 => lit(const_value.GetAsInt64()).cast(LongType).alias(outputColName)
            case Type.kFloat => lit(const_value.GetAsFloat()).cast(FloatType).alias(outputColName)
            case Type.kDouble => lit(const_value.GetAsDouble()).cast(DoubleType).alias(outputColName)
            case Type.kBool => {
              const_value.GetDataType() match {
                case FesqlDataType.kInt16 | FesqlDataType.kInt32 | FesqlDataType.kInt64 => lit(const_value.GetInt()).cast(BooleanType).alias(outputColName)
                case FesqlDataType.kFloat | FesqlDataType.kDouble => null
                case FesqlDataType.kVarchar => null
              }
            }
            case Type.kDate => {
              const_value.GetDataType() match {
                case FesqlDataType.kInt16 | FesqlDataType.kInt32 | FesqlDataType.kInt64 => lit(const_value.GetSmallInt()).cast(DateType).alias(outputColName)
                case FesqlDataType.kFloat | FesqlDataType.kDouble => null
                case FesqlDataType.kVarchar => to_date(lit(const_value.GetAsString()), "yyyy-MM-dd").alias(outputColName)
              }
            }
            case Type.kTimestamp => {
              const_value.GetDataType() match {
                case FesqlDataType.kInt16 | FesqlDataType.kInt32 | FesqlDataType.kInt64 => lit(const_value.GetAsInt64()).divide(1000).cast(TimestampType).alias(outputColName)
                case FesqlDataType.kFloat | FesqlDataType.kDouble => null
                case FesqlDataType.kVarchar => null
                case _ => throw new IllegalArgumentException(s"FESQL type from ${const_value.GetDataType()} to ${outputColTypeList(i)} is not supported")
              }
            }
            case Type.kVarchar => lit(const_value.GetAsString()).cast(StringType).alias(outputColName)
            case _ => throw new IllegalArgumentException(s"FESQL type ${outputColTypeList(i)} not supported")
          }

        }
      }

    })

    // Use Spark DataFrame to select columns
    val result = SparkColumnUtil.setDataframeNullable(inputDf.select(selectColList:_*), true)

    SparkInstance.fromDataFrame(result)
  }

}
