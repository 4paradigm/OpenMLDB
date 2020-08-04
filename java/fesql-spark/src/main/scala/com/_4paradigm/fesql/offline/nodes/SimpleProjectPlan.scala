package com._4paradigm.fesql.offline.nodes

import com._4paradigm.fesql.`type`.TypeOuterClass.Type
import com._4paradigm.fesql.offline._
import com._4paradigm.fesql.offline.utils.SparkColumnUtil
import com._4paradigm.fesql.vm.{PhysicalSimpleProjectNode, SourceType}
import org.slf4j.LoggerFactory
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{BooleanType, DataType, DateType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, TimestampType}

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
            case Type.kInt16 => lit(const_value.GetSmallInt()).cast(ShortType).alias(outputColName)
            case Type.kInt32 => lit(const_value.GetInt()).cast(IntegerType).alias(outputColName)
            case Type.kInt64 => lit(const_value.GetLong()).cast(LongType).alias(outputColName)
            case Type.kFloat => lit(const_value.GetFloat()).cast(FloatType).alias(outputColName)
            case Type.kDouble => lit(const_value.GetDouble()).cast(DoubleType).alias(outputColName)
            // case Type.kBool => lit(const_value.GetInt()).cast(BooleanType).alias(outputColName)
            case Type.kVarchar => lit(const_value.GetStr()).cast(StringType).alias(outputColName)
            //case Type.kDate => lit(const_value.GetSmallInt()).cast(DateType).alias(outputColName)
            //case Type.kTimestamp => lit(const_value.GetSmallInt()).cast(TimestampType).alias(outputColName)
            case _ => throw new IllegalArgumentException(s"FeSQL type ${outputColTypeList(i)} not supported")
          }

        }
      }

    })

    // Use Spark DataFrame to select columns
    val result = SparkColumnUtil.setDataframeNullable(inputDf.select(selectColList:_*), true)

    SparkInstance.fromDataFrame(result)
  }

}
