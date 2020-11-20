package com._4paradigm.fesql.spark.nodes

import com._4paradigm.fesql.spark._
import com._4paradigm.fesql.spark.utils.SparkColumnUtil
import com._4paradigm.fesql.vm.{PhysicalSimpleProjectNode, SourceType}
import org.slf4j.LoggerFactory
import org.apache.spark.sql.types.{BooleanType, DateType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, TimestampType}
import com._4paradigm.fesql.node.{DataType => FesqlDataType}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.to_date
import org.apache.spark.sql.functions.to_timestamp

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
    val inputDfTypes = inputDf.schema.fields.toList

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
          var sparkCol = SparkColumnUtil.getColumnFromIndex(inputDf, colIndex)
          var sparkColType = inputDfTypes(colIndex).dataType

          val castTypes = columnSource.cast_types()
          for(i <- 0 until castTypes.size()) {
            val castType = castTypes.get(i)

            castType match {
              case FesqlDataType.kInt16 => {
                sparkCol = sparkCol.cast(ShortType)
                sparkColType = ShortType
              }
              case FesqlDataType.kInt32 => {
                sparkColType = IntegerType

                sparkColType match {
                  case DateType => {
                    sparkCol = sparkCol.cast(IntegerType)
                  }
                  case _ => {
                    sparkCol = sparkCol.cast(IntegerType)
                  }
                }
              }
              case FesqlDataType.kInt64 => {
                sparkCol = sparkCol.cast(LongType)
                sparkColType = LongType
              }
              case FesqlDataType.kFloat => {
                sparkCol = sparkCol.cast(FloatType)
                sparkColType = FloatType
              }
              case FesqlDataType.kDouble => {
                sparkCol = sparkCol.cast(DoubleType)
                sparkColType = DoubleType
              }
              case FesqlDataType.kBool => {
                sparkCol = sparkCol.cast(BooleanType)
                sparkColType = BooleanType
              }
              case FesqlDataType.kDate => {
                sparkCol = sparkColType match {
                  case DateType => {
                    sparkCol
                  }
                  case StringType => {
                    // TODO: may support "yyyyMMdd", "yyyy-MM-dd HH:mm:ss"
                    to_date(sparkCol, "yyyy-MM-dd")
                  }
                  case TimestampType => {
                    sparkCol
                  }
                }
                sparkColType = DateType
              }
              case FesqlDataType.kTimestamp => {
                sparkCol = sparkColType match {
                  case ShortType | IntegerType | LongType | FloatType | DoubleType | DateType => {
                    sparkCol.cast(TimestampType)
                  }
                  case BooleanType => {
                    // TODO: Got "java.lang.Integer cannot be cast to java.lang.Long" if cast to timestamp directly
                    sparkCol.cast(LongType).cast(TimestampType)
                  }
                  case TimestampType => {
                    sparkCol
                  }
                  case StringType => {
                    to_timestamp(sparkCol) // format "yyyy/MM/dd HH:mm:ss"
                  }
                }
                sparkColType = TimestampType
              }
              case FesqlDataType.kVarchar => {
                sparkCol = sparkCol.cast(StringType)
                sparkColType = StringType
              }
            }
          }

          sparkCol.alias(outputColNameList(i))
        }
        case SourceType.kSourceConst => {
          val const_value = columnSource.const_value()
          val outputColName = outputColNameList(i)
          ConstProjectPlan.getConstCol(outputColTypeList(i), const_value, outputColName)
        }
      }

    })

    // Use Spark DataFrame to select columns
    val result = SparkColumnUtil.setDataframeNullable(inputDf.select(selectColList:_*), true)
    SparkInstance.fromDataFrame(result)

  }

}
