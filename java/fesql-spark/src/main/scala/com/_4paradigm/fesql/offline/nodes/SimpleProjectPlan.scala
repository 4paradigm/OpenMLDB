package com._4paradigm.fesql.offline.nodes

import com._4paradigm.fesql.offline._
import com._4paradigm.fesql.offline.utils.FesqlUtil
import com._4paradigm.fesql.offline.utils.FesqlUtil.getSparkType
import com._4paradigm.fesql.vm.PhysicalSimpleProjectNode
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.functions.col
import scala.collection.JavaConverters._

object SimpleProjectPlan {

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

    // Get the select column names from node
    val columnSourceList = node.getProject_().getColumn_sources_()

    val selectColList = (0 until columnSourceList.size()).map(i => {
      // Take the select column and rename with output schema
      col(columnSourceList.get(i).column_name()).as(outputColNameList(i))
    }).toList

    // Use Spark DataFrame to select
    val result = inputDf.select(selectColList:_*)

    SparkInstance.fromDataFrame(result)
  }

}
