package com._4paradigm.fesql.offline.nodes

import com._4paradigm.fesql.offline._
import com._4paradigm.fesql.offline.utils.SparkColumnUtil
import com._4paradigm.fesql.vm.PhysicalSimpleProjectNode
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

    // Get the select column indexes from node
    val columnSourceList = node.getProject_().getColumn_sources_()

    val selectColList = (0 until columnSourceList.size()).map(i => {
      // Resolved the column index to get column and rename
      val colIndex = SparkColumnUtil.resolveColumnIndex(columnSourceList.get(i).schema_idx(), columnSourceList.get(i).column_idx(), node.GetProducer(0))

      // TODO: Handle constant value which has no corresponding col index, make sure the data type consistent

      SparkColumnUtil.getCol(inputDf, colIndex).alias(outputColNameList(i));
    })

    // Use Spark DataFrame to select columns
    val result = inputDf.select(selectColList:_*)

    SparkInstance.fromDataFrame(result)
  }

}
