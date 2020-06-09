package com._4paradigm.fesql.offline.nodes

import com._4paradigm.fesql.offline._
import com._4paradigm.fesql.vm.PhysicalSimpleProjectNode

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

    val columnSourceList = node.getProject_().getColumn_sources_()
    val selectColNameList = (0 until columnSourceList.size()).map(i => {
      columnSourceList.get(i).column_name()
    }).toList

    val result = inputDf.select(selectColNameList.head, selectColNameList.tail: _*)
    SparkInstance.fromDataFrame(result)
  }

}
