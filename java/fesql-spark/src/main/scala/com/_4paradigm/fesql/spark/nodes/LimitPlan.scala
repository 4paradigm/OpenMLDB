package com._4paradigm.fesql.spark.nodes

import com._4paradigm.fesql.spark._
import com._4paradigm.fesql.vm.PhysicalLimitNode

object LimitPlan {

  def gen(ctx: PlanContext, node: PhysicalLimitNode, input: SparkInstance): SparkInstance = {
    val outputDf = input.getSparkDfConsideringIndex(ctx, node.GetNodeId()).limit(node.GetLimitCnt())

    SparkInstance.createWithNodeIndexInfo(ctx, node.GetNodeId(), outputDf)
  }

}
