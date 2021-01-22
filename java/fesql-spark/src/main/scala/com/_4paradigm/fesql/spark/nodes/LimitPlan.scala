package com._4paradigm.fesql.spark.nodes

import com._4paradigm.fesql.spark._
import com._4paradigm.fesql.vm.PhysicalLimitNode

object LimitPlan {

  def gen(ctx: PlanContext, node: PhysicalLimitNode, input: SparkInstance): SparkInstance = {
    val outputDf = input.getDfConsideringIndex(ctx, node.GetNodeId()).limit(node.GetLimitCnt())

    SparkInstance.createConsideringIndex(ctx, node.GetNodeId(), outputDf)
  }

}
