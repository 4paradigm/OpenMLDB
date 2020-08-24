package com._4paradigm.fesql.offline.nodes

import com._4paradigm.fesql.offline._
import com._4paradigm.fesql.vm.PhysicalLimitNode

object LimitPlan {

  def gen(ctx: PlanContext, node: PhysicalLimitNode, input: SparkInstance): SparkInstance = {
    SparkInstance.fromDataFrame(input.getDf(ctx.getSparkSession).limit(node.GetLimitCnt()))
  }

}
