package com._4paradigm.fesql.spark.nodes

import com._4paradigm.fesql.spark._
import com._4paradigm.fesql.vm.PhysicalLimitNode

object LimitPlan {

  def gen(ctx: PlanContext, node: PhysicalLimitNode, input: SparkInstance): SparkInstance = {
    SparkInstance.fromDataFrame(input.getDf(ctx.getSparkSession).limit(node.GetLimitCnt()))
  }

}
