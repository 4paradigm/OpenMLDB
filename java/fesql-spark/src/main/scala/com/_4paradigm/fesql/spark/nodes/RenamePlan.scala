package com._4paradigm.fesql.spark.nodes

import com._4paradigm.fesql.spark._
import com._4paradigm.fesql.vm.{PhysicalRenameNode}

object RenamePlan {

  def gen(ctx: PlanContext, node: PhysicalRenameNode, input: SparkInstance): SparkInstance = {
    SparkInstance.fromDataFrame(input.getDf(ctx.getSparkSession))
  }

}
