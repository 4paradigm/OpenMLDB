package com._4paradigm.fesql.offline.nodes

import com._4paradigm.fesql.offline.{FeSQLException, PlanContext, SparkInstance}
import com._4paradigm.fesql.vm.PhysicalDataProviderNode

object DataProviderPlan {

  def gen(ctx: PlanContext, node: PhysicalDataProviderNode, inputs: Seq[SparkInstance]): SparkInstance = {
    val tableName = node.GetName()
    val df = ctx.getDataFrame(tableName).getOrElse {
      throw new FeSQLException(s"Input table $tableName not found")
    }
    SparkInstance.fromDataFrame(tableName, df)
  }
}
