package com._4paradigm.fesql.spark.nodes

import com._4paradigm.fesql.common.FesqlException
import com._4paradigm.fesql.spark.{PlanContext, SparkInstance}
import com._4paradigm.fesql.vm.PhysicalDataProviderNode


object DataProviderPlan {

  def gen(ctx: PlanContext, node: PhysicalDataProviderNode, inputs: Seq[SparkInstance]): SparkInstance = {
    val tableName = node.GetName()
    val df = ctx.getDataFrame(tableName).getOrElse {
      throw new FesqlException(s"Input table $tableName not found")
    }

    // If limit has been set
    val outputDf = if (node.GetLimitCnt() > 0) df.limit(node.GetLimitCnt()) else df

    SparkInstance.createWithNodeIndexInfo(ctx, node.GetNodeId(), outputDf)
  }

}
