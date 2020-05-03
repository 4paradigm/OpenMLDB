package com._4paradigm.fesql.offline.nodes

import com._4paradigm.fesql.offline.{PlanContext, SparkColumnUtil, SparkInstance}
import com._4paradigm.fesql.vm.{PhysicalWindowAggrerationNode}
import org.apache.spark.sql.Column

import scala.collection.mutable

object GroupAndSortPlan {

  def gen(ctx: PlanContext, node: PhysicalWindowAggrerationNode, input: SparkInstance): SparkInstance = {
    val inputDf = input.getDf(ctx.getSparkSession)
    val windowOp = node.getWindow_();
    val groupByExprs = windowOp.getGroup_().getGroups_()

    val groupByCols = mutable.ArrayBuffer[Column]()
    for (i <- 0 until groupByExprs.GetChildNum()) {
      val expr = groupByExprs.GetChild(i)
      groupByCols += SparkColumnUtil.resolve(expr, inputDf, ctx)
    }

    val partitions = ctx.getConf("fesql.group.partitions", 0)
    val groupedDf = if (partitions > 0) {
      inputDf.repartition(partitions, groupByCols: _*)
    } else {
      inputDf.repartition(groupByCols: _*)
    }

    val orderExprs = windowOp.getSort_().getOrders_().GetOrderBy()
    val orderByCols = mutable.ArrayBuffer[Column]()
    for (i <- 0 until orderExprs.GetChildNum()) {
      val expr = orderExprs.GetChild(i)
      val column = SparkColumnUtil.resolve(expr, inputDf, ctx)
      if (windowOp.getSort_().getOrders_().getIs_asc_()) {
        orderByCols += column.asc
      } else {
        orderByCols += column.desc
      }
    }
    val sortedDf = groupedDf.sortWithinPartitions(groupByCols ++ orderByCols: _*)
    SparkInstance.fromDataFrame(sortedDf)
  }
}