package com._4paradigm.fesql.spark.nodes

import com._4paradigm.fesql.common.FesqlException
import com._4paradigm.fesql.node.JoinType
import com._4paradigm.fesql.spark._
import com._4paradigm.fesql.spark.utils.SparkUtil
import com._4paradigm.fesql.vm.PhysicalJoinNode
import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory


object ConcatJoinPlan {

  private val logger = LoggerFactory.getLogger(this.getClass)

  def gen(ctx: PlanContext, node: PhysicalJoinNode, left: SparkInstance, right: SparkInstance): SparkInstance = {
    // Check join type
    val joinType = node.join().join_type()
    if (joinType != JoinType.kJoinTypeConcat) {
      throw new FesqlException(s"Concat join type $joinType not supported")
    }

    val spark = ctx.getSparkSession

    // Add the index column for left and right dataframe
    val indexName = "__JOIN_INDEX__-" + System.currentTimeMillis()
    logger.info("Add the index column %s for left and right dataframe".format(indexName))
    val leftDf: DataFrame = SparkUtil.addIndexColumn(spark, left.getDf(spark), indexName)
    val rightDf: DataFrame = SparkUtil.addIndexColumn(spark, right.getDf(spark), indexName)

    // Use left join or native last join
    // Check if we can use native last join
    val supportNativeLastJoin = SparkUtil.supportNativeLastJoin(joinType, false)
    val resultDf = if (supportNativeLastJoin) {
      leftDf.join(rightDf, leftDf(indexName) === rightDf(indexName), "last")
    } else {
      leftDf.join(rightDf, leftDf(indexName) === rightDf(indexName), "left")
    }

    // Drop the index column
    logger.info("Drop the index column %s for output dataframe".format(indexName))
    SparkInstance.fromDataFrame(resultDf.drop(indexName))
  }

}