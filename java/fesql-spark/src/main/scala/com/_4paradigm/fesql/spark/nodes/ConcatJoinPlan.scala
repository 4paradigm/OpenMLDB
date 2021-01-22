package com._4paradigm.fesql.spark.nodes

import com._4paradigm.fesql.common.FesqlException
import com._4paradigm.fesql.node.JoinType
import com._4paradigm.fesql.spark._
import com._4paradigm.fesql.spark.utils.{SparkColumnUtil, SparkUtil}
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

    // TODO: Get index column name from child node instead of current node's index info
    val indexName = ctx.getIndexInfo(node.GetProducer(0).GetNodeId()).indexColumnName

    // Note that this is exception to use "getDfWithIndex" instead of "getSparkDfConsideringIndex" because ConcatJoin has not index flag but request input dataframe with index
    val leftDf: DataFrame = left.getDfWithIndex
    val rightDf: DataFrame = right.getDfWithIndex

    // Check if we can use native last join
    val supportNativeLastJoin = SparkUtil.supportNativeLastJoin(joinType, false)

    // Use the last column to join which are always index column in concat join node
    // Use left join or native last join
    val resultDf = if (supportNativeLastJoin) {
      leftDf.join(rightDf, SparkColumnUtil.getColumnFromIndex(leftDf, -1) === SparkColumnUtil.getColumnFromIndex(rightDf, -1), "last")
    } else {
      leftDf.join(rightDf, SparkColumnUtil.getColumnFromIndex(leftDf, leftDf.schema.size-1) === SparkColumnUtil.getColumnFromIndex(rightDf, rightDf.schema.size-1), "left")
    }

    // Drop the index column, this will drop two columns with the same index name
    logger.info("Drop the index column %s for output dataframe".format(indexName))
    val outputDf = resultDf.drop(indexName)

    SparkInstance.createConsideringIndex(ctx, node.GetNodeId(), outputDf)
  }

}