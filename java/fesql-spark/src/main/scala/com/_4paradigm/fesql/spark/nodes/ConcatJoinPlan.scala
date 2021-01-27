package com._4paradigm.fesql.spark.nodes

import com._4paradigm.fesql.common.FesqlException
import com._4paradigm.fesql.node.JoinType
import com._4paradigm.fesql.spark._
import com._4paradigm.fesql.spark.utils.{NodeIndexType, SparkColumnUtil, SparkUtil}
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

    val indexName = ctx.getIndexInfo(node.GetNodeId()).indexColumnName

    // Note that this is exception to use "getDfWithIndex" instead of "getSparkDfConsideringIndex" because ConcatJoin has not index flag but request input dataframe with index
    val leftDf: DataFrame = left.getDfWithIndex
    val rightDf: DataFrame = right.getDfWithIndex

    // Use left join or native last join and check if we can use native last join or not
    val resultDf = if (ctx.getConf.enableConcatJoinWithLastJoin && SparkUtil.supportNativeLastJoin(joinType, false)) {
      logger.info("Enable concat join with last join and support native last join")
      leftDf.join(rightDf, leftDf(indexName) === rightDf(indexName), "last")
    } else {
      // Should use inner join instead of left join so that Spark weill merge all join into single stage
      leftDf.join(rightDf, leftDf(indexName) === rightDf(indexName), "inner")
    }

    val nodeIndexType = ctx.getIndexInfo(node.GetNodeId()).nodeIndexType

    // Drop the index column, this will drop two columns with the same index name
    val outputDf = nodeIndexType match {
      case NodeIndexType.SourceConcatJoinNode => {
        logger.info("Drop all the index column %s for source concat join node".format(indexName))
        resultDf.drop(indexName)
      }
      case NodeIndexType.InternalConcatJoinNode => {
        logger.info("Drop the index column %s for internal concat join node from left dataframe".format(indexName))
        resultDf.drop(leftDf(indexName))
      }
      case _ => throw new FesqlException("Handle unsupported concat join node index type: %s".format(nodeIndexType))
    }

    SparkInstance.createConsideringIndex(ctx, node.GetNodeId(), outputDf)
  }

}