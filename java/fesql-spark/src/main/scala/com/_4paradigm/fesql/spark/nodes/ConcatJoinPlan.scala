/*
 * java/fesql-spark/src/main/scala/com/_4paradigm/fesql/spark/nodes/ConcatJoinPlan.scala
 * Copyright 2021 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
    val concatJoinJoinType = ctx.getConf.concatJoinJoinType
    logger.info("Concat join may use join type: " + concatJoinJoinType)
    val resultDf = concatJoinJoinType match {
      case "inner"  => leftDf.join(rightDf, leftDf(indexName) === rightDf(indexName), "inner")
      case "left" | "left_outer" => leftDf.join(rightDf, leftDf(indexName) === rightDf(indexName), "left")
      case "last" => {
        if (SparkUtil.supportNativeLastJoin(JoinType.kJoinTypeConcat, false)) {
          leftDf.join(rightDf, leftDf(indexName) === rightDf(indexName), "last")
        } else {
          logger.info("Unsupported native last join, fallback to left join")
          leftDf.join(rightDf, leftDf(indexName) === rightDf(indexName), "left")
        }
      }
      case _ => throw new FesqlException("Unsupported concat join join type: " + joinType)
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
