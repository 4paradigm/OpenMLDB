/*
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

package com._4paradigm.openmldb.batch.nodes

import com._4paradigm.hybridse.node.ExprListNode
import com._4paradigm.hybridse.vm.PhysicalFilterNode
import com._4paradigm.openmldb.batch.nodes.JoinPlan.JoinConditionUDF
import com._4paradigm.openmldb.batch.utils.{ExternalUdfUtil, HybridseUtil, SparkColumnUtil}
import com._4paradigm.openmldb.batch.{PlanContext, SparkInstance}
import com._4paradigm.openmldb.sdk.impl.SqlClusterExecutor
import org.apache.spark.sql.functions
import org.slf4j.LoggerFactory

object FilterPlan {
  private val logger = LoggerFactory.getLogger(this.getClass)

  def gen(ctx: PlanContext, node: PhysicalFilterNode, input: SparkInstance): SparkInstance = {
    val inputDf = input.getDfConsideringIndex(ctx, node.GetNodeId())

    var outputDf = inputDf
    val isUnsafeRowOpt = ctx.getConf.enableUnsafeRowOptimization
    val inputSchemaSlices = HybridseUtil.getOutputSchemaSlices(node, isUnsafeRowOpt)
    val filter = node.filter().condition()

    // Handle equal condiction
    if (node.filter().left_key() != null && node.filter().left_key().getKeys_ != null) {
      val leftKeys: ExprListNode = node.filter().left_key().getKeys_
      val rightKeys: ExprListNode = node.filter().right_key().getKeys_

      val keyNum = leftKeys.GetChildNum
      for (i <- 0 until keyNum) {
        val leftColumn = SparkColumnUtil.resolveExprNodeToColumn(leftKeys.GetChild(i), node.GetProducer(0), inputDf)
        val rightColumn = SparkColumnUtil.resolveExprNodeToColumn(rightKeys.GetChild(i), node.GetProducer(0), inputDf)
        // TODO: Add tests to check null equality in Spark and HybridSE core
        outputDf = outputDf.where(leftColumn === rightColumn)
      }
    }

    // Handle non-equal condiction
    if (filter.condition() != null) {
      val regName = "SPARKFE_FILTER_CONDITION_" + node.filter().condition().fn_info().fn_name()

      var externalFunMap = Map[String, com._4paradigm.openmldb.proto.Common.ExternalFun]()
      val openmldbSession = ctx.getOpenmldbSession
      if (ctx.getConf.openmldbZkCluster.nonEmpty && ctx.getConf.openmldbZkRootPath.nonEmpty
        && openmldbSession != null && openmldbSession.openmldbCatalogService != null) {
        externalFunMap = openmldbSession.openmldbCatalogService.getExternalFunctionsMap()
      }
      val conditionUDF = new JoinConditionUDF(
        functionName = filter.fn_info().fn_name(),
        inputSchemaSlices = inputSchemaSlices,
        outputSchema = filter.fn_info().fn_schema(),
        moduleTag = ctx.getTag,
        moduleBroadcast = ctx.getSerializableModuleBuffer,
        hybridseJsdkLibraryPath = ctx.getConf.openmldbJsdkLibraryPath,
        ctx.getConf.enableUnsafeRowOptimization,
        externalFunMap,
        ctx.getConf.taskmanagerExternalFunctionDir,
        ctx.getSparkSession.conf.get("spark.master").equalsIgnoreCase("yarn")
      )
      ctx.getSparkSession.udf.register(regName, conditionUDF)

      val allColumns = SparkColumnUtil.getColumnsFromDataFrame(inputDf)
      val allColWrap = functions.struct(allColumns: _*)
      val condictionCol = functions.callUDF(regName, allColWrap)

      outputDf = outputDf.where(condictionCol)
    }

    SparkInstance.createConsideringIndex(ctx, node.GetNodeId(), outputDf)
  }

}
