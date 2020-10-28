package com._4paradigm.fesql.spark.nodes

import com._4paradigm.fesql.node.{ExprListNode, ExprNode}
import com._4paradigm.fesql.spark._
import com._4paradigm.fesql.spark.nodes.JoinPlan.JoinConditionUDF
import com._4paradigm.fesql.spark.utils.{FesqlUtil, SparkColumnUtil}
import com._4paradigm.fesql.vm.PhysicalFliterNode
import org.apache.spark.sql.functions


object FilterPlan {

  def gen(ctx: PlanContext, node: PhysicalFliterNode, input: SparkInstance): SparkInstance = {
    val inputDf = input.getDf(ctx.getSparkSession)

    var outputDf = inputDf

    val inputSchemaSlices = FesqlUtil.getOutputSchemaSlices(node)
    val filter = node.filter().condition()

    // Handle equal condiction
    if (node.filter().left_key() != null && node.filter().left_key().getKeys_ != null) {
      val leftKeys: ExprListNode = node.filter().left_key().getKeys_
      val rightKeys: ExprListNode = node.filter().right_key().getKeys_

      val keyNum = leftKeys.GetChildNum
      for (i <- 0 until keyNum) {
        val leftColumn = SparkColumnUtil.resolveExprNodeToColumn(leftKeys.GetChild(i), node.GetProducer(0), inputDf)
        val rightColumn = SparkColumnUtil.resolveExprNodeToColumn(rightKeys.GetChild(i), node.GetProducer(0), inputDf)
        outputDf = outputDf.where(leftColumn === rightColumn)
      }
    }

    // Handle non-equal condiction
    if (filter.condition() != null) {
      val regName = "FESQL_FILTER_CONDITION_" + node.GetFnName()
      val conditionUDF = new JoinConditionUDF(
        functionName = filter.fn_info().fn_name(),
        inputSchemaSlices = inputSchemaSlices,
        outputSchema = filter.fn_info().fn_schema(),
        moduleTag = ctx.getTag,
        moduleBroadcast = ctx.getSerializableModuleBuffer
      )
      ctx.getSparkSession.udf.register(regName, conditionUDF)

      val allColWrap = functions.struct(inputDf.columns.map(inputDf.col): _*)
      val condictionCol = functions.callUDF(regName, allColWrap)

      outputDf = outputDf.where(condictionCol)
    }


    SparkInstance.fromDataFrame(outputDf)

  }

}
