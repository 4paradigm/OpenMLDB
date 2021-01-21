package com._4paradigm.fesql.spark.nodes

import com._4paradigm.fesql.node.{ExprListNode, ExprNode}
import com._4paradigm.fesql.spark._
import com._4paradigm.fesql.spark.nodes.JoinPlan.JoinConditionUDF
import com._4paradigm.fesql.spark.utils.SparkColumnUtil.getColumnFromIndex
import com._4paradigm.fesql.spark.utils.{FesqlUtil, SparkColumnUtil}
import com._4paradigm.fesql.vm.PhysicalFilterNode
import org.apache.spark.sql.{Column, functions}

import scala.collection.mutable


object FilterPlan {

  def gen(ctx: PlanContext, node: PhysicalFilterNode, input: SparkInstance): SparkInstance = {
    val inputDf = input.getDf()

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
        // TODO: Add tests to check null equality in Spark and FESQL core
        outputDf = outputDf.where(leftColumn === rightColumn)
      }
    }

    // Handle non-equal condiction
    if (filter.condition() != null) {
      val regName = "FESQL_FILTER_CONDITION_" + node.filter().condition().fn_info().fn_name()
      val conditionUDF = new JoinConditionUDF(
        functionName = filter.fn_info().fn_name(),
        inputSchemaSlices = inputSchemaSlices,
        outputSchema = filter.fn_info().fn_schema(),
        moduleTag = ctx.getTag,
        moduleBroadcast = ctx.getSerializableModuleBuffer
      )
      ctx.getSparkSession.udf.register(regName, conditionUDF)

      val allColumns = SparkColumnUtil.getColumnsFromDataFrame(inputDf)
      val allColWrap = functions.struct(allColumns:_*)
      val condictionCol = functions.callUDF(regName, allColWrap)

      outputDf = outputDf.where(condictionCol)
    }

    SparkInstance.createWithNodeIndexInfo(ctx, node.GetNodeId(), outputDf)
  }

}
