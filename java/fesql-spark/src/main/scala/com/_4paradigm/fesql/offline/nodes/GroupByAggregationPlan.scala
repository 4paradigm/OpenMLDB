package com._4paradigm.fesql.offline.nodes

import com._4paradigm.fesql.offline.nodes.RowProjectPlan.ProjectConfig
import com._4paradigm.fesql.offline.utils.{FesqlUtil, SparkColumnUtil}
import com._4paradigm.fesql.offline.{PlanContext, SparkInstance, SparkRowCodec}
import com._4paradigm.fesql.vm.{CoreAPI, GroupbyInterface, PhysicalGroupAggrerationNode}
import org.apache.spark.sql.{Column, Row}
import com._4paradigm.fesql.offline.JITManager

import scala.collection.mutable


object GroupByAggregationPlan {

  def gen(ctx: PlanContext, node: PhysicalGroupAggrerationNode, input: SparkInstance): SparkInstance = {
    val inputDf = input.getDf(ctx.getSparkSession)

    // Sort by partition keys
    val groupByExprs = node.getGroup_.keys()
    val groupByCols = mutable.ArrayBuffer[Column]()
    for (i <- 0 until groupByExprs.GetChildNum()) {
      val expr = groupByExprs.GetChild(i)

      val colIdx = SparkColumnUtil.resolveColumnIndex(expr, node.GetProducer(0))
      groupByCols += SparkColumnUtil.getCol(inputDf, colIdx)
    }
    val sortedInputDf = inputDf.sortWithinPartitions(groupByCols:_*)

    // Get schema info
    val inputSchemaSlices = FesqlUtil.getOutputSchemaSlices(node.GetProducer(0))
    val inputSchema = FesqlUtil.getSparkSchema(node.GetProducer(0).GetOutputSchema())
    val outputSchemaSlices = FesqlUtil.getOutputSchemaSlices(node)
    val outputSchema = FesqlUtil.getSparkSchema(node.GetOutputSchema())

    // spark closure
    val projectConfig = ProjectConfig(
      functionName = node.project().fn_name(),
      moduleTag = ctx.getTag,
      moduleBroadcast = ctx.getModuleBufferBroadcast,
      inputSchemaSlices = inputSchemaSlices,
      outputSchemaSlices = outputSchemaSlices,
      inputSchema = inputSchema
    )

    // project implementation
    val resultRDD = sortedInputDf.rdd.mapPartitions(iter => {
      // ensure worker native
      val tag = projectConfig.moduleTag
      val buffer = projectConfig.moduleBroadcast.value.getBuffer
      JITManager.initJITModule(tag, buffer)
      val jit = JITManager.getJIT(tag)

      // reusable output row inst
      val outputFields = projectConfig.outputSchemaSlices.map(_.size).sum
      val outputArr = Array.fill[Any](outputFields)(null)

      val fn = jit.FindFunction(projectConfig.functionName)

      val encoder = new SparkRowCodec(projectConfig.inputSchemaSlices)
      val decoder = new SparkRowCodec(projectConfig.outputSchemaSlices)

      val inputSchema = FesqlUtil.getFeSQLSchema(projectConfig.inputSchema)

      if (iter.isEmpty) {
        Option[Row](null).toIterator
      } else {
        // TODO: Delete the native schema and memTableHandler pointer
        // Create memory table and insert encoded rows
        val groupbyInterface = new GroupbyInterface(inputSchema)
        iter.foreach(row => {
          val nativeInputRow = encoder.encode(row)
          groupbyInterface.AddRow(nativeInputRow)
        })

        // Run group by aggregation
        val outputFesqlRow = CoreAPI.GroupbyProject(fn, groupbyInterface)
        decoder.decode(outputFesqlRow, outputArr)
        Option(Row.fromSeq(outputArr)).toIterator
      }

    })

    SparkInstance.fromRDD(outputSchema, resultRDD)
  }

}
