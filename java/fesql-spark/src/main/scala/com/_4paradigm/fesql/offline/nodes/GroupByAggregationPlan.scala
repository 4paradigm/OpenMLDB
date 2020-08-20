package com._4paradigm.fesql.offline.nodes

import com._4paradigm.fesql.offline.nodes.RowProjectPlan.ProjectConfig
import com._4paradigm.fesql.offline.utils.FesqlUtil
import com._4paradigm.fesql.offline.{PlanContext, SparkInstance, SparkRowCodec}
import com._4paradigm.fesql.vm.{CoreAPI, PhysicalGroupAggrerationNode}
import org.apache.spark.sql.Row
import com._4paradigm.fesql.offline.JITManager


object GroupByAggregationPlan {

  def gen(ctx: PlanContext, node: PhysicalGroupAggrerationNode, input: SparkInstance): SparkInstance = {
    val inputDf = input.getDf(ctx.getSparkSession)

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
    val resultRDD = inputDf.rdd.mapPartitions(iter => {
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

      val memTableName = "temp_table"
      val meTableDb = "temp_db"
      val schema = FesqlUtil.getFeSQLSchema(projectConfig.inputSchema)

      if (iter.isEmpty) {
        Option[Row](null).toIterator
      } else {
        // TODO: Delete the native schema and memTableHandler pointer
        // Create memory table and insert encoded rows
        val memTableHandler = CoreAPI.NewMemTableHandler(memTableName, meTableDb, schema)
        iter.foreach(row => {
          val nativeInputRow = encoder.encode(row)
          CoreAPI.AddRowToMemTable(memTableHandler, nativeInputRow)
        })

        // Run group by aggregation
        val outputFesqlRow = CoreAPI.GroupbyProject(fn, memTableHandler)
        decoder.decode(outputFesqlRow, outputArr)
        Option(Row.fromSeq(outputArr)).toIterator
      }

    })

    SparkInstance.fromRDD(outputSchema, resultRDD)

  }

}
