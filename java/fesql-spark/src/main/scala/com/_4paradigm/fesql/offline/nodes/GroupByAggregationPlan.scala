package com._4paradigm.fesql.offline.nodes

import com._4paradigm.fesql.offline.nodes.RowProjectPlan.ProjectConfig
import com._4paradigm.fesql.offline.utils.{FesqlUtil, SparkColumnUtil}
import com._4paradigm.fesql.offline.{PlanContext, SparkInstance, SparkRowCodec}
import com._4paradigm.fesql.vm.{CoreAPI, GroupbyInterface, PhysicalGroupAggrerationNode}
import com._4paradigm.fesql.codec.{Row => NativeRow}
import com._4paradigm.fesql.offline.JITManager
import org.apache.spark.sql.{Column, Row}
import scala.collection.mutable


object GroupByAggregationPlan {

  def gen(ctx: PlanContext, node: PhysicalGroupAggrerationNode, input: SparkInstance): SparkInstance = {
    val inputDf = input.getDf(ctx.getSparkSession)

    // Sort by partition keys
    val groupByExprs = node.getGroup_.keys()
    val groupByCols = mutable.ArrayBuffer[Column]()
    val groupIdxs = mutable.ArrayBuffer[Int]()
    for (i <- 0 until groupByExprs.GetChildNum()) {
      val expr = groupByExprs.GetChild(i)

      val colIdx = SparkColumnUtil.resolveColumnIndex(expr, node.GetProducer(0))
      groupIdxs += colIdx
      groupByCols += SparkColumnUtil.getCol(inputDf, colIdx)
    }
    val sortedInputDf = inputDf.sortWithinPartitions(groupByCols:_*)

    // Get schema info
    val inputSchemaSlices = FesqlUtil.getOutputSchemaSlices(node.GetProducer(0))
    val inputSchema = FesqlUtil.getSparkSchema(node.GetProducer(0).GetOutputSchema())
    val outputSchemaSlices = FesqlUtil.getOutputSchemaSlices(node)
    val outputSchema = FesqlUtil.getSparkSchema(node.GetOutputSchema())

    val groupKeyComparator = FesqlUtil.createGroupKeyComparator(groupIdxs.toArray, inputSchema)

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

      val fn = jit.FindFunction(projectConfig.functionName)

      val encoder = new SparkRowCodec(projectConfig.inputSchemaSlices)
      val decoder = new SparkRowCodec(projectConfig.outputSchemaSlices)

      val inputSchema = FesqlUtil.getFeSQLSchema(projectConfig.inputSchema)

      val resultList =  mutable.ArrayBuffer[Row]()

      if (!iter.isEmpty) { // Ignore the empty partition
        // Create memory table and insert encoded rows
        var groupbyInterface = new GroupbyInterface(inputSchema)
        var lastRow: Row = null
        val grouopNativeRows =  mutable.ArrayBuffer[NativeRow]()

        iter.foreach(row => {

          if (lastRow != null) { // Ignore the first row in partition
            val groupChanged = groupKeyComparator.apply(row, lastRow)
            if (groupChanged) {
              // Run group by for the same group
              val outputFesqlRow = CoreAPI.GroupbyProject(fn, groupbyInterface)
              val outputArr = Array.fill[Any](outputFields)(null)
              decoder.decode(outputFesqlRow, outputArr)
              resultList += Row.fromSeq(outputArr)

              // Reset group
              groupbyInterface.delete()
              groupbyInterface = new GroupbyInterface(inputSchema)
              // Release native rows
              grouopNativeRows.map(nativeRow => nativeRow.delete())
              grouopNativeRows.clear()
            }
          }

          // Buffer the row in the same group
          lastRow = row
          val nativeInputRow = encoder.encode(row)
          groupbyInterface.AddRow(nativeInputRow)
          grouopNativeRows += nativeInputRow
        })

        // Run group by for the last group
        val outputFesqlRow = CoreAPI.GroupbyProject(fn, groupbyInterface)
        val outputArr = Array.fill[Any](outputFields)(null)
        decoder.decode(outputFesqlRow, outputArr)
        resultList += Row.fromSeq(outputArr)

        groupbyInterface.delete()
        grouopNativeRows.map(nativeRow => nativeRow.delete())
        grouopNativeRows.clear()
      }

      resultList.toIterator
    })

    SparkInstance.fromRDD(outputSchema, resultRDD)
  }

}
