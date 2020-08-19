package com._4paradigm.fesql.offline.nodes

import com._4paradigm.fesql.offline.nodes.RowProjectPlan.ProjectConfig
import com._4paradigm.fesql.offline.utils.{AutoDestructibleIterator, FesqlUtil, SparkColumnUtil}
import com._4paradigm.fesql.offline.{PlanContext, SparkInstance, SparkRowCodec}
import com._4paradigm.fesql.vm.{PhysicalGroupAggrerationNode, PhysicalGroupNode}
import org.apache.spark.sql.{Column, Row}
import com._4paradigm.fesql.offline.JITManager
import scala.collection.mutable


object GroupByAggregationPlan {

  def gen(ctx: PlanContext, node: PhysicalGroupAggrerationNode, input: SparkInstance): SparkInstance = {
    val inputDf = input.getDf(ctx.getSparkSession)

    // Get schema info
    val inputSchemaSlices = FesqlUtil.getOutputSchemaSlices(node.GetProducer(0))
    val outputSchemaSlices = FesqlUtil.getOutputSchemaSlices(node)
    val outputSchema = FesqlUtil.getSparkSchema(node.GetOutputSchema())

    // spark closure
    val projectConfig = ProjectConfig(
      functionName = node.GetFnName(),
      moduleTag = ctx.getTag,
      moduleBroadcast = ctx.getModuleBufferBroadcast,
      inputSchemaSlices = inputSchemaSlices,
      outputSchemaSlices = outputSchemaSlices
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


      val resultIter = iter.map(row => {

        val nativeInputRow = encoder.encode(row)

        // call native compute
        //val outputNativeRow = CoreAPI.RowProject(fn, nativeInputRow, false)

        // call decode
        //decoder.decode(outputNativeRow, outputArr)
        decoder.decode(nativeInputRow, outputArr)

        // release swig jni objects
        nativeInputRow.delete()
        //outputNativeRow.delete()

        Row.fromSeq(outputArr) // can reuse backed array
      })


      AutoDestructibleIterator(resultIter) {
        encoder.delete()
        decoder.delete()
      }

    })

    SparkInstance.fromRDD(outputSchema, resultRDD)

  }
}
