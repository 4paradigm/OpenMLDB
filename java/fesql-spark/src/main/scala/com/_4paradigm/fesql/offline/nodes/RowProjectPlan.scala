package com._4paradigm.fesql.offline.nodes

import com._4paradigm.fesql.common.{JITManager, SerializableByteBuffer}
import com._4paradigm.fesql.offline._
import com._4paradigm.fesql.offline.utils.{AutoDestructibleIterator, FesqlUtil}
import com._4paradigm.fesql.vm.{CoreAPI, FeSQLJITWrapper, PhysicalTableProjectNode}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType

object RowProjectPlan {

  /**
   * @param ctx
   * @param node
   * @param inputs
   * @return
   */
  def gen(ctx: PlanContext, node: PhysicalTableProjectNode, inputs: Seq[SparkInstance]): SparkInstance = {
    val inputInstance = inputs.head
    val inputRDD = inputInstance.getRDD

    val inputSchemaSlices = FesqlUtil.getOutputSchemaSlices(node.GetProducer(0))
    val outputSchemaSlices = FesqlUtil.getOutputSchemaSlices(node)
    val outputSchema = FesqlUtil.getSparkSchema(node.GetOutputSchema())

    // spark closure
    val limitCnt = node.GetLimitCnt
    val projectConfig = ProjectConfig(
      functionName = node.project().fn_name(),
      moduleTag = ctx.getTag,
      moduleBroadcast = ctx.getModuleBufferBroadcast,
      inputSchemaSlices = inputSchemaSlices,
      outputSchemaSlices = outputSchemaSlices
    )

    // project implementation
    val projectRDD = inputRDD.mapPartitions(iter => {
      val limitIter = if (limitCnt > 0) iter.take(limitCnt) else iter

      // ensure worker native
      val tag = projectConfig.moduleTag
      val buffer = projectConfig.moduleBroadcast.value.getBuffer
      JITManager.initJITModule(tag, buffer)
      val jit = JITManager.getJIT(tag)

      projectIter(limitIter, jit, projectConfig)
    })

    SparkInstance.fromRDD(outputSchema, projectRDD)
  }


  def projectIter(inputIter: Iterator[Row], jit: FeSQLJITWrapper, config: ProjectConfig): Iterator[Row] = {
    // reusable output row inst
    val outputFields = config.outputSchemaSlices.map(_.size).sum
    val outputArr = Array.fill[Any](outputFields)(null)

    val fn = jit.FindFunction(config.functionName)

    val encoder = new SparkRowCodec(config.inputSchemaSlices)
    val decoder = new SparkRowCodec(config.outputSchemaSlices)

    val resultIter = inputIter.map(row =>
      projectRow(row, fn, encoder, decoder, outputArr)
    )

    AutoDestructibleIterator(resultIter) {
      encoder.delete()
      decoder.delete()
    }
  }


  def projectRow(row: Row, fn: Long,
                 encoder: SparkRowCodec,
                 decoder: SparkRowCodec,
                 outputArr: Array[Any]): Row = {
    // call encode
    val nativeInputRow = encoder.encode(row)

    // call native compute
    val outputNativeRow = CoreAPI.RowProject(fn, nativeInputRow, false)

    // call decode
    decoder.decode(outputNativeRow, outputArr)

    // release swig jni objects
    nativeInputRow.delete()
    outputNativeRow.delete()

    Row.fromSeq(outputArr)  // can reuse backed array
  }


  // spark closure class
  case class ProjectConfig(functionName: String,
                           moduleTag: String,
                           moduleBroadcast: Broadcast[SerializableByteBuffer],
                           inputSchemaSlices: Array[StructType],
                           outputSchemaSlices: Array[StructType],
                           inputSchema: StructType = null)

}
