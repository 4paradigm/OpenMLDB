package com._4paradigm.fesql.offline.nodes

import java.nio.ByteBuffer

import com._4paradigm.fesql.offline._
import com._4paradigm.fesql.vm.{CoreAPI, FeSQLJITWrapper, PhysicalTableProjectNode}
import com._4paradigm.fesql.codec.{Row => NativeRow}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import sun.nio.ch.DirectBuffer

object RowProjectPlan {

  def gen(ctx: PlanContext, node: PhysicalTableProjectNode, inputs: Seq[SparkInstance]): SparkInstance = {
    val inputInstance = inputs.head
    val inputRDD = inputInstance.getRDD
    val outputSchema = FesqlUtil.getSparkSchema(node.GetOutputSchema())

    // spark closure
    val projectConfig = ProjectConfig(
      functionName = node.GetFnName(),
      moduleTag = ctx.getTag,
      moduleBroadcast = ctx.getModuleBufferBroadcast,
      inputSchema = inputInstance.getSchema,
      outputSchema = outputSchema
    )

    // project implementation
    val projectRDD = inputRDD.mapPartitions(iter => {
      // ensure worker native
      val tag = projectConfig.moduleTag
      val buffer = projectConfig.moduleBroadcast.value.getBuffer
      JITManager.initJITModule(tag, buffer)
      val jit = JITManager.getJIT(tag)

      projectIter(iter, jit, projectConfig)
    })

    SparkInstance.fromRDD(outputSchema, projectRDD)
  }


  def projectIter(inputIter: Iterator[Row], jit: FeSQLJITWrapper, config: ProjectConfig): Iterator[Row] = {
    // reusable output row inst
    val outputArr = Array.fill[Any](config.outputSchema.size)(null)

    val fn = jit.FindFunction(config.functionName)

    // TODO: these objects are now leaked
    val encoder = new SparkRowCodec(config.inputSchema)
    val decoder = new SparkRowCodec(config.outputSchema)

    // try best to avoid native buffer allocation
    var nativeBuf = ByteBuffer.allocateDirect(1024)

    inputIter.map(row => {
      // ensure direct buffer
      val rowSize = encoder.getNativeRowSize(row)
      if (rowSize > nativeBuf.capacity()) {
        nativeBuf.asInstanceOf[DirectBuffer].cleaner().clean()  // gc
        nativeBuf = ByteBuffer.allocateDirect(rowSize)
      }

      projectRow(row, nativeBuf, fn, encoder, decoder, outputArr)
    })
  }


  def projectRow(row: Row, inputBuffer: ByteBuffer, fn: Long,
                 encoder: SparkRowCodec, decoder: SparkRowCodec, outputArr: Array[Any]): Row = {
    // call encode
    encoder.encode(row, inputBuffer)

    // call native compute
    val nativeInputRow = new NativeRow(inputBuffer)
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
                           inputSchema: StructType,
                           outputSchema: StructType)

}
