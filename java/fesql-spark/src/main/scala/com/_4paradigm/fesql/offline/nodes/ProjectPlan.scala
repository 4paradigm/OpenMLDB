package com._4paradigm.fesql.offline.nodes

import java.nio.ByteBuffer

import com._4paradigm.fesql.offline._
import com._4paradigm.fesql.vm.{DummyRunner, PhysicalProjectNode}
import com._4paradigm.fesql.codec.{Row => NativeRow}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import sun.nio.ch.DirectBuffer

object ProjectPlan {

  def gen(ctx: PlanContext, node: PhysicalProjectNode, inputs: Seq[SparkInstance]): SparkInstance = {
    val inputInstance = inputs.head
    val inputRDD = inputInstance.getRDD
    val outputSchema = SparkUtils.getSparkSchema(node.GetOutputSchema())

    // spark closure
    val projectConfig = ProjectConfig(
      functionName = node.GetFnName(),
      moduleTag = ctx.getTag,
      moduleBroadcast = ctx.getModuleBufferBroadcast,
      inputSchema = inputInstance.getSchema,
      outputSchema = outputSchema
    )

    // project implementation
    val projectRDD = inputRDD.mapPartitions(iter =>
      projectIter(iter, projectConfig)
    )

    SparkInstance.fromRDD(outputSchema, projectRDD)
  }


  def projectIter(inputIter: Iterator[Row], config: ProjectConfig): Iterator[Row] = {
    // ensure worker native
    FeSqlLibrary.init()

    // ensure worker side module
    if (! JITManager.hasModule(config.moduleTag)) {
      val buffer = config.moduleBroadcast.value.getBuffer
      JITManager.initModule(config.moduleTag, buffer)
    }

    // reusable output row inst
    val outputArr = Array.fill[Any](config.outputSchema.size)(null)

    // TODO: these objects are now leaked
    val jit = JITManager.getJIT
    val fn = jit.FindFunction(config.functionName)
    val runner = new DummyRunner()
    val encoder = new RowCodec(config.inputSchema)
    val decoder = new RowCodec(config.outputSchema)

    def doCompute(row: NativeRow): NativeRow = {
      runner.RunRowProject(fn, row)
    }

    // try best to avoid native buffer allocation
    var nativeBuf = ByteBuffer.allocateDirect(1024)

    inputIter.map(row => {
      // ensure direct buffer
      val rowSize = encoder.getNativeRowSize(row)
      if (rowSize > nativeBuf.capacity()) {
        nativeBuf.asInstanceOf[DirectBuffer].cleaner().clean()  // gc
        nativeBuf = ByteBuffer.allocateDirect(rowSize)
      }

      projectRow(row, nativeBuf, doCompute,
        encoder, decoder, outputArr)
    })
  }


  def projectRow(row: Row, inputBuffer: ByteBuffer, compute: NativeRow => NativeRow,
                 encoder: RowCodec, decoder: RowCodec, outputArr: Array[Any]): Row = {
    // call encode
    encoder.encode(row, inputBuffer)

    // call native compute
    val nativeInputRow = new NativeRow(inputBuffer)
    val outputNativeRow = compute(nativeInputRow)

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
