package com._4paradigm.hybridsql.spark.nodes

import java.nio.ByteBuffer

import com._4paradigm.hybridse.common.{JITManager, SerializableByteBuffer}
import com._4paradigm.hybridse.vm.{CoreAPI, PhysicalTableProjectNode}
import com._4paradigm.hybridsql.spark.utils.HybridseUtil
import com._4paradigm.hybridsql.spark.{PlanContext, SparkInstance}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory


object UnsafeRowProjectPlan {

  val logger = LoggerFactory.getLogger(this.getClass)

  /**
   * @param ctx
   * @param node
   * @param inputs
   * @return
   */
  def gen(ctx: PlanContext, node: PhysicalTableProjectNode, inputs: Seq[SparkInstance]): SparkInstance = {
    val inputInstance = inputs.head

    val inputSchemaSlices = HybridseUtil.getOutputSchemaSlices(node.GetProducer(0))
    val outputSchemaSlices = HybridseUtil.getOutputSchemaSlices(node)
    val outputSchema = HybridseUtil.getSparkSchema(node.GetOutputSchema())

    // spark closure
    val projectConfig = ProjectConfig(
      functionName = node.project().fn_info().fn_name(),
      moduleTag = ctx.getTag,
      moduleBroadcast = ctx.getSerializableModuleBuffer,
      inputSchemaSlices = inputSchemaSlices,
      outputSchemaSlices = outputSchemaSlices
    )

    val internalRowRdd = inputInstance.getDfConsideringIndex(ctx, node.GetNodeId()).queryExecution.toRdd

    val projectRDD = internalRowRdd.mapPartitions(partitionIter => {
      // ensure worker native
      val tag = projectConfig.moduleTag
      val buffer = projectConfig.moduleBroadcast.getBuffer
      JITManager.initJITModule(tag, buffer)
      val jit = JITManager.getJIT(tag)
      val fn = jit.FindFunction(projectConfig.functionName)

      partitionIter.map(internalRow => {
        // Convert to UnsafeRow
        val inputUnsafeRow = internalRow.asInstanceOf[UnsafeRow]

        // Get input UnsafeRow bytes
        val inputBaseObject = inputUnsafeRow.getBytes
        val inputRowSize = inputBaseObject.size
        val headerSize = 6

        // Copy and add header for input row
        // TODO: Set header version and size
        val versionHeaderBytes = ByteBuffer.allocate(2)
        val sizeHeaderBytes = ByteBuffer.allocate(4)
        val appendHeaderBytes = ByteBuffer.allocate(headerSize + inputRowSize)
          .put(versionHeaderBytes).put(sizeHeaderBytes).put(inputBaseObject).array()

        // Call native method to compute
        val outputHybridseRow = CoreAPI.UnsafeRowProject(fn, appendHeaderBytes, inputRowSize, false)

        // Create output UnsafeRow
        val outputColumnSize = outputSchema.size
        val outputRowWithoutHeaderSize = outputHybridseRow.size - 6
        val outputUnsafeRowWriter = new UnsafeRowWriter(outputColumnSize, outputRowWithoutHeaderSize)
        outputUnsafeRowWriter.reset()
        outputUnsafeRowWriter.zeroOutNullBytes()

        // Copy and remove header for output row
        CoreAPI.CopyRowToUnsafeRowBytes(outputHybridseRow, outputUnsafeRowWriter.getBuffer, outputRowWithoutHeaderSize)

        // Release native row memory
        outputHybridseRow.delete()

        // Convert to InternalRow
        val outputUnsafeRow = outputUnsafeRowWriter.getRow
        outputUnsafeRow.asInstanceOf[InternalRow]
      })

    })

    val sparkSessionClass = Class.forName("org.apache.spark.sql.SparkSession")
    val internalCreateDataFrameMethod = sparkSessionClass
      .getDeclaredMethod(s"internalCreateDataFrame",
        classOf[RDD[InternalRow]], classOf[StructType], classOf[Boolean])

    val outputDf =
      internalCreateDataFrameMethod.invoke(ctx.getSparkSession, projectRDD, outputSchema, false: java.lang.Boolean)
      .asInstanceOf[DataFrame]

    SparkInstance.fromDataFrame(outputDf)
  }

  // spark closure class
  case class ProjectConfig(functionName: String,
                           moduleTag: String,
                           moduleBroadcast: SerializableByteBuffer,
                           inputSchemaSlices: Array[StructType],
                           outputSchemaSlices: Array[StructType],
                           inputSchema: StructType = null)

}
