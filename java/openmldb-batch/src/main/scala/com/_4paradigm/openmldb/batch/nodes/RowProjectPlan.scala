/*
 * Copyright 2021 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com._4paradigm.openmldb.batch.nodes

import com._4paradigm.hybridse.codec
import com._4paradigm.hybridse.sdk.{JitManager, SerializableByteBuffer}
import com._4paradigm.hybridse.vm.{CoreAPI, PhysicalTableProjectNode}
import com._4paradigm.openmldb.batch.utils.{AutoDestructibleIterator, HybridseUtil, SparkUtil, UnsafeRowUtil}
import com._4paradigm.openmldb.batch.{PlanContext, SparkInstance, SparkRowCodec}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.types.{LongType, StructType}
import org.slf4j.LoggerFactory


object RowProjectPlan {

  val logger = LoggerFactory.getLogger(this.getClass)

  /** The planner to implement table project node.
   *
   * There is one input table and output one table after window aggregation.
   */
  def gen(ctx: PlanContext, node: PhysicalTableProjectNode, inputTable: SparkInstance): SparkInstance = {

    // Check if we should keep the index column
    val isKeepIndexColumn = SparkInstance.keepIndexColumn(ctx, node.GetNodeId())

    // Get schema info from physical node
    val inputSchemaSlices = HybridseUtil.getOutputSchemaSlices(node.GetProducer(0))
    val outputSchemaSlices = HybridseUtil.getOutputSchemaSlices(node)

    val projectConfig = ProjectConfig(
      functionName = node.project().fn_info().fn_name(),
      moduleTag = ctx.getTag,
      moduleNoneBroadcast = ctx.getSerializableModuleBuffer,
      keepIndexColumn = isKeepIndexColumn,
      inputSchemaSlices = inputSchemaSlices,
      outputSchemaSlices = outputSchemaSlices
    )

    val outputSchema = if (isKeepIndexColumn) {
      HybridseUtil.getSparkSchema(node.GetOutputSchema())
        .add(ctx.getIndexInfo(node.GetNodeId()).indexColumnName, LongType)
    } else {
      HybridseUtil.getSparkSchema(node.GetOutputSchema())
    }

    // Get Spark DataFrame and limit the number of rows
    val inputDf = if (node.GetLimitCnt > 0) {
      inputTable.getDfConsideringIndex(ctx, node.GetNodeId())
        .limit(node.GetLimitCnt())
    } else {
      inputTable.getDfConsideringIndex(ctx, node.GetNodeId())
    }

    val hybridseJsdkLibraryPath = ctx.getConf.hybridseJsdkLibraryPath

    val outputDf = if (ctx.getConf.enableUnsafeRowOptimization) { // Use UnsafeRow optimization

      val outputInternalRowRdd = inputDf.queryExecution.toRdd.mapPartitions(partitionIter => {
        val tag = projectConfig.moduleTag
        val buffer = projectConfig.moduleNoneBroadcast.getBuffer

        if (hybridseJsdkLibraryPath.equals("")) {
          JitManager.initJitModule(tag, buffer)
        } else {
          JitManager.initJitModule(tag, buffer, hybridseJsdkLibraryPath)
        }

        val jit = JitManager.getJit(tag)
        val fn = jit.FindFunction(projectConfig.functionName)

        partitionIter.map(internalRow => {
          val inputRowSize = internalRow.asInstanceOf[UnsafeRow].getBytes.size

          // Create native method input from Spark InternalRow
          val hybridseRowBytes = UnsafeRowUtil.internalRowToHybridseRowBytes(internalRow)

          // Call native method to compute
          val outputHybridseRow = CoreAPI.UnsafeRowProject(fn, hybridseRowBytes, inputRowSize, false)

          // Call methods to generate Spark InternalRow
          val ouputInternalRow = UnsafeRowUtil.hybridseRowToInternalRow(outputHybridseRow, outputSchema.size)

          // TODO: Add index column if needed

          outputHybridseRow.delete()
          ouputInternalRow
        })

      })

      SparkUtil.rddInternalRowToDf(ctx.getSparkSession, outputInternalRowRdd, outputSchema)

    } else { // enableUnsafeRowOptimization is false
      val ouputRdd = inputDf.rdd.mapPartitions(partitionIter => {

        // TODO: Do not use broadcast for prophet HybridSE op
        val tag = projectConfig.moduleTag
        val buffer = projectConfig.moduleNoneBroadcast

        if (hybridseJsdkLibraryPath.equals("")) {
          JitManager.initJitModule(tag, buffer.getBuffer)
        } else {
          JitManager.initJitModule(tag, buffer.getBuffer, hybridseJsdkLibraryPath)
        }


        val jit = JitManager.getJit(tag)
        val fn = jit.FindFunction(projectConfig.functionName)
        val encoder = new SparkRowCodec(projectConfig.inputSchemaSlices)
        val decoder = new SparkRowCodec(projectConfig.outputSchemaSlices)
        val outputFields = if (projectConfig.keepIndexColumn) {
          projectConfig.outputSchemaSlices.map(_.size).sum + 1
        } else {
          projectConfig.outputSchemaSlices.map(_.size).sum
        }
        val outputArr = Array.fill[Any](outputFields)(null)

        val resultIter = partitionIter.map(row => {

          // Encode the spark row to native row
          val nativeInputRow = encoder.encode(row)

          val emptyParameter = new codec.Row()

          // Call native project method
          val outputNativeRow = CoreAPI.RowProject(fn, nativeInputRow, emptyParameter, false)

          // Decode the native row to spark row
          decoder.decode(outputNativeRow, outputArr)

          // Release swig jni objects
          nativeInputRow.delete()
          outputNativeRow.delete()

          // Append the index column if needed
          if (projectConfig.keepIndexColumn) {
            outputArr(outputArr.size - 1) = row.get(row.size - 1)
          }

          Row.fromSeq(outputArr)
        })

        AutoDestructibleIterator(resultIter) {
          encoder.delete()
          decoder.delete()
        }
      })

      ctx.getSparkSession.createDataFrame(ouputRdd, outputSchema)
    }

    // Create Spark instance from Spark DataFrame
    SparkInstance.createConsideringIndex(ctx, node.GetNodeId(), outputDf)
  }


  /* Spark closure class */
  case class ProjectConfig(functionName: String,
                           moduleTag: String,
                           inputSchemaSlices: Array[StructType],
                           outputSchemaSlices: Array[StructType],
                           keepIndexColumn: Boolean,
                           inputSchema: StructType = null,
                           moduleNoneBroadcast: SerializableByteBuffer = null)

}
