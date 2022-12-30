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
import com._4paradigm.openmldb.batch.utils.{AutoDestructibleIterator, ExternalUdfUtil, HybridseUtil, SparkUtil,
  UnsafeRowUtil}
import com._4paradigm.openmldb.batch.{PlanContext, SparkInstance, SparkRowCodec}
import com._4paradigm.openmldb.common.codec.CodecUtil
import com._4paradigm.openmldb.sdk.impl.SqlClusterExecutor
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DateType, LongType, StructType, TimestampType}
import org.slf4j.LoggerFactory
import scala.collection.mutable

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
    val isUnsafeRowOpt = ctx.getConf.enableUnsafeRowOptimization
    val inputSchemaSlices = HybridseUtil.getOutputSchemaSlices(node.GetProducer(0), isUnsafeRowOpt)
    val outputSchemaSlices = HybridseUtil.getOutputSchemaSlices(node, isUnsafeRowOpt)

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
    val inputDf = if (node.GetLimitCntValue >= 0) {
      inputTable.getDfConsideringIndex(ctx, node.GetNodeId())
        .limit(node.GetLimitCntValue())
    } else {
      inputTable.getDfConsideringIndex(ctx, node.GetNodeId())
    }

    val inputSchema = inputDf.schema

    val openmldbJsdkLibraryPath = ctx.getConf.openmldbJsdkLibraryPath
    val unsaferowoptCopyDirectByteBuffer = ctx.getConf.unsaferowoptCopyDirectByteBuffer

    val config = ctx.getConf
    val openmldbSession = ctx.getOpenmldbSession

    // Get the external udf objects for Spark drivers which may not use OpenmldbSession directly
    var externalFunMap = Map[String, com._4paradigm.openmldb.proto.Common.ExternalFun]()
    if (config.openmldbZkCluster.nonEmpty && config.openmldbZkRootPath.nonEmpty
      && openmldbSession != null && openmldbSession.openmldbCatalogService != null) {
      externalFunMap = openmldbSession.openmldbCatalogService.getExternalFunctionsMap()
    }
    // TODO(tobe): openmldbSession may be null for UT
    //val isYarnMode = openmldbSession.isYarnMode()
    val isYarnMode = ctx.getSparkSession.conf.get("spark.master").equalsIgnoreCase("yarn")
    val taskmanagerExternalFunctionDir = config.taskmanagerExternalFunctionDir

    val outputDf = if (isUnsafeRowOpt && ctx.getConf.enableUnsafeRowOptForProject) { // Use UnsafeRow optimization

      val outputInternalRowRdd = inputDf.queryExecution.toRdd.mapPartitions(partitionIter => {
        val tag = projectConfig.moduleTag
        val buffer = projectConfig.moduleNoneBroadcast.getBuffer
        SqlClusterExecutor.initJavaSdkLibrary(openmldbJsdkLibraryPath)

        // Load external udf if exists
        ExternalUdfUtil.executorRegisterExternalUdf(externalFunMap, taskmanagerExternalFunctionDir, isYarnMode)

        JitManager.initJitModule(tag, buffer, isUnsafeRowOpt)
        val jit = JitManager.getJit(tag)
        val fn = jit.FindFunction(projectConfig.functionName)


        val inputTimestampColIndexes = mutable.ArrayBuffer[Int]()
        for (i <- 0 until inputSchema.size) {
          if (inputSchema(i).dataType == TimestampType) {
            inputTimestampColIndexes.append(i)
          }
        }

        val outputTimestampColIndexes = mutable.ArrayBuffer[Int]()
        for (i <- 0 until outputSchema.size) {
          if (outputSchema(i).dataType == TimestampType) {
            outputTimestampColIndexes.append(i)
          }
        }

        val inputDateColIndexes = mutable.ArrayBuffer[Int]()
        for (i <- 0 until inputSchema.size) {
          if (inputSchema(i).dataType == DateType) {
            inputDateColIndexes.append(i)
          }
        }

        val outputDateColIndexes = mutable.ArrayBuffer[Int]()
        for (i <- 0 until outputSchema.size) {
          if (outputSchema(i).dataType == DateType) {
            outputDateColIndexes.append(i)
          }
        }

        partitionIter.map(internalRow => {

          // Convert Spark UnsafeRow timestamp values for OpenMLDB Core
          for (colIdx <- inputTimestampColIndexes) {
            if(!internalRow.isNullAt(colIdx)) {
              internalRow.setLong(colIdx, internalRow.getLong(colIdx) / 1000)
            }
          }

          for (colIdx <- inputDateColIndexes) {
            if(!internalRow.isNullAt(colIdx)) {
              internalRow.setInt(colIdx, CodecUtil.daysToDateInt(internalRow.getInt(colIdx)))
            }
          }

          // Notice that we should use DirectByteBuffer instead of byte array
          //val hybridseRowBytes = UnsafeRowUtil.internalRowToHybridseRowBytes(internalRow)
          //val outputHybridseRow = CoreAPI.UnsafeRowProject(fn, hybridseRowBytes, hybridseRowBytes.length, false)

          // Create native method input from Spark InternalRow
          val hybridseRowDirectByteBuffer = UnsafeRowUtil.internalRowToHybridseByteBuffer(internalRow)
          val byteBufferSize = UnsafeRowUtil.getHybridseByteBufferSize(internalRow)

          // Call native method to compute
          val outputHybridseRow = CoreAPI.UnsafeRowProjectDirect(fn, hybridseRowDirectByteBuffer, byteBufferSize,
            false)

          // Call methods to generate Spark InternalRow
          val outputInternalRow = if (unsaferowoptCopyDirectByteBuffer) {
            UnsafeRowUtil.hybridseRowToInternalRowDirect(outputHybridseRow, outputSchema.size)
          } else {
            UnsafeRowUtil.hybridseRowToInternalRow(outputHybridseRow, outputSchema.size)
          }

          // Convert Spark UnsafeRow timestamp values for OpenMLDB Core
          for (tsColIdx <- outputTimestampColIndexes) {
            if(!outputInternalRow.isNullAt(tsColIdx)) {
              // TODO(tobe): warning if over LONG.MAX_VALUE
              outputInternalRow.setLong(tsColIdx, outputInternalRow.getLong(tsColIdx) * 1000)
            }
          }

          for (colIdx <- outputDateColIndexes) {
            if(!outputInternalRow.isNullAt(colIdx)) {
              outputInternalRow.setInt(colIdx, CodecUtil.dateIntToDays(outputInternalRow.getInt(colIdx)))
            }
          }

          // TODO: Add index column if needed
          outputInternalRow
        })

      })

      SparkUtil.rddInternalRowToDf(ctx.getSparkSession, outputInternalRowRdd, outputSchema)

    } else { // enableUnsafeRowOptimization is false
      val ouputRdd = inputDf.rdd.mapPartitions(partitionIter => {

        // TODO: Do not use broadcast for prophet HybridSE op
        val tag = projectConfig.moduleTag
        val buffer = projectConfig.moduleNoneBroadcast
        SqlClusterExecutor.initJavaSdkLibrary(openmldbJsdkLibraryPath)

        // Load external udf if exists
        ExternalUdfUtil.executorRegisterExternalUdf(externalFunMap, taskmanagerExternalFunctionDir, isYarnMode)

        JitManager.initJitModule(tag, buffer.getBuffer, isUnsafeRowOpt)
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
          emptyParameter.delete()

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
