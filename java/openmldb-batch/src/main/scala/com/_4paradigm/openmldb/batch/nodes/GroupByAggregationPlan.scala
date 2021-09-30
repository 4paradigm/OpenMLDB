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

import com._4paradigm.hybridse.codec.{Row => NativeRow}
import com._4paradigm.hybridse.sdk.JitManager
import com._4paradigm.hybridse.vm.{CoreAPI, GroupbyInterface, PhysicalGroupAggrerationNode, PhysicalOpNode}
import com._4paradigm.openmldb.batch.nodes.RowProjectPlan.ProjectConfig
import com._4paradigm.openmldb.batch.utils.{HybridseUtil, SparkColumnUtil}
import com._4paradigm.openmldb.batch.{PlanContext, SparkInstance, SparkRowCodec}
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{Column, Row}
import org.slf4j.LoggerFactory

import scala.collection.mutable


object GroupByAggregationPlan {

  val logger = LoggerFactory.getLogger(this.getClass)

  def gen(ctx: PlanContext, physicalNode: PhysicalGroupAggrerationNode,
          input: SparkInstance, physicalOpNode: PhysicalOpNode): SparkInstance = {
    val inputDf = input.getDfConsideringIndex(ctx, physicalNode.GetNodeId())

    // Check if we should keep the index column
    val keepIndexColumn = SparkInstance.keepIndexColumn(ctx, physicalNode.GetNodeId())

    // Get parition keys
    val groupByExprs = physicalNode.getGroup_.keys()
    val groupByCols = mutable.ArrayBuffer[Column]()
    val groupIdxs = mutable.ArrayBuffer[Int]()
    for (i <- 0 until groupByExprs.GetChildNum()) {
      val expr = groupByExprs.GetChild(i)
      val colIdx = SparkColumnUtil.resolveColumnIndex(expr, physicalNode.GetProducer(0))
      groupIdxs += colIdx
      groupByCols += SparkColumnUtil.getColumnFromIndex(inputDf, colIdx)
    }

    // Sort by partition keys
    val sortedInputDf = inputDf.sortWithinPartitions(groupByCols:_*)

    // Get schema info
    val inputSchemaSlices = HybridseUtil.getOutputSchemaSlices(physicalNode.GetProducer(0))
    val inputSchema = HybridseUtil.getSparkSchema(physicalNode.GetProducer(0).GetOutputSchema())
    val outputSchemaSlices = HybridseUtil.getOutputSchemaSlices(physicalNode)

    val outputSchema = if (keepIndexColumn) {
      HybridseUtil.getSparkSchema(physicalNode.GetOutputSchema())
        .add(ctx.getIndexInfo(physicalNode.GetNodeId()).indexColumnName, LongType)
    } else {
      HybridseUtil.getSparkSchema(physicalNode.GetOutputSchema())
    }

    // Wrap Spark closure
    val limitCnt = physicalNode.GetLimitCnt
    val projectConfig = ProjectConfig(
      functionName = physicalNode.project().fn_info().fn_name(),
      moduleTag = ctx.getTag,
      moduleNoneBroadcast = ctx.getSerializableModuleBuffer,
      inputSchemaSlices = inputSchemaSlices,
      keepIndexColumn = keepIndexColumn,
      outputSchemaSlices = outputSchemaSlices,
      inputSchema = inputSchema
    )
    val groupKeyComparator = HybridseUtil.createGroupKeyComparator(groupIdxs.toArray)

    val hybridseJsdkLibraryPath = ctx.getConf.hybridseJsdkLibraryPath

    // Map partition
    val resultRDD = sortedInputDf.rdd.mapPartitions(iter => {
      val resultRowList =  mutable.ArrayBuffer[Row]()

      if (iter.nonEmpty) { // Ignore the empty partition
        var currentLimitCnt = 0

        // Init JIT
        val tag = projectConfig.moduleTag
        val buffer = projectConfig.moduleNoneBroadcast.getBuffer

        if (hybridseJsdkLibraryPath.equals("")) {
          JitManager.initJitModule(tag, buffer)
        } else {
          JitManager.initJitModule(tag, buffer, hybridseJsdkLibraryPath)
        }

        val jit = JitManager.getJit(tag)
        val fn = jit.FindFunction(projectConfig.functionName)

        val encoder = new SparkRowCodec(projectConfig.inputSchemaSlices)
        val decoder = new SparkRowCodec(projectConfig.outputSchemaSlices)

        val inputHybridseSchema = HybridseUtil.getHybridseSchema(projectConfig.inputSchema)

        val outputFields =
          if (projectConfig.keepIndexColumn) {
            projectConfig.outputSchemaSlices.map(_.size).sum + 1
          } else {
            projectConfig.outputSchemaSlices.map(_.size).sum
          }

        // Init first groupby interface
        var groupbyInterface = new GroupbyInterface(inputHybridseSchema)
        var lastRow: Row = null
        val grouopNativeRows =  mutable.ArrayBuffer[NativeRow]()

        iter.foreach(row => {
          if (limitCnt <= 0 || currentLimitCnt < limitCnt) { // Do not set limit or not reach the limit
            if (lastRow != null) { // Ignore the first row in partition
              val groupChanged = groupKeyComparator.apply(row, lastRow)
              if (groupChanged) {
                // Run group by for the same group
                val outputHybridseRow = CoreAPI.GroupbyProject(fn, groupbyInterface)
                val outputArr = Array.fill[Any](outputFields)(null)
                decoder.decode(outputHybridseRow, outputArr)
                resultRowList += Row.fromSeq(outputArr)
                currentLimitCnt += 1

                // Reset group interface and release native rows
                groupbyInterface.delete()
                groupbyInterface = new GroupbyInterface(inputHybridseSchema)
                grouopNativeRows.map(nativeRow => nativeRow.delete())
                grouopNativeRows.clear()

                // Append the index column if needed
                if (keepIndexColumn) {
                  outputArr(outputArr.size-1) = row.get(row.size-1)
                }

              }
            }

            // Buffer the row in the same group
            lastRow = row
            val nativeInputRow = encoder.encode(row)
            groupbyInterface.AddRow(nativeInputRow)
            grouopNativeRows += nativeInputRow
          }
        })

        // Run group by for the last group
        if (limitCnt <= 0 || currentLimitCnt < limitCnt) {
          val outputHybridseRow = CoreAPI.GroupbyProject(fn, groupbyInterface)
          val outputArr = Array.fill[Any](outputFields)(null)
          decoder.decode(outputHybridseRow, outputArr)
          resultRowList += Row.fromSeq(outputArr)
        }

        groupbyInterface.delete()
        grouopNativeRows.foreach(nativeRow => nativeRow.delete())
        grouopNativeRows.clear()
      }

      resultRowList.toIterator
    })

    val outputDf = ctx.getSparkSession.createDataFrame(resultRDD, outputSchema)

    SparkInstance.createConsideringIndex(ctx, physicalNode.GetNodeId(), outputDf, physicalOpNode)
  }

}
