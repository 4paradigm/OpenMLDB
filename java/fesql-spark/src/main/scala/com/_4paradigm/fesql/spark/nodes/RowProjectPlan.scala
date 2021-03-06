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

package com._4paradigm.fesql.spark.nodes

import com._4paradigm.fesql.common.{JITManager, SerializableByteBuffer}
import com._4paradigm.fesql.spark._
import com._4paradigm.fesql.spark.utils.{AutoDestructibleIterator, FesqlUtil}
import com._4paradigm.fesql.vm.{CoreAPI, FeSQLJITWrapper, PhysicalTableProjectNode}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{LongType, StructType}
import org.slf4j.LoggerFactory

object RowProjectPlan {

  val logger = LoggerFactory.getLogger(this.getClass)

  /**
   * @param ctx
   * @param node
   * @param inputs
   * @return
   */
  def gen(ctx: PlanContext, node: PhysicalTableProjectNode, inputs: Seq[SparkInstance]): SparkInstance = {
    val inputInstance = inputs.head
    val inputRDD = inputInstance.getDfConsideringIndex(ctx, node.GetNodeId()).rdd

    val inputSchemaSlices = FesqlUtil.getOutputSchemaSlices(node.GetProducer(0))
    val outputSchemaSlices = FesqlUtil.getOutputSchemaSlices(node)


    // Check if we should keep the index column
    val keepIndexColumn = SparkInstance.keepIndexColumn(ctx, node.GetNodeId())

    val outputSchema = if (keepIndexColumn) {
      FesqlUtil.getSparkSchema(node.GetOutputSchema()).add(ctx.getIndexInfo(node.GetNodeId()).indexColumnName, LongType)
    } else {
      FesqlUtil.getSparkSchema(node.GetOutputSchema())
    }

    // spark closure
    val limitCnt = node.GetLimitCnt
    val projectConfig = ProjectConfig(
      functionName = node.project().fn_info().fn_name(),
      moduleTag = ctx.getTag,
      moduleNoneBroadcast = ctx.getSerializableModuleBuffer,
      keepIndexColumn = keepIndexColumn,
      inputSchemaSlices = inputSchemaSlices,
      outputSchemaSlices = outputSchemaSlices
    )

    // project implementation
    val projectRDD = inputRDD.mapPartitions(iter => {
      val limitIter = if (limitCnt > 0) iter.take(limitCnt) else iter

      // ensure worker native
      val tag = projectConfig.moduleTag

      // TODO: Do not use broadcast for prophet FESQL op
      val buffer = projectConfig.moduleNoneBroadcast

      JITManager.initJITModule(tag, buffer.getBuffer)
      val jit = JITManager.getJIT(tag)

      projectIter(limitIter, jit, projectConfig)
    })

    val outputDf = ctx.getSparkSession.createDataFrame(projectRDD, outputSchema)

    SparkInstance.createConsideringIndex(ctx, node.GetNodeId(), outputDf)
  }


  def projectIter(inputIter: Iterator[Row], jit: FeSQLJITWrapper, config: ProjectConfig): Iterator[Row] = {
    // reusable output row inst
    val outputFields = if (config.keepIndexColumn) config.outputSchemaSlices.map(_.size).sum + 1 else config.outputSchemaSlices.map(_.size).sum
    val outputArr = Array.fill[Any](outputFields)(null)

    val fn = jit.FindFunction(config.functionName)

    val encoder = new SparkRowCodec(config.inputSchemaSlices)
    val decoder = new SparkRowCodec(config.outputSchemaSlices)

    val resultIter = inputIter.map(row =>
      projectRow(row, fn, encoder, decoder, outputArr, config.keepIndexColumn)
    )

    AutoDestructibleIterator(resultIter) {
      encoder.delete()
      decoder.delete()
    }
  }


  def projectRow(row: Row, fn: Long,
                 encoder: SparkRowCodec,
                 decoder: SparkRowCodec,
                 outputArr: Array[Any],
                 keepIndexColumn: Boolean): Row = {
    // call encode
    val nativeInputRow = encoder.encode(row)

    // call native compute
    val outputNativeRow = CoreAPI.RowProject(fn, nativeInputRow, false)

    // call decode
    decoder.decode(outputNativeRow, outputArr)

    // release swig jni objects
    nativeInputRow.delete()
    outputNativeRow.delete()

    // Append the index column if needed
    if (keepIndexColumn) {
      outputArr(outputArr.size-1) = row.get(row.size-1)
    }

    Row.fromSeq(outputArr)  // can reuse backed array
  }


  // spark closure class
  case class ProjectConfig(functionName: String,
                           moduleTag: String,
                           // moduleBroadcast: Broadcast[SerializableByteBuffer],
                           inputSchemaSlices: Array[StructType],
                           outputSchemaSlices: Array[StructType],
                           keepIndexColumn: Boolean,
                           inputSchema: StructType = null,
                           moduleNoneBroadcast: SerializableByteBuffer = null)

}
