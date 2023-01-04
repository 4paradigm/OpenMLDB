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

package com._4paradigm.openmldb.batch.window

import com._4paradigm.hybridse.node.FrameType
import com._4paradigm.hybridse.sdk.{HybridSeException, JitManager, SerializableByteBuffer}
import com._4paradigm.hybridse.vm.PhysicalWindowAggrerationNode
import com._4paradigm.hybridse.vm.Window.WindowFrameType
import com._4paradigm.openmldb.batch.utils.{ExternalUdfUtil, HybridseUtil, SparkColumnUtil, SparkUtil}
import com._4paradigm.openmldb.batch.{OpenmldbBatchConfig, PlanContext, SparkInstance}
import com._4paradigm.openmldb.sdk.impl.SqlClusterExecutor
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{DataFrame, functions}
import org.apache.spark.sql.types.{LongType, StructType}
import org.apache.spark.util.SerializableConfiguration
import org.slf4j.LoggerFactory

import scala.collection.mutable


/* The util class for window agg plan */
object WindowAggPlanUtil {

  private val logger = LoggerFactory.getLogger(this.getClass)

  /** Union the tables in window specification before window aggregation.
   *
   * This is not a ANSI-SQL feature and the window union syntax only exist in HybridSE SQL. It allows user to iterate
   * the rows from left table but aggregate with the data from union table.
   *
   * The right tables will be computed first and we will add one column with unique name for left and right tables. The
   * values from left table will be true and the values from right tables will be false which will be used for window
   * aggregation.
   */
  def windowUnionTables(ctx: PlanContext,
                    physicalNode: PhysicalWindowAggrerationNode,
                    inputDf: DataFrame,
                    uniqueColName: String): DataFrame = {

    val isKeepIndexColumn = SparkInstance.keepIndexColumn(ctx, physicalNode.GetNodeId())
    val unionNum = physicalNode.window_unions().GetSize().toInt

    val rightTables = (0 until unionNum).map(i => {
      val windowUnionNode = physicalNode.window_unions().GetUnionNode(i)
      val rightDf = ctx.getSparkOutput(windowUnionNode).getDfConsideringIndex(ctx, windowUnionNode.GetNodeId())

      /* TODO(tobe): Do not check schema now because window union after window union may get unequal schemas
      if (isKeepIndexColumn) {
        // Notice that input df may has index column, check in another way
        if (!SparkUtil.checkSchemaIgnoreNullable(rightDf
          .schema.add(ctx.getIndexInfo(physicalNode.GetNodeId()).indexColumnName, LongType), inputDf.schema)) {
          throw new HybridSeException("Keep index column, {$i}th Window union with inconsistent schema:\n" +
            s"Expect ${inputDf.schema}\nGet ${rightDf
              .schema.add(ctx.getIndexInfo(physicalNode.GetNodeId()).indexColumnName, LongType)}")
        }
      } else {
        if (!SparkUtil.checkSchemaIgnoreNullable(rightDf.schema, inputDf.schema)) {
          throw new HybridSeException("{$i}th Window union with inconsistent schema:\n" +
            s"Expect ${inputDf.schema}\nGet ${rightDf.schema}")
        }
      }
      */

      /*
      If original left table schema is [c1, c2], it may be [c1, c2, _WINDOW_UNION_FLAG_1665546417076#55] now.
      If isKeepIndexColumn is true, it may be [c1, c2, _index_column_placeholder, _WINDOW_UNION_FLAG_1665546417076#55].

      If window union after window union, the schema sizes are not equal and we need to append null columns for right
      tables before union.
       */
      var outputRightTable = rightDf

      if (inputDf.schema.size != rightDf.schema.size) {
        if (isKeepIndexColumn) {
          for (i <- rightDf.schema.size until inputDf.schema.size - 1) {
            outputRightTable = outputRightTable.withColumn(inputDf.columns(i), lit(null))
          }
        } else {
          for (i <- rightDf.schema.size until inputDf.schema.size) {
            outputRightTable = outputRightTable.withColumn(inputDf.columns(i), lit(null))
          }
        }
      }

      if (isKeepIndexColumn) {
        // Add one more placeholder column for sub tables if main table has index column
        outputRightTable = outputRightTable.withColumn(uniqueColName + "_index_column_placeholder",
          functions.lit(0L)).withColumn(uniqueColName, functions.lit(false))
      } else {
        // Only add the union boolean column where the values are false
        outputRightTable = outputRightTable.withColumn(uniqueColName, functions.lit(false))
      }

      outputRightTable
    })

    // Add new column for left table where the values are true which will be used for window aggregate
    val leftTable = inputDf.withColumn(uniqueColName, functions.lit(true))

    // Union the left and right tables
    rightTables.foldLeft(leftTable)((x, y) => x.union(y))
  }


  /** The serializable Spark closure class for window compute information.
   *
   * This will be used for window agg plan and set the values if needed.
   */
  case class WindowAggConfig(windowName: String,
                             windowFrameTypeName: String,
                             startOffset: Long,
                             endOffset: Long,
                             rowPreceding: Long,
                             maxSize: Long,
                             orderIdx: Int,
                             groupIdxs: Array[Int],
                             functionName: String,
                             moduleTag: String,
                             moduleNoneBroadcast: SerializableByteBuffer,
                             inputSchema: StructType,
                             inputSchemaSlices: Array[StructType],
                             outputSchemaSlices: Array[StructType],
                             unionFlagIdx: Int,
                             var expandedFlagIdx: Int = 0,
                             var partIdIdx: Int = 0,
                             instanceNotInWindow: Boolean,
                             excludeCurrentTime: Boolean,
                             needAppendInput: Boolean,
                             limitCnt: Int,
                             keepIndexColumn: Boolean,
                             isUnsafeRowOpt: Boolean,
                             externalFunMap: Map[String, com._4paradigm.openmldb.proto.Common.ExternalFun],
                             isYarnMode: Boolean,
                             taskmanagerExternalFunctionDir: String)


  /** Get the data from context and physical node and create the WindowAggConfig object.
   *
   */
  def createWindowAggConfig(ctx: PlanContext,
                            node: PhysicalWindowAggrerationNode,
                            keepIndexColumn: Boolean
                           ): WindowAggConfig = {
    val isUnsafeRowOpt = ctx.getConf.enableUnsafeRowOptimization
    val inputSchemaSlices = HybridseUtil.getOutputSchemaSlices(node.GetProducer(0), isUnsafeRowOpt)
    val outputSchemaSlices = HybridseUtil.getOutputSchemaSlices(node, isUnsafeRowOpt)
    val inputSchema = HybridseUtil.getSparkSchema(node.GetProducer(0).GetOutputSchema())

    // process window op
    val windowOp = node.window()
    val windowName = if (windowOp.getName_.isEmpty) {
      "anonymous_" + System.currentTimeMillis()
    } else {
      windowOp.getName_
    }

    // process order key
    val orders = windowOp.sort().orders()
    val ordersExprListNode = orders.getOrder_expressions_()
    if (ordersExprListNode.GetChildNum() > 1) {
      throw new HybridSeException("Multiple window order not supported")
    }
    val orderIdx = SparkColumnUtil.resolveOrderColumnIndex(orders.GetOrderExpression(0), node.GetProducer(0))

    // process group-by keys
    val groups = windowOp.partition().keys()

    val groupIdxs = mutable.ArrayBuffer[Int]()
    for (k <- 0 until groups.GetChildNum()) {
      val colIdx = SparkColumnUtil.resolveColumnIndex(groups.GetChild(k), node.GetProducer(0))
      groupIdxs += colIdx
    }

    // window union flag is the last input column
    val flagIdx = if (node.window_unions().Empty()) {
      -1
    } else {
      if (keepIndexColumn) {
        // Notice that if keep index column, table will add union boolean column after index column
        inputSchema.size + 1
      } else {
        inputSchema.size
      }

    }

    val frameType = node.window.range.frame().frame_type()
    val windowFrameType = if (frameType.swigValue() == FrameType.kFrameRows.swigValue()) {
      WindowFrameType.kFrameRows
    } else if (frameType.swigValue() == FrameType.kFrameRowsMergeRowsRange.swigValue()) {
      WindowFrameType.kFrameRowsMergeRowsRange
    } else {
      WindowFrameType.kFrameRowsRange
    }

    val config = ctx.getConf
    val openmldbSession = ctx.getOpenmldbSession
    var externalFunMap = Map[String, com._4paradigm.openmldb.proto.Common.ExternalFun]()
    if (config.openmldbZkCluster.nonEmpty && config.openmldbZkRootPath.nonEmpty
      && openmldbSession != null && openmldbSession.openmldbCatalogService != null) {
      externalFunMap = openmldbSession.openmldbCatalogService.getExternalFunctionsMap()
    }
    // TODO(tobe): openmldbSession may be null
    //val isYarnMode = openmldbSession.isYarnMode()
    val isYarnMode = ctx.getSparkSession.conf.get("spark.master").equalsIgnoreCase("yarn")
    val taskmanagerExternalFunctionDir = config.taskmanagerExternalFunctionDir

    WindowAggConfig(
      windowName = windowName,
      windowFrameTypeName = windowFrameType.toString,
      startOffset = node.window().range().frame().GetHistoryRangeStart(),
      endOffset = node.window.range.frame.GetHistoryRangeEnd(),
      rowPreceding = -1 * node.window.range.frame.GetHistoryRowsStart(),
      maxSize = node.window.range.frame.frame_maxsize(),
      orderIdx = orderIdx,
      groupIdxs = groupIdxs.toArray,
      functionName = node.project.fn_info().fn_name(),
      moduleTag = ctx.getTag,
      moduleNoneBroadcast = ctx.getSerializableModuleBuffer,
      inputSchema = inputSchema,
      inputSchemaSlices = inputSchemaSlices,
      outputSchemaSlices = outputSchemaSlices,
      unionFlagIdx = flagIdx,
      instanceNotInWindow = node.instance_not_in_window(),
      excludeCurrentTime = node.exclude_current_time(),
      needAppendInput = node.need_append_input(),
      limitCnt = node.GetLimitCntValue(),
      keepIndexColumn = keepIndexColumn,
      isUnsafeRowOpt = ctx.getConf.enableUnsafeRowOptimization,
      externalFunMap = externalFunMap,
      isYarnMode = isYarnMode,
      taskmanagerExternalFunctionDir = taskmanagerExternalFunctionDir
    )
  }

  def createComputer(partitionIndex: Int,
                     hadoopConf: SerializableConfiguration,
                     sqlConfig: OpenmldbBatchConfig,
                     config: WindowAggConfig): WindowComputer = {
    // get jit in executor process
    val tag = config.moduleTag
    val buffer = config.moduleNoneBroadcast.getBuffer
    SqlClusterExecutor.initJavaSdkLibrary(sqlConfig.openmldbJsdkLibraryPath)

    // Load external udf if exists
    ExternalUdfUtil.executorRegisterExternalUdf(config.externalFunMap, config.taskmanagerExternalFunctionDir,
      config.isYarnMode)

    JitManager.initJitModule(tag, buffer, config.isUnsafeRowOpt)
    val jit = JitManager.getJit(tag)

    // create stateful computer
    val computer = new WindowComputer(config, jit, config.keepIndexColumn)

    // add statistic hooks
    if (sqlConfig.windowSampleMinSize > 0) {
      val fs = FileSystem.get(hadoopConf.value)
      logger.info("Enable window sample support: min_size=" + sqlConfig.windowSampleMinSize +
        ", output_path=" + sqlConfig.windowSampleOutputPath)
      computer.addHook(new WindowSampleSupport(fs, partitionIndex, config, sqlConfig, jit))
    }
    if (sqlConfig.print) {
      val isSkew = sqlConfig.enableWindowSkewOpt
      computer.addHook(new RowDebugger(sqlConfig, config, isSkew))
    }
    computer
  }

}
