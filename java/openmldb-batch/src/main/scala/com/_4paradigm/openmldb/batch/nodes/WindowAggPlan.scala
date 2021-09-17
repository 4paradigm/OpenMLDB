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

import com._4paradigm.hybridse.vm.PhysicalWindowAggrerationNode
import com._4paradigm.openmldb.batch.utils.{
  AutoDestructibleIterator, HybridseUtil, PhysicalNodeUtil, SkewDataFrameUtils, SparkUtil
}
import com._4paradigm.openmldb.batch.window.WindowAggPlanUtil.WindowAggConfig
import com._4paradigm.openmldb.batch.window.{WindowAggPlanUtil, WindowComputer}
import com._4paradigm.openmldb.batch.{OpenmldbBatchConfig, PlanContext, SparkInstance}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{LongType, StructType}
import org.apache.spark.sql.{DataFrame, Row, functions}
import org.apache.spark.util.SerializableConfiguration
import org.slf4j.LoggerFactory


/** The planner which implements window agg physical node.
 *
 * There is one input table and output one table after window aggregation.
 *
 * There are some kinds of window which may affect the implementation:
 * 1. Standard window. It is like SparkSQL window.
 * 2. Window with union. The window aggregation may include some union table data.
 * 3. Window skew optimization. The input table may be grouped for more partitions.
 * 4. Window skew optimization with skew config. Pre-compute the data distribution to accelerate the skew optimization.
 * 5. UnsafeRow optimization. Reuse the memory layout of Spark UnsafeRow.
 * 6. Window parallel optimization. Multiple windows could be computed in parallel and the input table would has new
 * index column.
 * */
object WindowAggPlan {

  private val logger = LoggerFactory.getLogger(this.getClass)

  /** The entrance function to generate Spark dataframe from window agg physical node. */
  def gen(ctx: PlanContext, physicalNode: PhysicalWindowAggrerationNode, inputTable: SparkInstance): SparkInstance = {
    // Check if we should support window with union or not
    val isWindowWithUnion = physicalNode.window_unions().GetSize().toInt > 0
    // Check if we should keep the index column
    val isKeepIndexColumn = SparkInstance.keepIndexColumn(ctx, physicalNode.GetNodeId())
    // Check if use UnsafeRow optimizaiton or not
    val isUnsafeRowOptimization = ctx.getConf.enableUnsafeRowOptimization
    // Check if we should keep the index column
    val isWindowSkewOptimization = ctx.getConf.enableWindowSkewOpt

    // Create serializable objects to call RDD methods
    val windowAggConfig = WindowAggPlanUtil.createWindowAggConfig(ctx, physicalNode, isKeepIndexColumn)
    val hadoopConf = new SerializableConfiguration(
      ctx.getSparkSession.sparkContext.hadoopConfiguration)
    val sparkFeConfig = ctx.getConf
    val dfWithIndex = inputTable.getDfConsideringIndex(ctx, physicalNode.GetNodeId())

    // Do union if physical node has union flag
    val unionTable = if (isWindowWithUnion) {
      WindowAggPlanUtil.windowUnionTables(ctx, physicalNode, dfWithIndex)
    } else {
      dfWithIndex
    }

    // Do groupby and sort with window skew optimization or not
    val repartitionDf = if (isWindowSkewOptimization) {
      windowPartitionWithSkewOpt(ctx, physicalNode, unionTable, windowAggConfig)
    } else {
      windowPartition(ctx, physicalNode, unionTable)
    }

    // Get the output schema which may add the index column
    val outputSchema = if (isKeepIndexColumn) {
      HybridseUtil.getSparkSchema(physicalNode.GetOutputSchema())
        .add(ctx.getIndexInfo(physicalNode.GetNodeId()).indexColumnName, LongType)
    } else {
      HybridseUtil.getSparkSchema(physicalNode.GetOutputSchema())
    }

    // Do window agg with UnsafeRow optimization or not
    val outputDf = if (isUnsafeRowOptimization) {

      // Combine row and internal row in the tuple for repartition
      val rowRdd = repartitionDf.rdd
      val internalRowRdd = repartitionDf.queryExecution.toRdd
      val zippedRdd = rowRdd.zip(internalRowRdd)

      val outputInternalRowRdd = zippedRdd.mapPartitionsWithIndex {
        case (partitionIndex, iter) =>
          val computer = WindowAggPlanUtil.createComputer(partitionIndex, hadoopConf, sparkFeConfig, windowAggConfig)
          unsafeWindowAggIter(computer, iter, sparkFeConfig, windowAggConfig, outputSchema)
      }
      SparkUtil.rddInternalRowToDf(ctx.getSparkSession, outputInternalRowRdd, outputSchema)

    } else { // isUnsafeRowOptimization is false
      val outputRdd = if (isWindowWithUnion) {
        repartitionDf.rdd.mapPartitionsWithIndex {
          case (partitionIndex, iter) =>
            val computer = WindowAggPlanUtil.createComputer(partitionIndex, hadoopConf, sparkFeConfig, windowAggConfig)
            windowAggIterWithUnionFlag(computer, iter, sparkFeConfig, windowAggConfig)
        }
      } else {
        repartitionDf.rdd.mapPartitionsWithIndex {
          case (partitionIndex, iter) =>
            val computer = WindowAggPlanUtil.createComputer(partitionIndex, hadoopConf, sparkFeConfig, windowAggConfig)
            windowAggIter(computer, iter, sparkFeConfig, windowAggConfig)
        }
      }
      // Create dataframe from rdd row and schema
      ctx.getSparkSession.createDataFrame(outputRdd, outputSchema)
    }

    SparkInstance.createConsideringIndex(ctx, physicalNode.GetNodeId(), outputDf)
  }


  /** Do repartition and sort for window skew optimization before aggregation.
   *
   * There are some steps to achieve this:
   * 1. Analyze the data distribution
   * 2. Add "part" column and "expand" column by joining the distribution table
   * (Step 1 and step 2 may be merged as new step with skew config)
   * 3. Expand the table data by union
   * 4. Repartition and orderby
   */
  def windowPartitionWithSkewOpt(ctx: PlanContext,
                                 windowAggNode: PhysicalWindowAggrerationNode,
                                 inputDf: DataFrame,
                                 windowAggConfig: WindowAggConfig): DataFrame = {
    val uniqueNamePostfix = ctx.getConf.windowSkewOptPostfix

    // Cache the input table which may be used for multiple times
    if (ctx.getConf.windowSkewOptCache) {
      inputDf.cache()
    }

    // Get repartition keys and orderBy key
    // TODO: Support multiple repartition keys and orderby keys
    val repartitionColIndexes = PhysicalNodeUtil.getRepartitionColumnIndexes(windowAggNode, inputDf)
    val orderByColIndex = PhysicalNodeUtil.getOrderbyColumnIndex(windowAggNode, inputDf)

    // Register the input table
    val partIdColName = "PART_ID" + uniqueNamePostfix
    val expandedRowColName = "EXPANDED_ROW" + uniqueNamePostfix
    val distinctCountColName = "DISTINCT_COUNT" + uniqueNamePostfix

    val quantile = ctx.getConf.skewedPartitionNum
    val approxRatio = 0.05

    // 1. Analyze the data distribution
    var distributionDf = if (ctx.getConf.windowSkewOptConfig.equals("")) {
      // Do not use skew config
      val partitionKeyColName = "PARTITION_KEY" + uniqueNamePostfix

      val distributionDf = if (!ctx.getConf.enableWindowSkewExpandedAllOpt) {
        SkewDataFrameUtils.genDistributionDf(inputDf, quantile.intValue(), repartitionColIndexes,
          orderByColIndex, partitionKeyColName, distinctCountColName, approxRatio)
      } else {
        SkewDataFrameUtils.genDistributionDf(inputDf, quantile.intValue(), repartitionColIndexes,
          orderByColIndex, partitionKeyColName)
      }
      logger.info("Generate distribution dataframe")

      if (ctx.getConf.windowSkewOptCache) {
        distributionDf.cache()
      }

      distributionDf
    } else {
      // Use skew config
      val distributionDf = ctx.getSparkSession.read.parquet(ctx.getConf.windowSkewOptConfig)
      logger.info("Load distribution dataframe")

      distributionDf
    }

    val minBlockSize = if (!ctx.getConf.enableWindowSkewExpandedAllOpt) {
      val approxMinCount = distributionDf.select(functions.min(distinctCountColName)).collect()(0).getLong(0)

      // The Count column is useless
      distributionDf = distributionDf.drop(distinctCountColName)

      val minCount = math.floor(approxMinCount / (1 + approxRatio))
      math.floor(minCount / quantile)
    } else {
      -1
    }

    // 2. Add "part" column and "expand" column by joining the distribution table
    val addColumnsDf = SkewDataFrameUtils.genAddColumnsDf(inputDf, distributionDf, quantile.intValue(),
      repartitionColIndexes, orderByColIndex, partIdColName, expandedRowColName)
    logger.info("Generate percentile_tag dataframe")

    if (ctx.getConf.windowSkewOptCache) {
      addColumnsDf.cache()
    }

    // Update the column indexes and repartition keys
    windowAggConfig.expandedFlagIdx = addColumnsDf.schema.fieldNames.length - 1
    windowAggConfig.partIdIdx = addColumnsDf.schema.fieldNames.length - 2

    // 3. Expand the table data by union
    val unionDf = if (!ctx.getConf.enableWindowSkewExpandedAllOpt && windowAggConfig.startOffset == 0) {
      // The Optimization for computing rows
      SkewDataFrameUtils.genUnionDf(addColumnsDf, quantile.intValue(), partIdColName, expandedRowColName,
        windowAggConfig.rowPreceding, minBlockSize)
    } else {
      SkewDataFrameUtils.genUnionDf(addColumnsDf, quantile.intValue(), partIdColName, expandedRowColName)
    }
    logger.info("Generate union dataframe")

    // 4. Repartition and order by
    var repartitionCols = PhysicalNodeUtil.getRepartitionColumns(windowAggNode, inputDf)
    repartitionCols = addColumnsDf(partIdColName) +: repartitionCols

    val repartitionDf = if (ctx.getConf.groupbyPartitions > 0) {
      unionDf.repartition(ctx.getConf.groupbyPartitions, repartitionCols: _*)
    } else {
      unionDf.repartition(repartitionCols: _*)
    }

    val sortedByCol = PhysicalNodeUtil.getOrderbyColumns(windowAggNode, addColumnsDf)
    val sortedByCols = repartitionCols ++ sortedByCol

    // Notice that we should make sure the keys in the same partition are ordering as well
    val sortedDf = repartitionDf.sortWithinPartitions(sortedByCols: _*)
    logger.info("Generate repartition and orderBy dataframe")

    sortedDf
  }

  /** Do repartition and sort for standard window computing before aggregation.
   *
   * There are two steps:
   * 1. Repartition the table with the "partition by" keys.
   * 2. Sort the data within partitions with the "order by" keys.
   */
  def windowPartition(ctx: PlanContext, windowAggNode: PhysicalWindowAggrerationNode, inputDf: DataFrame): DataFrame = {

    // Repartition the table with window keys
    val repartitionCols = PhysicalNodeUtil.getRepartitionColumns(windowAggNode, inputDf)
    val repartitionDf = if (ctx.getConf.groupbyPartitions > 0) {
      inputDf.repartition(ctx.getConf.groupbyPartitions, repartitionCols: _*)
    } else {
      inputDf.repartition(repartitionCols: _*)
    }

    // Sort with the window orderby keys
    val orderbyCols = PhysicalNodeUtil.getOrderbyColumns(windowAggNode, inputDf)

    // Notice that we should make sure the keys in the same partition are ordering as well
    val sortedDf = repartitionDf.sortWithinPartitions(repartitionCols ++ orderbyCols: _*)

    sortedDf
  }

  def unsafeWindowAggIter(computer: WindowComputer,
                          inputIter: Iterator[(Row, InternalRow)],
                          sqlConfig: OpenmldbBatchConfig,
                          config: WindowAggConfig,
                          outputSchema: StructType): Iterator[InternalRow] = {
    var lastRow: Row = null

    // Take the iterator if the limit has been set
    val limitInputIter = if (config.limitCnt > 0) inputIter.take(config.limitCnt) else inputIter

    if (config.partIdIdx != 0) {
      val skewGroups = config.groupIdxs :+ config.partIdIdx
      computer.resetGroupKeyComparator(skewGroups)
    }
    if (sqlConfig.print) {
      logger.info(s"windowAggIter mode: ${sqlConfig.enableWindowSkewOpt}")
    }

    val resIter = if (sqlConfig.enableWindowSkewOpt) {
      limitInputIter.flatMap(zippedRow => {

        val row = zippedRow._1
        val internalRow = zippedRow._2

        if (lastRow != null) {
          computer.checkPartition(row, lastRow)
        }
        lastRow = row

        val orderKey = computer.extractKey(row)
        val expandedFlag = row.getBoolean(config.expandedFlagIdx)
        if (!isValidOrder(orderKey)) {
          None
        } else if (!expandedFlag) {
          Some(computer.unsafeCompute(internalRow, orderKey, config.keepIndexColumn, config.unionFlagIdx, outputSchema))
        } else {
          computer.bufferRowOnly(row, orderKey)
          None
        }
      })
    } else {
      limitInputIter.flatMap(zippedRow => {

        val row = zippedRow._1
        val internalRow = zippedRow._2

        if (lastRow != null) {
          computer.checkPartition(row, lastRow)
        }
        lastRow = row
        val orderKey = computer.extractKey(row)
        if (isValidOrder(orderKey)) {
          Some(computer.unsafeCompute(internalRow, orderKey, config.keepIndexColumn, config.unionFlagIdx, outputSchema))
        } else {
          None
        }
      })
    }
    AutoDestructibleIterator(resIter) {
      computer.delete()
    }
  }

  def windowAggIter(computer: WindowComputer,
                    inputIter: Iterator[Row],
                    sqlConfig: OpenmldbBatchConfig,
                    config: WindowAggConfig): Iterator[Row] = {
    var lastRow: Row = null

    // Take the iterator if the limit has been set
    val limitInputIter = if (config.limitCnt > 0) inputIter.take(config.limitCnt) else inputIter

    if (config.partIdIdx != 0) {
      val skewGroups = config.groupIdxs :+ config.partIdIdx
      computer.resetGroupKeyComparator(skewGroups)
    }
    if (sqlConfig.print) {
      logger.info(s"windowAggIter mode: ${sqlConfig.enableWindowSkewOpt}")
    }

    val resIter = if (sqlConfig.enableWindowSkewOpt) {
      limitInputIter.flatMap(row => {

        if (lastRow != null) {
          computer.checkPartition(row, lastRow)
        }
        lastRow = row

        val orderKey = computer.extractKey(row)
        val expandedFlag = row.getBoolean(config.expandedFlagIdx)
        if (!isValidOrder(orderKey)) {
          None
        } else if (!expandedFlag) {
          Some(computer.compute(row, orderKey, config.keepIndexColumn, config.unionFlagIdx))
        } else {
          computer.bufferRowOnly(row, orderKey)
          None
        }
      })
    } else {
      limitInputIter.flatMap(row => {

        if (lastRow != null) {
          computer.checkPartition(row, lastRow)
        }
        lastRow = row
        val orderKey = computer.extractKey(row)
        if (isValidOrder(orderKey)) {
          Some(computer.compute(row, orderKey, config.keepIndexColumn, config.unionFlagIdx))
        } else {
          None
        }
      })
    }
    AutoDestructibleIterator(resIter) {
      computer.delete()
    }
  }

  def windowAggIterWithUnionFlag(computer: WindowComputer,
                                 inputIter: Iterator[Row],
                                 sqlConfig: OpenmldbBatchConfig,
                                 config: WindowAggConfig): Iterator[Row] = {
    val flagIdx = config.unionFlagIdx
    var lastRow: Row = null
    if (config.partIdIdx != 0) {
      val skewGroups = config.groupIdxs :+ config.partIdIdx
      computer.resetGroupKeyComparator(skewGroups)
    }

    val resIter = inputIter.flatMap(row => {
      if (lastRow != null) {
        computer.checkPartition(row, lastRow)
      }
      lastRow = row

      val orderKey = computer.extractKey(row)
      if (isValidOrder(orderKey)) {
        val unionFlag = row.getBoolean(flagIdx)
        if (unionFlag) {
          // primary
          if (sqlConfig.enableWindowSkewOpt) {
            val expandedFlag = row.getBoolean(config.expandedFlagIdx)
            if (!expandedFlag) {
              Some(computer.compute(row, orderKey, config.keepIndexColumn, config.unionFlagIdx))
            } else {
              if (!config.instanceNotInWindow) {
                computer.bufferRowOnly(row, orderKey)
              }
              None
            }
          } else {
            Some(computer.compute(row, orderKey, config.keepIndexColumn, config.unionFlagIdx))
          }
        } else {
          // secondary
          computer.bufferRowOnly(row, orderKey)
          None
        }
      } else {
        None
      }
    })

    AutoDestructibleIterator(resIter) {
      computer.delete()
    }
  }

  def isValidOrder(key: Long): Boolean = key >= 0

}
