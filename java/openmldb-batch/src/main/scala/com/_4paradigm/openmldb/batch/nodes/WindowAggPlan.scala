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
  AutoDestructibleIterator, HybridseUtil, PhysicalNodeUtil,
  SkewDataFrameUtils, SparkColumnUtil, SparkUtil
}
import com._4paradigm.openmldb.batch.window.WindowAggPlanUtil.WindowAggConfig
import com._4paradigm.openmldb.batch.window.{WindowAggPlanUtil, WindowComputer}
import com._4paradigm.openmldb.batch.{OpenmldbBatchConfig, PlanContext, SparkInstance}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{IntegerType, LongType, StructType, TimestampType}
import org.apache.spark.sql.{Column, DataFrame, Row}
import org.apache.spark.util.SerializableConfiguration
import org.slf4j.LoggerFactory

import scala.collection.mutable

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

    // Get repartition keys and orderby key
    // TODO: Support multiple repartition keys and orderby keys
    val repartitionColIndexes = PhysicalNodeUtil.getRepartitionColumnIndexes(windowAggNode, inputDf)
    val orderByColIndex = PhysicalNodeUtil.getOrderbyColumnIndex(windowAggNode, inputDf)
    val orderByColType = inputDf.schema(orderByColIndex).dataType

    // Register the input table
    val partColName = "_PART_" + uniqueNamePostfix
    val expandColName = "_EXPAND_" + uniqueNamePostfix
    // "_GREATER_FLAG_" is used to determine whether distinct count is greater than quantile
    val greaterFlagColName = "_GREATER_FLAG_" + uniqueNamePostfix
    val countColName = "_COUNT_" + uniqueNamePostfix

    var minCount = Long.MaxValue

    val quantile = math.pow(2, ctx.getConf.skewLevel.toDouble)

    val addColumnsDf = if (ctx.getConf.windowSkewOptConfig.equals("")) {
      // Do not use skew config
      // 1. Analyze the data distribution
      val partitionColName = "_PARTITION_" + uniqueNamePostfix

      val distributionDf = SkewDataFrameUtils.genDistributionDf(inputDf, quantile.intValue(), repartitionColIndexes,
        orderByColIndex, partitionColName, greaterFlagColName, countColName)
      logger.info("Generate distribution dataframe")

      if (ctx.getConf.windowSkewOptCache) {
        distributionDf.cache()
      }

      val flags = distributionDf.select(distributionDf(greaterFlagColName), distributionDf(countColName)).collect()
      for (flag <- flags) {
        val greaterFlag = flag.getBoolean(0)
        val count = flag.getLong(1)
        // When greaterFlag == false, quantile can not be divided equally
        if (!greaterFlag) {
          logger.info("Unnecessary to open skew optimization")
          logger.info("Re-Execute in windowPartition")
          ctx.getConf.enableWindowSkewOpt = false
          return windowPartition(ctx, windowAggNode, inputDf)
        } else {
          minCount = math.min(minCount, count)
        }
      }

      // 2. Add "part" column and "expand" column by joining the distribution table
      val addColumnsDf = SkewDataFrameUtils.genAddColumnsDf(inputDf, distributionDf, quantile.intValue(),
        repartitionColIndexes, orderByColIndex, partColName, expandColName, countColName)
      logger.info("Generate percentile_tag dataframe")

      addColumnsDf
    } else {
      // Use skew config
      val distributionDf = ctx.getSparkSession.read.parquet(ctx.getConf.windowSkewOptConfig)
      val distributionCollect = distributionDf.collect()

      val distributionMap = Map(distributionCollect.map(p => (p.get(0), p.get(1))): _*)

      val outputSchema = inputDf.schema.add("_PART_", IntegerType, false)
        .add("_EXPAND_", IntegerType, false)

      val outputRdd = inputDf.rdd.map(row => {
        // Combine the repartition keys to one string which is equal to the first column of skew config
        val combineString = repartitionColIndexes.map(index => row.get(index)).mkString("_")
        // TODO: Support for more datatype of orderby columns
        val condition = if (orderByColType.equals(TimestampType)) {
          row.get(orderByColIndex).asInstanceOf[java.sql.Timestamp].compareTo(distributionMap(combineString)
            .asInstanceOf[java.sql.Timestamp])
        } else if (orderByColType.equals(LongType)) {
          row.get(orderByColIndex).asInstanceOf[Long].compareTo(distributionMap(combineString).asInstanceOf[Long])
        } else {
          row.get(orderByColIndex).asInstanceOf[Int].compareTo(distributionMap(combineString).asInstanceOf[Int])
        }

        val partValue = if (condition <= 0) {
          2
        } else {
          1
        }

        Row.fromSeq(row.toSeq :+ partValue :+ partValue)
      })
      ctx.getSparkSession.createDataFrame(outputRdd, outputSchema)
    }

    if (ctx.getConf.windowSkewOptCache) {
      addColumnsDf.cache()
    }

    // Update the column indexes and repartition keys
    windowAggConfig.skewTagIdx = addColumnsDf.schema.fieldNames.length - 2
    windowAggConfig.skewPositionIdx = addColumnsDf.schema.fieldNames.length - 1

    // 3. Expand the table data by union
    val unionDf = SkewDataFrameUtils.genUnionDf(addColumnsDf, quantile.intValue(), partColName, expandColName,
      minCount, windowAggConfig.rowPreceding, windowAggConfig.startOffset)
    logger.info("Generate union dataframe")

    // 4. Repartition and order by
    val repartitionCols = mutable.ArrayBuffer[Column]()
    repartitionCols += addColumnsDf(partColName)
    for (i <- repartitionColIndexes.indices) {
      repartitionCols += SparkColumnUtil.getColumnFromIndex(addColumnsDf, repartitionColIndexes(i))
    }

    val repartitionDf = if (ctx.getConf.groupbyPartitions > 0) {
      unionDf.repartition(ctx.getConf.groupbyPartitions, repartitionCols: _*)
    } else {
      unionDf.repartition(repartitionCols: _*)
    }

    val sortedByCol = PhysicalNodeUtil.getOrderbyColumns(windowAggNode, addColumnsDf)
    val sortedByCols = repartitionCols ++ sortedByCol

    val sortedDf = repartitionDf.sortWithinPartitions(sortedByCols: _*)
    logger.info("Generate repartition and orderby dataframe")

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

    if (config.skewTagIdx != 0) {
      sqlConfig.enableWindowSkewOpt = true
      val skewGroups = config.groupIdxs :+ config.skewTagIdx
      computer.resetGroupKeyComparator(skewGroups, config.inputSchema)
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
        val tag = row.getInt(config.skewTagIdx)
        val position = row.getInt(config.skewPositionIdx)
        if (!isValidOrder(orderKey)) {
          None
        } else if (tag == position) {
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

    if (config.skewTagIdx != 0) {
      sqlConfig.enableWindowSkewOpt = true
      val skewGroups = config.groupIdxs :+ config.skewTagIdx
      computer.resetGroupKeyComparator(skewGroups, config.inputSchema)
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
        val tag = row.getInt(config.skewTagIdx)
        val position = row.getInt(config.skewPositionIdx)
        if (!isValidOrder(orderKey)) {
          None
        } else if (tag == position) {
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
    if (config.skewTagIdx != 0) {
      sqlConfig.enableWindowSkewOpt = true
      val skewGroups = config.groupIdxs :+ config.skewTagIdx
      computer.resetGroupKeyComparator(skewGroups, config.inputSchema)
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
            val tag = row.getInt(config.skewTagIdx)
            val position = row.getInt(config.skewPositionIdx)
            if (tag == position) {
              Some(computer.compute(row, orderKey, config.keepIndexColumn, config.unionFlagIdx))
            } else {
              if(!config.instanceNotInWindow) {
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
