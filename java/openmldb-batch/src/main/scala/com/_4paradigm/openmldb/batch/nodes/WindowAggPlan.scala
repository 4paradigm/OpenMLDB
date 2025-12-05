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
import com._4paradigm.openmldb.batch.utils.{AutoDestructibleIterator, HybridseUtil, PhysicalNodeUtil,
  SkewDataFrameUtils, SparkUtil}
import com._4paradigm.openmldb.batch.window.WindowAggPlanUtil.WindowAggConfig
import com._4paradigm.openmldb.batch.window.{WindowAggPlanUtil, WindowComputer}
import com._4paradigm.openmldb.batch.{OpenmldbBatchConfig, PlanContext, SparkInstance}
import com._4paradigm.openmldb.common.codec.CodecUtil
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.types.{DateType, LongType, StructType, TimestampType}
import org.apache.spark.sql.{Column, DataFrame, Row, functions}
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
    val isUnsafeRowOpt = ctx.getConf.enableUnsafeRowOptimization
    val isUnsafeRowOptWindow = ctx.getConf.enableUnsafeRowOptForWindow
    // Check if we should keep the index column
    val isWindowSkewOptimization = ctx.getConf.enableWindowSkewOpt

    // Create serializable objects to call RDD methods
    val windowAggConfig = WindowAggPlanUtil.createWindowAggConfig(ctx, physicalNode, isKeepIndexColumn)
    val hadoopConf = new SerializableConfiguration(
      ctx.getSparkSession.sparkContext.hadoopConfiguration)
    val sparkFeConfig = ctx.getConf
    val dfWithIndex = inputTable.getDfConsideringIndex(ctx, physicalNode.GetNodeId())

    // Do union if physical node has union flag
    val uniqueColName = "_WINDOW_UNION_FLAG_" + System.currentTimeMillis()
    val unionTable = if (isWindowWithUnion) {
      WindowAggPlanUtil.windowUnionTables(ctx, physicalNode, dfWithIndex, uniqueColName)
    } else {
      dfWithIndex
    }

    // Use order by to make sure that rows with same timestamp from primary will be placed in last
    // TODO(tobe): support desc if we get config from physical plan
    val unionSparkCol: Option[Column] = if (isWindowWithUnion) {
      Some(unionTable.col(uniqueColName))
    } else {
      None
    }

    // Do group by and sort with window skew optimization or not
    val repartitionDf = if (isWindowSkewOptimization) {
      windowPartitionWithSkewOpt(ctx, physicalNode, unionTable, windowAggConfig, unionSparkCol)
    } else {
      windowPartition(ctx, physicalNode, unionTable, unionSparkCol)
    }

    // Get the output schema which may add the index column
    val outputSchema = if (isKeepIndexColumn) {
      HybridseUtil.getSparkSchema(physicalNode.GetOutputSchema())
        .add(ctx.getIndexInfo(physicalNode.GetNodeId()).indexColumnName, LongType)
    } else {
      HybridseUtil.getSparkSchema(physicalNode.GetOutputSchema())
    }

    // Do window agg with UnsafeRow optimization or not
    val outputDf = if (isUnsafeRowOpt && isUnsafeRowOptWindow) {

      val internalRowRdd = repartitionDf.queryExecution.toRdd
      val inputSchema = repartitionDf.schema
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

      val outputInternalRowRdd = if (isWindowWithUnion) {
        val rowRdd = repartitionDf.rdd
        // Combine row and internal row in the tuple for repartition
        val zippedRdd = rowRdd.zip(internalRowRdd)
        zippedRdd.mapPartitionsWithIndex {
          case (partitionIndex, iter) =>
            val computer = WindowAggPlanUtil.createComputer(partitionIndex, hadoopConf, sparkFeConfig, windowAggConfig)
            unsafeWindowAggIterWithUnionFlag(computer, iter, sparkFeConfig, windowAggConfig, outputSchema,
              inputTimestampColIndexes, outputTimestampColIndexes, inputDateColIndexes, outputDateColIndexes)
        }
      } else {
        if (isWindowSkewOptimization) {
          val rowRdd = repartitionDf.rdd
          // Combine row and internal row in the tuple for repartition
          val zippedRdd = rowRdd.zip(internalRowRdd)
          zippedRdd.mapPartitionsWithIndex {
            case (partitionIndex, iter) =>
              val computer = WindowAggPlanUtil.createComputer(partitionIndex, hadoopConf, sparkFeConfig,
                windowAggConfig)
              unsafeWindowAggIterWithSkewOpt(computer, iter, sparkFeConfig, windowAggConfig, outputSchema,
                inputTimestampColIndexes, outputTimestampColIndexes, inputDateColIndexes, outputDateColIndexes)
          }
        } else { // Not window skew opt
          internalRowRdd.mapPartitionsWithIndex {
            case (partitionIndex, iter) =>
              val computer = WindowAggPlanUtil.createComputer(partitionIndex, hadoopConf, sparkFeConfig,
                windowAggConfig)
              unsafeWindowAggIter(computer, iter, sparkFeConfig, windowAggConfig, outputSchema,
                inputTimestampColIndexes, outputTimestampColIndexes, inputDateColIndexes, outputDateColIndexes)
          }
        }

      }

      SparkUtil.rddInternalRowToDf(ctx.getSparkSession, outputInternalRowRdd, outputSchema)

    } else { // isUnsafeRowOptimization is false
      val outputRdd = if (isWindowWithUnion) {
        repartitionDf.rdd.mapPartitionsWithIndex {
          case (partitionIndex, iter) =>
            val computer = WindowAggPlanUtil.createComputer(partitionIndex, hadoopConf, sparkFeConfig, windowAggConfig)
            windowAggIterWithUnionFlag(computer, iter, sparkFeConfig, windowAggConfig, outputSchema)
        }
      } else {
        repartitionDf.rdd.mapPartitionsWithIndex {
          case (partitionIndex, iter) =>
            val computer = WindowAggPlanUtil.createComputer(partitionIndex, hadoopConf, sparkFeConfig, windowAggConfig)
            windowAggIter(computer, iter, sparkFeConfig, windowAggConfig, outputSchema)
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
                                 windowAggConfig: WindowAggConfig,
                                 unionSparkCol: Option[Column]): DataFrame = {
    val uniqueNamePostfix = ctx.getConf.windowSkewOptPostfix

    // Cache the input table which may be used for multiple times
    if (ctx.getConf.windowSkewOptCache) {
      inputDf.cache()
    }

    // Get repartition keys and orderBy key
    // TODO: Support multiple repartition keys and orderby keys
    val repartitionColIndexes = PhysicalNodeUtil.getRepartitionColumnIndexes(windowAggNode, inputDf)
    val orderByColIndex = PhysicalNodeUtil.getOrderbyColumnIndex(windowAggNode, inputDf)

    if (orderByColIndex < 0) {
      throw new Exception("WindowSkewOpt can not run for WINDOW without ORDER BY")
    }

    // Register the input table
    val partIdColName = "PART_ID" + uniqueNamePostfix
    val expandedRowColName = "EXPANDED_ROW" + uniqueNamePostfix
    val distinctCountColName = "DISTINCT_COUNT" + uniqueNamePostfix
    val partitionKeyColName = "PARTITION_KEY" + uniqueNamePostfix

    val quantile = ctx.getConf.skewedPartitionNum
    val approxRatio = 0.05

    // 1. Analyze the data distribution
    var distributionDf = if (ctx.getConf.windowSkewOptConfig.equals("")) {
      // Do not use skew config

      val distributionDf = if (!ctx.getConf.enableWindowSkewExpandedAllOpt) {
        SkewDataFrameUtils.genDistributionDf(inputDf, quantile.intValue(), repartitionColIndexes,
          orderByColIndex, partitionKeyColName, distinctCountColName, approxRatio)
      } else {
        SkewDataFrameUtils.genDistributionDf(inputDf, quantile.intValue(), repartitionColIndexes,
          orderByColIndex, partitionKeyColName)
      }
      logger.info("Generate distribution dataframe")

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

    if (ctx.getConf.windowSkewOptCache) {
      distributionDf.cache()
    }

    // 2. Add "part" column and "expand" column by joining the distribution table
    val addColumnsDf = SkewDataFrameUtils.genAddColumnsDf(inputDf, distributionDf, quantile.intValue(),
      repartitionColIndexes, orderByColIndex, partitionKeyColName, partIdColName, expandedRowColName,
      ctx.getConf.windowSkewOptBroadcastJoin)
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

    val sortedByCols = if (unionSparkCol.isEmpty) {
      repartitionCols ++ sortedByCol
    } else {
      repartitionCols ++ sortedByCol ++ Array(unionSparkCol.get)
    }

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
  def windowPartition(ctx: PlanContext, windowAggNode: PhysicalWindowAggrerationNode, inputDf: DataFrame,
                      unionSparkCol: Option[Column]): DataFrame = {

    // Repartition the table with window keys
    val repartitionCols = PhysicalNodeUtil.getRepartitionColumns(windowAggNode, inputDf)
    val repartitionDf = if (ctx.getConf.groupbyPartitions > 0) {
      inputDf.repartition(ctx.getConf.groupbyPartitions, repartitionCols: _*)
    } else {
      inputDf.repartition(repartitionCols: _*)
    }

    // Sort with the window orderby keys
    val orderbyCols = PhysicalNodeUtil.getOrderbyColumns(windowAggNode, inputDf)

    val sortedDf = if (unionSparkCol.isEmpty) {
      repartitionDf.sortWithinPartitions(repartitionCols ++ orderbyCols: _*)
    } else {
      repartitionDf.sortWithinPartitions(repartitionCols ++ orderbyCols ++ Array(unionSparkCol.get): _*)
    }
    // Notice that we should make sure the keys in the same partition are ordering as well
    sortedDf
  }

  def unsafeWindowAggIter(computer: WindowComputer,
                          inputIter: Iterator[InternalRow],
                          sqlConfig: OpenmldbBatchConfig,
                          config: WindowAggConfig,
                          outputSchema: StructType,
                          inputTimestampColIndexes: mutable.ArrayBuffer[Int],
                          outputTimestampColIndexes: mutable.ArrayBuffer[Int],
                          inputDateColIndexes: mutable.ArrayBuffer[Int],
                          outputDateColIndexes: mutable.ArrayBuffer[Int]): Iterator[InternalRow] = {

    var lastUnsafeRow: UnsafeRow = null

    // Take the iterator if the limit has been set
    val limitInputIter = if (config.limitCnt > 0) inputIter.take(config.limitCnt) else inputIter

    if (config.partIdIdx != 0) {
      val skewGroups = config.groupIdxs :+ config.partIdIdx
      computer.resetUnsafeGroupKeyComparator(skewGroups)
    }

    val resIter = if (sqlConfig.enableWindowSkewOpt) {
      throw new Exception("WindowSkewOpt is not supported for this method")
    } else {
      limitInputIter.flatMap(internalRow => {

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

        if (lastUnsafeRow != null) {
          computer.checkUnsafePartition(internalRow.asInstanceOf[UnsafeRow], lastUnsafeRow)
        }
        // Notice that we need to deep copy the UnsafeRow object to avoid reused pointer
        lastUnsafeRow = internalRow.asInstanceOf[UnsafeRow].copy()

        val orderKey = computer.extractUnsafeKey(internalRow.asInstanceOf[UnsafeRow])

        if (isValidOrder(orderKey)) {
          val outputInternalRow = computer.unsafeCompute(internalRow, orderKey, config.keepIndexColumn,
            config.unionFlagIdx, outputSchema, sqlConfig.enableUnsafeRowOptimization,
            sqlConfig.unsaferowoptCopyDirectByteBuffer)

          // Convert Spark UnsafeRow timestamp values for OpenMLDB Core
          for (colIdx <- outputTimestampColIndexes) {
            if(!outputInternalRow.isNullAt(colIdx)) {
              // TODO(tobe): warning if over LONG.MAX_VALUE
              outputInternalRow.setLong(colIdx, outputInternalRow.getLong(colIdx) * 1000)
            }
          }

          for (colIdx <- outputDateColIndexes) {
            if(!outputInternalRow.isNullAt(colIdx)) {
              outputInternalRow.setInt(colIdx, CodecUtil.dateIntToDays(outputInternalRow.getInt(colIdx)))
            }
          }

          Some(outputInternalRow)
        } else {
          None
        }
      })
    }
    AutoDestructibleIterator(resIter) {
      computer.delete()
    }
  }

  def unsafeWindowAggIterWithSkewOpt(computer: WindowComputer,
                          inputIter: Iterator[(Row, InternalRow)],
                          sqlConfig: OpenmldbBatchConfig,
                          config: WindowAggConfig,
                          outputSchema: StructType,
                          inputTimestampColIndexes: mutable.ArrayBuffer[Int],
                          outputTimestampColIndexes: mutable.ArrayBuffer[Int],
                          inputDateColIndexes: mutable.ArrayBuffer[Int],
                          outputDateColIndexes: mutable.ArrayBuffer[Int]): Iterator[InternalRow] = {

    var lastRow: Row = null

    // Take the iterator if the limit has been set
    val limitInputIter = if (config.limitCnt > 0) inputIter.take(config.limitCnt) else inputIter

    if (config.partIdIdx != 0) {
      val skewGroups = config.groupIdxs :+ config.partIdIdx
      computer.resetGroupKeyComparator(skewGroups)
    }

    val resIter = if (sqlConfig.enableWindowSkewOpt) {
      limitInputIter.flatMap(zippedRow => {

        val row = zippedRow._1
        val internalRow = zippedRow._2

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

        if (lastRow != null) {
          computer.checkPartition(row, lastRow)
        }
        lastRow = row

        val orderKey = computer.extractKey(row)
        val expandedFlag = row.getBoolean(config.expandedFlagIdx)
        if (!isValidOrder(orderKey)) {
          None
        } else if (!expandedFlag) {
          val outputInternalRow = computer.unsafeCompute(internalRow, orderKey, config.keepIndexColumn,
            config.unionFlagIdx, outputSchema, sqlConfig.enableUnsafeRowOptimization,
            sqlConfig.unsaferowoptCopyDirectByteBuffer)

          // Convert Spark UnsafeRow timestamp values for OpenMLDB Core
          for (colIdx <- outputTimestampColIndexes) {
            if(!outputInternalRow.isNullAt(colIdx)) {
              // TODO(tobe): warning if over LONG.MAX_VALUE
              outputInternalRow.setLong(colIdx, outputInternalRow.getLong(colIdx) * 1000)
            }
          }

          for (colIdx <- outputDateColIndexes) {
            if(!outputInternalRow.isNullAt(colIdx)) {
              outputInternalRow.setInt(colIdx, CodecUtil.dateIntToDays(outputInternalRow.getInt(colIdx)))
            }
          }

          Some(outputInternalRow)
        } else {
          computer.bufferRowOnly(row, orderKey)
          None
        }
      })
    } else { // Not window skew opt
      throw new Exception("WindowSkewOpt should be set for this method")
    }

    AutoDestructibleIterator(resIter) {
      computer.delete()
    }
  }

  def windowAggIter(computer: WindowComputer,
                    inputIter: Iterator[Row],
                    sqlConfig: OpenmldbBatchConfig,
                    config: WindowAggConfig,
                    outputSchema: StructType): Iterator[Row] = {
    var lastRow: Row = null

    // Take the iterator if the limit has been set
    val limitInputIter = if (config.limitCnt > 0) inputIter.take(config.limitCnt) else inputIter

    if (config.partIdIdx != 0) {
      val skewGroups = config.groupIdxs :+ config.partIdIdx
      computer.resetGroupKeyComparator(skewGroups)
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
          Some(computer.compute(row, orderKey, config.keepIndexColumn, config.unionFlagIdx, config.inputSchema.length,
            outputSchema, sqlConfig.enableUnsafeRowOptimization))
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
          Some(computer.compute(row, orderKey, config.keepIndexColumn, config.unionFlagIdx, config.inputSchema.length,
            outputSchema, sqlConfig.enableUnsafeRowOptimization))
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
                                 config: WindowAggConfig,
                                 outputSchema: StructType): Iterator[Row] = {
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
              Some(computer.compute(row, orderKey, config.keepIndexColumn,
                config.unionFlagIdx, config.inputSchema.length, outputSchema, sqlConfig.enableUnsafeRowOptimization))
            } else {
              if (!config.instanceNotInWindow) {
                computer.bufferRowOnly(row, orderKey)
              }
              None
            }
          } else {
            Some(computer.compute(row, orderKey, config.keepIndexColumn,
              config.unionFlagIdx, config.inputSchema.length, outputSchema, sqlConfig.enableUnsafeRowOptimization))
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

  def unsafeWindowAggIterWithUnionFlag(computer: WindowComputer,
                                       inputIter: Iterator[(Row, InternalRow)],
                                       sqlConfig: OpenmldbBatchConfig,
                                       config: WindowAggConfig,
                                       outputSchema: StructType,
                                       inputTimestampColIndexes: mutable.ArrayBuffer[Int],
                                       outputTimestampColIndexes: mutable.ArrayBuffer[Int],
                                       inputDateColIndexes: mutable.ArrayBuffer[Int],
                                       outputDateColIndexes: mutable.ArrayBuffer[Int]): Iterator[InternalRow] = {
    val flagIdx = config.unionFlagIdx
    var lastRow: Row = null

    // Take the iterator if the limit has been set
    val limitInputIter = if (config.limitCnt > 0) inputIter.take(config.limitCnt) else inputIter

    if (config.partIdIdx != 0) {
      val skewGroups = config.groupIdxs :+ config.partIdIdx
      computer.resetGroupKeyComparator(skewGroups)
    }

    val resIter = if (sqlConfig.enableWindowSkewOpt) {
      limitInputIter.flatMap(zippedRow => {

        val row = zippedRow._1
        val internalRow = zippedRow._2

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

        if (lastRow != null) {
          computer.checkPartition(row, lastRow)
        }
        lastRow = row

        val orderKey = computer.extractKey(row)
        val expandedFlag = row.getBoolean(config.expandedFlagIdx)
        if (!isValidOrder(orderKey)) {
          None
        } else if (!expandedFlag) {
          val outputInternalRow = computer.unsafeCompute(internalRow, orderKey, config.keepIndexColumn,
            config.unionFlagIdx, outputSchema,
            sqlConfig.enableUnsafeRowOptimization,
            sqlConfig.unsaferowoptCopyDirectByteBuffer)

          // Convert Spark UnsafeRow timestamp values for OpenMLDB Core
          for (colIdx <- outputTimestampColIndexes) {
            if(!outputInternalRow.isNullAt(colIdx)) {
              // TODO(tobe): warning if over LONG.MAX_VALUE
              outputInternalRow.setLong(colIdx, outputInternalRow.getLong(colIdx) * 1000)
            }
          }

          for (colIdx <- outputDateColIndexes) {
            if(!outputInternalRow.isNullAt(colIdx)) {
              outputInternalRow.setInt(colIdx, CodecUtil.dateIntToDays(outputInternalRow.getInt(colIdx)))
            }
          }

          Some(outputInternalRow)
        } else {
          computer.bufferRowOnly(row, orderKey)
          None
        }
      })
    } else {
      limitInputIter.flatMap(zippedRow => {

        val row = zippedRow._1
        val internalRow = zippedRow._2

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

        if (lastRow != null) {
          computer.checkPartition(row, lastRow)
        }
        lastRow = row

        val orderKey = computer.extractKey(row)
        if (isValidOrder(orderKey)) {

          val unionFlag = row.getBoolean(flagIdx)
          if (unionFlag) {
            if (sqlConfig.enableWindowSkewOpt) {
              val expandedFlag = row.getBoolean(config.expandedFlagIdx)
              if (!expandedFlag) {
                val outputInternalRow = computer.unsafeCompute(internalRow, orderKey, config.keepIndexColumn,
                  config.unionFlagIdx, outputSchema, sqlConfig.enableUnsafeRowOptimization,
                  sqlConfig.unsaferowoptCopyDirectByteBuffer)

                // Convert Spark UnsafeRow timestamp values for OpenMLDB Core
                for (colIdx <- outputTimestampColIndexes) {
                  if(!outputInternalRow.isNullAt(colIdx)) {
                    // TODO(tobe): warning if over LONG.MAX_VALUE
                    outputInternalRow.setLong(colIdx, outputInternalRow.getLong(colIdx) * 1000)
                  }
                }

                for (colIdx <- outputDateColIndexes) {
                  if(!outputInternalRow.isNullAt(colIdx)) {
                    outputInternalRow.setInt(colIdx, CodecUtil.dateIntToDays(outputInternalRow.getInt(colIdx)))
                  }
                }

                Some(outputInternalRow)
              } else {
                if (!config.instanceNotInWindow) {
                  computer.bufferRowOnly(row, orderKey)
                }
                None
              }
            } else {
              val outputInternalRow = computer.unsafeCompute(internalRow, orderKey, config.keepIndexColumn,
                config.unionFlagIdx, outputSchema, sqlConfig.enableUnsafeRowOptimization,
                sqlConfig.unsaferowoptCopyDirectByteBuffer)

              // Convert Spark UnsafeRow timestamp values for OpenMLDB Core
              for (tsColIdx <- outputTimestampColIndexes) {
                if(!outputInternalRow.isNullAt(tsColIdx)) {
                  /*
                   * If we run window without select, we get JoinedRow which contains two UnsafeRow.
                   * We would not ues JoinedRow.setLong() which will can underlying UnsafeRow.update() and it throw
                   * java.lang.UnsupportedOperationException so we change to OpenmldbJoinedRow.
                   */
                   outputInternalRow.setLong(tsColIdx, outputInternalRow.getLong(tsColIdx) * 1000)
                }
              }

              for (colIdx <- outputDateColIndexes) {
                if(!outputInternalRow.isNullAt(colIdx)) {
                  outputInternalRow.setInt(colIdx, CodecUtil.dateIntToDays(outputInternalRow.getInt(colIdx)))
                }
              }

              Some(outputInternalRow)
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
    }
    AutoDestructibleIterator(resIter) {
      computer.delete()
    }
  }

  def isValidOrder(key: java.lang.Long): Boolean = {
    // TODO: Ignore the null value, maybe handle null in the future
    if (key == null) {
      false
    } else {
      key >= 0
    }
  }

}
