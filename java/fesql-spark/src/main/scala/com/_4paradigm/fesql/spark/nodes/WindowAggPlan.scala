package com._4paradigm.fesql.spark.nodes

import java.util

import com._4paradigm.fesql.common.{FesqlException, JITManager, SerializableByteBuffer}
import com._4paradigm.fesql.node.FrameType
import com._4paradigm.fesql.spark._
import com._4paradigm.fesql.spark.nodes.window.{RowDebugger, WindowComputer, WindowSampleSupport}
import com._4paradigm.fesql.spark.utils.{AutoDestructibleIterator, FesqlUtil, SparkColumnUtil}
import com._4paradigm.fesql.utils.SkewUtils
import com._4paradigm.fesql.vm.Window.WindowFrameType
import com._4paradigm.fesql.vm.PhysicalWindowAggrerationNode
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, DataFrame, Row, functions}
import org.apache.spark.sql.types._
import org.apache.spark.util.SerializableConfiguration
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.mutable


object WindowAggPlan {

  private val logger = LoggerFactory.getLogger(this.getClass)

  def gen(ctx: PlanContext, node: PhysicalWindowAggrerationNode, input: SparkInstance): SparkInstance = {
    // process unions
    val unionNum = node.window_unions().GetSize().toInt

    val outputRDD = if (unionNum > 0) {
      genWithUnion(ctx, node, input)
    } else {
      genDefault(ctx, node, input)
    }

    val outputSchema = FesqlUtil.getSparkSchema(node.GetOutputSchema())
    val outputDf = ctx.getSparkSession.createDataFrame(outputRDD, outputSchema)

    SparkInstance.createWithNodeIndexInfo(ctx, node.GetNodeId(), outputDf)
  }


  def genDefault(ctx: PlanContext, node: PhysicalWindowAggrerationNode, input: SparkInstance): RDD[Row] = {
    val config = ctx.getConf
    val windowAggConfig = createWindowAggConfig(ctx, node)

    // group and sort
    if (config.print) {
      logger.info(s"genDefault mode: ${config.skewMode}")
    }

    val inputDf = if (config.skewMode == FeSQLConfig.SKEW) {
      improveSkew(ctx, node, input.getSparkDfConsideringIndex(ctx, node.GetNodeId()), config, windowAggConfig)
    } else {
      groupAndSort(ctx, node, input.getSparkDfConsideringIndex(ctx, node.GetNodeId()))
    }

    val hadoopConf = new SerializableConfiguration(
      ctx.getSparkSession.sparkContext.hadoopConfiguration)

    val resultRDD = inputDf.rdd.mapPartitionsWithIndex {
      case (partitionIndex, iter) =>
        if (config.print) {
          logger.info(s"partitionIndex $partitionIndex")
        }
        // create computer
        val computer = createComputer(partitionIndex, hadoopConf, config, windowAggConfig)

        // window iteration
        windowAggIter(computer, iter, config, windowAggConfig)
    }
    resultRDD
  }

  def genWithUnion(ctx: PlanContext, node: PhysicalWindowAggrerationNode, input: SparkInstance): RDD[Row] = {
    val sess = ctx.getSparkSession
    val config = ctx.getConf
    val flagColName = "__FESQL_WINDOW_UNION_FLAG__" + System.currentTimeMillis()
    val union = doUnionTables(ctx, node, input.getSparkDfConsideringIndex(ctx, node.GetNodeId()), flagColName)
    val windowAggConfig = createWindowAggConfig(ctx, node)
    val inputDf =  if (config.skewMode == FeSQLConfig.SKEW) {
      improveSkew(ctx, node, union, config, windowAggConfig)
    } else {
      groupAndSort(ctx, node, union)
    }

    val hadoopConf = new SerializableConfiguration(
      ctx.getSparkSession.sparkContext.hadoopConfiguration)

    val resultRDD = inputDf.rdd.mapPartitionsWithIndex {
      case (partitionIndex, iter) =>
        // create computer
        val computer = createComputer(partitionIndex, hadoopConf, config, windowAggConfig)

        // window iteration
        windowAggIterWithUnionFlag(computer, iter, config, windowAggConfig)
    }
    resultRDD
  }

  def doUnionTables(ctx: PlanContext,
                    node: PhysicalWindowAggrerationNode,
                    source: DataFrame,
                    flagColumnName: String): DataFrame = {
    val sess = ctx.getSparkSession
    val unionNum = node.window_unions().GetSize().toInt

    val subTables = (0 until unionNum).map(i => {
      val subNode = node.window_unions().GetUnionNode(i)
      // TODO: Please check if this works to use lower API within ConcatJoin
      val df = ctx.getSparkOutput(subNode).getSparkDfConsideringIndex(ctx, subNode.GetNodeId())
      if (df.schema != source.schema) {
        throw new FesqlException("{$i}th Window union with inconsistent schema:\n" +
          s"Expect ${source.schema}\nGet ${df.schema}")
      }
      df.withColumn(flagColumnName, functions.lit(false))
    })

    val mainTable = source.withColumn(flagColumnName, functions.lit(true))
    subTables.foldLeft(mainTable)((x, y) => x.union(y))
  }


  def createWindowAggConfig(ctx: PlanContext,
                            node: PhysicalWindowAggrerationNode
                           ): WindowAggConfig = {
    val inputSchemaSlices = FesqlUtil.getOutputSchemaSlices(node.GetProducer(0))
    val outputSchemaSlices = FesqlUtil.getOutputSchemaSlices(node)
    val inputSchema = FesqlUtil.getSparkSchema(node.GetProducer(0).GetOutputSchema())

    // process window op
    val windowOp = node.window()
    val windowName = if (windowOp.getName_.isEmpty) {
      "anonymous_" + System.currentTimeMillis()
    } else {
      windowOp.getName_
    }

    // process order key
    val orders = windowOp.sort().orders().order_by()
    if (orders.GetChildNum() > 1) {
      throw new FesqlException("Multiple window order not supported")
    }
    val orderIdx = SparkColumnUtil.resolveColumnIndex(orders.GetChild(0), node.GetProducer(0))

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
      inputSchema.size
    }

    val sampleOutputPath = ctx.getConf.windowSampleOutputPath
    val sampleMinSize = ctx.getConf.windowSampleMinSize

    val frameType = node.window.range.frame().frame_type()
    val windowFrameType = if (frameType.swigValue() == FrameType.kFrameRows.swigValue()) {
      WindowFrameType.kFrameRows
    } else if (frameType.swigValue() == FrameType.kFrameRowsMergeRowsRange.swigValue()) {
      WindowFrameType.kFrameRowsMergeRowsRange
    } else {
      WindowFrameType.kFrameRowsRange
    }

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
      needAppendInput = node.need_append_input(),
      limitCnt = node.GetLimitCnt(),
      sampleOutputPath = sampleOutputPath,
      sampleMinSize = sampleMinSize
    )
  }

  def improveSkew(ctx: PlanContext,
                  node: PhysicalWindowAggrerationNode,
                  input: DataFrame,
                  sqlConfig: FeSQLConfig,
                  config: WindowAggConfig): DataFrame = {
    input.cache()
    val windowOp = node.window()
    val groupByExprs = windowOp.partition().keys()
    val keysName = new util.ArrayList[String]()
    var ts: String = ""

    val groupByCols = mutable.ArrayBuffer[Column]()
    for (i <- 0 until groupByExprs.GetChildNum()) {
      val expr = groupByExprs.GetChild(i)
      val colIdx = SparkColumnUtil.resolveColumnIndex(expr, node.GetProducer(0))
      if (colIdx < 0) {
        logger.error(s"skew dataframe: $input")
        throw new FesqlException("window skew colIdx is less than zero")
      }
      groupByCols += SparkColumnUtil.getColumnFromIndex(input, colIdx)
      keysName.add(input.schema.apply(colIdx).name)
    }

    val orders = windowOp.sort().orders()
    val orderExprs = orders.order_by()
    val orderByCols = mutable.ArrayBuffer[Column]()
    for (i <- 0 until orderExprs.GetChildNum()) {
      val expr = orderExprs.GetChild(i)
      val colIdx = SparkColumnUtil.resolveColumnIndex(expr, node.GetProducer(0))
      if (colIdx < 0) {
        logger.error(s"skew dataframe: $input")
        throw new FesqlException("window skew colIdx is less than zero")
      }
      ts = input.schema.apply(colIdx).name
      val column = SparkColumnUtil.getColumnFromIndex(input, colIdx)
      if (orders.is_asc()) {
        orderByCols += column.asc
      } else {
        orderByCols += column.desc
      }
    }

    val table = "FESQL_TEMP_WINDOW_" + System.currentTimeMillis()
    val reportTable = "FESQL_TEMP_WINDOW_REPORT_" + System.currentTimeMillis()
    logger.info("skew main table {}", table)
    logger.info("skew main table report{}", reportTable)
    val quantile = math.pow(2, sqlConfig.skewLevel.toDouble)
    val analyzeSQL = SkewUtils.genPercentileSql(table, quantile.intValue(), keysName, ts, sqlConfig.skewCntName)
    logger.info(s"skew analyze sql : $analyzeSQL")
    input.createOrReplaceTempView(table)
    val reportDf = ctx.sparksql(analyzeSQL)
//    reportDf.show()
    reportDf.createOrReplaceTempView(reportTable)
    val keysMap = new util.HashMap[String, String]()
    var keyScala = keysName.asScala
    keyScala.foreach(e => keysMap.put(e, e))
    val schemas = scala.collection.JavaConverters.seqAsJavaList(input.schema.fieldNames)

    val tagSQL = SkewUtils.genPercentileTagSql(table, reportTable, quantile.intValue(), schemas, keysMap, ts,
      sqlConfig.skewTag, sqlConfig.skewPosition, sqlConfig.skewCntName, sqlConfig.skewCnt.longValue())
    logger.info(s"skew tag sql : $tagSQL")
    var skewDf = ctx.sparksql(tagSQL)

    config.skewTagIdx = skewDf.schema.fieldNames.length - 2
    config.skewPositionIdx = skewDf.schema.fieldNames.length - 1

    keyScala = keyScala :+ sqlConfig.skewTag
//    skewDf = skewDf.repartition(keyScala.map(skewDf(_)): _*)
//    skewDf = expansionData(skewDf, config)
//    skewDf.cache()
    val skewTable = "FESQL_TEMP_WINDOW_SKEW_" + System.currentTimeMillis()
    logger.info("skew explode table {}", skewTable)
    skewDf.createOrReplaceTempView(skewTable)
    val explodeSql = SkewUtils.explodeDataSql(skewTable, quantile.toInt, schemas,
      sqlConfig.skewTag, sqlConfig.skewPosition, sqlConfig.skewCnt.toLong, config.rowPreceding)
    logger.info(s"skew explode sql : $explodeSql")
    skewDf = ctx.sparksql(explodeSql)
    skewDf.cache()
//    skewDf.show(100)
    val partitions = sqlConfig.groupPartitions
    val partitionKeys = sqlConfig.skewTag +: keyScala

    val groupedDf = if (partitions > 0) {
//      skewDf.repartition(partitions, keyScala.map(skewDf(_)): _*)
      skewDf.repartition(partitions, partitionKeys.map(skewDf(_)): _*)
    } else {
//      skewDf.repartition(keyScala.map(skewDf(_)): _*)
      skewDf.repartition(partitionKeys.map(skewDf(_)): _*)
    }
    keyScala = keyScala :+ ts
    // todo order desc asc
    val sortedDf = groupedDf.sortWithinPartitions(keyScala.map(skewDf(_)): _*)
    sortedDf.cache()
    sortedDf
  }

  def groupAndSort(ctx: PlanContext, node: PhysicalWindowAggrerationNode, input: DataFrame): DataFrame = {
    val windowOp = node.window()
    val groupByExprs = windowOp.partition().keys()

    val groupByCols = mutable.ArrayBuffer[Column]()
    for (i <- 0 until groupByExprs.GetChildNum()) {
      val expr = groupByExprs.GetChild(i)
      val colIdx = SparkColumnUtil.resolveColumnIndex(expr, node.GetProducer(0))
      groupByCols += SparkColumnUtil.getColumnFromIndex(input, colIdx)
    }

    val partitions = ctx.getConf.groupPartitions
    val groupedDf = if (partitions > 0) {
      input.repartition(partitions, groupByCols: _*)
    } else {
      input.repartition(groupByCols: _*)
    }

    val orders = windowOp.sort().orders()
    val orderExprs = orders.order_by()
    val orderByCols = mutable.ArrayBuffer[Column]()
    for (i <- 0 until orderExprs.GetChildNum()) {
      val expr = orderExprs.GetChild(i)
      val colIdx = SparkColumnUtil.resolveColumnIndex(expr, node.GetProducer(0))
      val column = SparkColumnUtil.getColumnFromIndex(input, colIdx)
      if (orders.is_asc()) {
        orderByCols += column.asc
      } else {
        orderByCols += column.desc
      }
    }
    val sortedDf = groupedDf.sortWithinPartitions(groupByCols ++ orderByCols: _*)
    sortedDf
  }

  def createComputer(partitionIndex: Int,
                     hadoopConf: SerializableConfiguration,
                     sqlConfig: FeSQLConfig,
                     config: WindowAggConfig): WindowComputer = {
    // get jit in executor process
    val tag = config.moduleTag
    val buffer = config.moduleNoneBroadcast.getBuffer
    JITManager.initJITModule(tag, buffer)
    val jit = JITManager.getJIT(tag)

    // create stateful computer
    val computer = new WindowComputer(sqlConfig, config, jit)

    // add statistic hooks
    if (config.sampleMinSize > 0) {
      val fs = FileSystem.get(hadoopConf.value)
      logger.info("Enable window sample support: min_size=" + config.sampleMinSize +
        ", output_path=" + config.sampleOutputPath)
      computer.addHook(new WindowSampleSupport(fs, partitionIndex, config, jit))
    }
    if (sqlConfig.print) {
      val isSkew = sqlConfig.skewMode == FeSQLConfig.SKEW
      computer.addHook(new RowDebugger(sqlConfig, config, isSkew))
    }
    System.currentTimeMillis()
    computer
  }

  def windowAggIter(computer: WindowComputer,
                    inputIter: Iterator[Row],
                    sqlConfig: FeSQLConfig,
                    config: WindowAggConfig): Iterator[Row] = {
    var lastRow: Row = null

    // Take the iterator if the limit has been set
    val limitInputIter = if (config.limitCnt > 0) inputIter.take(config.limitCnt) else inputIter

    if (config.skewTagIdx != 0) {
      sqlConfig.skewMode = FeSQLConfig.SKEW
    }
    if (sqlConfig.print) {
      logger.info(s"windowAggIter mode: ${sqlConfig.skewMode}")
    }

    val resIter = if (sqlConfig.skewMode == FeSQLConfig.SKEW) {
      limitInputIter.flatMap(row => {
        if (lastRow != null) {
          computer.checkPartition(row, lastRow)
        }
        lastRow = row

        val tag = row.getInt(config.skewTagIdx)
        val position = row.getInt(config.skewPositionIdx)
        if (tag == position) {
          Some(computer.compute(row))
        } else {
          computer.bufferRowOnly(row)
          None
        }
      })
    } else {
      limitInputIter.flatMap(row => {
        if (lastRow != null) {
          computer.checkPartition(row, lastRow)
        }
        lastRow = row
        Some(computer.compute(row))
      })
    }
    AutoDestructibleIterator(resIter) {
      computer.delete()
    }
  }

  def windowAggIterWithUnionFlag(computer: WindowComputer,
                                 inputIter: Iterator[Row],
                                 sqlConfig: FeSQLConfig,
                                 config: WindowAggConfig): Iterator[Row] = {
    val flagIdx = config.unionFlagIdx
    var lastRow: Row = null
    if (config.skewTagIdx != 0) {
      sqlConfig.skewMode = "skew"
    }

    val resIter = inputIter.flatMap(row => {
      if (lastRow != null) {
        computer.checkPartition(row, lastRow)
      }
      lastRow = row

      val unionFlag = row.getBoolean(flagIdx)
      if (unionFlag) {
        // primary
        if (sqlConfig.skewMode == FeSQLConfig.SKEW) {
          val tag = row.getInt(config.skewTagIdx)
          val position = row.getInt(config.skewPositionIdx)
          if (tag == position) {
            Some(computer.compute(row))
          } else {
            computer.bufferRowOnly(row)
            None
          }
        } else {
          Some(computer.compute(row))
        }
      } else {
        // secondary
        computer.bufferRowOnly(row)
        None
      }
    })

    AutoDestructibleIterator(resIter) {
      computer.delete()
    }
  }

  /**
   * Spark closure class for window compute information
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
                             var skewTagIdx: Int = 0,
                             var skewPositionIdx: Int = 0,
                             instanceNotInWindow: Boolean,
                             needAppendInput: Boolean,
                             limitCnt: Int,
                             sampleMinSize: Int,
                             sampleOutputPath: String)
  
}
