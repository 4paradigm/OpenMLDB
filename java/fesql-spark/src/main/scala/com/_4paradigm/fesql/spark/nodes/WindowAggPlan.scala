package com._4paradigm.fesql.spark.nodes

import java.util

import com._4paradigm.fesql.common.{FesqlException, JITManager, SerializableByteBuffer}
import com._4paradigm.fesql.spark._
import com._4paradigm.fesql.spark.element.FesqlConfig
import com._4paradigm.fesql.spark.nodes.window.WindowComputerWithSampleSupport
import com._4paradigm.fesql.spark.utils.{AutoDestructibleIterator, FesqlUtil, SparkColumnUtil, SparkRowUtil}
import com._4paradigm.fesql.utils.SkewUtils
import com._4paradigm.fesql.vm.{CoreAPI, FeSQLJITWrapper, PhysicalWindowAggrerationNode, WindowInterface}
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.util.StringUtils
import org.apache.spark.sql.{Column, DataFrame, Row, functions}
import org.apache.spark.sql.types._
import org.apache.spark.util.SerializableConfiguration

import scala.collection.mutable
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.{JavaConverters, mutable}


object WindowAggPlan {
  val logger = LoggerFactory.getLogger(this.getClass)

  def gen(ctx: PlanContext, node: PhysicalWindowAggrerationNode, input: SparkInstance): SparkInstance = {
    // process unions
    val unionNum = node.window_unions().GetSize().toInt

    val outputRDD = if (unionNum > 0) {
      genWithUnion(ctx, node, input)
    } else {
      genDefault(ctx, node, input)
    }

    val outputSchema = FesqlUtil.getSparkSchema(node.GetOutputSchema())
    SparkInstance.fromRDD(outputSchema, outputRDD)
  }


  def genDefault(ctx: PlanContext, node: PhysicalWindowAggrerationNode, input: SparkInstance): RDD[Row] = {

    val windowAggConfig = createWindowAggConfig(ctx, node)
    // group and sort
    logger.info(s"genDefault mode: ${FesqlConfig.mode}")
    val inputDf = if (FesqlConfig.mode.equals(FesqlConfig.skew)) {
      improveSkew(ctx, node, input.getDf(ctx.getSparkSession), windowAggConfig)
    } else {
      groupAndSort(ctx, node, input.getDf(ctx.getSparkSession))
    }

    val hadoopConf = new SerializableConfiguration(
      ctx.getSparkSession.sparkContext.hadoopConfiguration)

    val resultRDD = inputDf.rdd.mapPartitionsWithIndex {
      case (partitionIndex, iter) =>
        logger.info(s"partitionIndex ${partitionIndex}")
        // create computer
        val computer = createComputer(partitionIndex, hadoopConf, windowAggConfig)

        // window iteration
        windowAggIter(computer, iter, windowAggConfig)
    }
    resultRDD
  }

  def genWithUnion(ctx: PlanContext, node: PhysicalWindowAggrerationNode, input: SparkInstance): RDD[Row] = {
    val sess = ctx.getSparkSession
    val flagColName = "__FESQL_WINDOW_UNION_FLAG__" + System.currentTimeMillis()
    val union = doUnionTables(ctx, node, input.getDf(sess), flagColName)
    val windowAggConfig = createWindowAggConfig(ctx, node)
    val inputDf =  if (FesqlConfig.mode.equals(FesqlConfig.skew)) {
      improveSkew(ctx, node, union, windowAggConfig)
    } else {
      groupAndSort(ctx, node, union)
    }

    val hadoopConf = new SerializableConfiguration(
      ctx.getSparkSession.sparkContext.hadoopConfiguration)

    val resultRDD = inputDf.rdd.mapPartitionsWithIndex {
      case (partitionIndex, iter) =>
        // create computer
        val computer = createComputer(partitionIndex, hadoopConf, windowAggConfig)

        // window iteration
        windowAggIterWithUnionFlag(computer, iter, windowAggConfig)
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
      val df = ctx.visitPhysicalNodes(subNode).getDf(sess)
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


    val sampleOutputPath = ctx.getConf("fesql.window.sampleOutputPath", "")
    val sampleMinSize = ctx.getConf("fesql.window.sampleMinSize", -1)

    WindowAggConfig(
      windowName = windowName,
      startOffset = node.window.range.frame.GetHistoryRangeStart(),
      rowPreceding = -1 * node.window.range.frame.GetHistoryRowsStart(),
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

  /**
   *
   * @param input
   * @param config
   * @return
   */
  def expansionData(input: DataFrame, config: WindowAggConfig): DataFrame = {
    val tag_index = config.skewTagIdx
    logger.info(config.windowName + ":" + config.orderIdx)
    var cnt = 0

    // partitions
    val res = input.rdd.mapPartitionsWithIndex((index, iters) => {
      iters.flatMap(row => {
        cnt += 1
        if (cnt % 10000 == 0) {
          logger.info(index + row.toString())
        }

        var arrays = mutable.ListBuffer(row)
        val value = row.getInt(tag_index)
        for (i <- 1 until  value.asInstanceOf[Int]) {
          val temp_arr = row.toSeq.toArray
          temp_arr(tag_index) = i
          arrays += Row.fromSeq(temp_arr)
        }
        arrays
      })
    })
    input.sqlContext.createDataFrame(res, input.schema)
  }

  /**
   *
   * @param ctx
   * @param node
   * @param input
   * @param config
   * @return
   */
  def improveSkew(ctx: PlanContext, node: PhysicalWindowAggrerationNode, input: DataFrame, config: WindowAggConfig): DataFrame = {
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
    val quantile = math.pow(2, FesqlConfig.skewLevel.toDouble)
    val analyzeSQL = SkewUtils.genPercentileSql(table, quantile.intValue(), keysName, ts, FesqlConfig.skewCntName)
    logger.info(s"skew analyze sql : ${analyzeSQL}")
    input.createOrReplaceTempView(table)
    val reportDf = ctx.sparksql(analyzeSQL)
//    reportDf.show()
    reportDf.createOrReplaceTempView(reportTable)
    val keysMap = new util.HashMap[String, String]()
    var keyScala = keysName.asScala
    keyScala.foreach(e => keysMap.put(e, e))
    val schemas = scala.collection.JavaConverters.seqAsJavaList(input.schema.fieldNames)


    val tagSQL = SkewUtils.genPercentileTagSql(table, reportTable, quantile.intValue(), schemas, keysMap, ts, FesqlConfig.skewTag, FesqlConfig.skewPosition, FesqlConfig.skewCntName)
    logger.info(s"skew tag sql : ${tagSQL}")
    var skewDf = ctx.sparksql(tagSQL)

    config.skewTagIdx = skewDf.schema.fieldNames.length - 2
    config.skewPositionIdx = skewDf.schema.fieldNames.length - 1

    keyScala = keyScala :+ FesqlConfig.skewTag
//    skewDf = skewDf.repartition(keyScala.map(skewDf(_)): _*)
//    skewDf = expansionData(skewDf, config)
//    skewDf.cache()
    val skewTable = "FESQL_TEMP_WINDOW_SKEW_" + System.currentTimeMillis()
    logger.info("skew explode table {}", skewTable)
    skewDf.createOrReplaceTempView(skewTable)
    val explodeSql = SkewUtils.explodeDataSql(skewTable, quantile.intValue(), schemas, FesqlConfig.skewTag, FesqlConfig.skewPosition, FesqlConfig.skewCnt.longValue(), config.rowPreceding)
    logger.info(s"skew explode sql : ${explodeSql}")
    skewDf = ctx.sparksql(explodeSql)
    skewDf.cache()
//    skewDf.show(100)
    val partitions = FesqlConfig.paritions
    val partitionKeys = FesqlConfig.skewTag +: keyScala

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
//    sortedDf.show()
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

    val partitions = FesqlConfig.paritions
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
                     config: WindowAggConfig): WindowComputer = {
    // get jit in executor process
    val tag = config.moduleTag
    val buffer = config.moduleNoneBroadcast.getBuffer
    JITManager.initJITModule(tag, buffer)
    val jit = JITManager.getJIT(tag)

    if (partitionIndex == 0 && config.sampleMinSize > 0) {
      val fs = FileSystem.get(hadoopConf.value)
      new WindowComputerWithSampleSupport(fs, config, jit)
    } else {
      new WindowComputer(config, jit)
    }
  }

  def windowAggIter(computer: WindowComputer,
                    inputIter: Iterator[Row],
                    config: WindowAggConfig): Iterator[Row] = {
    var lastRow: Row = null

    // Take the iterator if the limit has been set
    val limitInputIter = if (config.limitCnt > 0) inputIter.take(config.limitCnt) else inputIter

    // todo isSkew need to be check
    var cnt: Long = 0L

    if (config.skewTagIdx != 0) {
      FesqlConfig.mode = "skew"
    }
    logger.info(s"windowAggIter mode: ${FesqlConfig.mode}")
    val resIter = if (FesqlConfig.mode.equals(FesqlConfig.skew)) {
      limitInputIter.flatMap(row => {
        if (lastRow != null) {
          computer.checkPartition(row, lastRow)
        }
        lastRow = row

        val tag = row.getInt(config.skewTagIdx)
        val position = row.getInt(config.skewPositionIdx)
        if (cnt % FesqlConfig.printSamplePartition == 0) {
          val str = new StringBuffer()
          str.append(row.get(config.orderIdx))
          str.append(",")
          for (e <- config.groupIdxs) {
            str.append(row.get(e))
            str.append(",")
          }
          str.append(" window size = " + computer.getWindow.size())
          logger.info(s"tag : postion = ${tag} : ${position} threadId = ${Thread.currentThread().getId} cnt = ${cnt} rowInfo = ${str.toString}")
        }
        cnt += 1
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
        if (cnt % FesqlConfig.printSamplePartition == 0) {
          val str = new StringBuffer()
          str.append(row.get(config.orderIdx))
          str.append(",")
          for (e <- config.groupIdxs) {
            str.append(row.get(e))
            str.append(",")
          }
          str.append(" window size = " + computer.getWindow.size())
          logger.info(s"threadId = ${Thread.currentThread().getId} cnt = ${cnt} rowInfo = ${str.toString}")
        }
        cnt += 1
        Some(computer.compute(row))
      })
    }
//    computer.delete()
    resIter
//    AutoDestructibleIterator(resIter) {
//      computer.delete()
//    }
  }

  def windowAggIterWithUnionFlag(computer: WindowComputer,
                                 inputIter: Iterator[Row],
                                 config: WindowAggConfig): Iterator[Row] = {
    val flagIdx = config.unionFlagIdx
    var lastRow: Row = null
    if (config.skewTagIdx != 0) {
      FesqlConfig.mode = "skew"
    }

    val resIter = inputIter.flatMap(row => {
      if (lastRow != null) {
        computer.checkPartition(row, lastRow)
      }
      lastRow = row

      val unionFlag = row.getBoolean(flagIdx)
      if (unionFlag) {
        // primary
        if (FesqlConfig.mode.equals(FesqlConfig.skew)) {
          val tag = row(config.skewTagIdx)
          val position = row(config.skewPositionIdx)
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
                             startOffset: Long,
                             rowPreceding: Long,
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


  /**
   * Stateful class for window computation during row iteration
   */
  class WindowComputer(config: WindowAggConfig, jit: FeSQLJITWrapper) {

    // reuse spark output row backed array
    private val outputFieldNum = config.outputSchemaSlices.map(_.size).sum
    private val outputArr = Array.fill[Any](outputFieldNum)(null)

    // native row codecs
    protected var encoder = new SparkRowCodec(config.inputSchemaSlices)
    private var decoder = new SparkRowCodec(config.outputSchemaSlices)

    // order key extractor
    private val orderField = config.inputSchema(config.orderIdx)
    private val orderKeyExtractor = SparkRowUtil.createOrderKeyExtractor(
      config.orderIdx, orderField.dataType, orderField.nullable)

    // append slices cnt = needAppendInput ? inputSchemaSlices.size : 0
    private val appendSlices = if (config.needAppendInput) config.inputSchemaSlices.length else 0
    // group key comparation
    private val groupKeyComparator = FesqlUtil.createGroupKeyComparator(
      config.groupIdxs, config.inputSchema)

    // native function handle
    private val fn = jit.FindFunction(config.functionName)

    // window state
    protected var window = new WindowInterface(
      config.instanceNotInWindow, config.startOffset, 0, config.rowPreceding, config.rowPreceding.intValue())

    def compute(row: Row): Row = {
      // call encode
      val nativeInputRow = encoder.encode(row)

      // extract key
      val key = orderKeyExtractor.apply(row)

      // call native compute
      // note: row is buffered automatically by core api
      val outputNativeRow = CoreAPI.WindowProject(fn, key, nativeInputRow, true, appendSlices, window)

      // call decode
      decoder.decode(outputNativeRow, outputArr)

      // release swig jni objects
      nativeInputRow.delete()
      outputNativeRow.delete()

      Row.fromSeq(outputArr) // can reuse backed array
    }

    def bufferRowOnly(row: Row): Unit = {
      val nativeInputRow = encoder.encode(row)
      val key = orderKeyExtractor.apply(row)
      window.BufferData(key, nativeInputRow)
    }

    def checkPartition(prev: Row, cur: Row): Unit = {
      val groupChanged = groupKeyComparator.apply(cur, prev)
      if (groupChanged) {
        resetWindow()
      }
    }

    def resetWindow(): Unit = {
      // TODO: wrap iter to hook iter end; now last window is leak
      window.delete()
      window = new WindowInterface(
        config.instanceNotInWindow, config.startOffset, 0, config.rowPreceding, config.rowPreceding.intValue())
    }

    def delete(): Unit = {
      encoder.delete()
      encoder = null

      decoder.delete()
      decoder = null

      window.delete()
      window = null
    }

    def getWindow: WindowInterface = window
    def getFn: Long = fn
  }

}
