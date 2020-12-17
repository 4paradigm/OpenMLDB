package com._4paradigm.fesql.spark.nodes

import java.util

import com._4paradigm.fesql.common.{FesqlException, JITManager, SerializableByteBuffer}
import com._4paradigm.fesql.spark._
import com._4paradigm.fesql.spark.element.FesqlConfig
import com._4paradigm.fesql.spark.utils.{AutoDestructibleIterator, FesqlUtil, SparkColumnUtil, SparkRowUtil}
import com._4paradigm.fesql.utils.SkewUtils
import com._4paradigm.fesql.vm.{CoreAPI, FeSQLJITWrapper, PhysicalWindowAggrerationNode, WindowInterface}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, DataFrame, Row, functions}
import org.apache.spark.sql.types._
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
    val inputDf = if (FesqlConfig.mode.equals(FesqlConfig.skew)) {
      improveSkew(ctx, node, input.getDf(ctx.getSparkSession), windowAggConfig)
    } else {
      groupAndSort(ctx, node, input.getDf(ctx.getSparkSession))
    }

    val resultRDD = inputDf.rdd.mapPartitions(iter => {
      // ensure worker native
      val tag = windowAggConfig.moduleTag
      val buffer = windowAggConfig.moduleNoneBroadcast.getBuffer
      JITManager.initJITModule(tag, buffer)
      val jit = JITManager.getJIT(tag)

      windowAggIter(iter, jit, windowAggConfig)
    })
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

    val resultRDD = inputDf.rdd.mapPartitions(iter => {
      // ensure worker native
      val tag = windowAggConfig.moduleTag
      val buffer = windowAggConfig.moduleNoneBroadcast.getBuffer
      JITManager.initJITModule(tag, buffer)
      val jit = JITManager.getJIT(tag)

      windowAggIterWithUnionFlag(iter, jit, windowAggConfig)
    })
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


    WindowAggConfig(
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
      limitCnt = node.GetLimitCnt()
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
    val res = input.rdd.flatMap(row => {
        val arr = row.toSeq.toArray
        var arrays = Seq(row)
        val value = arr(tag_index)
//              System.out.println(String.format("value = %d, keys = %s, %s ts = %d", value.asInstanceOf[Int], arr(0).asInstanceOf[String], arr(1).asInstanceOf[String], arr(2).asInstanceOf[Int]))
        for (i <- 1 until value.asInstanceOf[Int]) {
//        println("i = " + i)
        val temp_arr = row.toSeq.toArray
        temp_arr(tag_index) = i
        arrays = arrays :+ Row.fromSeq(temp_arr)
      }
        arrays
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
    val windowOp = node.window()
    val groupByExprs = windowOp.partition().keys()
    val keysName = new util.ArrayList[String]()
    var ts: String = ""

    val groupByCols = mutable.ArrayBuffer[Column]()
    for (i <- 0 until groupByExprs.GetChildNum()) {
      val expr = groupByExprs.GetChild(i)
      val colIdx = SparkColumnUtil.resolveColumnIndex(expr, node.GetProducer(0))
      groupByCols += SparkColumnUtil.getColumnFromIndex(input, colIdx)
      keysName.add(input.schema.apply(colIdx).name)
    }

    val orders = windowOp.sort().orders()
    val orderExprs = orders.order_by()
    val orderByCols = mutable.ArrayBuffer[Column]()
    for (i <- 0 until orderExprs.GetChildNum()) {
      val expr = orderExprs.GetChild(i)
      val colIdx = SparkColumnUtil.resolveColumnIndex(expr, node.GetProducer(0))
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
    val quantile = math.pow(2, ctx.getConf(FesqlConfig.configSkewLevel, FesqlConfig.skewLevel))
    val analyzeSQL = SkewUtils.genPercentileSql(table, quantile.intValue(), keysName, ts, FesqlConfig.skewCntName)
    logger.info(s"skew analyze sql : ${analyzeSQL}")
    input.createOrReplaceTempView(table)
    val reportDf = input.sqlContext.sql(analyzeSQL)
    reportDf.createOrReplaceTempView(reportTable)
    val keysMap = new util.HashMap[String, String]()
    var keyScala = keysName.asScala
    keyScala.foreach(e => keysMap.put(e, e))
    val schemas = scala.collection.JavaConverters.seqAsJavaList(input.schema.fieldNames)


    val tagSQL = SkewUtils.genPercentileTagSql(table, reportTable, quantile.intValue(), schemas, keysMap, ts, FesqlConfig.skewTag, FesqlConfig.skewPosition, FesqlConfig.skewCntName)
    logger.info(s"skew tag sql : ${tagSQL}")
    var skewDf = input.sqlContext.sql(tagSQL)

    config.skewTagIdx = skewDf.schema.fieldNames.length - 2
    config.skewPositionIdx = skewDf.schema.fieldNames.length - 1

    skewDf = expansionData(skewDf, config)

    keyScala = keyScala :+ ctx.getConf(FesqlConfig.configSkewTag, FesqlConfig.skewTag)



    val partitions = ctx.getConf(FesqlConfig.configPartitions, FesqlConfig.paritions)
    val groupedDf = if (partitions > 0) {
      skewDf.repartition(partitions, keyScala.map(skewDf(_)): _*)
    } else {
      skewDf.repartition(keyScala.map(skewDf(_)): _*)
    }
    keyScala = keyScala :+ ts
    // todo order desc asc
    val sortedDf = groupedDf.sortWithinPartitions(keyScala.map(skewDf(_)): _*)
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

    val partitions = ctx.getConf(FesqlConfig.configPartitions, FesqlConfig.paritions)
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


  def windowAggIter(inputIter: Iterator[Row],
                    jit: FeSQLJITWrapper,
                    config: WindowAggConfig): Iterator[Row] = {
    val computer = new WindowComputer(config, jit)
    var lastRow: Row = null

    // Take the iterator if the limit has been set
    val limitInputIter = if (config.limitCnt > 0) inputIter.take(config.limitCnt) else inputIter

    // todo isSkew need to be check
    val resIter = if (FesqlConfig.mode == FesqlConfig.skew) {
      limitInputIter.flatMap(row => {
        if (lastRow != null) {
          computer.checkPartition(row, lastRow)
        }
        lastRow = row

        val tag = row(config.skewTagIdx)
        val position = row(config.skewPositionIdx)
        if (tag == position) {
          Some(computer.compute(row))
        } else {
          computer.bufferRowOnly(row)
          None
        }
//        computer.compute(row)
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

//    val resIter = limitInputIter.flatMap(row => {
//      if (lastRow != null) {
//        computer.checkPartition(row, lastRow)
//      }
//      lastRow = row
//      Some(computer.compute(row))
////      computer.compute(row)
//    })

    AutoDestructibleIterator(resIter) {
      computer.delete()
    }
  }


  def windowAggIterWithUnionFlag(inputIter: Iterator[Row],
                                 jit: FeSQLJITWrapper,
                                 config: WindowAggConfig): Iterator[Row] = {
    val computer = new WindowComputer(config, jit)
    val flagIdx = config.unionFlagIdx
    var lastRow: Row = null

    val resIter = inputIter.flatMap(row => {
      if (lastRow != null) {
        computer.checkPartition(row, lastRow)
      }
      lastRow = row

      val unionFlag = row.getBoolean(flagIdx)
      if (unionFlag) {
        // primary
        if (FesqlConfig.mode == FesqlConfig.skew) {
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
  case class WindowAggConfig(startOffset: Long,
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
                             limitCnt: Int)


  /**
   * Stateful class for window computation during row iteration
   */
  class WindowComputer(config: WindowAggConfig, jit: FeSQLJITWrapper) {

    // reuse spark output row backed array
    private val outputFieldNum = config.outputSchemaSlices.map(_.size).sum
    private val outputArr = Array.fill[Any](outputFieldNum)(null)

    // native row codecs
    private var encoder = new SparkRowCodec(config.inputSchemaSlices)
    private var decoder = new SparkRowCodec(config.outputSchemaSlices)

    // order key extractor
    private val orderField = config.inputSchema(config.orderIdx)
    private val orderKeyExtractor = SparkRowUtil.createOrderKeyExtractor(
      config.orderIdx, orderField.dataType, orderField.nullable)

    // append slices cnt = needAppendInput ? inputSchemaSlices.size : 0
    private val appendSlices = if (config.needAppendInput) config.inputSchemaSlices.size else 0
    // group key comparation
    private val groupKeyComparator = FesqlUtil.createGroupKeyComparator(
      config.groupIdxs, config.inputSchema)

    // native function handle
    private val fn = jit.FindFunction(config.functionName)

    // window state
    private var window = new WindowInterface(
      config.instanceNotInWindow, config.startOffset, 0, config.rowPreceding, 0)


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
        // TODO: wrap iter to hook iter end; now last window is leak
        window.delete()
        window = new WindowInterface(
          config.instanceNotInWindow, config.startOffset, 0, config.rowPreceding, 0)
      }
    }


    def delete(): Unit = {
      encoder.delete()
      encoder = null

      decoder.delete()
      decoder = null

      window.delete()
      window = null
    }
  }

}
