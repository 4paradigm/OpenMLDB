package com._4paradigm.fesql.offline.nodes

import com._4paradigm.fesql.offline._
import com._4paradigm.fesql.offline.utils.{AutoDestructibleIterator, FesqlUtil, SparkColumnUtil, SparkRowUtil}
import com._4paradigm.fesql.vm.{CoreAPI, FeSQLJITWrapper, PhysicalWindowAggrerationNode, WindowInterface}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, DataFrame, Row, functions}
import org.apache.spark.sql.types._

import scala.collection.mutable


object WindowAggPlan {

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
    // group and sort
    val inputDf = groupAndSort(ctx, node, input.getDf(ctx.getSparkSession))
    val windowAggConfig = createWindowAggConfig(ctx, node)

    val resultRDD = inputDf.rdd.mapPartitions(iter => {
      // ensure worker native
      val tag = windowAggConfig.moduleTag
      val buffer = windowAggConfig.moduleBroadcast.value.getBuffer
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

    val inputDf = groupAndSort(ctx, node, union)
    val windowAggConfig = createWindowAggConfig(ctx, node)

    val resultRDD = inputDf.rdd.mapPartitions(iter => {
      // ensure worker native
      val tag = windowAggConfig.moduleTag
      val buffer = windowAggConfig.moduleBroadcast.value.getBuffer
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
        throw new FeSQLException("{$i}th Window union with inconsistent schema:\n" +
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
      throw new FeSQLException("Multiple window order not supported")
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
    val flagIdx = if (node.window_unions().Empty()) -1 else inputSchema.size

    WindowAggConfig(
      startOffset = node.window.range.frame.GetHistoryRangeStart(),
      rowPreceding = -1 * node.window.range.frame.GetHistoryRowsStart(),
      orderIdx = orderIdx,
      groupIdxs = groupIdxs.toArray,
      functionName = node.project.fn_name,
      moduleTag = ctx.getTag,
      moduleBroadcast = ctx.getModuleBufferBroadcast,
      inputSchema = inputSchema,
      inputSchemaSlices = inputSchemaSlices,
      outputSchemaSlices = outputSchemaSlices,
      unionFlagIdx = flagIdx,
      instanceNotInWindow = node.instance_not_in_window(),
      needAppendInput = node.need_append_input(),
      limitCnt = node.GetLimitCnt()
    )
  }


  def groupAndSort(ctx: PlanContext, node: PhysicalWindowAggrerationNode, input: DataFrame): DataFrame = {
    val windowOp = node.window()
    val groupByExprs = windowOp.partition().keys()

    val groupByCols = mutable.ArrayBuffer[Column]()
    for (i <- 0 until groupByExprs.GetChildNum()) {
      val expr = groupByExprs.GetChild(i)
      val colIdx = SparkColumnUtil.resolveColumnIndex(expr, node.GetProducer(0))
      groupByCols += SparkColumnUtil.getCol(input, colIdx)
    }

    val partitions = ctx.getConf("fesql.group.partitions", 0)
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
      val column = SparkColumnUtil.getCol(input, colIdx)
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
    val limitInputIter = if (config.limitCnt >= 0) inputIter.take(config.limitCnt) else inputIter

    val resIter = limitInputIter.map(row => {
      if (lastRow != null) {
        computer.checkPartition(row, lastRow)
      }
      lastRow = row
      computer.compute(row)
    })
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
        Some(computer.compute(row))
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
                             moduleBroadcast: Broadcast[SerializableByteBuffer],
                             inputSchema: StructType,
                             inputSchemaSlices: Array[StructType],
                             outputSchemaSlices: Array[StructType],
                             unionFlagIdx: Int,
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
