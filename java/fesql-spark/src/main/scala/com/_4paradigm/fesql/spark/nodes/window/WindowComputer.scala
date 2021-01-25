package com._4paradigm.fesql.spark.nodes.window

import com._4paradigm.fesql.spark.{FeSQLConfig, SparkRowCodec}
import com._4paradigm.fesql.spark.nodes.WindowAggPlan.{WindowAggConfig, logger}
import com._4paradigm.fesql.spark.utils.{FesqlUtil, SparkRowUtil}
import com._4paradigm.fesql.vm.{CoreAPI, FeSQLJITWrapper, WindowInterface}
import org.apache.spark.sql.Row
import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
  * Stateful class for window computation during row iteration
  */
class WindowComputer(sqlConfig: FeSQLConfig,
                     config: WindowAggConfig,
                     jit: FeSQLJITWrapper) {

  private val logger = LoggerFactory.getLogger(this.getClass)

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

  // hooks
  private val hooks = mutable.ArrayBuffer[WindowHook]()

  // window state
  protected var window = new WindowInterface(
    config.instanceNotInWindow,
    config.excludeCurrentTime,
    config.windowFrameTypeName,
    config.startOffset, config.endOffset, config.rowPreceding, config.maxSize)

  def compute(row: Row): Row = {
    if (hooks.nonEmpty) {
      hooks.foreach(_.preCompute(this, row))
    }

    // call encode
    val nativeInputRow = encoder.encode(row)

    // extract key
    val key = orderKeyExtractor.apply(row)

    // call native compute
    // note: row is buffered automatically by core api
    val outputNativeRow  = CoreAPI.WindowProject(fn, key, nativeInputRow, true, appendSlices, window)

    // call decode
    decoder.decode(outputNativeRow, outputArr)

    if (hooks.nonEmpty) {
      hooks.foreach(_.postCompute(this, row))
    }

    // release swig jni objects
    nativeInputRow.delete()
    outputNativeRow.delete()

    Row.fromSeq(outputArr) // can reuse backed array
  }

  def bufferRowOnly(row: Row): Unit = {
    if (hooks.nonEmpty) {
      hooks.foreach(_.preBufferOnly(this, row))
    }

    val nativeInputRow = encoder.encode(row)
    val key = orderKeyExtractor.apply(row)
    if (!window.BufferData(key, nativeInputRow)) {
      logger.error(s"BufferData Fail, please check order key: $key")
    }
    nativeInputRow.delete()

    // release swig jni objects
    nativeInputRow.delete()

    if (hooks.nonEmpty) {
      hooks.foreach(_.postBufferOnly(this, row))
    }
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
    var max_size = 0
    if (config.startOffset == 0 && config.rowPreceding > 0) {
      max_size = config.rowPreceding.intValue() + 1
    }
    window = new WindowInterface(
      config.instanceNotInWindow, config.excludeCurrentTime,
      config.windowFrameTypeName,
      config.startOffset, config.endOffset, config.rowPreceding, config.maxSize)
  }

  def extractKey(curRow: Row): Long = {
    this.orderKeyExtractor.apply(curRow)
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
  def getEncoder: SparkRowCodec = encoder
  def getDecoder: SparkRowCodec = decoder

  def addHook(hook: WindowHook): Unit = hooks += hook
}