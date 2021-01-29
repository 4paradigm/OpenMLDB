package com._4paradigm.fesql.spark.nodes.window

import com._4paradigm.fesql.spark.{FeSQLConfig, SparkRowCodec}
import com._4paradigm.fesql.spark.nodes.WindowAggPlan.WindowAggConfig
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
                     jit: FeSQLJITWrapper,
                     keepIndexColumn: Boolean) {

  private val logger = LoggerFactory.getLogger(this.getClass)

  // Add the index column at the end of generated row if this node need to output dataframe with index column
  // reuse spark output row backed array
  private val outputFieldNum = if (keepIndexColumn) config.outputSchemaSlices.map(_.size).sum + 1 else config.outputSchemaSlices.map(_.size).sum
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
  private var groupKeyComparator = FesqlUtil.createGroupKeyComparator(
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

  def compute(row: Row, keepIndexColumn: Boolean, unionFlagIdx: Int): Row = {
    if (hooks.nonEmpty) {
      hooks.foreach(hook => try {
        hook.preCompute(this, row)
      } catch {
        case e: Exception => e.printStackTrace()
      })
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
      hooks.foreach(hook => try {
        hook.postCompute(this, row)
      } catch {
        case e: Exception => e.printStackTrace()
      })
    }

    // release swig jni objects
    nativeInputRow.delete()
    outputNativeRow.delete()

    // Append the index column if needed
    if (keepIndexColumn) {
      if (unionFlagIdx == -1) {
        // No union column, use the last one
        outputArr(outputArr.size-1) = row.get(row.size-1)
      } else {
        // Has union column, use the last but one
        outputArr(outputArr.size-1) = row.get(row.size-2)
      }
    }

    Row.fromSeq(outputArr) // can reuse backed array
  }

  def bufferRowOnly(row: Row): Unit = {
    if (hooks.nonEmpty) {
      hooks.foreach(hook => try {
        hook.preBufferOnly(this, row)
      } catch {
        case e: Exception => e.printStackTrace()
      })
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
      hooks.foreach(hook => try {
        hook.postBufferOnly(this, row)
      } catch {
        case e: Exception => e.printStackTrace()
      })
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
  import org.apache.spark.sql.types._
  def resetGroupKeyComparator(keyIdxs: Array[Int], schema: StructType): Unit = {
    groupKeyComparator = FesqlUtil.createGroupKeyComparator(keyIdxs, schema)
  }

  def getWindow: WindowInterface = window
  def getFn: Long = fn
  def getEncoder: SparkRowCodec = encoder
  def getDecoder: SparkRowCodec = decoder

  def addHook(hook: WindowHook): Unit = hooks += hook
}