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

import com._4paradigm.hybridse.vm.{CoreAPI, HybridSeJitWrapper, WindowInterface}
import com._4paradigm.openmldb.batch.spark.OpenmldbJoinedRow
import com._4paradigm.openmldb.batch.SparkRowCodec
import com._4paradigm.openmldb.batch.utils.{HybridseUtil, SparkRowUtil, UnsafeRowUtil}
import com._4paradigm.openmldb.batch.window.WindowAggPlanUtil.WindowAggConfig
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory
import scala.collection.mutable

/**
  * Stateful class for window computation during row iteration
  */
class WindowComputer(config: WindowAggConfig, jit: HybridSeJitWrapper, keepIndexColumn: Boolean) {

  private val logger = LoggerFactory.getLogger(this.getClass)

  // Add the index column at the end of generated row if this node need to output dataframe with index column
  // reuse Spark output row backed array
  private val outputFieldNum =
  if (keepIndexColumn) config.outputSchemaSlices.map(_.size).sum + 1 else config.outputSchemaSlices.map(_.size).sum
  private val outputArr = Array.fill[Any](outputFieldNum)(null)

  // native row codecs
  protected var encoder = new SparkRowCodec(config.inputSchemaSlices)
  private var decoder = new SparkRowCodec(config.outputSchemaSlices)

  // order key field
  private val orderField = config.inputSchema(config.orderIdx)

  // append slices cnt = needAppendInput ? inputSchemaSlices.size : 0
  private val appendSlices = if (config.needAppendInput) config.inputSchemaSlices.length else 0

  // group key comparation
  private var groupKeyComparator = HybridseUtil.createGroupKeyComparator(config.groupIdxs)

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

  def compute(row: Row, key: Long, keepIndexColumn: Boolean, unionFlagIdx: Int, inputSchemaSize: Int,
              outputSchema: StructType, enableUnsafeRowFormat: Boolean): Row = {
    if (hooks.nonEmpty) {
      hooks.foreach(hook => try {
        hook.preCompute(this, row)
      } catch {
        case e: Exception => e.printStackTrace()
      })
    }

    // call encode
    val inputNativeRow = encoder.encode(row)

    // call native compute
    // note: row is buffered automatically by core api
    val outputNativeRow  = CoreAPI.WindowProject(fn, key, inputNativeRow, true, appendSlices, window)

    val outputInternalRowWithAppend =  if (appendSlices > 0 && enableUnsafeRowFormat) {
      /**
       * When enabling UnsafeRowOpt and appendSlices, it has different processing.
       *
       * 1. C WindowProject will only return the newly added columns.
       * 1. Only get the output row without input row and calculate the size.
       * 2. Join the output row and input row.
       */
      // If window with union, the union input will have one more column at the end of input column, ignore when encode
      val inputRowColNum = if (unionFlagIdx >= 0) {
        row.size - 1
      } else {
        row.size
      }

      val row2Size = inputRowColNum
      val row1Size = outputSchema.size - row2Size

      decoder.decode(outputNativeRow, inputNativeRow, row1Size, row2Size, outputArr)
    } else {
      decoder.decode(outputNativeRow, outputArr)
    }

    if (hooks.nonEmpty) {
      hooks.foreach(hook => try {
        hook.postCompute(this, row)
      } catch {
        case e: Exception => e.printStackTrace()
      })
    }

    // release swig jni objects
    inputNativeRow.delete()
    outputNativeRow.delete()

    // Append the index column if needed
    if (keepIndexColumn) {
      if (unionFlagIdx == -1) {
        // No union column, use the last one
        outputArr(outputArr.length - 1) = row.get(inputSchemaSize)
      } else {
        // Has union column, use the last but one
        outputArr(outputArr.length - 1) = row.get(inputSchemaSize)
      }
    }

    Row.fromSeq(outputArr) // can reuse backed array
  }

  def unsafeCompute(internalRow: InternalRow, key: Long, keepIndexColumn: Boolean, unionFlagIdx: Int,
                    outputSchema: StructType, enableUnsafeRowFormat: Boolean): InternalRow = {
    val inputUnsaferow = internalRow.asInstanceOf[UnsafeRow]

    // Create native method input from Spark InternalRow
    val hybridseRowBytes = UnsafeRowUtil.internalRowToHybridseRowBytes(internalRow)

    // Call native method to compute
    val outputHybridseRow  =
      CoreAPI.UnsafeWindowProject(fn, key, hybridseRowBytes, hybridseRowBytes.length, true, appendSlices, window)

    // TODO: Support append slice in JIT function instead of merge in offline
    val outputInternalRowWithAppend =  if (appendSlices > 0 && enableUnsafeRowFormat) {
      /**
       * When enabling UnsafeRowOpt and appendSlices, it has different processing.
       *
       * 1. C WindowProject will only return the newly added columns.
       * 1. Only get the output row without input row and calculate the size.
       * 2. Join the output row and input row.
       */

      val inputRowColNum = if (unionFlagIdx >= 0) {
        internalRow.numFields - 1
      } else {
        internalRow.numFields
      }

      val outputInternalRow = UnsafeRowUtil.hybridseRowToInternalRow(outputHybridseRow,
        outputSchema.size - inputRowColNum)

      new OpenmldbJoinedRow(outputInternalRow, inputUnsaferow)
    } else {
      // Call methods to generate Spark InternalRow
      UnsafeRowUtil.hybridseRowToInternalRow(outputHybridseRow, outputSchema.size)
    }

    // TODO: Add index column if needed
    outputHybridseRow.delete()

    outputInternalRowWithAppend
  }

  def bufferRowOnly(row: Row, key: Long): Unit = {
    if (hooks.nonEmpty) {
      hooks.foreach(hook => try {
        hook.preBufferOnly(this, row)
      } catch {
        case e: Exception => e.printStackTrace()
      })
    }

    val nativeInputRow = encoder.encode(row)
    if (!window.BufferData(key, nativeInputRow)) {
      logger.error(s"BufferData Fail, please check order key: $key")
    }

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
    SparkRowUtil.getLongFromIndex(config.orderIdx, orderField.dataType, curRow)
  }

  def delete(): Unit = {
    encoder.delete()
    encoder = null

    decoder.delete()
    decoder = null

    window.delete()
    window = null
  }

  def printWindowCols(windowName: String, cols: Array[String]): Unit = {
    val windowData = new java.util.ArrayList[String]()
    if (!config.windowName.equals(windowName) || window.size() <= 0) {
      return
    }
    windowData.add("window " + config.windowName + " data, window size = " + window.size())
    windowData.add(config.inputSchema.toDDL + "\n")
    val indexs = new java.util.ArrayList[Int]()
    for (col <- cols) {
      indexs.add(config.inputSchema.fieldIndex(col))
    }
    val id = config.inputSchema.fieldIndex("reqId")
    val firstArr = new Array[Any](config.inputSchema.size)
    encoder.decode(window.Get(0), firstArr)

    for (index <- 0 until window.size().toInt) {
      val arr = new Array[Any](config.inputSchema.size)
      encoder.decode(window.Get(index), arr)
      val filterArr = new Array[Any](indexs.size())
      for (i <- 0 until indexs.size()) {
        filterArr(i) = arr(indexs.get(i))
      }
      windowData.add(filterArr.mkString(","))
    }
    if (windowData.size() > 0) {
      logger.info(StringUtils.join(windowData, "\n"))
    }
  }

  def resetGroupKeyComparator(keyIdxs: Array[Int]): Unit = {
    groupKeyComparator = HybridseUtil.createGroupKeyComparator(keyIdxs)
  }


  def getWindow: WindowInterface = window
  def getFn: Long = fn
  def getEncoder: SparkRowCodec = encoder
  def getDecoder: SparkRowCodec = decoder

  def addHook(hook: WindowHook): Unit = hooks += hook
}
