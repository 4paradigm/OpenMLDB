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

package com._4paradigm.openmldb.batch

import java.sql.{Date, Timestamp}

import com._4paradigm.hybridse.codec.{RowBuilder, RowView, Row => NativeRow, RowBuilder2}
import com._4paradigm.std.{ExprNodeVector}
import com._4paradigm.hybridse.sdk.HybridSeException
import com._4paradigm.hybridse.sdk.JitManager
import com._4paradigm.hybridse.vm.CoreAPI
import com._4paradigm.hybridse.node.{NodeManager, ExprNode}
import com._4paradigm.openmldb.batch.utils.HybridseUtil
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{BooleanType, DateType, DoubleType, FloatType, IntegerType, LongType, ShortType,
  StringType, StructType, TimestampType, MapType, ArrayType, DataType}
import org.slf4j.LoggerFactory
import java.util.Calendar
import java.util.concurrent.atomic.AtomicInteger
import java.text.SimpleDateFormat
import scala.collection.JavaConverters.asScalaBufferConverter

import scala.collection.mutable
import com._4paradigm.hybridse.`type`.TypeOuterClass.ColumnDef


class SparkRowCodec(sliceSchemas: Array[StructType]) {

  private val logger = LoggerFactory.getLogger(this.getClass)
  private val sliceNum = sliceSchemas.length
  private val columnDefSegmentList = sliceSchemas.map(HybridseUtil.getHybridseSchema)

  private val newEncoder =
    columnDefSegmentList.foldLeft(false)((res, sc) => res || requireNewEncoder(sc.asScala.toArray))

  // for encode
  private var rowBuilders = columnDefSegmentList.map(cols => new RowBuilder(cols))

  // for decode
  private var rowViews = columnDefSegmentList.map(cols => new RowView(cols))

  private val stringFields = inferStringFields()

  private val sliceFieldOffsets = sliceSchemas.scanLeft(0)(
    (cur, schema) => cur + schema.size)

  private val jitCounter = new AtomicInteger(0)

  def encode(row: Row): NativeRow = {
    if (newEncoder) {
      val cnt = jitCounter.getAndIncrement();
      // FIXME(#3748): native codegen row builder have issue adding multiple modules to
      // the same jit instance: duplicated symbol error.
      // this is the work-around that ensure every row encoded with distinct jit instance
      val tag = "rowbuilder2_" + Thread.currentThread().getId + "_" + cnt;
      try {
        val jit = JitManager.getJit(tag)
        var newRowBuilder = new RowBuilder2(jit, sliceSchemas.size)
        for (i <- 0 until columnDefSegmentList.size) {
          val s = newRowBuilder.InitSchema(i, columnDefSegmentList(i))
          if (!s.isOK()) {
            throw new HybridSeException(s.str())
          }
        }
        var s = newRowBuilder.Init()
        if (!s.isOK()) {
          throw new HybridSeException(s.str())
        }

        var nm = new NodeManager();
        var vec = sparkRowToNativeExprVec(row, nm)

        var nativeRow = new NativeRow()
        s = newRowBuilder.Build(vec, nativeRow)
        if (!s.isOK()) {
          throw new HybridSeException(s.str())
        }
        return nativeRow
      } finally {
        JitManager.removeModule(tag)
      }
    }

    var result: NativeRow = null
    // collect slice size and string raw bytes
    val sliceSizes = Array.fill(sliceNum)(0)
    val sliceStrings = Array.fill(sliceNum)(mutable.ArrayBuffer[Array[Byte]]())
    for (i <- 0 until sliceNum) {
      var strTotalLength = 0
      val buffer = sliceStrings(i)
      stringFields(i).foreach(idx => {
        if (!row.isNullAt(idx)) {
          val str = row.getString(idx)
          val bytes = str.getBytes("utf-8")
          strTotalLength += bytes.length
          buffer += bytes
        } else {
          buffer += null
        }
      })
      sliceSizes(i) = rowBuilders(i).CalTotalLength(strTotalLength)
    }

    for (i <- 0 until sliceNum) {
      val sliceSize = sliceSizes(i)
      if (i == 0) {
        result = CoreAPI.NewRow(sliceSize)
        val buf = CoreAPI.GetRowBuf(result, 0)
        encodeSingle(row, buf, sliceSize, sliceStrings(i), i)
      } else {
        val buf = CoreAPI.AppendRow(result, sliceSize)
        encodeSingle(row, buf, sliceSize, sliceStrings(i), i)
      }
    }
    result
  }


  def decode(nativeRow: NativeRow, output: Array[Any]): Unit = {
    for (i <- 0 until sliceNum) {
      decodeSingle(nativeRow, output, i)
    }
  }

  /**
   * This is only used for UnsafeRowOpt and AppendSlice case, only work for single slice encoding.
   */
  def decode(nativeRow1: NativeRow, nativeRow2: NativeRow, row1Size: Int, row2Size: Int, output: Array[Any]): Unit = {
    val sliceIndex = 0
    val rowView = rowViews(sliceIndex)
    val schema = sliceSchemas(sliceIndex)
    var inputRowIndex = 0;

    for (outputColIndex <- 0 until row1Size + row2Size) {

      if (outputColIndex == 0) {
        if (!rowView.Reset(nativeRow1.buf(sliceIndex), nativeRow1.size(sliceIndex))) {
          throw new HybridSeException("Fail to setup row builder, maybe row buf is corrupted")
        }
      } else if (outputColIndex == row1Size) {
        if (!rowView.Reset(nativeRow2.buf(sliceIndex), nativeRow2.size(sliceIndex))) {
          throw new HybridSeException("Fail to setup row builder, maybe row buf is corrupted")
        }
        // Reset the inputRowIndex to get data from row2
        inputRowIndex = 0
      }

      val field = schema(outputColIndex)
      // Notice that we should check the actual col index for row1 or row2
      // TODO(tobe): refactor this to explicitly access row1 or row2
      //if (rowView.IsNULL(outputColIndex)) {
      if (rowView.IsNULL(inputRowIndex)) {
        output(outputColIndex) = null
      } else {
        field.dataType match {
          case ShortType =>
            output(outputColIndex) = rowView.GetInt16Unsafe(inputRowIndex)
          case IntegerType =>
            output(outputColIndex) = rowView.GetInt32Unsafe(inputRowIndex)
          case LongType =>
            output(outputColIndex) = rowView.GetInt64Unsafe(inputRowIndex)
          case FloatType =>
            output(outputColIndex) = rowView.GetFloatUnsafe(inputRowIndex)
          case DoubleType =>
            output(outputColIndex) = rowView.GetDoubleUnsafe(inputRowIndex)
          case BooleanType =>
            output(outputColIndex) = rowView.GetBoolUnsafe(inputRowIndex)
          case StringType =>
            output(outputColIndex) = rowView.GetStringUnsafe(inputRowIndex)
          case TimestampType =>
            output(outputColIndex) = new Timestamp(rowView.GetTimestampUnsafe(inputRowIndex))
          case DateType =>
            val days = rowView.GetDateUnsafe(inputRowIndex)
            // Avoid using new Date(year, month, days) which is deprecated
            val date = java.sql.Date.valueOf("%d-%d-%d".format(rowView.GetYearUnsafe(days),
              rowView.GetMonthUnsafe(days), rowView.GetDayUnsafe(days)))
            output(outputColIndex) = date
          case _ => throw new IllegalArgumentException(
            s"Spark type ${field.dataType} not supported")
        }
      }

      inputRowIndex += 1
    }
  }

  private def encodeSingle(row: Row, outBuf: Long, outSize: Int,
                   sliceStrings: Seq[Array[Byte]], sliceIndex: Int): Unit = {
    val rowBuilder = rowBuilders(sliceIndex)
    val schema = sliceSchemas(sliceIndex)
    rowBuilder.SetBuffer(outBuf, outSize)

    val fieldNum = schema.size
    var fieldOffset = sliceFieldOffsets(sliceIndex)
    var curStringCnt = 0

    for (i <- 0 until fieldNum) {
      val field = schema(i)
      if (row.isNullAt(fieldOffset)) {
        rowBuilder.AppendNULL()
        if (field.dataType == StringType) {
          curStringCnt += 1
        }
      } else {
        var appendOK = false
        field.dataType match {
          case ShortType =>
            appendOK = rowBuilder.AppendInt16(row.getShort(fieldOffset))
          case IntegerType =>
            appendOK = rowBuilder.AppendInt32(row.getInt(fieldOffset))
          case LongType =>
            appendOK = rowBuilder.AppendInt64(row.getLong(fieldOffset))
          case FloatType =>
            appendOK = rowBuilder.AppendFloat(row.getFloat(fieldOffset))
          case DoubleType =>
            appendOK = rowBuilder.AppendDouble(row.getDouble(fieldOffset))
          case BooleanType =>
            appendOK = rowBuilder.AppendBool(row.getBoolean(fieldOffset))
          case StringType =>
            val str = row.getString(fieldOffset)
            val strBytes = sliceStrings(curStringCnt)
            appendOK = rowBuilder.AppendString(str, strBytes.length)
            curStringCnt += 1
          case TimestampType =>
            appendOK = rowBuilder.AppendTimestamp(row.getTimestamp(fieldOffset).getTime)
          case DateType =>
            val date = row.getDate(fieldOffset)
            val cal = Calendar.getInstance
            cal.setTime(date)
            if (!rowBuilder.AppendDate(cal.get(Calendar.YEAR), cal.get(Calendar.MONTH) + 1,
              cal.get(Calendar.DAY_OF_MONTH))) {
              logger.warn(s"Encode date $date failed, encode as null")
              rowBuilder.AppendNULL()
            }
            appendOK = true
          case _ => throw new IllegalArgumentException(
            s"Spark type ${field.dataType} not supported")
        }
        if (!appendOK) {
          throw new IllegalArgumentException(
            s"Fail to encode ${row.get(fieldOffset)} for $i th column " +
            s"${field.name}:${field.dataType} at slice $sliceIndex")
        }
      }
      fieldOffset += 1
    }
  }


  private def decodeSingle(nativeRow: NativeRow, output: Array[Any], sliceIndex: Int): Unit = {
    val rowView = rowViews(sliceIndex)
    val schema = sliceSchemas(sliceIndex)

    if (!rowView.Reset(nativeRow.buf(sliceIndex), nativeRow.size(sliceIndex))) {
      throw new HybridSeException("Fail to setup row builder, maybe row buf is corrupted")
    }

    val fieldNum = schema.size
    var fieldOffset = sliceFieldOffsets(sliceIndex)
    for (i <- 0 until fieldNum) {
      val field = schema(i)
      if (rowView.IsNULL(i)) {
        output(fieldOffset) = null
      } else {
        field.dataType match {
          case ShortType =>
            output(fieldOffset) = rowView.GetInt16Unsafe(i)
          case IntegerType =>
            output(fieldOffset) = rowView.GetInt32Unsafe(i)
          case LongType =>
            output(fieldOffset) = rowView.GetInt64Unsafe(i)
          case FloatType =>
            output(fieldOffset) = rowView.GetFloatUnsafe(i)
          case DoubleType =>
            output(fieldOffset) = rowView.GetDoubleUnsafe(i)
          case BooleanType =>
            output(fieldOffset) = rowView.GetBoolUnsafe(i)
          case StringType =>
            output(fieldOffset) = rowView.GetStringUnsafe(i)
          case TimestampType =>
            output(fieldOffset) = new Timestamp(rowView.GetTimestampUnsafe(i))
          case DateType =>
            val days = rowView.GetDateUnsafe(i)
            // Avoid using new Date(year, month, days) which is deprecated
            val date = java.sql.Date.valueOf("%d-%d-%d".format(rowView.GetYearUnsafe(days),
              rowView.GetMonthUnsafe(days), rowView.GetDayUnsafe(days)))
            output(fieldOffset) = date
          case _ => throw new IllegalArgumentException(
            s"Spark type ${field.dataType} not supported")
        }
      }
      fieldOffset += 1
    }
  }


  private def inferStringFields(): Array[Array[Int]] = {
    var fieldOffset = 0
    sliceSchemas.map(schema => {
      val idxs = schema.zipWithIndex
        .filter(f => f._1.dataType == StringType)
        .map(fieldOffset + _._2).toArray

      fieldOffset += schema.size

      idxs
    })
  }

  private def requireNewEncoder(schema: Array[ColumnDef]): Boolean = {
    schema.foldLeft(false)((res, col) => {
      res || col.getSchema().hasArrayType() || col.getSchema().hasMapType()
    })
  }

  private def sparkRowToNativeExprVec(row: Row, nm: NodeManager): ExprNodeVector = {
    var vec = new ExprNodeVector()

    var idx = 0
    sliceSchemas.foreach(st => {
      st.foreach(col => {
        var expr = sparkColToNativeExpr(row, idx, col.dataType, nm)
        vec.add(expr)
        idx += 1
      })
    })

    vec
  }

  private def sparkColToNativeExpr(row: Row, idx: Int, dataType: DataType, nm: NodeManager): ExprNode = {
      if (row.isNullAt(idx)) {
        nm.MakeConstNode()
      } else {
        dataType match {
          case ShortType =>
            nm.MakeConstNode(row.getShort(idx))
          case IntegerType =>
            nm.MakeConstNode(row.getInt(idx))
          case LongType =>
            nm.MakeConstNode(row.getLong(idx))
          case FloatType =>
            nm.MakeConstNode(row.getFloat(idx))
          case DoubleType =>
            nm.MakeConstNode(row.getDouble(idx))
          case BooleanType =>
            nm.MakeConstNode(row.getBoolean(idx))
          case StringType => {
            // generally safe, native ConstNode copyed the string
            val str = row.getString(idx)
            nm.MakeConstNode(str)
          }
          case TimestampType => {
            var args = nm.MakeExprList()
            args.AddChild(nm.MakeConstNode(row.getTimestamp(idx).getTime))
            nm.MakeFuncNode("timestamp", args, null)
          }
          case DateType => {
            val date = row.getDate(idx)
            val fmt = new SimpleDateFormat("yyyy-MM-dd")
            var str = fmt.format(date)
            var args = nm.MakeExprList()
            args.AddChild(nm.MakeConstNode(str))
            nm.MakeFuncNode("date", args, null)
          }
          case MapType(keyType, valType, valContainsNull) => {
            var mapVal = row.getMap[Any, Any](idx)
            // logger.warn(s"map val ${mapVal}")
            var args = nm.MakeExprList()
            mapVal.foreach(kv => {
              args.AddChild(valToNativeExpr(kv._1, keyType, nm))
              args.AddChild(valToNativeExpr(kv._2, valType, nm))
            })
            nm.MakeFuncNode("map", args, null)
          }
          case _ => throw new IllegalArgumentException(
            s"Spark type ${dataType} not supported")
        }
      }
  }

  private def valToNativeExpr(v: Any, dataType: DataType, nm: NodeManager): ExprNode = {
    dataType match {
      case ShortType => nm.MakeConstNode(v.asInstanceOf[Short])
      case IntegerType => nm.MakeConstNode(v.asInstanceOf[Int])
      case LongType => nm.MakeConstNode(v.asInstanceOf[Long])
      case FloatType => nm.MakeConstNode(v.asInstanceOf[Float])
      case DoubleType => nm.MakeConstNode(v.asInstanceOf[Double])
      case BooleanType => nm.MakeConstNode(v.asInstanceOf[Boolean])
      case StringType => nm.MakeConstNode(v.asInstanceOf[String])
      case TimestampType =>
        var args = nm.MakeExprList()
        args.AddChild(nm.MakeConstNode(v.asInstanceOf[Long]))
        nm.MakeFuncNode("timestamp", args, null)
      case DateType =>
        // date from string literal
        var args = nm.MakeExprList()
        args.AddChild(nm.MakeConstNode(v.asInstanceOf[String]))
        nm.MakeFuncNode("date", args, null)
      case MapType(keyType, valType, _) => {
        var mapVal = v.asInstanceOf[Map[Any, Any]]
        var args = nm.MakeExprList()
        mapVal.foreach(kv => {
          args.AddChild(valToNativeExpr(kv._1, keyType, nm))
          args.AddChild(valToNativeExpr(kv._2, valType, nm))
        })
        // TODO(someone): support empty map, since 'map()' inferred as map<void, void>
        // we need construst a extra cast operation to hint the true type from schema
        nm.MakeFuncNode("map", args, null)
      }
      case _ => throw new IllegalArgumentException(
        s"Spark type ${dataType} not supported")
    }
  }

  def delete(): Unit = {
    if (rowViews != null) {
      rowViews.foreach(_.delete())
      rowViews = null
    }

    if (rowBuilders != null) {
      rowBuilders.foreach(_.delete())
      rowBuilders = null
    }
  }
}

