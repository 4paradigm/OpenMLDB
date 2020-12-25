package com._4paradigm.fesql.spark

import java.sql.{Date, Timestamp}

import com._4paradigm.fesql.codec.{RowBuilder, RowView, Row => NativeRow}
import com._4paradigm.fesql.common.FesqlException
import com._4paradigm.fesql.spark.utils.FesqlUtil
import com._4paradigm.fesql.vm.CoreAPI
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory


class SparkRowCodec(sliceSchemas: Array[StructType]) {

  private val logger = LoggerFactory.getLogger(this.getClass)
  private val sliceNum = sliceSchemas.length
  private val columnDefSegmentList = sliceSchemas.map(FesqlUtil.getFeSQLSchema)

  // for encode
  private var rowBuilders = columnDefSegmentList.map(cols => new RowBuilder(cols))

  // for decode
  private var rowViews = columnDefSegmentList.map(cols => new RowView(cols))

  private val stringFields = inferStringFields()

  private val sliceFieldOffsets = sliceSchemas.scanLeft(0)(
    (cur, schema) => cur + schema.size)


  def encode(row: Row): NativeRow = {
    var result: NativeRow = null
    val sliceSizes = getNativeRowSliceSizes(row)

    for (i <- 0 until sliceNum) {
      val sliceSize = sliceSizes(i)
      if (i == 0) {
        result = CoreAPI.NewRow(sliceSize)
        val buf = CoreAPI.GetRowBuf(result, 0)
        encodeSingle(row, buf, sliceSize, i)
      } else {
        val buf = CoreAPI.AppendRow(result, sliceSize)
        encodeSingle(row, buf, sliceSize, i)
      }
    }
    result
  }


  def decode(nativeRow: NativeRow, output: Array[Any]): Unit = {
    for (i <- 0 until sliceNum) {
      decodeSingle(nativeRow, output, i)
    }
  }


  def encodeSingle(row: Row, outBuf: Long, outSize: Int, sliceIndex: Int): Unit = {
    val rowBuilder = rowBuilders(sliceIndex)
    val schema = sliceSchemas(sliceIndex)
    rowBuilder.SetBuffer(outBuf, outSize)

    val fieldNum = schema.size
    var fieldOffset = sliceFieldOffsets(sliceIndex)

    for (i <- 0 until fieldNum) {
      val field = schema(i)
      if (row.isNullAt(fieldOffset)) {
        rowBuilder.AppendNULL()
      } else {
        field.dataType match {
          case ShortType =>
            rowBuilder.AppendInt16(row.getShort(fieldOffset))
          case IntegerType =>
            rowBuilder.AppendInt32(row.getInt(fieldOffset))
          case LongType =>
            rowBuilder.AppendInt64(row.getLong(fieldOffset))
          case FloatType =>
            rowBuilder.AppendFloat(row.getFloat(fieldOffset))
          case DoubleType =>
            rowBuilder.AppendDouble(row.getDouble(fieldOffset))
          case BooleanType =>
            rowBuilder.AppendBool(row.getBoolean(fieldOffset))
          case StringType =>
            val str = row.getString(fieldOffset)
            rowBuilder.AppendString(str, str.length)
          case TimestampType =>
            rowBuilder.AppendTimestamp(row.getTimestamp(fieldOffset).getTime)
          case DateType =>
            val date = row.getDate(fieldOffset)
            rowBuilder.AppendDate(date.getYear + 1900, date.getMonth + 1, date.getDate)
          case _ => throw new IllegalArgumentException(
            s"Spark type ${field.dataType} not supported")
        }
      }
      fieldOffset += 1
    }
  }


  def decodeSingle(nativeRow: NativeRow, output: Array[Any], sliceIndex: Int): Unit = {
    val rowView = rowViews(sliceIndex)
    val schema = sliceSchemas(sliceIndex)

    if (!rowView.Reset(nativeRow.buf(sliceIndex), nativeRow.size(sliceIndex))) {
      throw new FesqlException("Fail to setup row builder, maybe row buf is corrupted")
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
            output(fieldOffset) = new Date(rowView.GetYearUnsafe(days) - 1900,
              rowView.GetMonthUnsafe(days) - 1, rowView.GetDayUnsafe(days))
          case _ => throw new IllegalArgumentException(
            s"Spark type ${field.dataType} not supported")
        }
      }
      fieldOffset += 1
    }
  }


  def getNativeRowSliceSizes(row: Row): Array[Int] = {
    stringFields.zipWithIndex.map { case (fields, idx) =>
      var length = 0
      fields.foreach(idx => {
        if (!row.isNullAt(idx)) {
          val str = row.getString(idx)
          if (str != null) {
            length += str.getBytes("utf-8").length
          }
        }
      })
      rowBuilders(idx).CalTotalLength(length)
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


  def delete(): Unit = {
    rowViews.foreach(_.delete())
    rowViews = null

    rowBuilders.foreach(_.delete())
    rowBuilders = null
  }
}
