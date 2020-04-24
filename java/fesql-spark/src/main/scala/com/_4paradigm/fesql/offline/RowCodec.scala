package com._4paradigm.fesql.offline

import java.nio.ByteBuffer

import com._4paradigm.fesql.codec.{RowBuilder, RowView, Row => NativeRow}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

class RowCodec(schema: StructType) {

  private val columnDefs = FesqlUtil.getFeSQLSchema(schema)

  // for encode
  private var rowBuilder = new RowBuilder(columnDefs)

  // for decode
  private var rowView = new RowView(columnDefs)

  private val stringFields = inferStringFields()

  def encode(row: Row, buffer: ByteBuffer): Unit = {
    rowBuilder.SetBuffer(buffer)

    val fieldNum = schema.size
    for (i <- 0 until fieldNum) {
      val field = schema(i)
      if (row.isNullAt(i)) {
        rowBuilder.AppendNULL()
      } else {
        field.dataType match {
          case ShortType =>
            rowBuilder.AppendInt16(row.getShort(i))
          case IntegerType =>
            rowBuilder.AppendInt32(row.getInt(i))
          case LongType =>
            rowBuilder.AppendInt64(row.getLong(i))
          case FloatType =>
            rowBuilder.AppendFloat(row.getFloat(i))
          case DoubleType =>
            rowBuilder.AppendDouble(row.getDouble(i))
          case BooleanType =>
            rowBuilder.AppendBool(row.getBoolean(i))
          case StringType =>
            val str = row.getString(i)
            rowBuilder.AppendString(str, str.length)
          case TimestampType =>
            rowBuilder.AppendTimestamp(row.getTimestamp(i).getTime)
          case _ => throw new IllegalArgumentException(
            s"Spark type ${field.dataType} not supported")
        }
      }
    }
  }


  def decode(nativeRow: NativeRow, output: Array[Any]): Unit = {
    rowView.Reset(nativeRow.buf(), nativeRow.size())

    val fieldNum = schema.size
    for (i <- 0 until fieldNum) {
      val field = schema(i)
      if (rowView.IsNULL(i)) {
        output(i) = null
      } else {
        field.dataType match {
          case ShortType =>
            output(i) = rowView.GetInt16Unsafe(i)
          case IntegerType =>
            output(i) = rowView.GetInt32Unsafe(i)
          case LongType =>
            output(i) = rowView.GetInt64Unsafe(i)
          case FloatType =>
            output(i) = rowView.GetFloatUnsafe(i)
          case DoubleType =>
            output(i) = rowView.GetDoubleUnsafe(i)
          case BooleanType =>
            output(i) = rowView.GetBoolUnsafe(i)
          case StringType =>
            output(i) = rowView.GetStringUnsafe(i)
          case TimestampType =>
            output(i) = rowView.GetTimestampUnsafe(i)
          case _ => throw new IllegalArgumentException(
            s"Spark type ${field.dataType} not supported")
        }
      }
    }
  }


  def getNativeRowSize(row: Row): Int = {
    var length = 0
    for (idx <- stringFields) {
      if (! row.isNullAt(idx)) {
        val str = row.getString(idx)
        if (str != null) {
          length += str.length
        }
      }
    }
    rowBuilder.CalTotalLength(length)
  }


  private def inferStringFields(): Array[Int] = {
    schema.zipWithIndex
      .filter(f => f._1.dataType == StringType)
      .map(_._2).toArray
  }


  def delete(): Unit = {
    rowView.delete()
    rowView = null

    rowBuilder.delete()
    rowBuilder = null
  }
}
