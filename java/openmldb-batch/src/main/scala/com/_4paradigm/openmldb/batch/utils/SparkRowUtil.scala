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

package com._4paradigm.openmldb.batch.utils

import com._4paradigm.hybridse.sdk.HybridSeException
import com._4paradigm.openmldb.proto.Type
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{BooleanType, DataType, DateType, DoubleType, FloatType, IntegerType, LongType,
  ShortType, StringType, StructType, TimestampType}

object SparkRowUtil {

  def rowToString(schema: StructType, row: Row): String = {
    val rowStr = new StringBuilder("Spark row: ")
    (0 until schema.size).foreach(i => {
      if (i == 0) {
        rowStr ++= s"${schema(i).dataType}: ${getColumnStringValue(schema, row, i)}"
      } else {
        rowStr ++= s", ${schema(i).dataType}: ${getColumnStringValue(schema, row, i)}"
      }
    })
    rowStr.toString()
  }

  /**
   * Get the string value of the specified column.
   *
   * @param row
   * @param index
   * @return
   */
  def getColumnStringValue(schema: StructType, row: Row, index: Int): String = {
    if (row.isNullAt(index)) {
      "null"
    } else {
      val colType = schema(index).dataType
      colType match {
        case BooleanType => String.valueOf(row.getBoolean(index))
        case ShortType => String.valueOf(row.getShort(index))
        case DoubleType => String.valueOf(row.getDouble(index))
        case IntegerType => String.valueOf(row.getInt(index))
        case LongType => String.valueOf(row.getLong(index))
        case TimestampType => String.valueOf(row.getTimestamp(index))
        case DateType => String.valueOf(row.getDate(index))
        case StringType => row.getString(index)
        case _ =>
          throw new HybridSeException(s"Unsupported data type: $colType")
      }
    }
  }

  def getLongFromIndex(keyIdx: Int, colType: DataType, row: Row): java.lang.Long = {
    if (row.isNullAt(keyIdx)) {
      null
    } else {
      colType match {
        case ShortType => row.getShort(keyIdx).toLong
        case IntegerType => row.getInt(keyIdx).toLong
        case LongType => row.getLong(keyIdx)
        case TimestampType => row.getTimestamp(keyIdx).getTime
        case DateType => row.getDate(keyIdx).getTime
        case _ =>
          throw new HybridSeException(s"Illegal window key type: $colType")
      }
    }
  }

  def protoTypeToScalaType(dataType: Type.DataType): DataType = {
    dataType match {
      case Type.DataType.kBool => BooleanType
      case Type.DataType.kSmallInt => ShortType
      case Type.DataType.kBigInt => LongType
      case Type.DataType.kInt => IntegerType
      case Type.DataType.kFloat => FloatType
      case Type.DataType.kDouble => DoubleType
      case Type.DataType.kDate => DateType
      // In online storage, timestamp format is int64. But in offline storage, we use the spark sql timestamp type
      // (DateTime)
      case Type.DataType.kTimestamp => TimestampType
      case Type.DataType.kVarchar | Type.DataType.kString => StringType
    }
  }

}
