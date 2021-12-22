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

import com._4paradigm.hybridse.sdk.{HybridSeException, UnsupportedHybridSeException}
import com._4paradigm.openmldb.proto.Type
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{BooleanType, DataType, DateType, DoubleType, FloatType, IntegerType, LongType,
  ShortType, StringType, TimestampType}

object SparkRowUtil {

  def getLongFromIndex(keyIdx: Int, sparkType: DataType, row: Row): Long = {
    sparkType match {
      case ShortType => row.getShort(keyIdx).toLong
      case IntegerType => row.getInt(keyIdx).toLong
      case LongType => row.getLong(keyIdx)
      case TimestampType => row.getTimestamp(keyIdx).getTime
      case DateType => row.getDate(keyIdx).getTime
      case _ =>
        throw new HybridSeException(s"Illegal window key type: $sparkType")
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
      case Type.DataType.kTimestamp => LongType // in openmldb, timestamp format is int64
      case Type.DataType.kVarchar | Type.DataType.kString => StringType
      case e: Any => throw new UnsupportedHybridSeException(s"unsupported proto DataType $e")
    }
  }

}
