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

import com._4paradigm.hybridse.`type`.TypeOuterClass.{Type => HybridseProtoType}
import org.apache.spark.sql.types.{BooleanType, DataType, DateType, DoubleType, FloatType, IntegerType, LongType,
  ShortType, StringType, TimestampType}
import com._4paradigm.hybridse.node.{DataType => OpenmldbDataType}

object DataTypeUtil {

  def sparkTypeToHybridseProtoType(dtype: DataType): HybridseProtoType = {
    dtype match {
      case ShortType => HybridseProtoType.kInt16
      case IntegerType => HybridseProtoType.kInt32
      case LongType => HybridseProtoType.kInt64
      case FloatType => HybridseProtoType.kFloat
      case DoubleType => HybridseProtoType.kDouble
      case BooleanType => HybridseProtoType.kBool
      case StringType => HybridseProtoType.kVarchar
      case DateType => HybridseProtoType.kDate
      case TimestampType => HybridseProtoType.kTimestamp
      case _ => throw new IllegalArgumentException(
        s"Spark type $dtype not supported")
    }
  }

  def openmldbTypeToProtoType(dtype: OpenmldbDataType): HybridseProtoType = {
    dtype match {
      case OpenmldbDataType.kInt16 => HybridseProtoType.kInt16
      case OpenmldbDataType.kInt32 => HybridseProtoType.kInt32
      case OpenmldbDataType.kInt64 => HybridseProtoType.kInt64
      case OpenmldbDataType.kFloat => HybridseProtoType.kFloat
      case OpenmldbDataType.kDouble => HybridseProtoType.kDouble
      case OpenmldbDataType.kBool => HybridseProtoType.kBool
      case OpenmldbDataType.kVarchar => HybridseProtoType.kVarchar
      case OpenmldbDataType.kDate => HybridseProtoType.kDate
      case OpenmldbDataType.kTimestamp => HybridseProtoType.kTimestamp
      case _ => throw new IllegalArgumentException(
        s"Inner type $dtype not supported")
    }
  }

  def hybridseProtoTypeToSparkType(dtype: HybridseProtoType): DataType = {
    dtype match {
      case HybridseProtoType.kInt16 => ShortType
      case HybridseProtoType.kInt32 => IntegerType
      case HybridseProtoType.kInt64 => LongType
      case HybridseProtoType.kFloat => FloatType
      case HybridseProtoType.kDouble => DoubleType
      case HybridseProtoType.kBool => BooleanType
      case HybridseProtoType.kVarchar => StringType
      case HybridseProtoType.kDate => DateType
      case HybridseProtoType.kTimestamp => TimestampType
      case _ => throw new IllegalArgumentException(
        s"HybridSE type $dtype not supported")
    }
  }

  def protoTypeToOpenmldbType(dtype: com._4paradigm.openmldb.proto.Type.DataType): OpenmldbDataType = {
    dtype match {
      case com._4paradigm.openmldb.proto.Type.DataType.kSmallInt => OpenmldbDataType.kInt16
      case com._4paradigm.openmldb.proto.Type.DataType.kInt => OpenmldbDataType.kInt32
      case com._4paradigm.openmldb.proto.Type.DataType.kBigInt => OpenmldbDataType.kInt64
      case com._4paradigm.openmldb.proto.Type.DataType.kFloat => OpenmldbDataType.kFloat
      case com._4paradigm.openmldb.proto.Type.DataType.kDouble => OpenmldbDataType.kDouble
      case com._4paradigm.openmldb.proto.Type.DataType.kBool => OpenmldbDataType.kBool
      case com._4paradigm.openmldb.proto.Type.DataType.kString => OpenmldbDataType.kVarchar
      case com._4paradigm.openmldb.proto.Type.DataType.kVarchar => OpenmldbDataType.kVarchar
      case com._4paradigm.openmldb.proto.Type.DataType.kDate => OpenmldbDataType.kDate
      case com._4paradigm.openmldb.proto.Type.DataType.kTimestamp => OpenmldbDataType.kTimestamp
      case _ => throw new IllegalArgumentException(
        s"Openmldb proto data type $dtype not supported")
    }
  }

  def protoTypeToSparkType(dtype: com._4paradigm.openmldb.proto.Type.DataType): DataType = {
    dtype match {
      case com._4paradigm.openmldb.proto.Type.DataType.kSmallInt => ShortType
      case com._4paradigm.openmldb.proto.Type.DataType.kInt => IntegerType
      case com._4paradigm.openmldb.proto.Type.DataType.kBigInt => LongType
      case com._4paradigm.openmldb.proto.Type.DataType.kFloat => FloatType
      case com._4paradigm.openmldb.proto.Type.DataType.kDouble => DoubleType
      case com._4paradigm.openmldb.proto.Type.DataType.kBool => BooleanType
      case com._4paradigm.openmldb.proto.Type.DataType.kString => StringType
      case com._4paradigm.openmldb.proto.Type.DataType.kVarchar => StringType
      case com._4paradigm.openmldb.proto.Type.DataType.kDate => DateType
      case com._4paradigm.openmldb.proto.Type.DataType.kTimestamp => TimestampType
      case _ => throw new IllegalArgumentException(
        s"HybridSE proto data type $dtype not supported")
    }
  }

  def openmldbTypeToSparkType(dtype: com._4paradigm.hybridse.node.DataType): DataType = {
    dtype match {
      case com._4paradigm.hybridse.node.DataType.kInt16 => ShortType
      case com._4paradigm.hybridse.node.DataType.kInt32 => IntegerType
      case com._4paradigm.hybridse.node.DataType.kInt64 => LongType
      case com._4paradigm.hybridse.node.DataType.kFloat => FloatType
      case com._4paradigm.hybridse.node.DataType.kDouble => DoubleType
      case com._4paradigm.hybridse.node.DataType.kBool => BooleanType
      case com._4paradigm.hybridse.node.DataType.kVarchar => StringType
      case com._4paradigm.hybridse.node.DataType.kDate => DateType
      case com._4paradigm.hybridse.node.DataType.kTimestamp => TimestampType
      case _ => throw new IllegalArgumentException(
        s"HybridSE node data type $dtype not supported")
    }
  }

  def hybridseProtoTypeToOpenmldbType(dtype: HybridseProtoType): OpenmldbDataType = {
    dtype match {
      case HybridseProtoType.kInt16 => OpenmldbDataType.kInt16
      case HybridseProtoType.kInt32 => OpenmldbDataType.kInt32
      case HybridseProtoType.kInt64 => OpenmldbDataType.kInt64
      case HybridseProtoType.kFloat => OpenmldbDataType.kFloat
      case HybridseProtoType.kDouble => OpenmldbDataType.kDouble
      case HybridseProtoType.kBool => OpenmldbDataType.kBool
      case HybridseProtoType.kVarchar => OpenmldbDataType.kVarchar
      case HybridseProtoType.kDate => OpenmldbDataType.kDate
      case HybridseProtoType.kTimestamp => OpenmldbDataType.kTimestamp
      case _ => throw new IllegalArgumentException(
        s"Schema type $dtype not supported")
    }
  }

  def sparkTypeToString(dataType: DataType): String = {
    dataType match {
      case ShortType => "short"
      case IntegerType => "int"
      case LongType => "int64"
      case FloatType => "float"
      case DoubleType => "double"
      case BooleanType => "bool"
      case StringType => "string"
      case DateType => "date"
      case TimestampType => "timestamp"
      case _ => throw new IllegalArgumentException(
        s"Spark type $dataType not supported")
    }
  }

}
