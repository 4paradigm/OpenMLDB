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

import com._4paradigm.hybridse.`type`.TypeOuterClass.{Type => HybridseProtoType, ColumnSchema}
import org.apache.spark.sql.types.{BooleanType, DataType, DateType, DoubleType, FloatType, IntegerType, LongType,
  ShortType, StringType, TimestampType, MapType, ArrayType, StructField}
import com._4paradigm.hybridse.node.{DataType => OpenmldbDataType}
import com._4paradigm.hybridse.`type`.TypeOuterClass.ColumnSchema.TypeCase.BASE_TYPE
import com._4paradigm.hybridse.`type`.TypeOuterClass.ColumnSchema.TypeCase.ARRAY_TYPE
import com._4paradigm.hybridse.`type`.TypeOuterClass.ColumnSchema.TypeCase.MAP_TYPE
import com._4paradigm.hybridse.`type`.TypeOuterClass.{ArrayType => HSArrayType, MapType => HSMapType}

object DataTypeUtil {

  def sparkTypeToHybridseProtoType(dtype: DataType): ColumnSchema = {
    var builder = ColumnSchema.newBuilder()
    dtype match {
      case ShortType => builder.setBaseType(HybridseProtoType.kInt16)
      case IntegerType => builder.setBaseType(HybridseProtoType.kInt32)
      case LongType => builder.setBaseType(HybridseProtoType.kInt64)
      case FloatType => builder.setBaseType(HybridseProtoType.kFloat)
      case DoubleType => builder.setBaseType(HybridseProtoType.kDouble)
      case BooleanType => builder.setBaseType(HybridseProtoType.kBool)
      case StringType => builder.setBaseType(HybridseProtoType.kVarchar)
      case DateType => builder.setBaseType(HybridseProtoType.kDate)
      case TimestampType => builder.setBaseType(HybridseProtoType.kTimestamp)
      case ArrayType(eleType, containsNull) => {
        var hsArrType = HSArrayType.newBuilder()
          .setEleType(sparkTypeToHybridseProtoType(eleType)
            .toBuilder().setIsNotNull(!containsNull))
          .build()
        builder.setArrayType(hsArrType)
      }
      case MapType(keyType, valueType, valContainsNull) => {
        var hsKeyType = sparkTypeToHybridseProtoType(keyType)
        var hsValType = sparkTypeToHybridseProtoType(valueType)
          .toBuilder().setIsNotNull(!valContainsNull)
        builder.setMapType(HSMapType.newBuilder()
          .setKeyType(hsKeyType)
          .setValueType(hsValType)
          .build())
      }
      case _ => throw new IllegalArgumentException(
        s"Spark type $dtype not supported")
    }

    builder.build()
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

  def hybridseProtoTypeToSparkType(dtype: ColumnSchema): DataType = {
    dtype.getTypeCase() match {
      case BASE_TYPE => {
        dtype.getBaseType() match {
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
      case ARRAY_TYPE => {
        var ele = hybridseProtoTypeToSparkType(dtype.getArrayType().getEleType())
        ArrayType(ele, !dtype.getIsNotNull())
      }
      case MAP_TYPE => {
        var keyType = hybridseProtoTypeToSparkType(dtype.getMapType().getKeyType())
        var valueType = hybridseProtoTypeToSparkType(dtype.getMapType().getValueType())
        var valNull = !dtype.getMapType().getValueType().getIsNotNull()
        MapType(keyType, valueType, valNull)
      }
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
      case MapType(keyType, valueType, valueContainsNull) => {
        var valueAttr = if (valueContainsNull) { "" } else { "NOT NULL" }
        s"MAP<${sparkTypeToString(keyType)}, ${sparkTypeToString(valueType)} $valueAttr>"
      }
      case _ => throw new IllegalArgumentException(
        s"Spark type $dataType not supported")
    }
  }

}
