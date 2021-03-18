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

package com._4paradigm.hybridse.spark.utils

import java.util

import com._4paradigm.hybridse.`type`.TypeOuterClass.{ColumnDef, Database, TableDef, Type}
import com._4paradigm.hybridse.vm.PhysicalOpNode
import com._4paradigm.hybridse.node.{DataType => InnerDataType}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._


object FesqlUtil {

  def getOutputSchemaSlices(node: PhysicalOpNode): Array[StructType] = {
    (0 until node.GetOutputSchemaSourceSize().toInt).map(i => {
      val columnDefs = node.GetOutputSchemaSource(i).GetSchema()
      getSparkSchema(columnDefs)
    }).toArray
  }

  def getDatabase(databaseName: String, dict: Map[String, DataFrame]): Database = {
    val databaseBuilder = Database.newBuilder()
    databaseBuilder.setName(databaseName)
    dict.foreach { case (name, df) =>
      databaseBuilder.addTables(getTableDef(name, df))
    }
    databaseBuilder.build()
  }

  def getTableDef(tableName: String, dataFrame: DataFrame): TableDef = {
    val tblBulder = TableDef.newBuilder()
    dataFrame.schema.foreach(field => {
      tblBulder.addColumns(ColumnDef.newBuilder()
        .setName(field.name)
        .setIsNotNull(! field.nullable)
        .setType(getFeSQLType(field.dataType))
        .build()
      )
    })
    tblBulder.setName(tableName)
    tblBulder.build()
  }

  def getFeSQLSchema(structType: StructType): java.util.List[ColumnDef] = {
    val list = new util.ArrayList[ColumnDef]()
    structType.foreach(field => {
      list.add(ColumnDef.newBuilder()
        .setName(field.name)
        .setIsNotNull(!field.nullable)
        .setType(getFeSQLType(field.dataType)).build())
    })
    list
  }

  def getSparkSchema(columns: java.util.List[ColumnDef]): StructType = {
    StructType(columns.asScala.map(col => {
      StructField(col.getName, getSparkType(col.getType), !col.getIsNotNull)
    }))
  }

  def getSparkType(dtype: Type): DataType = {
    dtype match {
      case Type.kInt16 => ShortType
      case Type.kInt32 => IntegerType
      case Type.kInt64 => LongType
      case Type.kFloat => FloatType
      case Type.kDouble => DoubleType
      case Type.kBool => BooleanType
      case Type.kVarchar => StringType
      case Type.kDate => DateType
      case Type.kTimestamp => TimestampType
      case _ => throw new IllegalArgumentException(
        s"FeSQL type $dtype not supported")
    }
  }

  def getFeSQLType(dtype: DataType): Type = {
    dtype match {
      case ShortType => Type.kInt16
      case IntegerType => Type.kInt32
      case LongType => Type.kInt64
      case FloatType => Type.kFloat
      case DoubleType => Type.kDouble
      case BooleanType => Type.kBool
      case StringType => Type.kVarchar
      case DateType => Type.kDate
      case TimestampType => Type.kTimestamp
      case _ => throw new IllegalArgumentException(
        s"Spark type $dtype not supported")
    }
  }

  def getSchemaTypeFromInnerType(dtype: InnerDataType): Type = {
    dtype match {
      case InnerDataType.kInt16 => Type.kInt16
      case InnerDataType.kInt32 => Type.kInt32
      case InnerDataType.kInt64 => Type.kInt64
      case InnerDataType.kFloat => Type.kFloat
      case InnerDataType.kDouble => Type.kDouble
      case InnerDataType.kBool => Type.kBool
      case InnerDataType.kVarchar => Type.kVarchar
      case InnerDataType.kDate => Type.kDate
      case InnerDataType.kTimestamp => Type.kTimestamp
      case _ => throw new IllegalArgumentException(
        s"Inner type $dtype not supported")
    }
  }

  def getInnerTypeFromSchemaType(dtype: Type): InnerDataType = {
    dtype match {
      case Type.kInt16 => InnerDataType.kInt16
      case Type.kInt32 => InnerDataType.kInt32
      case Type.kInt64 => InnerDataType.kInt64
      case Type.kFloat => InnerDataType.kFloat
      case Type.kDouble => InnerDataType.kDouble
      case Type.kBool => InnerDataType.kBool
      case Type.kVarchar => InnerDataType.kVarchar
      case Type.kDate => InnerDataType.kDate
      case Type.kTimestamp => InnerDataType.kTimestamp
      case _ => throw new IllegalArgumentException(
        s"Schema type $dtype not supported")
    }
  }

  def createGroupKeyComparator(keyIdxs: Array[Int], schema: StructType): (Row, Row) => Boolean = {
    if (keyIdxs.length == 1) {
      val idx = keyIdxs(0)
      (row1, row2) => {
        row1.get(idx) != row2.get(idx)
      }
    } else {
      (row1, row2) => {
        keyIdxs.exists(i => row1.get(i) != row2.get(i))
      }
    }
  }

}
