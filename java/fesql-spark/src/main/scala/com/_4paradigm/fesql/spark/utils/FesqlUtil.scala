package com._4paradigm.fesql.spark.utils

import java.util

import com._4paradigm.fesql.`type`.TypeOuterClass.{ColumnDef, Database, TableDef, Type}
import com._4paradigm.fesql.vm.PhysicalOpNode
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._


object FesqlUtil {

  def getOutputSchemaSlices(node: PhysicalOpNode): Array[StructType] = {
    (0 until node.GetOutputSchemaListSize().toInt).map(i => {
      val columnDefs = node.GetOutputSchemaSlice(i)
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
