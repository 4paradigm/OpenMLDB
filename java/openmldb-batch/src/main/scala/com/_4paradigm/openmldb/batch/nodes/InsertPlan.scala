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

package com._4paradigm.openmldb.batch.nodes

import com._4paradigm.hybridse.vm.PhysicalInsertNode
import com._4paradigm.openmldb.batch.utils.SparkRowUtil
import com._4paradigm.openmldb.batch.{PlanContext, SparkInstance}
import com._4paradigm.openmldb.proto.Common.ColumnDesc
import com._4paradigm.openmldb.proto.NS.OfflineTableInfo
import com._4paradigm.std.{ExprNodeVector, VectorString}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{BooleanType, DataType, DateType, DoubleType, FloatType, IntegerType, LongType,
  ShortType, StringType, StructField, StructType, TimestampType}

import java.sql.{Date, Timestamp}
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

object InsertPlan {
  def gen(ctx: PlanContext, node: PhysicalInsertNode): SparkInstance = {
    val stmt = node.GetInsertStmt()
    require(stmt != null, "Fail to get insert statement")

    val dbInStmt = stmt.getDb_name_
    val db = if (dbInStmt.nonEmpty) dbInStmt else ctx.getConf.defaultDb
    val table = stmt.getTable_name_
    val tableInfo = ctx.getOpenmldbSession.openmldbCatalogService.getTableInfo(db, table)
    require(tableInfo != null && tableInfo.getName.nonEmpty, s"table $db.$table info is not existed(no table name): $tableInfo")

    val colDescList = tableInfo.getColumnDescList
    var oriSchema = new StructType
    val fieldMap = mutable.Map[String, StructField]()
    colDescList.foreach(col => {
      val colName = col.getName
      val field = StructField(colName, SparkRowUtil.protoTypeToScalaType(col.getDataType), !col.getNotNull)
      oriSchema = oriSchema.add(field)
      fieldMap.put(colName, field)
    })

    val (insertSchema, insertDataTypes) = parseInsertCols(stmt.getColumns_, oriSchema, fieldMap, colDescList)
    val insertRows = parseInsertRows(stmt.getValues_, insertDataTypes)

    val spark = ctx.getSparkSession
    var insertDf = spark.createDataFrame(spark.sparkContext.parallelize(insertRows), insertSchema)
    val schemaDf = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], oriSchema)
    insertDf = schemaDf.unionByName(insertDf, allowMissingColumns = true)

    val offlineDataPath = getOfflineDataPath(ctx, db, table)
    val newTableInfoBuilder = tableInfo.toBuilder
    val hasOfflineTableInfo = tableInfo.hasOfflineTableInfo
    if (!hasOfflineTableInfo) {
      val newOfflineInfo = OfflineTableInfo
        .newBuilder()
        .setPath(offlineDataPath)
        .setFormat("csv")
        .build()
      newTableInfoBuilder.setOfflineTableInfo(newOfflineInfo)
    }
    val newTableInfo = newTableInfoBuilder.build()

    insertDf.write.mode("append").format(newTableInfo.getOfflineTableInfo.getFormat).save(offlineDataPath)
    if (!hasOfflineTableInfo) {
      ctx.getOpenmldbSession.openmldbCatalogService.updateOfflineTableInfo(newTableInfo)
    }

    SparkInstance.fromDataFrame(spark.emptyDataFrame)
  }

  def parseInsertCols(cols: VectorString, oriSchema: StructType, fieldMap:mutable.Map[String, StructField],
                      colDescList: java.util.List[ColumnDesc]): (StructType, ArrayBuffer[DataType]) = {
    var insertSchema = new StructType
    val insertDataTypes = ArrayBuffer[DataType]()
    if (cols == null || cols.size() == 0) {
      insertSchema = oriSchema
      insertSchema.foreach(field => insertDataTypes += field.dataType)
    } else {
      val insertColSet = mutable.Set[String]()
      for (i <- 0 until cols.size()) {
        val colName = cols.get(i)
        require(fieldMap.contains(colName), s"Fail to get insert info--can't find column $colName")

        val structField = fieldMap(colName)
        insertSchema = insertSchema.add(structField)
        insertDataTypes += structField.dataType
        insertColSet.add(colName)
      }

      colDescList.foreach(col => require(!col.getNotNull || insertColSet.contains(col.getName),
        s"Fail to get insert info--require not null column ${col.getName}"))
    }
    (insertSchema, insertDataTypes)
  }

  def parseInsertRows(valuesExpr: ExprNodeVector, insertDataTypes: ArrayBuffer[DataType]): ListBuffer[Row] = {
    val insertRows = ListBuffer[Row]()
    for (i <- 0 until valuesExpr.size()) {
      val rowExpr = valuesExpr.get(i)
      val valNum = rowExpr.GetChildNum()
      require(valNum == insertDataTypes.size,
        s"Fail to get insert info--fail to parse row[$i]: ${rowExpr.GetExprString()}")
      val rowValues = ListBuffer[Any]()
      for (j <- 0 until valNum) {
        val valueExpr = rowExpr.GetChild(j)
        try {
          rowValues += castVal(valueExpr.GetExprString(), insertDataTypes(j))
        } catch {
          case _: IllegalArgumentException | _: NumberFormatException =>
            throw new IllegalArgumentException(
              s"Fail to get insert info--fail to parse row[$i]: ${rowExpr.GetExprString()}"
            )
        }
      }
      insertRows += Row.fromSeq(rowValues)
    }
    insertRows
  }

  def castVal(oriStr: String, dataType: DataType): Any = {
    dataType match {
      case BooleanType => oriStr.toBoolean
      case ShortType => oriStr.toShort
      case LongType => oriStr.toLong
      case IntegerType => oriStr.toInt
      case FloatType => oriStr.toFloat
      case DoubleType => oriStr.toDouble
      case DateType => Date.valueOf(oriStr)
      case TimestampType => castStr2Timestamp(oriStr)
      case StringType => oriStr
    }
  }

  def castStr2Timestamp(oriStr: String): Timestamp = {
    try {
      Timestamp.valueOf(oriStr)
    } catch {
      case _: IllegalArgumentException =>
        new Timestamp(oriStr.toLong)
    }
  }

  def getOfflineDataPath(ctx: PlanContext, db: String, table: String): String = {
    val offlineDataPrefix = if (ctx.getConf.offlineDataPrefix.endsWith("/")) {
      ctx.getConf.offlineDataPrefix.dropRight(1)
    } else {
      ctx.getConf.offlineDataPrefix
    }
    s"$offlineDataPrefix/$db/$table"
  }
}
