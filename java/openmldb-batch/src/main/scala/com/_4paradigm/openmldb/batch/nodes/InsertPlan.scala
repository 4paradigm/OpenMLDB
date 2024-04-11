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

import com._4paradigm.hybridse.node.ExprNode
import com._4paradigm.hybridse.vm.PhysicalInsertNode
import com._4paradigm.openmldb.batch.utils.SparkRowUtil
import com._4paradigm.openmldb.batch.{PlanContext, SparkInstance}
import com._4paradigm.openmldb.proto.Common.ColumnDesc
import com._4paradigm.openmldb.proto.NS.OfflineTableInfo
import com._4paradigm.std.{ExprNodeVector, VectorString}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{BooleanType, DataType, DateType, DoubleType, FloatType, IntegerType, LongType,
  ShortType, StringType, StructField, StructType, TimestampType}
import org.slf4j.LoggerFactory

import java.sql.{Date, Timestamp}
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

object InsertPlan {
  case class ColInfo(colDesc: ColumnDesc, field: StructField)

  private val logger = LoggerFactory.getLogger(this.getClass)

  def gen(ctx: PlanContext, node: PhysicalInsertNode): SparkInstance = {
    val stmt = node.GetInsertStmt()
    require(stmt != null, "Fail to get insert statement")

    val dbInStmt = stmt.getDb_name_
    val db = if (dbInStmt.nonEmpty) dbInStmt else ctx.getConf.defaultDb
    val table = stmt.getTable_name_
    val tableInfo = ctx.getOpenmldbSession.openmldbCatalogService.getTableInfo(db, table)
    require(tableInfo != null && tableInfo.getName.nonEmpty,
      s"table $db.$table info is not existed(no table name): $tableInfo")

    val hasOfflineTableInfo = tableInfo.hasOfflineTableInfo
    logger.info(s"hasOfflineTableInfo: $hasOfflineTableInfo")
    if (hasOfflineTableInfo) {
      val symbolicPaths = tableInfo.getOfflineTableInfo.getSymbolicPathsList
      require(symbolicPaths == null || symbolicPaths.isEmpty, "can't insert into table with soft copied data")
    }

    val colDescList = tableInfo.getColumnDescList
    var oriSchema = new StructType
    val colInfoMap = mutable.Map[String, ColInfo]()
    colDescList.foreach(col => {
      val colName = col.getName
      val field = StructField(colName, SparkRowUtil.protoTypeToScalaType(col.getDataType), !col.getNotNull)
      oriSchema = oriSchema.add(field)
      colInfoMap.put(colName, ColInfo(col, field))
    })

    val (insertSchema, insertColInfos, stmtColNum) = parseInsertCols(stmt.getColumns_, oriSchema, colInfoMap)
    val insertRows = parseInsertRows(stmt.getValues_, insertColInfos, stmtColNum)

    val spark = ctx.getSparkSession
    var insertDf = spark.createDataFrame(spark.sparkContext.parallelize(insertRows), insertSchema)
    val schemaDf = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], oriSchema)
    insertDf = schemaDf.unionByName(insertDf, allowMissingColumns = true)

    val offlineDataPath = getOfflineDataPath(ctx, db, table)
    val newTableInfoBuilder = tableInfo.toBuilder
    if (!hasOfflineTableInfo) {
      val newOfflineInfo = OfflineTableInfo
        .newBuilder()
        .setPath(offlineDataPath)
        .setFormat("parquet")
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

  def parseInsertCols(cols: VectorString, oriSchema: StructType, colInfoMap: mutable.Map[String, ColInfo]):
  (StructType, ArrayBuffer[ColInfo], Int) = {
    var insertSchema = new StructType
    val insertColInfos = ArrayBuffer[ColInfo]()
    var stmtColNum = 0
    if (cols == null || cols.size() == 0) {
      insertSchema = oriSchema
      insertSchema.foreach(field => insertColInfos += colInfoMap(field.name))
      stmtColNum = oriSchema.size
    } else {
      stmtColNum = cols.size()
      val insertColSet = mutable.Set[String]()
      for (i <- 0 until stmtColNum) {
        val colName = cols.get(i)
        require(colInfoMap.contains(colName), s"Fail to get insert info--can't recognize column $colName")

        val colInfo = colInfoMap(colName)
        insertColInfos += colInfo
        insertSchema = insertSchema.add(colInfo.field)
        insertColSet.add(colName)
      }

      for ((colName, colInfo) <- colInfoMap) {
        val colDesc = colInfo.colDesc
        if (colDesc.hasDefaultValue && !insertColSet.contains(colName)) {
          val colInfo = colInfoMap(colName)
          insertColInfos += colInfo
          insertSchema = insertSchema.add(colInfo.field)
          insertColSet.add(colName)
        }
        require(!colDesc.getNotNull || insertColSet.contains(colName),
          s"Fail to get insert info--require not null column ${colName}")
      }
    }

    (insertSchema, insertColInfos, stmtColNum)
  }

  def parseInsertRows(valuesExpr: ExprNodeVector, insertColInfos: ArrayBuffer[ColInfo], stmtColNum: Int):
  ListBuffer[Row] = {
    val defaultValues = getDefaultColValues(insertColInfos, stmtColNum)

    val insertRows = ListBuffer[Row]()
    for (i <- 0 until valuesExpr.size()) {
      val rowExpr = valuesExpr.get(i)
      var rowValues = ListBuffer[Any]()
      try {
        rowValues = parseRowExpr(rowExpr, insertColInfos, stmtColNum)
      } catch {
        case _: IllegalArgumentException | _: NumberFormatException =>
          throw new IllegalArgumentException(
            s"Fail to get insert info--fail to parse row[$i]: ${rowExpr.GetExprString()}"
          )
      }

      rowValues = rowValues ++ defaultValues
      insertRows += Row.fromSeq(rowValues)
    }
    insertRows
  }

  def getDefaultColValues(insertColInfos: ArrayBuffer[ColInfo], stmtColNum: Int): ListBuffer[Any] = {
    val defaultValues = ListBuffer[Any]()
    for (i <- stmtColNum until insertColInfos.size) {
      val colInfo = insertColInfos(i)
      defaultValues += castVal(colInfo.colDesc.getDefaultValue, colInfo.field.dataType)
    }
    defaultValues
  }

  def parseRowExpr(rowExpr: ExprNode, insertColInfos: ArrayBuffer[ColInfo], stmtColNum: Int): ListBuffer[Any] = {
    val valNum = rowExpr.GetChildNum()
    require(valNum == stmtColNum)

    val rowValues = ListBuffer[Any]()
    for (i <- 0 until valNum) {
      val valueExpr = rowExpr.GetChild(i)
      val colInfo = insertColInfos(i)
      val value = castVal(valueExpr.GetExprString(), colInfo.field.dataType)
      require(value != null || !colInfo.colDesc.getNotNull)

      rowValues += value
    }
    rowValues
  }

  def castVal(oriStr: String, dataType: DataType): Any = {
    if (dataType == StringType) {
      return oriStr
    }
    if ("null".equals(oriStr.toLowerCase)) {
      return null
    }
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
