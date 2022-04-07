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

import java.util

import com._4paradigm.hybridse.vm.PhysicalLoadDataNode
import com._4paradigm.openmldb.batch.utils.{HybridseUtil, SparkRowUtil}
import com._4paradigm.openmldb.batch.{PlanContext, SparkInstance}
import com._4paradigm.openmldb.proto.NS.OfflineTableInfo
import com._4paradigm.openmldb.proto.{Common, Type}
import org.apache.spark.sql.functions.{col, first}
import org.apache.spark.sql.types.{DataType, LongType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, DataFrameReader}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters.{mapAsJavaMapConverter, mapAsScalaMapConverter}
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`
import scala.collection.mutable

object LoadDataPlan {
  private val logger = LoggerFactory.getLogger(this.getClass)

  // result 'readSchema' & 'tsCols' is only for csv format, may not be used
  def extractOriginAndReadSchema(columns: util.List[Common.ColumnDesc]): (StructType, StructType, List[String]) = {
    var oriSchema = new StructType
    var readSchema = new StructType
    val tsCols = mutable.ArrayBuffer[String]()
    columns.foreach(col => {
      var ty = col.getDataType
      oriSchema = oriSchema.add(col.getName, SparkRowUtil.protoTypeToScalaType(ty), !col
        .getNotNull)
      if (ty.equals(Type.DataType.kTimestamp)) {
        tsCols += col.getName
        // use string to parse ts column, to avoid getting null(parse wrong format), can't distinguish between the
        // parsed null and the real `null`.
        ty = Type.DataType.kString
      }
      readSchema = readSchema.add(col.getName, SparkRowUtil.protoTypeToScalaType(ty), !col
        .getNotNull)
    }
    )
    (oriSchema, readSchema, tsCols.toList)
  }

  def parseLongTsCols(reader: DataFrameReader, readSchema: StructType, tsCols: List[String], file: String)
  : List[String] = {
    val longTsCols = mutable.ArrayBuffer[String]()
    if (tsCols.nonEmpty) {
      // normal timestamp format is TimestampType(Y-M-D H:M:S...)
      // and we support one more timestamp format LongType(ms)
      // read one row to auto detect the format, if int64, use LongType to read file, then convert it to TimestampType
      // P.S. don't use inferSchema, cuz we just need to read the first non-null row, not all
      val df = reader.schema(readSchema).load(file)
      // check timestamp cols
      for (col <- tsCols) {
        val i = readSchema.fieldIndex(col)
        var ty: DataType = LongType
        try {
          // value is string, try to parse to long
          df.select(first(df.col(col), ignoreNulls = true)).first().getString(0).toLong
          longTsCols.append(col)
        } catch {
          case e: Any =>
            logger.debug(s"col '$col' parse long failed, use TimestampType to read", e)
            ty = TimestampType
        }

        val newField = StructField(readSchema.fields(i).name, ty, readSchema.fields(i).nullable)
        readSchema.fields(i) = newField
      }
    }
    longTsCols.toList
  }

  // We want df with oriSchema, but if the file format is csv:
  // 1. we support two format of timestamp
  // 2. spark read may change the df schema to all nullable
  // So we should fix it.
  def autoLoad(reader: DataFrameReader, file: String, format: String, columns: util.List[Common.ColumnDesc])
  : DataFrame = {
    val (oriSchema, readSchema, tsCols) = extractOriginAndReadSchema(columns)
    if (format != "csv") {
      return reader.schema(oriSchema).format(format).load(file)
    }
    // csv should auto detect the timestamp format

    logger.info(s"set file format: $format")
    reader.format(format)
    // use string to read, then infer the format by the first non-null value of the ts column
    val longTsCols = parseLongTsCols(reader, readSchema, tsCols, file)
    logger.info(s"read schema: $readSchema, file $file")
    var df = reader.schema(readSchema).load(file)
    if (longTsCols.nonEmpty) {
      // convert long type to timestamp type
      for (tsCol <- longTsCols) {
        df = df.withColumn(tsCol, (col(tsCol) / 1000).cast("timestamp"))
      }
    }

    // if we read non-streaming files, the df schema fields will be set as all nullable.
    // so we need to set it right
    logger.info(s"after read schema: ${df.schema}")
    if (!df.schema.equals(oriSchema)) {
      df = df.sqlContext.createDataFrame(df.rdd, oriSchema)
    }

    require(df.schema == oriSchema, "df schema must == table schema")
    if (logger.isDebugEnabled()) {
      logger.debug("read dataframe count: {}", df.count())
      df.show(10)
    }
    df
  }

  def gen(ctx: PlanContext, node: PhysicalLoadDataNode): SparkInstance = {
    val inputFile = node.File()
    val db = if (node.Db().nonEmpty) node.Db() else ctx.getConf.defaultDb
    val table = node.Table()
    val spark = ctx.getSparkSession

    // get target storage
    val storage = ctx.getConf.loadDataMode
    require(storage == "offline" || storage == "online")

    // read settings
    val (format, options, mode, deepCopyOpt) = HybridseUtil.parseOptions(node)
    require(deepCopyOpt.nonEmpty)
    val deepCopy = deepCopyOpt.get
    logger.info("load data to storage {}, reader[format {}, options {}], writer[mode {}], is deep? {}", storage, format,
      options, mode, deepCopy.toString)
    val readerWithOptions = spark.read.options(options)

    require(ctx.getOpenmldbSession != null, "LOAD DATA must use OpenmldbSession, not SparkSession")
    val info = ctx.getOpenmldbSession.openmldbCatalogService.getTableInfo(db, table)
    require(info != null && info.getName.nonEmpty, s"table $db.$table info is not existed(no table name): $info")
    logger.info("table info: {}", info)

    // write
    if (storage == "online") {
      require(deepCopy && mode == "append", "import to online storage, can't do deep copy, and mode must be append")

      val writeOptions = Map("db" -> db, "table" -> table,
        "zkCluster" -> ctx.getConf.openmldbZkCluster,
        "zkPath" -> ctx.getConf.openmldbZkRootPath)

      val df = autoLoad(readerWithOptions, inputFile, format, info.getColumnDescList)
      df.write.options(writeOptions).format("openmldb").mode(mode).save()
    } else {
      // only in some case, do not need to update info
      var needUpdateInfo = true
      val newInfoBuilder = info.toBuilder

      val infoExists = info.hasOfflineTableInfo
      if (!deepCopy) {
        // soft copy, no need to read files
        require(!infoExists, "offline info has already existed, we don't know whether to delete the existing data")

        // because it's soft-copy, format+options should be the same with read settings
        val offlineBuilder = OfflineTableInfo.newBuilder().setPath(inputFile).setFormat(format).setDeepCopy(false)
          .putAllOptions(options.asJava)
        // update to ns later
        newInfoBuilder.setOfflineTableInfo(offlineBuilder)
      } else {
        // deep copy
        // Generate new offline address by db name, table name and config of prefix
        val offlineDataPrefix = if (ctx.getConf.offlineDataPrefix.endsWith("/")) {
          ctx.getConf.offlineDataPrefix.dropRight(1)
        } else {
          ctx.getConf.offlineDataPrefix
        }
        // If we recreate table, this dir will be cleaned too. It should be safe.
        val offlineDataPath = s"$offlineDataPrefix/$db/$table"
        // write default settings: no option and parquet format
        var (writePath, writeFormat) = (offlineDataPath, "parquet")
        var writeOptions: mutable.Map[String, String] = mutable.Map()
        if (infoExists) {
          require(mode != "errorifexists", "offline info exists")
          val old = info.getOfflineTableInfo
          if (!old.getDeepCopy) {
            require(mode == "overwrite", "Only overwrite mode works. Old offline data is soft-coped, only can " +
              "overwrite the offline info, leave the soft-coped data as it is.")
            // if old offline data is soft-coped, we need to reject the old info, use the 'offlineDataPath' and
            // normal settings
            needUpdateInfo = true
          } else {
            // if old offline data is deep-coped, we need to use the old info, and don't need to update info to ns
            writeFormat = old.getFormat
            writeOptions = old.getOptionsMap.asScala
            writePath = old.getPath
            needUpdateInfo = false
          }
        }

        // do deep copy
        require(inputFile != writePath, "read and write paths shouldn't be the same, it may clean data in the path")
        val df = autoLoad(readerWithOptions, inputFile, format, info.getColumnDescList)
        df.write.mode(mode).format(writeFormat).options(writeOptions.toMap).save(writePath)
        val offlineBuilder = OfflineTableInfo.newBuilder().setPath(writePath).setFormat(writeFormat).setDeepCopy(true)
          .putAllOptions(writeOptions.asJava)
        newInfoBuilder.setOfflineTableInfo(offlineBuilder)
      }

      if (needUpdateInfo) {
        val newInfo = newInfoBuilder.build()
        logger.info(s"new info:\n$newInfo")
        require(ctx.getOpenmldbSession.openmldbCatalogService.updateOfflineTableInfo(newInfo), s"update new info " +
          s"failed: $newInfo")
      }
    }

    SparkInstance.fromDataFrame(spark.emptyDataFrame)
  }
}
