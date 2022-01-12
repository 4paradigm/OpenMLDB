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

import com._4paradigm.hybridse.vm.PhysicalLoadDataNode
import com._4paradigm.openmldb.batch.utils.{HybridseUtil, SparkRowUtil}
import com._4paradigm.openmldb.batch.{PlanContext, SparkInstance}
import com._4paradigm.openmldb.proto.NS.OfflineTableInfo
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters.{mapAsJavaMapConverter, mapAsScalaMapConverter}
import scala.collection.mutable

object LoadDataPlan {
  private val logger = LoggerFactory.getLogger(this.getClass)

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
    logger.info("load data to storage {}, read[format {}, options {}], write[mode {}], is deep? {}", storage, format,
      options, mode, deepCopy.toString)

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
      // The dataframe which be read should have the correct column types.
      var struct = new StructType
      info.getColumnDescList.forEach(
        col => struct = struct.add(col.getName, SparkRowUtil.protoTypeToScalaType(col.getDataType), !col.getNotNull)
      )
      logger.info("read schema: {}", struct)
      val df = spark.read.options(options).format(format).schema(struct).load(inputFile)
      if (logger.isDebugEnabled()) {
        logger.debug("read dataframe count: {}", df.count())
        df.show(10)
      }
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
        val df = spark.read.options(options).format(format).load(inputFile)
        if (logger.isDebugEnabled()) {
          logger.debug("read dataframe count: {}", df.count())
          df.show(10)
        }
        df.write.mode(mode).format(writeFormat).options(writeOptions.toMap).save(writePath)
        val offlineBuilder = OfflineTableInfo.newBuilder().setPath(writePath).setFormat(writeFormat).setDeepCopy(true)
          .putAllOptions(writeOptions.asJava)
        newInfoBuilder.setOfflineTableInfo(offlineBuilder)
      }

      if (needUpdateInfo) {
        val newInfo = newInfoBuilder.build()
        logger.info("new info: {}", newInfo)
        require(ctx.getOpenmldbSession.openmldbCatalogService.updateOfflineTableInfo(newInfo), s"update info " +
          s"failed: $info")
      }
    }

    SparkInstance.fromDataFrame(spark.emptyDataFrame)
  }
}
