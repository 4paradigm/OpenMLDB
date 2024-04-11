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
import com._4paradigm.openmldb.batch.utils.{DataSourceUtil, HybridseUtil}
import com._4paradigm.openmldb.batch.{PlanContext, SparkInstance}
import com._4paradigm.openmldb.proto.NS.OfflineTableInfo
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters.{mapAsJavaMapConverter, mapAsScalaMapConverter}
import scala.collection.mutable
import scala.collection.JavaConverters.{asScalaBufferConverter, mapAsScalaMap, mapAsScalaMapConverter}

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
    val (format, options, mode, extra) = HybridseUtil.parseOptions(inputFile, node)
    // load have the option deep_copy
    val deepCopy = extra.get("deep_copy").get.toBoolean
    // auto schema conversion option skip_cvt
    val skipCvt = extra.getOrElse("skip_cvt", "false").toBoolean

    require(ctx.getOpenmldbSession != null, "LOAD DATA must use OpenmldbSession, not SparkSession")
    val info = ctx.getOpenmldbSession.openmldbCatalogService.getTableInfo(db, table)

    logger.info("table info: {}", info)
    require(info != null && info.getName.nonEmpty, s"table $db.$table info is not existed(no table name): $info")

    val loadDataSql = extra.get("sql").get

    // we read input file even in soft copy,
    // cause we want to check if "the input file schema == openmldb table schema"
    val df = DataSourceUtil.autoLoad(ctx.getOpenmldbSession, inputFile, format, options, info.getColumnDescList,
      loadDataSql, skipCvt)

    // write
    logger.info("write data to storage {}, writer mode {}, is deep {}", storage, mode, deepCopy.toString)
    if (storage == "online") { // Import online data
      require(deepCopy && mode == "append", "import to online storage, can't do soft copy, and mode must be append")
      val writeType = extra.get("writer_type").get
      val putIfAbsent = extra.get("put_if_absent").get.toBoolean
      logger.info(s"online write type ${writeType}, put if absent ${putIfAbsent}")
      val writeOptions = Map(
        "db" -> db,
        "table" -> table,
        "zkCluster" -> ctx.getConf.openmldbZkCluster,
        "zkPath" -> ctx.getConf.openmldbZkRootPath,
        "writerType" -> writeType,
        "putIfAbsent" -> putIfAbsent.toString
      )
      df.write.options(writeOptions).format("openmldb").mode(mode).save()
    } else { // Import offline data
      // only in some cases, do not need to update info, set false in these cases
      var needUpdateInfo = true
      val newInfoBuilder = info.toBuilder

      // If symbolic import
      if (!deepCopy) {

        // Get error if exists
        if (mode.equals("errorifexists") && info.hasOfflineTableInfo) {
          throw new IllegalArgumentException("offline info exists")
        }

        val oldOfflineTableInfo = if (info.hasOfflineTableInfo) { // Have offline table info
          info.getOfflineTableInfo
        } else { // No offline table info, use new format and options
          OfflineTableInfo
            .newBuilder()
            .setPath("")
            .setFormat(format)
            .putAllOptions(options.asJava)
            .build()
        }

        val newOfflineInfoBuilder = OfflineTableInfo.newBuilder(oldOfflineTableInfo)

        if (mode.equals("append") || mode.equals("errorifexists")) {
          // Check if new path is already existed or not
          val symbolicPathsSize = newOfflineInfoBuilder.getSymbolicPathsCount()

          val symbolicPaths = if (symbolicPathsSize > 0) {
            newOfflineInfoBuilder.getSymbolicPathsList().asScala.toList
          } else {
            List.empty[String]
          }

          if (symbolicPaths.contains(inputFile)) {
            logger.warn(s"The path of $inputFile is already in symbolic paths, do not import again")
          } else {
            logger.info(s"Add the path of $inputFile to offline table info symbolic paths")
            newOfflineInfoBuilder.addSymbolicPaths(inputFile)
          }
          // if no offline info berore, the format is set by new format
          require(
            oldOfflineTableInfo.getFormat.equals(format) &&
              oldOfflineTableInfo.getOptionsMap.asScala.toMap.equals(options),
            s"format and options must be the same with existed, but old is ${oldOfflineTableInfo.getFormat}, " +
              s"${oldOfflineTableInfo.getOptionsMap.asScala.toMap}, new is $format, $options"
          )
        } else if (mode.equals("overwrite")) {
          // overwrite mode, we need to clean all paths(hard+symbolic)
          // TODO(tobe): May remove data files from copy import
          newOfflineInfoBuilder.setPath("")
          newOfflineInfoBuilder.clearSymbolicPaths()
          newOfflineInfoBuilder.addSymbolicPaths(inputFile)
          newOfflineInfoBuilder.setFormat(format)
        }

        if (!format.equals("hive")) {
          // hive source discard all read options
          newOfflineInfoBuilder.putAllOptions(options.asJava)
        }

        // update to ns later
        newInfoBuilder.setOfflineTableInfo(newOfflineInfoBuilder.build())
      } else { // deep copy, only work on OfflineTableInfo.path
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

        var offlineBuilder = OfflineTableInfo
          .newBuilder()
          .setPath(writePath)
          .setFormat(writeFormat)
          .putAllOptions(writeOptions.asJava)
        if (info.hasOfflineTableInfo) {
          require(!mode.equals("errorifexists"), "has offline info(even no deep path), can't do errorifexists")

          val old = info.getOfflineTableInfo
          // only symbolic paths need to be copied, and if overwrite, all fields will be overwritten as default write
          if (!mode.equals("overwrite")) {
            old.getSymbolicPathsList.forEach(path => {
              offlineBuilder.addSymbolicPaths(path)
            })
            // if has symbolic paths, the format and options may don't proper for deep copy
            require(
              old.getFormat == writeFormat && old.getOptionsMap.asScala.toMap.equals(writeOptions.toMap),
              s"can't do deep copy in existed paths' format or options in mode $mode, " +
                s"old is ${old.getFormat}, ${old.getOptionsMap.asScala.toMap}, new is $writeFormat, $writeOptions"
            )
            if (old.getPath.equals(writePath)) {
              needUpdateInfo = false
            }
            // write path may changed by offline path setting, so don't require path equals
          }
        }

        // do deep copy
        require(
          !inputFile.equals(writePath),
          "read and write paths shouldn't be the same, it may clean data in the path"
        )

        df.write.mode(mode).format(writeFormat).options(writeOptions.toMap).save(writePath)
        newInfoBuilder.setOfflineTableInfo(offlineBuilder)
      }

      if (needUpdateInfo) {
        val newInfo = newInfoBuilder.build()
        logger.info(s"new info:\n$newInfo")
        require(
          ctx.getOpenmldbSession.openmldbCatalogService.updateOfflineTableInfo(newInfo),
          s"update new info failed: $newInfo"
        )
      }
    }

    SparkInstance.fromDataFrame(spark.emptyDataFrame)
  }
}
