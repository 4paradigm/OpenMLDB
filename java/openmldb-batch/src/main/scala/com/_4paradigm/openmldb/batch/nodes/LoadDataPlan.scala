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
import com._4paradigm.openmldb.batch.utils.HybridseUtil
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
    val (format, options, mode, deepCopyOpt) = HybridseUtil.parseOptions(inputFile, node)
    require(deepCopyOpt.nonEmpty) // PhysicalLoadDataNode must have the option deepCopy
    val deepCopy = deepCopyOpt.get

    require(ctx.getOpenmldbSession != null, "LOAD DATA must use OpenmldbSession, not SparkSession")
    val info = ctx.getOpenmldbSession.openmldbCatalogService.getTableInfo(db, table)

    logger.info("table info: {}", info)
    require(info != null && info.getName.nonEmpty, s"table $db.$table info is not existed(no table name): $info")


    // we read input file even in soft copy,
    // cause we want to check if "the input file schema == openmldb table schema"
    val df = HybridseUtil.autoLoad(ctx.getOpenmldbSession, inputFile, format, options, info.getColumnDescList)

    // write
    logger.info("write data to storage {}, writer[mode {}], is deep? {}", storage, mode, deepCopy.toString)
    if (storage == "online") { // Import online data
      require(deepCopy && mode == "append", "import to online storage, can't do soft copy, and mode must be append")

      val writeOptions = Map("db" -> db, "table" -> table,
        "zkCluster" -> ctx.getConf.openmldbZkCluster,
        "zkPath" -> ctx.getConf.openmldbZkRootPath)
      df.write.options(writeOptions).format("openmldb").mode(mode).save()
    } else { // Import offline data
      // only in some cases, do not need to update info
      var needUpdateInfo = true
      val newInfoBuilder = info.toBuilder

      // If symbolic import
      if (!deepCopy) {

        // Get error if exists
        if (mode.equals("errorifexists") && info.hasOfflineTableInfo){
          throw new IllegalArgumentException("offline info exists")
        }

        val oldOfflineTableInfo = if (info.hasOfflineTableInfo) { // Have offline table info
          info.getOfflineTableInfo
        } else { // No offline table info
          OfflineTableInfo.newBuilder()
            .setPath("")
            .setFormat(format)
            .build()
        }

        val newOfflineInfoBuilder = OfflineTableInfo.newBuilder(oldOfflineTableInfo)

        // If mode=="append"
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
        } else if (mode.equals("overwrite")) {
          // TODO(tobe): May remove data files from copy import
          newOfflineInfoBuilder.setPath("")
          newOfflineInfoBuilder.clearSymbolicPaths()
          newOfflineInfoBuilder.addSymbolicPaths(inputFile)
        }

        if (!format.equals("hive")) {
          // hive source discard all read options
          newOfflineInfoBuilder.putAllOptions(options.asJava)
        }

        // update to ns later
        newInfoBuilder.setOfflineTableInfo(newOfflineInfoBuilder.build())
      } else { // deep copy
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
        if (info.hasOfflineTableInfo) {
          if (mode.equals("errorifexists")) {
            throw new IllegalArgumentException("offline info exists")
          } else if (mode.equals("append")) {
            throw new IllegalArgumentException("Deep copy with append mode is not supported yet")
          }

          val old = info.getOfflineTableInfo
          if (!old.getDeepCopy) {
            require(mode.equals("overwrite"), "Only overwrite mode works. Old offline data is soft-coped, only can " +
              "overwrite the offline info, leave the soft-coped data as it is.")
            // if old offline data is soft-coped, we need to reject the old info, use the 'offlineDataPath' and
            // normal settings
            needUpdateInfo = true
          } else {
            // if old offline data is deep-copied and mode is append/overwrite,
            // we need to use the old info and don't need to update info to ns
            //writeFormat = old.getFormat
            //writeOptions = old.getOptionsMap.asScala
            // Generated the path to deep copy
            //writePath = s"$offlineDataPrefix/$db/$table"
            needUpdateInfo = true
          }
        }

        // do deep copy
        require(!inputFile.equals(writePath), "read and write paths shouldn't be the same, it may clean data in " +
          "the path")

        df.write.mode(mode).format(writeFormat).options(writeOptions.toMap).save(writePath)
        val offlineBuilder = OfflineTableInfo.newBuilder().setPath(writePath).setFormat(writeFormat)
          .setDeepCopy(true).clearSymbolicPaths().putAllOptions(writeOptions.asJava)

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
