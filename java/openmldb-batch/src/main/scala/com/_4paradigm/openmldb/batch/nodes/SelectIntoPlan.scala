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

import com._4paradigm.hybridse.vm.PhysicalSelectIntoNode
import com._4paradigm.openmldb.batch.utils.{HybridseUtil, OpenmldbTableUtil, DataSourceUtil}
import com._4paradigm.openmldb.batch.{PlanContext, SparkInstance}
import org.slf4j.LoggerFactory

object SelectIntoPlan {
  private val logger = LoggerFactory.getLogger(this.getClass)

  def gen(ctx: PlanContext, node: PhysicalSelectIntoNode, input: SparkInstance): SparkInstance = {
    val outPath = node.OutFile()
    require(outPath.nonEmpty)

    if (logger.isDebugEnabled()) {
      logger.debug("session catalog {}", ctx.getSparkSession.sessionState.catalog)
      logger.debug("select {} rows", input.getDf().count())
      input.getDf().show(10)
    }
    // write options don't need deepCopy, may have coalesce
    val (format, options, mode, extra) = HybridseUtil.parseOptions(outPath, node)
    if (input.getSchema.size == 0 && input.getDf().isEmpty) {
      throw new Exception("select empty, skip save")
    }

    if (DataSourceUtil.isCatalog(format)) {
      // we won't check if the database exists, if not, save will throw exception
      // Hive: DO NOT create database in here(the table location will be spark warehouse)
      val dbt = DataSourceUtil.catalogDest(outPath)
      logger.info(s"offline select into: $format catalog, write mode[${mode}], out table ${dbt}")
      input.getDf().write.format(format).mode(mode).saveAsTable(dbt)
    } else if (format == "openmldb") {

      val (db, table) = HybridseUtil.getOpenmldbDbAndTable(outPath)

      val createIfNotExists = extra.get("create_if_not_exists").get.toBoolean
      if (createIfNotExists) {
        logger.info("Try to create openmldb output table: " + table)

        OpenmldbTableUtil.createOpenmldbTableFromDf(ctx.getOpenmldbSession, input.getDf(), db, table)
      }

      val writeOptions = Map(
        "db" -> db,
        "table" -> table,
        "zkCluster" -> ctx.getConf.openmldbZkCluster,
        "zkPath" -> ctx.getConf.openmldbZkRootPath)

      input.getDf().write.options(writeOptions).format("openmldb").mode(mode).save()

    } else {
      logger.info("offline select into: format[{}], options[{}], write mode[{}], out path {}", format, options,
        mode, outPath)
      var ds = input.getDf()
      val coalesce = extra.get("coalesce").map(_.toInt)
      if (coalesce.nonEmpty && coalesce.get > 0) {
        ds = ds.coalesce(coalesce.get)
        logger.info("coalesce to {} part", coalesce.get)
      }
      ds.write.format(format).options(options).mode(mode).save(outPath)
    }

    SparkInstance.fromDataFrame(ctx.getSparkSession.emptyDataFrame)
  }
}
