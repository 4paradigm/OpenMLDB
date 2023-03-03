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
import com._4paradigm.openmldb.batch.utils.HybridseUtil
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

    // write options don't need deepCopy
    val (format, options, mode, _) = HybridseUtil.parseOptions(outPath, node)
    if (input.getSchema.size == 0 && input.getDf().isEmpty) {
      throw new Exception("select empty, skip save")
    }

    if (format == "hive") {
      // we won't check if the database exists, if not, save will throw exception
      // DO NOT create database in here(the table location will be spark warehouse)
      val dbt = HybridseUtil.hiveDest(outPath)
      logger.info(s"offline select into: hive way, write mode[${mode}], out table ${dbt}")
      input.getDf().write.format("hive").mode(mode).saveAsTable(dbt)
    } else {
      logger.info("offline select into: format[{}], options[{}], write mode[{}], out path {}", format, options,
          mode, outPath)
      input.getDf().write.format(format).options(options).mode(mode).save(outPath)
    }

    SparkInstance.fromDataFrame(ctx.getSparkSession.emptyDataFrame)
  }
}
