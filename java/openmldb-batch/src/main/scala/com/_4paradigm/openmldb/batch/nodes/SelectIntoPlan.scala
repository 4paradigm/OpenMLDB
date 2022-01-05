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
      logger.debug("select {} rows", input.getDf().count())
      input.getDf().show(10)
    }

    // write options don't need deepCopy
    val (format, options, mode, _) = HybridseUtil.parseOptions(node)
    logger.info("select into offline storage: format[{}], options[{}], write mode[{}], out path {}", format, options,
      mode, outPath)
    if (input.getDf().isEmpty) {
      logger.info("select empty, skip save")
    } else {
      input.getDf().write.format(format).options(options).mode(mode).save(outPath)
    }

    SparkInstance.fromDataFrame(ctx.getSparkSession.emptyDataFrame)
  }
}
