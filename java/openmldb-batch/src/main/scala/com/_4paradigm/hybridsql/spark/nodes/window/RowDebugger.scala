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

package com._4paradigm.hybridsql.spark.nodes.window

import com._4paradigm.hybridsql.spark.SparkFeConfig
import com._4paradigm.hybridsql.spark.nodes.WindowAggPlan.WindowAggConfig
import org.apache.spark.sql.Row
import org.slf4j.LoggerFactory


class RowDebugger(sqlConfig: SparkFeConfig, config: WindowAggConfig, isSkew: Boolean) extends WindowHook {

  private val logger = LoggerFactory.getLogger(this.getClass)

  private val sampleInterval = sqlConfig.printSampleInterval
  private var cnt: Long = 0

  override def preCompute(computer: WindowComputer, curRow: Row): Unit = {
    printRow(computer, curRow)
  }

  override def preBufferOnly(computer: WindowComputer, curRow: Row): Unit = {
    printRow(computer, curRow)
  }

  def printRow(computer: WindowComputer, row: Row): Unit = {
    if (cnt % sampleInterval == 0) {
      val str = new StringBuffer()
      str.append(row.get(config.orderIdx))
      str.append(",")
      for (e <- config.groupIdxs) {
        str.append(row.get(e))
        str.append(",")
      }
      str.append(" window size = " + computer.getWindow.size())
      if (isSkew) {
        val tag = row.getInt(config.skewTagIdx)
        val position = row.getInt(config.skewPositionIdx)
        logger.info(s"tag : position = $tag : $position, " +
          s"threadId = ${Thread.currentThread().getId}, " +
          s" cnt = $cnt, rowInfo = ${str.toString}")
      } else {
        logger.info(s"threadId = ${Thread.currentThread().getId}," +
          s" cnt = $cnt, rowInfo = ${str.toString}")
      }
      if (sqlConfig.printRowContent) {
        logger.info(row.toString())
      }
    }
    cnt += 1
  }
}
