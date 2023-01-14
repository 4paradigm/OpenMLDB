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

package com._4paradigm.openmldb.batch

import java.nio.ByteBuffer
import com._4paradigm.hybridse.sdk.SerializableByteBuffer
import com._4paradigm.hybridse.vm.PhysicalOpNode
import com._4paradigm.openmldb.batch.api.OpenmldbSession
import com._4paradigm.openmldb.batch.utils.NodeIndexInfo
import org.apache.spark.sql.catalyst.QueryPlanningTracker
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.slf4j.LoggerFactory

import scala.collection.mutable

class PlanContext(tag: String, session: SparkSession, planner: SparkPlanner, config: OpenmldbBatchConfig) {

  private val logger = LoggerFactory.getLogger(this.getClass)

  private var moduleBuffer: SerializableByteBuffer = _
  // private var moduleBroadCast: Broadcast[SerializableByteBuffer] = _

  private val planResults = mutable.HashMap[Long, SparkInstance]()

  private var registeredTables = mutable.Map[String, mutable.Map[String, DataFrame]]()

  // Record the index info for all the physical node, key is physical node id, value is index info
  private val nodeIndexInfoMap = mutable.HashMap[Long, NodeIndexInfo]()

  private var openmldbSession: OpenmldbSession = _

  def getTag: String = tag

  def getSparkSession: SparkSession = session

  def setOpenmldbSession(openmldbSession: OpenmldbSession): Unit = {
    this.openmldbSession = openmldbSession
  }

  def getOpenmldbSession: OpenmldbSession = openmldbSession

  def setModuleBuffer(buf: ByteBuffer): Unit = {
    moduleBuffer = new SerializableByteBuffer(buf)
    // moduleBroadCast = session.sparkContext.broadcast(moduleBuffer)
  }

  def getModuleBuffer: ByteBuffer = moduleBuffer.getBuffer

  def getSerializableModuleBuffer: SerializableByteBuffer = moduleBuffer

  def getConf: OpenmldbBatchConfig = config

  // def getModuleBufferBroadcast: Broadcast[SerializableByteBuffer] = moduleBroadCast

  def getPlanResult(nodeId: Long): Option[SparkInstance] = {
    planResults.get(nodeId)
  }

  def putPlanResult(nodeId: Long, res: SparkInstance): Unit = {
    planResults.put(nodeId, res)
  }

  def setRegisteredTables(registeredTables: mutable.Map[String, mutable.Map[String, DataFrame]]): Unit = {
    this.registeredTables = registeredTables
  }

  def getDataFrame(dbName: String, tableName: String): Option[DataFrame] = {
    if (!registeredTables.contains(dbName)) {
      return None
    }
    registeredTables(dbName).get(tableName)
  }

  def getDataFrame(tableName: String): Option[DataFrame] = {
    getDataFrame(config.defaultDb, tableName)
  }

  def getSparkOutput(root: PhysicalOpNode): SparkInstance = {
    planner.getSparkOutput(root, this)
  }

  def getNodeIndexInfoMap: mutable.HashMap[Long, NodeIndexInfo] = {
    nodeIndexInfoMap
  }

  def hasIndexInfo(nodeId: Long): Boolean = {
    nodeIndexInfoMap.contains(nodeId)
  }

  def getIndexInfo(nodeId: Long): NodeIndexInfo = {
    nodeIndexInfoMap(nodeId)
  }

  def putNodeIndexInfo(nodeId: Long, nodeIndexInfo: NodeIndexInfo): Unit = {
    logger.debug("Bind the nodeId(%d) with nodeIndexType(%s)"
      .format(nodeId, nodeIndexInfo.indexColumnName, nodeIndexInfo.nodeIndexType))
    nodeIndexInfoMap.put(nodeId, nodeIndexInfo)
  }

  /**
   * Run sql with Spark SQL API.
   *
   * @param sqlText the SQL script
   * @return
   */
  def sparksql(sqlText: String): DataFrame = {
    // Use Spark internal implementation because we may override sql function in 4PD Spark distribution
    val tracker = new QueryPlanningTracker
    val plan = tracker.measurePhase(QueryPlanningTracker.PARSING) {
      session.sessionState.sqlParser.parsePlan(sqlText)
    }

    // Call private method Dataset.ofRows()
    val datasetClass = Class.forName("org.apache.spark.sql.Dataset")
    val datasetOfRowsMethod = datasetClass
      .getDeclaredMethod(s"ofRows", classOf[SparkSession], classOf[LogicalPlan], classOf[QueryPlanningTracker])
    datasetOfRowsMethod.invoke(null, session, plan, tracker).asInstanceOf[Dataset[Row]]
  }
}

