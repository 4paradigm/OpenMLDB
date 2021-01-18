package com._4paradigm.fesql.spark

import java.nio.ByteBuffer

import com._4paradigm.fesql.`type`.TypeOuterClass.Type
import com._4paradigm.fesql.common.SerializableByteBuffer
import com._4paradigm.fesql.spark.nodes._
import com._4paradigm.fesql.vm._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.QueryPlanningTracker
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.collection.mutable

class PlanContext(tag: String, session: SparkSession, planner: SparkPlanner, config: FeSQLConfig) {

  private var moduleBuffer: SerializableByteBuffer = _
  // private var moduleBroadCast: Broadcast[SerializableByteBuffer] = _

  private val planResults = mutable.HashMap[Long, SparkInstance]()

  private val namedSparkDataFrames = mutable.HashMap[String, DataFrame]()

  def getTag: String = tag

  def getSparkSession: SparkSession = session

  def setModuleBuffer(buf: ByteBuffer): Unit = {
    moduleBuffer = new SerializableByteBuffer(buf)
    // moduleBroadCast = session.sparkContext.broadcast(moduleBuffer)
  }

  def getModuleBuffer: ByteBuffer = moduleBuffer.getBuffer

  def getSerializableModuleBuffer: SerializableByteBuffer = moduleBuffer

  def getConf: FeSQLConfig = config

  // def getModuleBufferBroadcast: Broadcast[SerializableByteBuffer] = moduleBroadCast

  def getPlanResult(node: PhysicalOpNode): Option[SparkInstance] = {
    planResults.get(PhysicalOpNode.getCPtr(node))
  }

  def putPlanResult(node: PhysicalOpNode, res: SparkInstance): Unit = {
    planResults += PhysicalOpNode.getCPtr(node) -> res
  }

  def registerDataFrame(name: String, df: DataFrame): Unit = {
    namedSparkDataFrames += name -> df
  }

  def getDataFrame(name: String): Option[DataFrame] = {
    namedSparkDataFrames.get(name)
  }

  def getSparkOutput(root: PhysicalOpNode): SparkInstance = {
    planner.getSparkOutput(root, this)
  }

  /**
   * Run sql with Spark SQL API.
   *
   * @param sqlText
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
    val outputDataset =  datasetOfRowsMethod.invoke(null, session, plan, tracker).asInstanceOf[Dataset[Row]]
    outputDataset
  }
}

