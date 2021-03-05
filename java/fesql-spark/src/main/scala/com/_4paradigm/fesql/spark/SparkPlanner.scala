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

package com._4paradigm.fesql.spark

import com._4paradigm.fesql.FeSqlLibrary
import com._4paradigm.fesql.`type`.TypeOuterClass._
import com._4paradigm.fesql.common.{FesqlException, SQLEngine, UnsupportedFesqlException}
import com._4paradigm.fesql.node.JoinType
import com._4paradigm.fesql.spark.nodes._
import com._4paradigm.fesql.spark.utils.{FesqlUtil, GraphvizUtil, NodeIndexInfo, NodeIndexType}
import com._4paradigm.fesql.vm._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.slf4j.LoggerFactory

import scala.collection.mutable


class SparkPlanner(session: SparkSession, config: FeSQLConfig) {

  private val logger = LoggerFactory.getLogger(this.getClass)

  // Ensure native initialized
  FeSqlLibrary.initCore()
  Engine.InitializeGlobalLLVM()

  def this(session: SparkSession) = {
    this(session, FeSQLConfig.fromSparkSession(session))
  }

  def plan(sql: String, tableDict: Map[String, DataFrame]): SparkInstance = {
    // spark translation state
    val planCtx = new PlanContext(sql, session, this, config)

    // set spark input tables
    tableDict.foreach {
      case (name, df) => planCtx.registerDataFrame(name, df)
    }

    withSQLEngine(sql, FesqlUtil.getDatabase(config.configDBName, tableDict), config) { engine =>
      val irBuffer = engine.getIRBuffer
      planCtx.setModuleBuffer(irBuffer)

      val root = engine.getPlan
      logger.info("Get FeSQL physical plan: ")
      root.Print()

      if (!config.physicalPlanGraphvizPath.equals("")) {
        logger.info("Draw the physical plan and save to " + config.physicalPlanGraphvizPath)
        GraphvizUtil.drawPhysicalPlan(root, config.physicalPlanGraphvizPath)
      }

      logger.info("Visit physical plan to find ConcatJoin node")
      val concatJoinNodes = mutable.ArrayBuffer[PhysicalJoinNode]()
      findConcatJoinNode(root, concatJoinNodes)

      logger.info("Visit concat join node to add node index info")
      val processedConcatJoinNodeIds = mutable.HashSet[Long]()
      val indexColumnName = "__CONCATJOIN_INDEX__"+ System.currentTimeMillis()
      concatJoinNodes.map(joinNode => bindNodeIndexInfo(joinNode, planCtx, processedConcatJoinNodeIds, indexColumnName))

      if (config.slowRunCacheDir != null) {
        slowRunWithHDFSCache(root, planCtx, config.slowRunCacheDir, isRoot = true)
      } else {
        getSparkOutput(root, planCtx)
      }
    }
  }

  // Visit the physical plan to get all ConcatJoinNode
  def findConcatJoinNode(node: PhysicalOpNode, concatJoinNodes: mutable.ArrayBuffer[PhysicalJoinNode]): Unit = {
    if (node.GetOpType() == PhysicalOpType.kPhysicalOpJoin) {
      val joinNode = PhysicalJoinNode.CastFrom(node)
      if (joinNode.join().join_type() == JoinType.kJoinTypeConcat) {
        concatJoinNodes.append(joinNode)
      }
    }
    for (i <- 0 until node.GetProducerCnt().toInt) {
      findConcatJoinNode(node.GetProducer(i), concatJoinNodes)
    }
  }

  // Bind the node index info for the nodes which use ConcatJoin for computing window concurrently
  def bindNodeIndexInfo(concatJoinNode: PhysicalJoinNode, ctx: PlanContext, processedConcatJoinNodeIds: mutable.HashSet[Long], indexColumnName: String): Unit = {

    val concatJoinNodeId = concatJoinNode.GetNodeId()
    if (ctx.hasIndexInfo(concatJoinNodeId)) {
      // The node has been processed, do not handle again
      return
    }

    logger.info("Visit physical plan to find the dest node which should add the index column")
    val visitedNodeSet = mutable.HashSet[Long]()
    val destNodeId = findLowestCommonAncestorNode(concatJoinNode, visitedNodeSet)

    logger.info("Bind the node index info for all suitable nodes")
    if (processedConcatJoinNodeIds.contains(concatJoinNodeId)) {
      // Ignore the concat join node which has been process
      return
    }

    visitAndBindNodeIndexInfo(ctx, concatJoinNode, destNodeId, indexColumnName, processedConcatJoinNodeIds)
    // Reset the fist concat join node to source concat join node
    ctx.getIndexInfo(concatJoinNodeId).nodeIndexType = NodeIndexType.SourceConcatJoinNode
  }

  def findLowestCommonAncestorNode(root: PhysicalOpNode, visitedNodeSet: mutable.HashSet[Long]): Long = {
    // If node has been visited again, return the node as common ancestor
    if (visitedNodeSet.contains(root.GetNodeId())) {
      return root.GetNodeId()
    }

    // Add current node to visited set
    visitedNodeSet.add(root.GetNodeId())

    for (i <- 0 until root.GetProducerCnt().toInt) {
      // Find the common node in child nodes, return if found
      val result = findLowestCommonAncestorNode(root.GetProducer(i), visitedNodeSet)
      if (result != 0) {
        return result
      }
    }

    // Return 0 if the node without input is not match
    return 0
  }

  def visitAndBindNodeIndexInfo(ctx: PlanContext, node: PhysicalOpNode, destNodeId: Long, indexColumnName: String, processedConcatJoinNodeIds: mutable.HashSet[Long]): Unit = {
    if (ctx.hasIndexInfo(node.GetNodeId())) {
      // No need to set the node with index info again
      return
    }

    if (node.GetNodeId() == destNodeId) {
      ctx.putNodeIndexInfo(node.GetNodeId(), new NodeIndexInfo(indexColumnName, NodeIndexType.DestNode))
      // Return if handle the dest node
      return
    }

    // Check if it is concat join node
    if (node.GetOpType() == PhysicalOpType.kPhysicalOpJoin && PhysicalJoinNode.CastFrom(node).join().join_type() == JoinType.kJoinTypeConcat) {
      ctx.putNodeIndexInfo(node.GetNodeId(), new NodeIndexInfo(indexColumnName, NodeIndexType.InternalConcatJoinNode))
      processedConcatJoinNodeIds.add(node.GetNodeId())
    } else {
      ctx.putNodeIndexInfo(node.GetNodeId(), new NodeIndexInfo(indexColumnName, NodeIndexType.InternalComputeNode))
      processedConcatJoinNodeIds.add(node.GetNodeId())
    }

    // Visit and bind the child node, notice that the final child node should always be the destNodeId
    for (i <- 0 until node.GetProducerCnt().toInt) {
      visitAndBindNodeIndexInfo(ctx, node.GetProducer(i), destNodeId, indexColumnName, processedConcatJoinNodeIds)
    }
  }

  def getSparkOutput(root: PhysicalOpNode, ctx: PlanContext): SparkInstance = {
    val optCache = ctx.getPlanResult(root.GetNodeId())
    if (optCache.isDefined) {
      return optCache.get
    }

    val children = mutable.ArrayBuffer[SparkInstance]()
    for (i <- 0 until root.GetProducerCnt().toInt) {
      children += getSparkOutput(root.GetProducer(i), ctx)
    }
    visitNode(root, ctx, children.toArray)
  }

  def visitNode(root: PhysicalOpNode, ctx: PlanContext, children: Array[SparkInstance]): SparkInstance = {
    val opType = root.GetOpType()
    val outputSpatkInstance = opType match {
      case PhysicalOpType.kPhysicalOpDataProvider =>
        DataProviderPlan.gen(ctx, PhysicalDataProviderNode.CastFrom(root), children)
      case PhysicalOpType.kPhysicalOpSimpleProject =>
        SimpleProjectPlan.gen(ctx, PhysicalSimpleProjectNode.CastFrom(root), children)
      case PhysicalOpType.kPhysicalOpConstProject =>
        ConstProjectPlan.gen(ctx, PhysicalConstProjectNode.CastFrom(root))
      case PhysicalOpType.kPhysicalOpProject =>
        val projectNode = PhysicalProjectNode.CastFrom(root)
        projectNode.getProject_type_ match {
          case ProjectType.kTableProject =>
            RowProjectPlan.gen(ctx, PhysicalTableProjectNode.CastFrom(projectNode), children)

          case ProjectType.kWindowAggregation =>
            WindowAggPlan.gen(ctx, PhysicalWindowAggrerationNode.CastFrom(projectNode), children.head)

          case ProjectType.kGroupAggregation =>
            GroupByAggregationPlan.gen(ctx, PhysicalGroupAggrerationNode.CastFrom(projectNode), children.head)

          case _ => throw new UnsupportedFesqlException(
            s"Project type ${projectNode.getProject_type_} not supported")
        }
      case PhysicalOpType.kPhysicalOpGroupBy =>
        GroupByPlan.gen(ctx, PhysicalGroupNode.CastFrom(root), children.head)
      case PhysicalOpType.kPhysicalOpJoin =>
        JoinPlan.gen(ctx, PhysicalJoinNode.CastFrom(root), children.head, children.last)
      case PhysicalOpType.kPhysicalOpLimit =>
        LimitPlan.gen(ctx, PhysicalLimitNode.CastFrom(root), children.head)
      case PhysicalOpType.kPhysicalOpRename =>
        RenamePlan.gen(ctx, PhysicalRenameNode.CastFrom(root), children.head)
      //case PhysicalOpType.kPhysicalOpFilter =>
      //  FilterPlan.gen(ctx, PhysicalFilterNode.CastFrom(root), children.head)
      case _ =>
        throw new UnsupportedFesqlException(s"Plan type $opType not supported")
    }

    // Set the output to context cache
    ctx.putPlanResult(root.GetNodeId(), outputSpatkInstance)

    outputSpatkInstance
  }


  /**
    * Run plan slowly by storing and loading each intermediate result from external data path.
    */
  def slowRunWithHDFSCache(root: PhysicalOpNode, ctx: PlanContext,
                           cacheDir: String, isRoot: Boolean): SparkInstance = {
    val sess = ctx.getSparkSession
    val fileSystem = FileSystem.get(sess.sparkContext.hadoopConfiguration)
    val rootKey = root.GetTypeName() + "_" + CoreAPI.GetUniqueID(root)

    val children = mutable.ArrayBuffer[SparkInstance]()
    for (i <- 0 until root.GetProducerCnt().toInt) {
      val child = root.GetProducer(i)
      val key = child.GetTypeName() + "_" + CoreAPI.GetUniqueID(child)
      logger.info(s"Compute $rootKey ${i}th child: $key")

      val cacheDataPath = cacheDir + "/" + key + "/data"
      val existCache = fileSystem.isDirectory(new Path(cacheDataPath)) &&
        fileSystem.exists(new Path(cacheDataPath + "/_SUCCESS"))
      val childResult = if (existCache) {
        logger.info(s"Load cached $key: $cacheDataPath")
        SparkInstance.fromDataFrame(sess.read.parquet(cacheDataPath))
      } else if (child.GetOpType() == PhysicalOpType.kPhysicalOpDataProvider) {
        visitNode(child, ctx, Array())
      } else {
        slowRunWithHDFSCache(child, ctx, cacheDir, isRoot=false)
      }
      children += childResult
    }
    logger.info(s"Schedule $rootKey")
    val rootResult = visitNode(root, ctx, children.toArray)
    if (isRoot) {
      return rootResult
    }
    val cacheDataPath = cacheDir + "/" + rootKey + "/data"
    logger.info(s"Store $rootKey: $cacheDataPath")
    rootResult.getDf().write.parquet(cacheDataPath)

    logger.info(s"Reload $rootKey: $cacheDataPath")
    SparkInstance.fromDataFrame(sess.read.parquet(cacheDataPath))
  }

  private def withSQLEngine[T](sql: String, db: Database, config: FeSQLConfig)(body: SQLEngine => T): T = {
    var engine: SQLEngine = null

    val engineOptions = SQLEngine.createDefaultEngineOptions()
    
    if (config.enableWindowParallelization) {
      logger.info("Enable window parallelization optimization")
      engineOptions.set_enable_batch_window_parallelization(true)
    } else {
      logger.info("Disable window parallelization optimization, enable by setting fesql.window.parallelization")
    }

    try {
      engine = new SQLEngine(sql, db, engineOptions)
      val res = body(engine)
      res
    } finally {
      if (engine != null) {
        engine.close()
      }
    }
  }
}


