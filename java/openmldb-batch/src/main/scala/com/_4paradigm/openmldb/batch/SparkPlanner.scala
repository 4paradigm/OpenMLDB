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

import com._4paradigm.hybridse.HybridSeLibrary
import com._4paradigm.hybridse.`type`.TypeOuterClass.Database
import com._4paradigm.hybridse.node.JoinType
import com._4paradigm.hybridse.sdk.{SqlEngine, UnsupportedHybridSeException}
import com._4paradigm.hybridse.vm.{CoreAPI, Engine, PhysicalConstProjectNode, PhysicalDataProviderNode,
  PhysicalGroupAggrerationNode, PhysicalGroupNode, PhysicalJoinNode, PhysicalLimitNode, PhysicalLoadDataNode,
  PhysicalOpNode, PhysicalOpType, PhysicalProjectNode, PhysicalRenameNode, PhysicalSelectIntoNode,
  PhysicalSimpleProjectNode, PhysicalSortNode, PhysicalTableProjectNode, PhysicalWindowAggrerationNode, ProjectType}
import com._4paradigm.openmldb.batch.api.OpenmldbSession
import com._4paradigm.openmldb.batch.nodes.{ConstProjectPlan, DataProviderPlan, GroupByAggregationPlan, GroupByPlan,
  JoinPlan, LimitPlan, LoadDataPlan, RenamePlan, RowProjectPlan, SelectIntoPlan, SimpleProjectPlan, SortByPlan,
  WindowAggPlan}
import com._4paradigm.openmldb.batch.utils.{GraphvizUtil, HybridseUtil, NodeIndexInfo, NodeIndexType}
import com._4paradigm.openmldb.sdk.impl.SqlClusterExecutor
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.collection.JavaConversions.seqAsJavaList

class SparkPlanner(session: SparkSession, config: OpenmldbBatchConfig, sparkAppName: String) {

  private val logger = LoggerFactory.getLogger(this.getClass)

  var openmldbSession: OpenmldbSession = _

  // Ensure native initialized
  SqlClusterExecutor.initJavaSdkLibrary(config.openmldbJsdkLibraryPath)
  Engine.InitializeGlobalLLVM()

  def this(session: SparkSession, sparkAppName: String) = {
    this(session, OpenmldbBatchConfig.fromSparkSession(session), sparkAppName)
  }

  def this(session: SparkSession, config: OpenmldbBatchConfig) = {
    this(session, config, session.conf.get("spark.app.name"))
  }

  def this(session: SparkSession) = {
    this(session, OpenmldbBatchConfig.fromSparkSession(session), session.conf.get("spark.app.name"))
  }

  def this(openmldbSession: OpenmldbSession, config: OpenmldbBatchConfig) = {
    this(openmldbSession.getSparkSession, config)
    this.openmldbSession = openmldbSession
  }

  def plan(sql: String, registeredTables: mutable.Map[String, mutable.Map[String, DataFrame]]): SparkInstance = {
    // Translation state
    val tag = s"$sparkAppName-$sql"
    val planCtx = new PlanContext(tag, session, this, config)

    if (openmldbSession != null) {
      planCtx.setOpenmldbSession(openmldbSession)
    }
    // Set input tables
    planCtx.setRegisteredTables(registeredTables)

    val databases: List[Database] = HybridseUtil.getDatabases(registeredTables)

    withSQLEngine(sql, databases, config) { engine =>

      val irBuffer = engine.getIrBuffer
      planCtx.setModuleBuffer(irBuffer)

      val root = engine.getPlan
      logger.info("Get HybridSE physical plan: ")

      if (config.printPhysicalPlan) {
        root.Print()
      }

      if (!config.physicalPlanGraphvizPath.equals("")) {
        logger.info("Draw the physical plan and save to " + config.physicalPlanGraphvizPath)
        GraphvizUtil.drawPhysicalPlan(root, config.physicalPlanGraphvizPath)
      }

      logger.info("Visit physical plan to find ConcatJoin node")
      val concatJoinNodes = mutable.ArrayBuffer[PhysicalJoinNode]()
      findConcatJoinNode(root, concatJoinNodes)

      logger.info("Visit concat join node to add node index info")
      val processedConcatJoinNodeIds = mutable.HashSet[Long]()
      val indexColumnName = "__CONCATJOIN_INDEX__" + System.currentTimeMillis()
      concatJoinNodes.foreach(joinNode => bindNodeIndexInfo(joinNode, planCtx, processedConcatJoinNodeIds,
        indexColumnName))

      if (config.slowRunCacheDir != null) {
        slowRunWithHDFSCache(root, planCtx, config.slowRunCacheDir, isRoot = true)
      } else {
        getSparkOutput(root, planCtx)
      }
    }

  }

  def plan(sql: String, registeredDefaultDbTables: Map[String, DataFrame]): SparkInstance = {
    val registeredTables = new mutable.HashMap[String, mutable.Map[String, DataFrame]]()
    registeredTables.put(config.defaultDb, collection.mutable.Map(registeredDefaultDbTables.toSeq: _*))
    plan(sql, registeredTables)
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

  // Bind the node index info for the integration which use ConcatJoin for computing window concurrently
  def bindNodeIndexInfo(concatJoinNode: PhysicalJoinNode,
                        ctx: PlanContext,
                        processedConcatJoinNodeIds: mutable.HashSet[Long], indexColumnName: String): Unit = {

    val concatJoinNodeId = concatJoinNode.GetNodeId()
    if (ctx.hasIndexInfo(concatJoinNodeId)) {
      // The node has been processed, do not handle again
      return
    }

    logger.info("Visit physical plan to find the dest node which should add the index column")
    val visitedNodeIdSet = mutable.HashSet[Long]()
    val nodeArray = mutable.ArrayBuffer[PhysicalOpNode]()
    nodeArray.append(concatJoinNode)
    val destNodeId = findLowestCommonAncestorNode(nodeArray, visitedNodeIdSet)

    logger.info("Bind the node index info for all suitable integration")
    if (processedConcatJoinNodeIds.contains(concatJoinNodeId)) {
      // Ignore the concat join node which has been process
      return
    }

    visitAndBindNodeIndexInfo(ctx, concatJoinNode, destNodeId, indexColumnName, processedConcatJoinNodeIds)
    // Reset the fist concat join node to source concat join node
    ctx.getIndexInfo(concatJoinNodeId).nodeIndexType = NodeIndexType.SourceConcatJoinNode
  }

  def findLowestCommonAncestorNode(nodeArray: mutable.ArrayBuffer[PhysicalOpNode],
                                   visitedNodeIdSet: mutable.HashSet[Long]): Long = {
    if (nodeArray.isEmpty) {
      return 0
    }

    val producerNodeArray = mutable.ArrayBuffer[PhysicalOpNode]()

    for (root <- nodeArray) {
      for (i <- 0 until root.GetProducerCnt().toInt) {
        val producerNode = root.GetProducer(i)
        // If node has been visited again, return the node as common ancestor
        if (visitedNodeIdSet.contains(producerNode.GetNodeId())) {
          return producerNode.GetNodeId()
        } else {
          // Add current node to visited set
          visitedNodeIdSet.add(producerNode.GetNodeId())
          producerNodeArray.append(producerNode)
        }
      }
    }
    findLowestCommonAncestorNode(producerNodeArray, visitedNodeIdSet)
  }

  def visitAndBindNodeIndexInfo(ctx: PlanContext,
                                node: PhysicalOpNode,
                                destNodeId: Long,
                                indexColumnName: String,
                                processedConcatJoinNodeIds: mutable.HashSet[Long]): Unit = {
    if (ctx.hasIndexInfo(node.GetNodeId())) {
      // No need to set the node with index info again
      return
    }

    if (node.GetNodeId() == destNodeId) {
      ctx.putNodeIndexInfo(node.GetNodeId(), NodeIndexInfo(indexColumnName, NodeIndexType.DestNode))
      // Return if handle the dest node
      return
    }

    // Check if it is concat join node
    if (node.GetOpType() == PhysicalOpType.kPhysicalOpJoin
      && PhysicalJoinNode.CastFrom(node).join().join_type() == JoinType.kJoinTypeConcat) {
      ctx.putNodeIndexInfo(node.GetNodeId(), NodeIndexInfo(indexColumnName, NodeIndexType.InternalConcatJoinNode))
      processedConcatJoinNodeIds.add(node.GetNodeId())
    } else {
      ctx.putNodeIndexInfo(node.GetNodeId(), NodeIndexInfo(indexColumnName, NodeIndexType.InternalComputeNode))
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
            RowProjectPlan.gen(ctx, PhysicalTableProjectNode.CastFrom(projectNode), children.head)
          case ProjectType.kWindowAggregation =>
            WindowAggPlan.gen(ctx, PhysicalWindowAggrerationNode.CastFrom(projectNode), children.head)
          case ProjectType.kGroupAggregation =>
            GroupByAggregationPlan.gen(ctx, PhysicalGroupAggrerationNode.CastFrom(projectNode), children.head)
          case _ => throw new UnsupportedHybridSeException(
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
      case PhysicalOpType.kPhysicalOpSortBy =>
        SortByPlan.gen(ctx, PhysicalSortNode.CastFrom(root), children.head)
      //case PhysicalOpType.kPhysicalOpFilter =>
      //  FilterPlan.gen(ctx, PhysicalFilterNode.CastFrom(root), children.head)
      case PhysicalOpType.kPhysicalOpLoadData =>
        LoadDataPlan.gen(ctx, PhysicalLoadDataNode.CastFrom(root))
      case PhysicalOpType.kPhysicalOpSelectInto =>
        SelectIntoPlan.gen(ctx, PhysicalSelectIntoNode.CastFrom(root), children.head)
      case _ =>
        throw new UnsupportedHybridSeException(s"Plan type $opType not supported")
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
        slowRunWithHDFSCache(child, ctx, cacheDir, isRoot = false)
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

  private def withSQLEngine[T](sql: String, dbs: List[Database],
                               config: OpenmldbBatchConfig)(body: SqlEngine => T): T = {
    var engine: SqlEngine = null

    val engineOptions = SqlEngine.createDefaultEngineOptions()

    if (config.enableWindowParallelization) {
      logger.info("Enable window parallelization optimization")
      engineOptions.SetEnableBatchWindowParallelization(true)
    } else {
      logger.info("Disable window parallelization optimization, enable by setting openmldb.window.parallelization")
    }

    if (config.enableUnsafeRowOptimization) {
      engineOptions.SetEnableSparkUnsaferowFormat(true)
    }

    try {
      engine = new SqlEngine(sql, dbs, engineOptions, config.defaultDb)
      val res = body(engine)
      res
    } finally {
      if (engine != null) {
        engine.close()
      }
    }
  }
}


