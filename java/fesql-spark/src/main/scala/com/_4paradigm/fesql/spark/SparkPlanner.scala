package com._4paradigm.fesql.spark

import com._4paradigm.fesql.FeSqlLibrary
import com._4paradigm.fesql.`type`.TypeOuterClass._
import com._4paradigm.fesql.common.{SQLEngine, UnsupportedFesqlException}
import com._4paradigm.fesql.node.JoinType
import com._4paradigm.fesql.spark.nodes._
import com._4paradigm.fesql.spark.utils.{FesqlUtil, NodeIndexInfo}
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
  var node: PhysicalOpNode = _

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
      node = root
      logger.info("Get FeSQL physical plan: ")
      root.Print()

      logger.info("Visit physical plan to find ConcatJoin node")
      val concatJoinNodes = mutable.ArrayBuffer[PhysicalJoinNode]()
      findConcatJoinNode(root, concatJoinNodes)

      logger.info("Visit physical plan to add node index info")
      concatJoinNodes.map(joinNode => bindNodeIndexInfo(joinNode, planCtx))

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
  def bindNodeIndexInfo(concatJoinNode: PhysicalJoinNode, ctx: PlanContext): Unit = {
    val nodeId = concatJoinNode.GetNodeId()
    val indexColumnName = "__JOIN_INDEX__" + nodeId + "__"+ System.currentTimeMillis()

    // Bind the ConcatJoin child nodes but not ConcatJoin, the node with this flag will output Spark DataFrame with index column
    for (i <- 0 until concatJoinNode.GetProducerCnt().toInt) {
      ctx.putNodeIndexInfo(concatJoinNode.GetProducer(i).GetNodeId(), NodeIndexInfo(indexColumnName, false))
    }

    // TODO: Need heuristic method to find the LCA node
    // Bind the source node, need to set shouldAddIndexColumn as true
    ctx.putNodeIndexInfo(concatJoinNode.GetProducer(0).GetProducer(0).GetNodeId(), NodeIndexInfo(indexColumnName, true))
  }

  def getSparkOutput(root: PhysicalOpNode, ctx: PlanContext): SparkInstance = {
    val optCache = ctx.getPlanResult(root)
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
    opType match {
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


