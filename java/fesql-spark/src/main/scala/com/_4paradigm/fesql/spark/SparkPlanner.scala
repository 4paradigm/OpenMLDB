package com._4paradigm.fesql.spark

import com._4paradigm.fesql.FeSqlLibrary
import com._4paradigm.fesql.`type`.TypeOuterClass._
import com._4paradigm.fesql.common.{SQLEngine, UnsupportedFesqlException}
import com._4paradigm.fesql.spark.api.{FesqlDataframe, FesqlSession}
import com._4paradigm.fesql.spark.element.FesqlConfig
import com._4paradigm.fesql.spark.nodes._
import com._4paradigm.fesql.spark.utils.FesqlUtil
import com._4paradigm.fesql.vm._
import org.apache.spark.sql.catalyst.QueryPlanningTracker
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.slf4j.LoggerFactory

import scala.collection.mutable


class SparkPlanner(session: SparkSession, config: Map[String, Any]) {

  private val logger = LoggerFactory.getLogger(this.getClass)

  // Ensure native initialized
  FeSqlLibrary.initCore()
  Engine.InitializeGlobalLLVM()
  var node: PhysicalOpNode = _


  def this(session: SparkSession) = {
    this(session, session.conf.getAll)
    for ((k, v) <- config.asInstanceOf[Map[String, String]]) {
      logger.info(s"spark plan fesql config: ${k} = ${v}")
      k match {
        case FesqlConfig.configSkewRadio => FesqlConfig.skewRatio = v.toDouble
        case FesqlConfig.configSkewLevel => FesqlConfig.skewLevel = v.toInt
        case FesqlConfig.configSkewCnt => FesqlConfig.skewCnt = v.toInt
        case FesqlConfig.configSkewCntName => FesqlConfig.skewCntName = v.asInstanceOf[String]
        case FesqlConfig.configSkewTag => FesqlConfig.skewTag = v.asInstanceOf[String]
        case FesqlConfig.configSkewPosition => FesqlConfig.skewPosition = v.asInstanceOf[String]
        case FesqlConfig.configMode => FesqlConfig.mode = v.asInstanceOf[String]
        case FesqlConfig.configPartitions => FesqlConfig.paritions = v.toInt
        case FesqlConfig.configTimeZone => FesqlConfig.timeZone = v.asInstanceOf[String]
        case FesqlConfig.configTinyData => FesqlConfig.tinyData = v.toLong
        case _ => ""
      }
    }
  }

  def plan(sql: String, tableDict: Map[String, DataFrame]): SparkInstance = {
    // spark translation state
    val planCtx = new PlanContext(sql, session, this, config)

    // set spark input tables
    tableDict.foreach {
      case (name, df) => planCtx.registerDataFrame(name, df)
    }

    withSQLEngine(sql, FesqlUtil.getDatabase(FesqlConfig.configDBName, tableDict)) { engine =>
      val irBuffer = engine.getIRBuffer
      planCtx.setModuleBuffer(irBuffer)

      val root = engine.getPlan
      node = root
      logger.info("Get FeSQL physical plan: ")
      root.Print()
      visitPhysicalNodes(root, planCtx)
    }
  }

  def visitPhysicalNodes(root: PhysicalOpNode, ctx: PlanContext): SparkInstance = {
    val optCache = ctx.getPlanResult(root)
    if (optCache.isDefined) {
      return optCache.get
    }

    val children = mutable.ArrayBuffer[SparkInstance]()
    for (i <- 0 until root.GetProducerCnt().toInt) {
      children += visitPhysicalNodes(root.GetProducer(i), ctx)
    }

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

  private def withSQLEngine[T](sql: String, db: Database)(body: SQLEngine => T): T = {
    val engine = new SQLEngine(sql, db)
    val res = body(engine)
    engine.close()
    res
  }
}


