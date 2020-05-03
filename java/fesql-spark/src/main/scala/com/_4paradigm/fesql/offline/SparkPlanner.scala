package com._4paradigm.fesql.offline

import com._4paradigm.fesql.`type`.TypeOuterClass._
import com._4paradigm.fesql.offline.nodes.{DataProviderPlan, GroupAndSortPlan, RowProjectPlan, WindowAggPlan}
import com._4paradigm.fesql.vm._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

import scala.collection.mutable


class SparkPlanner(session: SparkSession, config: Map[String, Any]) {

  private val logger = LoggerFactory.getLogger(this.getClass)

  // Ensure native initialized
  FeSqlLibrary.init()


  def this(session: SparkSession) = {
    this(session, Map())
  }

  def plan(sql: String, tableDict: Map[String, DataFrame]): SparkInstance = {
    // spark translation state
    val planCtx = new PlanContext(sql, session, config)

    // set spark input tables
    tableDict.foreach {
      case (name, df) => planCtx.registerDataFrame(name, df)
    }

    withSQLEngine(sql, FesqlUtil.getDatabase("spark_db", tableDict)) { engine =>
      val irBuffer = engine.getIRBuffer
      planCtx.setModuleBuffer(irBuffer)

      val root = engine.getPlan
      logger.info("Get FeSQL physical plan: ")
      root.Print()
      visitPhysicalNodes(root, planCtx)
    }
  }


  private def visitPhysicalNodes(root: PhysicalOpNode, ctx: PlanContext): SparkInstance = {
    val optCache = ctx.getPlanResult(root)
    if (optCache.isDefined) {
      return optCache.get
    }

    val children = mutable.ArrayBuffer[SparkInstance]()
    for (i <- 0 until root.GetProducerCnt().toInt) {
      children += visitPhysicalNodes(root.GetProducer(i), ctx)
    }

    val opType = root.getType_
    opType match {
      case PhysicalOpType.kPhysicalOpDataProvider =>
        DataProviderPlan.gen(ctx, PhysicalDataProviderNode.CastFrom(root), children)

      case PhysicalOpType.kPhysicalOpProject =>
        val projectNode = PhysicalProjectNode.CastFrom(root)
        projectNode.getProject_type_ match {
          case ProjectType.kTableProject =>
            RowProjectPlan.gen(ctx, PhysicalTableProjectNode.CastFrom(projectNode), children)

          case ProjectType.kWindowAggregation =>
            WindowAggPlan.gen(ctx, PhysicalWindowAggrerationNode.CastFrom(projectNode), children.head)

          case _ => throw new FeSQLException(
            s"Project type ${projectNode.getProject_type_} not supported")
        }


      case _ =>
        throw new IllegalArgumentException(s"Plan type $opType not supported")
    }
  }


  private def withSQLEngine[T](sql: String, db: Database)(body: SQLEngine => T): T = {
    val engine = new SQLEngine(sql, db)
    val res = body(engine)
    engine.close()
    res
  }
}


