package com._4paradigm.fesql.offline

import com._4paradigm.fesql.`type`.TypeOuterClass._
import com._4paradigm.fesql.offline.nodes.{DataProviderPlan, ProjectPlan}
import com._4paradigm.fesql.vm._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

import scala.collection.mutable


class SparkPlanner(session: SparkSession) {

  private val logger = LoggerFactory.getLogger(this.getClass)

  // Ensure native initialized
  FeSqlLibrary.init()


  def plan(sql: String, tableDict: Map[String, DataFrame]): SparkInstance = {
    // spark translation state
    val planCtx = new PlanContext(sql, session)

    // set spark input tables
    tableDict.foreach {
      case (name, df) => planCtx.registerDataFrame(name, df)
    }

    withSQLEngine(sql, SparkUtils.getDatabase("spark_db", tableDict)) { engine =>
      val irBuffer = engine.getIRBuffer
      planCtx.setModuleBuffer(irBuffer)

      val root = engine.getPlan
      logger.info("Get FeSQL physical plan: ")
      root.Print()
      planPhysicalNodes(root, planCtx)
    }
  }


  private def planPhysicalNodes(root: PhysicalOpNode, ctx: PlanContext): SparkInstance = {
    val optCache = ctx.getPlanResult(root)
    if (optCache.isDefined) {
      return optCache.get
    }

    val children = mutable.ArrayBuffer[SparkInstance]()
    for (k <- 0 until root.GetProducerCnt().toInt) {
      children += planPhysicalNodes(root.GetProducer(k), ctx)
    }

    val opType = root.getType_
    opType match {
      case PhysicalOpType.kPhysicalOpDataProvider =>
        DataProviderPlan.gen(ctx, PhysicalDataProviderNode.CastFrom(root), children)

      case PhysicalOpType.kPhysicalOpProject =>
        ProjectPlan.gen(ctx, PhysicalProjectNode.CastFrom(root), children)

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


