package com._4paradigm.fesql.spark.analysis
import java.io.File
import java.util
import java.util.{ArrayList, List}

import com._4paradigm.fesql.common.DDLEngine
import com._4paradigm.fesql.node.ColumnRefNode
import com._4paradigm.fesql.spark.PlanContext
import com._4paradigm.fesql.spark.nodes.WindowAggPlan.createWindowAggConfig
import com._4paradigm.fesql.spark.utils.SparkColumnUtil
import com._4paradigm.fesql.vm.{CoreAPI, PhysicalOpNode, PhysicalWindowAggrerationNode}
import com.esotericsoftware.kryo.util.IntMap.Keys
import org.apache.commons.io.FileUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, DataFrame, Encoders, SparkSession}
import org.slf4j.LoggerFactory
import org.apache.spark.sql.functions._
import com._4paradigm.fesql.spark.udf.PercentileApprox._
import com._4paradigm.fesql.utils.SkewUtils

import scala.collection.JavaConverters


/**
 * 智能数据分析
 */
class DataSkewAnalyzer {
    val logger = LoggerFactory.getLogger(this.getClass)

  def demo(): Unit = {
    //  val sparkConfig =
    val spark = SparkSession.builder().master("local").getOrCreate()
    // 默认是0，仅支持0-2
    val percentileFactor: Int = 2
    val skewFactor: Double = 0.5
    //    val mainTablePath = ""
    //    val otherTablePath = ""
    //    val df1 = spark.read.parquet(mainTablePath)
    //    val df2 = spark.read.parquet(otherTablePath)

    //  df1.union(df2)
    //  val ts = ""
    //  val keys = Seq("", "")
    //  val sortKeys = keys + ts
    //  df1.groupBy().agg(max("xx"))
    //  df1.groupBy().stat.approxQuantile()
    //  df1.groupBy()


    import spark.implicits._
    val simpleData = Seq(("James","Sales","NY",90000,34,10000),
      ("James","Sales","NY",86000,56,20000),
      ("James","Sales","CA",81000,30,30000),
      ("James","Finance","CA",90000,24,40000),
      ("James","Finance","CA",99000,40,50000),
      ("James","Finance","NY",83000,36,60000),
      ("James","Finance","NY",83000,36,70000),
      ("James","Finance","NY",83000,36,80000),
      ("James","Finance","NY",83000,36,90000),
      ("James","Finance","NY",83000,36,100000),
      ("Jen","Finance","NY",79000,53,15000),
      ("Jen","Marketing","CA",80000,25,18000),
      ("Jen","Marketing","NY",91000,50,21000)
    )
    val df1 = simpleData.toDF("employee_name","department","state","salary","age","bonus")


    df1.show()

    val ts = "bonus"
    val keys = Seq("employee_name", "department")
    val sortKeys = keys + ts
    println("df cnt:" + df1.count())
    //    printRepartitionResult(df1, keys)
    import org.apache.spark.sql.functions._
    val coordinate = df1.groupBy(keys.map(df1(_)): _*).agg(sum("bonus") as "total_sum",
      approx_count_distinct("bonus") as "order_bonus",
      mean("bonus"),
      percentile_approx($"bonus", lit(0)) as "percentile_0",
      percentile_approx($"bonus", lit(0.25)) as "percentile_1",
      percentile_approx($"bonus", lit(0.5)) as "percentile_2",
      percentile_approx($"bonus", lit(0.75)) as "percentile_3",
      percentile_approx($"bonus", lit(1)) as "percentile_4",
      max("bonus"),
      min("bonus"),
      count("employee_name")
    )
    coordinate.show()

    df1.agg(
      max("bonus"),
      min("bonus"),
      //      count(),
    ).show()

    df1.createOrReplaceTempView("main_table")
    coordinate.createOrReplaceTempView("info_table")
    val sqlContext = spark.sqlContext
    val sql_path = "/Users/magnetowang/Documents/GitHub/paste/scala-paste/src/main/resources/spark/add_column.sql"
    val sql_code = FileUtils.readFileToString(new File(sql_path), "UTF-8")
    sqlContext.sql(sql_code).show()

    //    df1.join(coordinate).where("")
    //    val res = df1.repartition(keys.map(df1(_)): _*).stat.approxQuantile(ts, Array[Double](0, 0.25, 0.5, 0.75, 1), 0.1)
    //    res.foreach(println(_))
    //    knifeData(df1, coordinate, keys, ts)
  }


  //  def knifeData(data: DataFrame, coordinate: DataFrame, keys: Seq[String], ts: String): RDD[] = {
  //    val df = data.repartition(keys.map(data(_)): _*)
  //    df.rdd.mapPartitions(
  //      rows => {
  //        rows.flatMap(
  //          row => {
  //
  //            Option(null)
  //          }
  //        )
  //      }
  //    )
  //  }

  def increaseRows(data: DataFrame, tag: String): Unit = {
    import org.apache.spark.sql.functions._
    data.agg(avg("xx"))
  }

  def improveSkew(ctx: PlanContext, node: PhysicalWindowAggrerationNode, input: DataFrame) {
    val windowOp = node.window()
    val groupByExprs = windowOp.partition().keys()
    val groupByCols = new util.ArrayList[String]()
    for (i <- 0 until groupByExprs.GetChildNum()) {
      val expr = groupByExprs.GetChild(i)
      val key = CoreAPI.ResolveSourceColumnName(node, ColumnRefNode.CastFrom(expr))
      groupByCols.add(key)
    }
    val ts = CoreAPI.ResolveSourceColumnName(node, ColumnRefNode.CastFrom(node.window.sort.orders.order_by.GetChild(0)))
    val keyScala = JavaConverters.asScalaIteratorConverter(groupByCols.iterator()).asScala.toSeq
    val table_report = takeTableReport(input, keyScala, ts)
//    val windowAggConfig = createWindowAggConfig(ctx, node)
    val table = "FESQL_TEMP_WINDOW_" + System.currentTimeMillis()
    val reportTable = "FESQL_TEMP_WINDOW_REPORT_" + System.currentTimeMillis()
    logger.info("skew main table {}", table)
    logger.info("skew main table report{}", reportTable)
    val keysMap = new util.HashMap[String, String]()
    keyScala.foreach(_ => keysMap.put(_, _))
    val sqlCode = SkewUtils.genPercentileSql(table, reportTable, 4, keysMap, ts)




//    val percentileSql = SkewUtils.genPercentileSql(windowOp)

  }
  
  def takeTableReport(data: DataFrame, keys: Seq[String], ts: String): DataFrame = {
    val coordinate = data.groupBy(keys.map(data(_)): _*).agg(

      sum(ts) as "total_order",
      approx_count_distinct(ts) as "order",
      mean(ts) as "mean_order",
      max(ts) as "max_order",
      min(ts) as "min_order",
      percentile_approx(data(ts), lit(0)) as "percentile_0",
      percentile_approx(data(ts), lit(0.25)) as "percentile_1",
      percentile_approx(data(ts), lit(0.5)) as "percentile_2",
      percentile_approx(data(ts), lit(0.75)) as "percentile_3",
      percentile_approx(data(ts), lit(1)) as "percentile_4"
    )
    coordinate
  }


  def printRepartitionResult(data: DataFrame, keys: Seq[String]): Unit = {
    //    import spark.implicits._
    //    import org.apache.spark.sql.catalyst.encoders
    val df = data.repartition(keys.map(data(_)): _*)
    //    df.show()
    data.rdd.flatMap(row => {
      println(row.toString())
      Option(null)
    })

  }
}

object DataSkewAnalyzer {
    val logger = LoggerFactory.getLogger(this.getClass)
  def main(args: Array[String]): Unit = {
    logger.info("???????")

    val ds = new DataSkewAnalyzer()
    ds.demo()
  }
}
