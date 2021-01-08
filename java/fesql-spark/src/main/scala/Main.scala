import java.io.File

import com._4paradigm.fesql.spark.api.FesqlSession
import com._4paradigm.fesql.spark.element.FesqlConfig
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.io.Source


object Main {

  private val logger = LoggerFactory.getLogger(this.getClass)

  private val inputSpecs = mutable.HashMap[String, String]()
  private val configs = mutable.HashMap[String, Any]()
  private var sql: String = _
  private var outputPath: String = _
  private var sparkMaster = "local"
  private var appName: String = _
  private var useSparkSQL = false
  private var jsonPath: String = _
  private var slowHdfsCacheDir: String = _

  def main(args: Array[String]): Unit = {
    ArgParser(args).parseArgs()
    run()
  }

  def run(): Unit = {
    logger.info("Create FeSQL Spark Planner...")
    val sessionBuilder = SparkSession.builder().master(sparkMaster)
    if (appName != null) {
      sessionBuilder.appName(appName)
    }
    for ((k, v) <- configs) {
      if (k.startsWith("spark.")) {
        sessionBuilder.config(k, v.toString)
      }
    }
    val sparkSession = sessionBuilder.getOrCreate()

    val sess = new FesqlSession(sparkSession)

    logger.info("Resolve input tables...")
    for ((name, path) <- inputSpecs) {
      logger.info(s"Try load table $name from: $path")
      sess.read(path).createOrReplaceTempView(name)
    }

    if (sql == null) {
      throw new IllegalArgumentException("No sql script specified")
    }
    val sqlFile = new File(sql)
    if (sqlFile.exists()) {
      sql = Source.fromFile(sqlFile).mkString("")
    }
    logger.info("SQL Script:\n" + sql)

    var startTime = System.currentTimeMillis()
    val outputDf = if (useSparkSQL) {
      sess.sparksql(sql)
    } else {
      sess.sql(sql)
    }

    var endTime = System.currentTimeMillis()
    logger.info(f"Compile SQL time cost: ${(endTime - startTime) / 1000.0}%.2f seconds")

    startTime = System.currentTimeMillis()
    if (outputPath != null) {
      logger.info(s"Save result to: $outputPath")
      outputDf.write(outputPath)
    } else {
      val count = outputDf.getSparkDf().queryExecution.toRdd.count()
      logger.info(s"Result records count: $count")
    }
    endTime = System.currentTimeMillis()
    logger.info(f"Execution time cost: ${(endTime - startTime) / 1000.0}%.2f seconds")
  }


  case class ArgParser(args: Array[String]) {
    private var idx = 0
    private var curKey: String = _

    def parseKey(key: String): Unit = {
      key match {
        case "-i" | "--input" => inputSpecs += parsePair()
        case "-s" | "--sql" => sql = parseValue()
        case "-o" | "--output" => outputPath = parseValue()
        case "-c" | "--conf" => configs += parsePair()
        case "--master" => sparkMaster = parseValue()
        case "--name" => appName = parseValue()
        case "--spark-sql" => useSparkSQL = true
        case "--json" => jsonPath = parseValue()
        case "--slow-hdfs-cache" => FesqlConfig.slowRunCacheDir = parseValue()
        case _ =>
          logger.warn(s"Unknown argument: $key")
      }
    }

    def parseArgs(): Unit = {
      while (idx < args.length) {
        curKey = args(idx)
        parseKey(curKey)
        idx += 1
      }
    }

    def parsePair(): (String, String) = {
      val value = parseValue()
      val splitPos = value.indexOf("=")
      if (splitPos < 0) {
        throw new IllegalArgumentException(
          s"Illegal value for $curKey: $value")
      }
      val (k, v) = (value.substring(0, splitPos), value.substring(splitPos + 1))
      k -> v
    }

    def parseValue(): String = {
      idx += 1
      if (idx >= args.length) {
        throw new IllegalArgumentException(
          s"Argument index out of bound for $curKey")
      }
      args(idx)
    }
  }
}
