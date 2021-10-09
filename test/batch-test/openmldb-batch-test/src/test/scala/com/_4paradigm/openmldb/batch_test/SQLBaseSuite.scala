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

package com._4paradigm.openmldb.batch_test

import java.io.{File, FileInputStream}
import java.sql.{Date, Timestamp}
import java.text.SimpleDateFormat
import com._4paradigm.hybridse.sdk.{JitManager, UnsupportedHybridSeException}
import com._4paradigm.openmldb.test_common.model.{CaseFile, ExpectDesc, InputDesc, SQLCase, TableFile}
import com._4paradigm.openmldb.batch.api.{OpenmldbDataframe, OpenmldbSession}
import com._4paradigm.openmldb.batch.utils.SparkUtil
import com._4paradigm.openmldb.test_common.model.CaseFile
//import com._4paradigm.hybridse.sqlcase.model._
import org.apache.commons.collections.CollectionUtils
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import org.slf4j.LoggerFactory
import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.constructor.Constructor

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

// TODO: Do not use sqlcase
class SQLBaseSuite extends SparkTestSuite {

  private val logger = LoggerFactory.getLogger(this.getClass)

  final private val rootDir = {
    var pwd = new File(System.getProperty("user.dir"))
    if (pwd.getAbsolutePath.endsWith("openmldb-batch-test")) {
      // TODO: Use the better method to get the root dir
      pwd = pwd.getParentFile.getParentFile.getParentFile // ../../../
    }
    pwd
  }

  def testCases(yamlPath: String) {
    val caseFile = loadYaml[CaseFile](yamlPath)
    // TODO: Only run the cases whose level is 0
    caseFile.getCases.asScala.filter(c => keepCase(c)).filter(c => c.getLevel == 0).foreach(c => testCase(c))
  }

  def testCase(yamlPath: String, id: String): Unit = {
    val caseFile = loadYaml[CaseFile](yamlPath)
    val sqlCase = caseFile.getCases.asScala.find(_.getId == id).orNull
    testCase(sqlCase)
  }

  def keepCase(sqlCase: SQLCase): Boolean = {
    if (sqlCase.getMode != null) {
      !sqlCase.getMode.contains("offline-unsupport") && !sqlCase.getMode.contains("batch-unsupport")
    } else {
      true
    }
  }

  def createSQLString(sql: String, inputNames: ListBuffer[(Int, String)]): String = {
    var new_sql = sql
    inputNames.foreach(item => {
      new_sql = new_sql.replaceAll("\\{" + item._1 + "\\}", item._2)
    })
    new_sql
  }

  def testCase(sqlCase: SQLCase): Unit = {
    test(SQLBaseSuite.getTestName(sqlCase)) {
      logger.info(s"Test ${sqlCase.getId}:${sqlCase.getDesc}")

      // TODO: may set config of Map("sparkfe.group.partitions" -> 1)
      val spark = new OpenmldbSession(getSparkSession)

      var table_id = 0
      val inputNames = mutable.ListBuffer[(Int, String)]()
      sqlCase.getInputs.asScala.foreach(desc => {
        val (name, df) = loadInputData(desc, table_id)
        OpenmldbDataframe(spark, df).createOrReplaceTempView(name)
        inputNames += Tuple2[Int, String](table_id, name)
        table_id += 1
      })

      val sql = sqlCase.getSql
      if (sqlCase.getExpect != null && !sqlCase.getExpect.getSuccess) {
        assertThrows[UnsupportedHybridSeException] {
          spark.sql(sql).sparkDf
        }
      } else {
        val df = spark.sql(sql).sparkDf
        df.cache()
        if (sqlCase.getExpect != null) {
          checkOutput(df, sqlCase.getExpect)
        }

        // Run SparkSQL to test and compare the generated Spark dataframes
        if (sqlCase.isStandard_sql) {
          logger.info("Use the standard sql, test result with SparkSQL")
          // Remove the ";" in sql text
          var sqlText: String = sqlCase.getSql.trim
          if (sqlText.endsWith(";")) {
            sqlText = sqlText.substring(0, sqlText.length - 1)
          }
          checkTwoDataframe(df, spark.openmldbSql(sqlText).sparkDf)
        }
      }

      JitManager.removeModule(sqlCase.getSql)
    }
  }

  def checkTwoDataframe(df1: DataFrame, df2: DataFrame): Unit = {
    assert(df1.except(df2).count() == df2.except(df1).count())
  }

  def checkOutput(data: DataFrame, expect: ExpectDesc): Unit = {
    if (CollectionUtils.isEmpty(expect.getColumns) && CollectionUtils.isEmpty(expect.getRows)) {
      logger.info("pass: no columns and rows in expect block")
      return
    }

    val expectSchema = parseSchema(expect.getColumns)
    // Notice that only check schema name and type, but not nullable attribute
    assert(SparkUtil.checkSchemaIgnoreNullable(data.schema, expectSchema))

    val expectData = parseData(expect.getRows, expectSchema)
      .zipWithIndex.sortBy(_._1.mkString(","))

    val actualData = data.collect().map(_.toSeq.toArray)
      .zipWithIndex.sortBy(_._1.mkString(","))

    assert(expectData.lengthCompare(actualData.length) == 0,
      s"Output size mismatch, get ${actualData.length} but expect ${expectData.length}")

    val size = expectData.length
    //    for (i <- 0 until size) {
    //      val expectId = expectData(i)._2
    //      val expectArr = expectData(i)._1
    //      val outputArr = actualData(i)._1
    //      print(s"Expect: ${expectArr.mkString(", ")} -- " +
    //        s"Output: ${outputArr.mkString(", ")}\n")
    //    }
    for (i <- 0 until size) {
      val expectId = expectData(i)._2
      val expectArr = expectData(i)._1
      val outputArr = actualData(i)._1

      assert(expectArr.lengthCompare(outputArr.length) == 0,
        s"Row size mismatch at ${expectId}th row")

      expectArr.zip(outputArr).zipWithIndex.foreach {
        case ((expectVal, outputVal), colIdx) =>
          assert(compareVal(expectVal, outputVal, expectSchema(colIdx).dataType),
            s"${colIdx}th col mismatch at ${expectId}th row: " +
              s"expect $expectVal but get $outputVal\n" +
              s"Expect: ${expectArr.mkString(", ")}\n" +
              s"Output: ${outputArr.mkString(", ")}")
      }
    }
  }

  def compareVal(left: Any, right: Any, dtype: DataType): Boolean = {
    if (left == null) {
      return right == null
    } else if (right == null) {
      return left == null
    }
    dtype match {
      case FloatType =>
        val leftFloat = toFloat(left)
        val rightFloat = toFloat(right)
        if (leftFloat.isNaN) {
          rightFloat.isNaN
        } else if (leftFloat.isInfinity) {
          rightFloat.isInfinity
        } else {
          math.abs(toFloat(left) - toFloat(right)) < 1e-5
        }
      case DoubleType =>
        val leftDouble = toDouble(left)
        val rightDouble = toDouble(right)
        if (leftDouble.isNaN) {
          rightDouble.isNaN
        } else if (leftDouble.isInfinity) {
          rightDouble.isInfinity
        } else {
          math.abs(toDouble(left) - toDouble(right)) < 1e-5
        }
      case _ =>
        left == right
    }
  }

  def formatVal(value: Any): String = {
    value.toString
  }

  def toFloat(value: Any): Float = {
    value match {
      case f: Float => f
      case s: String if s.toLowerCase == "nan" => Float.NaN
      case s: String => s.trim.toFloat
      case _ => value.toString.toFloat
    }
  }

  def toDouble(value: Any): Double = {
    value match {
      case f: Double => f
      case s: String if s.toLowerCase == "nan" => Double.NaN
      case s: String => s.trim.toDouble
      case _ => value.toString.toDouble
    }
  }

  def loadInputData(inputDesc: InputDesc, table_id: Int): (String, DataFrame) = {
    val sess = getSparkSession
    if (inputDesc.getResource != null) {
      val (name, df) = loadTable(inputDesc.getResource)
      name -> df
    } else {
      val schema = parseSchema(inputDesc.getColumns)
      val data = parseData(inputDesc.getRows, schema)
        .map(arr => Row.fromSeq(arr)).toList.asJava
      val df = sess.createDataFrame(data, schema)
      inputDesc.getName -> df
    }
  }

  def loadTable(path: String): (String, DataFrame) = {
    val absPath = if (path.startsWith("/")) path else rootDir.getAbsolutePath + "/" + path
    val caseFile = loadYaml[TableFile](absPath)
    val tbl = caseFile.getTable
    val schema = parseSchema(tbl.getColumns)
    val data = parseData(tbl.getRows, schema)
      .map(arr => Row.fromSeq(arr)).toList.asJava
    val df = getSparkSession.createDataFrame(data, schema)
    tbl.getName -> df
  }

  def parseSchema(columns: java.util.List[String]): StructType = {

    val parts = columns.toArray.map(_.toString()).map(_.trim).filter(_ != "").map(_.reverse.replaceFirst(" ", ":").reverse.split(":"))
    parseSchema(parts)
  }


  def parseSchema(parts: Array[Array[String]]): StructType = {
    val fields = parts.map(part => {
      val colName = part(0)
      val typeName = part(1)
      val dataType = typeName match {
        case "i16" => ShortType
        case "int16" => ShortType
        case "smallint" => ShortType
        case "int" => IntegerType
        case "i32" => IntegerType
        case "int32" => IntegerType
        case "i64" => LongType
        case "long" => LongType
        case "bigint" => LongType
        case "int64" => LongType
        case "float" => FloatType
        case "double" => DoubleType
        case "string" => StringType
        case "timestamp" => TimestampType
        case "date" => DateType
        case "bool" => BooleanType
        case _ => throw new IllegalArgumentException(
          s"Unknown type name $typeName")
      }
      StructField(colName, dataType)
    })
    StructType(fields)
  }

  def parseData(rows: java.util.List[java.util.List[Object]], schema: StructType): Array[Array[Any]] = {

    val data = rows.asScala.map(_.asScala.map(x => if (null == x) "null" else x.toString).toArray).toArray
    parseData(data, schema)
  }

  def parseData(rows: Array[Array[String]], schema: StructType): Array[Array[Any]] = {

    rows.flatMap(parts => {
      if (parts.length != schema.size) {
        logger.error(s"Broken line: $parts")
        None
      } else {
        Some(schema.zip(parts).map {
          case (field, str) =>
            if (str == "NULL" || str == "null") {
              null
            } else {
              field.dataType match {
                case ByteType => str.trim.toByte
                case ShortType => str.trim.toShort
                case IntegerType => str.trim.toInt
                case LongType => str.trim.toLong
                case FloatType => toFloat(str)
                case DoubleType => toDouble(str)
                case StringType => str
                case TimestampType => new Timestamp(str.trim.toLong)
                case DateType =>
                  new Date(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(str.trim + " 00:00:00").getTime)
                case BooleanType => str.trim.toBoolean
                case _ => throw new IllegalArgumentException(
                  s"Unknown type ${field.dataType}")
              }
            }
        }.toArray)
      }
    })
  }

  def loadYaml[T: ClassTag](path: String): T = {
    val yaml = new Yaml(new Constructor(implicitly[ClassTag[T]].runtimeClass))
    val absPath = if (path.startsWith("/")) path else rootDir.getAbsolutePath + "/" + path
    val is = new FileInputStream(absPath)
    try {
      yaml.load(is).asInstanceOf[T]
    } finally {
      is.close()
    }
  }
}


object SQLBaseSuite {

  private val testNameCounter = mutable.HashMap[String, Int]()

  def getTestName(sqlCase: SQLCase): String = {
    this.synchronized {
      val prefix = sqlCase.getId + "_" + sqlCase.getDesc
      testNameCounter.get(prefix) match {
        case Some(idx) =>
          val res = prefix + "-" + idx
          testNameCounter += prefix -> (idx + 1)
          res

        case None =>
          testNameCounter += prefix -> 1
          prefix
      }
    }
  }
}