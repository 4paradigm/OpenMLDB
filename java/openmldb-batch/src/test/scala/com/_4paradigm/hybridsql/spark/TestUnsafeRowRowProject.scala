package com._4paradigm.hybridsql.spark

import com._4paradigm.hybridsql.spark.api.SparkFeSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}


class TestUnsafeRowRowProject extends SparkTestSuite {

  test("TestUnsafeRowWindowProject") {

    val spark = SparkSession
      .builder
      .master("local")
      .appName("SparkApp")
      .config("sparkfe.enable.unsaferow.optimization", true)
      .getOrCreate()
    val sc = spark.sparkContext

    val sess = new SparkFeSession(spark)

    val data = Seq(
      Row(10, 112233),
      Row(20, 223311),
      Row(30, 331122))

    val schema = StructType(List(
      StructField("id", IntegerType),
      StructField("age", IntegerType)))

    val df = spark.createDataFrame(sc.makeRDD(data), schema)
    sess.registerTable("t1", df)

    val sql = "SELECT id + 10, age - 10 FROM t1"

    val outputDf = sess.sql(sql)
    outputDf.show()

    spark.stop()
  }

}
