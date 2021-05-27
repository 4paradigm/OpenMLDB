package com._4paradigm.hybridsql.spark

import com._4paradigm.hybridsql.spark.api.SparkFeSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}


class TestUnsafeRowWindowProject extends SparkTestSuite {

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

    val sql = "SELECT min(id) OVER w1 as min_age FROM t1 WINDOW w1 as (PARTITION BY age ORDER by age ROWS BETWEEN 10 PRECEDING AND CURRENT ROW)"

    val outputDf = sess.sql(sql)
    outputDf.show()

    spark.stop()
  }

}
