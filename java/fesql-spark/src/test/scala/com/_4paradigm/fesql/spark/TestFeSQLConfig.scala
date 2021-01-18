package com._4paradigm.fesql.spark

import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

class TestFeSQLConfig extends FunSuite {

  test("test make config from spark") {
    val sess = SparkSession.builder()
        .config("spark.fesql.group.partitions", 100)
        .config("spark.fesql.test.print", value=true)
        .master("local[1]")
        .getOrCreate()
    val config = FeSQLConfig.fromSparkSession(sess)
    assert(config.groupPartitions == 100)
    assert(config.print)
    sess.close()
  }

  test("test make config from dict") {
    val dict = Map(
      "fesql.group.partitions" -> 100,
      "fesql.test.print" -> true
    )
    val config = FeSQLConfig.fromDict(dict)
    assert(config.groupPartitions == 100)
    assert(config.print)
  }

}
