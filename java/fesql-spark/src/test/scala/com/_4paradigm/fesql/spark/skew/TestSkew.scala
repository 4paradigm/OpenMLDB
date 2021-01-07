package com._4paradigm.fesql.spark.skew

import com._4paradigm.fesql.spark.{Fesql, SparkTestSuite}

class TestSkew extends SparkTestSuite {
  test("normal skew test") {
    //    val args = Array[String]("/home/wangzixian/ferrari/idea/docker-code/fesql/java/fesql-spark/src/test/resources/fz/luoji/luoji.json")
    //    Fesql.run(args)

    //    val args = Array[String]("/home/wangzixian/ferrari/idea/docker-code/FEX-979-data-skew/fesql/java/fesql-spark/src/test/resources/debug/skew/data.json")
    //    Fesql.run(args)


    val args = Array[String]("/Users/chenjing/work/fedb/rtidb/fesql/java/fesql-spark/src/test/resources/fz/fengdian/spark.json")
    Fesql.run(args);
  }
}