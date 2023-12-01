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

package com._4paradigm.openmldb.batch

import com._4paradigm.openmldb.batch.api.OpenmldbSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{
  IntegerType,
  StringType,
  StructField,
  StructType
}
import com._4paradigm.openmldb.batch.utils.SparkUtil

class TestSetOperation extends SparkTestSuite {

  test("Test UNION ALL") {
    val spark = getSparkSession
    val sess = new OpenmldbSession(spark)

    val schema = StructType(
      List(StructField("id", IntegerType), StructField("user", StringType))
    )
    val data1 = Seq(Row(1, "tom"), Row(2, "amy"))
    val df1 = spark.createDataFrame(spark.sparkContext.makeRDD(data1), schema)
    val data2 = Seq(Row(1, "tom"))
    val df2 = spark.createDataFrame(spark.sparkContext.makeRDD(data2), schema)

    sess.registerTable("t1", df1)
    sess.registerTable("t2", df2)
    df1.createOrReplaceTempView("t1")
    df2.createOrReplaceTempView("t2")

    val sqlText = "SELECT * FROM t1 UNION ALL SELECT * FROM t2"

    val outputDf = sess.sql(sqlText)
    outputDf.show()
    val sparksqlOutputDf = sess.sparksql(sqlText)
    sparksqlOutputDf.show()
    assert(outputDf.getSparkDf().count() == 3)
    assert(
      SparkUtil.approximateDfEqual(
        outputDf.getSparkDf(),
        sparksqlOutputDf,
        true
      )
    )
  }

  test("Test UNION DISTINCT") {
    val spark = getSparkSession
    val sess = new OpenmldbSession(spark)

    val schema = StructType(
      List(StructField("id", IntegerType), StructField("user", StringType))
    )
    val data1 = Seq(Row(1, "tom"), Row(2, "amy"))
    val df1 = spark.createDataFrame(spark.sparkContext.makeRDD(data1), schema)
    val data2 = Seq(Row(1, "tom"))
    val df2 = spark.createDataFrame(spark.sparkContext.makeRDD(data2), schema)

    sess.registerTable("t1", df1)
    sess.registerTable("t2", df2)
    df1.createOrReplaceTempView("t1")
    df2.createOrReplaceTempView("t2")

    val sqlText = "SELECT * FROM t1 UNION DISTINCT SELECT * FROM t2"

    val outputDf = sess.sql(sqlText)
    outputDf.show()
    val sparksqlOutputDf = sess.sparksql(sqlText)
    sparksqlOutputDf.show()
    assert(outputDf.getSparkDf().count() == 2)
    assert(
      SparkUtil.approximateDfEqual(
        outputDf.getSparkDf(),
        sparksqlOutputDf,
        true
      )
    )
  }

}
