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

package com._4paradigm.openmldb.batch.end2end

import com._4paradigm.openmldb.batch.SparkTestSuite
import com._4paradigm.openmldb.batch.api.OpenmldbSession
import com._4paradigm.openmldb.batch.utils.SparkUtil
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType, MapType}


class TestProject extends SparkTestSuite {

  test("Test end2end window aggregation") {

    val spark = getSparkSession
    val sess = new OpenmldbSession(spark)

    val df = DataUtil.getTestDf(spark)

    sess.registerTable("t1", df)
    df.createOrReplaceTempView("t1")

    val sql = "SELECT id + 10 AS id_10, name FROM t1"

    val outputDf = sess.sql(sql)
    val sparkOutputDf = sess.sparksql(sql)
    assert(SparkUtil.approximateDfEqual(outputDf.getSparkDf(), sparkOutputDf, false))

  }

  test("Test end2end row project with map values") {
    val spark = getSparkSession
    val sess = new OpenmldbSession(spark)

    val data = Seq(
      Row(1, Map.apply(1 -> "11", 12 -> "99")),
      Row(2, Map.apply(13 -> "99")))
      // Row(2, Map.empty[Int, String]))
    val schema = StructType(List(
      StructField("id", IntegerType),
      StructField("val", MapType(IntegerType, StringType))))
    val df = spark.createDataFrame(spark.sparkContext.makeRDD(data), schema)

    sess.registerTable("t1", df)
    df.createOrReplaceTempView("t1")

    val sqlText = "select id, val[12] as ele from t1"
    val outputDf = sess.sql(sqlText)
    outputDf.show()

    val sparksqlOutputDf = sess.sparksql(sqlText)
    assert(SparkUtil.approximateDfEqual(outputDf.getSparkDf(), sparksqlOutputDf, false))
  }
}
