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
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}


class TestUnsafeRowRowProject extends SparkTestSuite {

  test("TestUnsafeRowWindowProject") {

    val spark = SparkSession
      .builder
      .master("local")
      .appName("SparkApp")
      .config("openmldb.enable.unsaferow.optimization", true)
      .getOrCreate()
    val sc = spark.sparkContext

    val sess = new OpenmldbSession(spark)

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
