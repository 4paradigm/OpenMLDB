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

package com._4paradigm.openmldb.batch.end2end.simple_project

import com._4paradigm.openmldb.batch.SparkTestSuite
import com._4paradigm.openmldb.batch.api.OpenmldbSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import java.sql.Timestamp

class TestStringToTimestamp extends SparkTestSuite {

  test("Test string to timestamp") {
    val spark = getSparkSession
    val sess = new OpenmldbSession(spark)

    val data = Seq(
      Row("2022-01-22"),
      Row("2022-01-23 22:22:22"),
      Row("20220124"),
      Row(null))
    val schema = StructType(List(
      StructField("str_timestamp", StringType)))
    val df = spark.createDataFrame(spark.sparkContext.makeRDD(data), schema)

    sess.registerTable("t1", df)

    val sql = "SELECT cast(str_timestamp as timestamp) from t1"
    val outputDf = sess.sql(sql)

    val rows = outputDf.collect()
    assert(Timestamp.valueOf("2022-01-22 0:0:0") == rows(0).getTimestamp(0))
    assert(Timestamp.valueOf("2022-01-23 22:22:22") == rows(1).getTimestamp(0))
    assert(Timestamp.valueOf("2022-01-24 0:0:0") == rows(2).getTimestamp(0))
    assert(null == rows(3).getTimestamp(0))
  }

}
