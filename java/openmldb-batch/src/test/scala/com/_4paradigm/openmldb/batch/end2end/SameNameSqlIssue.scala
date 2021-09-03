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
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}


class SameNameSqlIssue extends SparkTestSuite {

  test("Test run same SQL in same process") {

    val sqlText = "SELECT id, user, trans_amount, trans_time + 1 as trans_time_one FROM t1"

    // Run the first SQL
    var spark = SparkSession.builder().master("local[*]").getOrCreate()
    var sess = new OpenmldbSession(spark)

    var data = Seq(
      Row(10, 222, 1000, 110))
    var schema = StructType(List(
      StructField("id", IntegerType),
      StructField("user", IntegerType),
      StructField("trans_amount", IntegerType),
      StructField("trans_time", IntegerType)))
    var df = spark.createDataFrame(spark.sparkContext.makeRDD(data), schema)

    df.createOrReplaceTempView("t1")
    sess.registerTable("t1", df)

    assert(SparkUtil.approximateDfEqual(spark.sql(sqlText), sess.sql(sqlText).getSparkDf(), true))
    spark.close()

    // Run the same SQL but different schema
    spark = SparkSession.builder().master("local[*]").getOrCreate()
    sess = new OpenmldbSession(spark)

    data = Seq(
      Row(10, "abc", 100, 200))
    schema = StructType(List(
      StructField("id", IntegerType),
      StructField("user", StringType),
      StructField("trans_amount", IntegerType),
      StructField("trans_time", IntegerType)))
    df = spark.createDataFrame(spark.sparkContext.makeRDD(data), schema)

    df.createOrReplaceTempView("t1")
    sess.registerTable("t1", df)

    assert(SparkUtil.approximateDfEqual(spark.sql(sqlText), sess.sql(sqlText).getSparkDf(), true))
    spark.close()

  }

}
