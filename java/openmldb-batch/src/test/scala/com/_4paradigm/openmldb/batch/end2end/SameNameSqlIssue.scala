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

  test("Test end2end window aggregation") {

    val sqlText = "SELECT id, user, trans_amount, trans_time + 1 FROM t1"

    var spark = SparkSession.builder().master("local[*]").config("openmldb.dbName", "db1").getOrCreate()
    var sess = new OpenmldbSession(spark)

    var data = Seq(
      Row(10, 222, 1000, 110))
    var schema = StructType(List(
      StructField("id", IntegerType),
      StructField("user", IntegerType),
      StructField("trans_amount", IntegerType),
      StructField("trans_time", IntegerType)))
    var df = spark.createDataFrame(spark.sparkContext.makeRDD(data), schema)

    var expect = Seq(
      Row(10, 222, 1000, 111))
    var expectSchema = StructType(List(
      StructField("id", IntegerType),
      StructField("user", IntegerType),
      StructField("trans_amount", IntegerType),
      StructField("trans_time + 1", IntegerType)))
    var expectDf = spark.createDataFrame(spark.sparkContext.makeRDD(expect), expectSchema)

    sess.registerTable("t1", df)

    var res = sess.sql(sqlText)
    assert(SparkUtil.approximateDfEqual(expectDf, res.getSparkDf(), true))
    spark.close()


    spark = SparkSession.builder().master("local[*]").config("openmldb.dbName", "db2").getOrCreate()
    sess = new OpenmldbSession(spark)

    data = Seq(
      Row(10, "weramy234124", 122000, 123423410))
    schema = StructType(List(
      StructField("id", IntegerType),
      StructField("user", StringType),
      StructField("trans_amount", IntegerType),
      StructField("trans_time", IntegerType)))
    df = spark.createDataFrame(spark.sparkContext.makeRDD(data), schema)

    expect = Seq(
      Row(10, "weramy234124", 122000, 123423411))
    expectSchema = StructType(List(
      StructField("id", IntegerType),
      StructField("user", StringType),
      StructField("trans_amount", IntegerType),
      StructField("trans_time + 1", IntegerType)))
    expectDf = spark.createDataFrame(spark.sparkContext.makeRDD(expect), expectSchema)

    sess.registerTable("t1", df)
    res = sess.sql(sqlText)
    res.show();
    assert(SparkUtil.approximateDfEqual(expectDf, res.getSparkDf(), true))
    spark.close()

  }

}
