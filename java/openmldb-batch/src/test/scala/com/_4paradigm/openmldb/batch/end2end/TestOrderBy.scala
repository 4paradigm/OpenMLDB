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

import com._4paradigm.openmldb.batch.api.OpenmldbSession
import com._4paradigm.openmldb.batch.utils.SparkUtil.smallDfEqual
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.FunSuite;

class TestOrderBy extends FunSuite {

  test("Test end2end Order by") {

    val spark = SparkSession.builder().master("local[*]")
      .getOrCreate()
    val sess = new OpenmldbSession(spark)

    val data = Seq(
      Row(5, 111, 500, 5),
      Row(6, 222, 600, 6),
      Row(8, 222, 800, 8),
      Row(9, 111, 900, 9),
      Row(1, 111, 100, 1),
      Row(2, 222, 200, 2),
      Row(3, 111, 300, 3),
      Row(4, 222, 400, 4),
      Row(7, 111, 700, 7),
      Row(10, 222, 1000, 10))
    val schema = StructType(List(
      StructField("id", IntegerType),
      StructField("user", IntegerType),
      StructField("trans_amount", IntegerType),
      StructField("trans_time", IntegerType)))
    val df = spark.createDataFrame(spark.sparkContext.makeRDD(data), schema)

    sess.registerTable("t1", df)
    df.createOrReplaceTempView("t1")

    val sqlText1 =
      """
        | SELECT * FROM t1 ORDER BY trans_amount
     """.stripMargin

    val outputDfAsc = sess.sql(sqlText1).getSparkDf()
    val sparksqlOutputDfAsc = sess.sparksql(sqlText1)

    assert(smallDfEqual(outputDfAsc,sparksqlOutputDfAsc))

    val sqlText2 =
      """
        | SELECT * FROM t1 ORDER BY trans_amount DESC
     """.stripMargin

    val outputDfDesc = sess.sql(sqlText2).getSparkDf()
    val sparksqlOutputDfDesc = sess.sparksql(sqlText2)

    assert(smallDfEqual(outputDfDesc,sparksqlOutputDfDesc))
  }

}
