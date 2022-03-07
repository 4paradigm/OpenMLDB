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

package com._4paradigm.openmldb.batch.end2end.unsafe

import com._4paradigm.openmldb.batch.SparkTestSuite
import com._4paradigm.openmldb.batch.api.OpenmldbSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{BooleanType, DateType, DoubleType, FloatType, IntegerType, LongType, ShortType,
  StringType, StructField, StructType, TimestampType}
import java.sql.Timestamp

class TestWindowWithoutSelect extends SparkTestSuite {

  override def customizedBefore(): Unit = {
    val spark = getSparkSession
    spark.conf.set("spark.openmldb.unsaferow.opt", true)
    //spark.conf.set("spark.openmldb.opt.unsaferow.window", true)
  }

  test("Test subquery") {
    val spark = getSparkSession
    val sess = new OpenmldbSession(spark)

    val data = Seq(
      Row(1, "aaaaaaaaaa",1, new Timestamp(1590738989L), 1.1f)
    )
    val schema = StructType(List(
      StructField("id", IntegerType),
      StructField("card_no", StringType),
      StructField("merchant_id", IntegerType),
      StructField("trx_time", TimestampType),
      StructField("trx_amt", FloatType)
      ))
    val t1 = spark.createDataFrame(spark.sparkContext.makeRDD(data), schema)
    
    val data2 = Seq(
      Row(new Timestamp(1590738988L), "aaaaaaaaaa")
    )
    val schema2 = StructType(List(
      StructField("crd_lst_isu_dte", TimestampType),
      StructField("crd_nbr", StringType)
    ))
    val t2 = spark.createDataFrame(spark.sparkContext.makeRDD(data2), schema2)

    sess.registerTable("t1", t1)
    sess.registerTable("t2", t2)


    val sqlText =
      """      select
        |      sum(trx_amt) over w30d as w30d_amt_sum,
        |      count(id) over w10d as w10d_id_cnt
        |      from t1
        |      window
        |        w30d as (PARTITION BY card_no ORDER BY trx_time ROWS_RANGE BETWEEN 30d PRECEDING AND CURRENT ROW),
        |        w10d as (PARTITION BY id ORDER BY trx_time ROWS_RANGE BETWEEN 10d PRECEDING AND CURRENT ROW);
        |""".stripMargin

    val outputDf = sess.sql(sqlText)
    outputDf.show()

  }

  override def customizedAfter(): Unit = {
    val spark = getSparkSession
    spark.conf.set("spark.openmldb.unsaferow.opt", false)
    spark.conf.set("spark.openmldb.opt.unsaferow.project", false)
  }

}
