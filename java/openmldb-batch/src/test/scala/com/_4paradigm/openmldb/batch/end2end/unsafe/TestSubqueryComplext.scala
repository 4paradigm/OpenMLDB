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

import com._4paradigm.openmldb.batch.UnsaferowoptSparkTestSuite
import com._4paradigm.openmldb.batch.api.OpenmldbSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{FloatType, IntegerType, StringType, StructField, StructType, TimestampType}
import java.sql.Timestamp

class TestSubqueryComplext extends UnsaferowoptSparkTestSuite {

  test("Test subquery") {
    val spark = getSparkSession
    val sess = new OpenmldbSession(spark)

    val data = Seq(
      Row(1, "aaaaaaaaaa",1, new Timestamp(1590738989000L), 1.1f),
      Row(2, "aaaaaaaaaa",1, new Timestamp(1590738991000L), 2.2f),
      Row(3, "bb", 10, new Timestamp(1590738990000L), 3.3f)
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
      Row(new Timestamp(1590738988000L), "aaaaaaaaaa"),
      Row(new Timestamp(1590738990000L), "aaaaaaaaaa"),
      Row(new Timestamp(1590738989000L), "cc"),
      Row(new Timestamp(1590738992000L), "cc")
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
        |      id,
        |      card_no,
        |      trx_time,
        |      substr(card_no, 1, 6) as card_no_prefix,
        |      sum(trx_amt) over w30d as w30d_amt_sum,
        |      count(id) over w10d as w10d_id_cnt
        |      from  (select id, card_no, trx_time, trx_amt from t1) as t_instance
        |      window w30d as (PARTITION BY card_no ORDER BY trx_time ROWS_RANGE BETWEEN 30d PRECEDING AND CURRENT ROW),
        |      w10d as (UNION (select 0 as id, crd_nbr as card_no, crd_lst_isu_dte as trx_time, 0.0f
        |      as trx_amt from t2) PARTITION BY card_no ORDER BY trx_time ROWS_RANGE BETWEEN 10d PRECEDING
        |      AND CURRENT ROW);
        |""".stripMargin

    val outputDf = sess.sql(sqlText)
    assert(outputDf.count() == 3)
  }

}
