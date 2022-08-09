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
import com._4paradigm.openmldb.batch.utils.SparkUtil
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

class TestWindowWithoutSelect extends UnsaferowoptSparkTestSuite {

  test("Test window without select") {
    val spark = getSparkSession
    val sess = new OpenmldbSession(spark)

    val data = Seq(
      Row(1, 2, 3)
    )
    val schema = StructType(List(
      StructField("col1", IntegerType),
      StructField("col2", IntegerType),
      StructField("col3", IntegerType)
      ))
    val t1 = spark.createDataFrame(spark.sparkContext.makeRDD(data), schema)

    sess.registerTable("t1", t1)
    t1.createOrReplaceTempView("t1")

    val sqlText =
      """      select
        |        sum(col1) over w1 as w1_count_col1,
        |        sum(col3) over w2 as w2_sum_col3
        |      from t1
        |      window
        |        w1 as (PARTITION BY col1 ORDER BY col2 ROWS BETWEEN 10 PRECEDING AND CURRENT ROW),
        |        w2 as (PARTITION BY col2 ORDER BY col2 ROWS BETWEEN 10 PRECEDING AND CURRENT ROW)
        |""".stripMargin

    val outputDf = sess.sql(sqlText)
    val sparksqlOutputDf = sess.sparksql(sqlText)
    assert(SparkUtil.approximateDfEqual(outputDf.getSparkDf(), sparksqlOutputDf, false))
  }

}
