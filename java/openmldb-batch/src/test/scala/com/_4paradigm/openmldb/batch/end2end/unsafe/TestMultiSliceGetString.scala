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
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.Row

class TestMultiSliceGetString extends UnsaferowoptSparkTestSuite {

  test("Test window over window and get string") {
    val spark = getSparkSession
    val sess = new OpenmldbSession(spark)

    val data = Seq[Row](
      Row(null, null, null, null, null, null, null, null, null, null, null, null)
    )
    val schema = StructType(List(
      StructField("eventTime", TimestampType),
      StructField("latitude", DoubleType),
      StructField("description", StringType),
      StructField("source", StringType),
      StructField("tag_type", StringType)
    ))
    val df = spark.createDataFrame(spark.sparkContext.makeRDD(data), schema)

    sess.registerTable("t1", df)

    val sqlText =
      """
        |select
        |    at(tag_type, 0) over w1 as w1_case,
        |    count(`latitude`) over w2 as w2_sum
        |from
        |    t1
        |window
        |    w1 as (partition by source order by eventTime rows between 9 preceding and 0 preceding),
        |    w2 as (partition by description order by eventTime rows between 9 preceding and 0 preceding)
        |""".stripMargin

    val outputDf = sess.sql(sqlText)
    assert(outputDf.count() == 1)
  }

}
