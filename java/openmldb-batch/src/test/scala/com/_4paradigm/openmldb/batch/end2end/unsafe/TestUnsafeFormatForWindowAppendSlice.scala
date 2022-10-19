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
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}
import java.sql.Timestamp

class TestUnsafeFormatForWindowAppendSlice extends UnsaferowoptSparkTestSuite {

  test("Test unsafe row format for window over window(window append slice)") {
    val spark = getSparkSession
    val sess = new OpenmldbSession(spark)

    val data = Seq(
      Row("a", "4817", "583e2", "14447111", new Timestamp(1480464000000L), null),
      Row("b", "3425", "5233a", "24142424", null, new Timestamp(1990464000000L)),
      Row("b", "3425", "5233a", "24142424", null, null)
    )
    val schema = StructType(List(
      StructField("reqId", StringType),
      StructField("f_cId", StringType),
      StructField("f_requestId", StringType),
      StructField("f_uId", StringType),
      StructField("eventTime", TimestampType),
      StructField("f_cPubTime", TimestampType)))
    val df = spark.createDataFrame(spark.sparkContext.makeRDD(data), schema)

    sess.registerTable("t1", df)
    df.createOrReplaceTempView("t1")

    val sqlText = """
        |select
        |    count(`f_cId`) over w1,
        |    count(`f_cId`) over w2
        |from
        |    `t1`
        |window w1 as (partition by `f_requestId` order by `eventTime` rows between 6048 preceding and CURRENT ROW),
        |       w2 as (partition by `f_uId` order by `eventTime` rows between 6048 preceding and CURRENT ROW)
        |
        |""".stripMargin

    // val outputDf = sess.sql(sqlText)
    // val sparksqlOutputDf = sess.sparksql(sqlText)
    // assert(SparkUtil.approximateDfEqual(outputDf.getSparkDf(), sparksqlOutputDf, false))
  }

}
