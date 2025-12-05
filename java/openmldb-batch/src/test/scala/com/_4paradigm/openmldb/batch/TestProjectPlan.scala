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

import com._4paradigm.openmldb.batch.utils.SparkUtil
import com._4paradigm.openmldb.batch.api.OpenmldbSession
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, StructField, StructType}

import java.sql.Date
import scala.collection.JavaConverters.seqAsJavaListConverter
import scala.collection.mutable



class TestProjectPlan extends SparkTestSuite {

  test("Test groupBy and limit") {
    val sess = getSparkSession

    val schema = StructType(Seq(
      StructField("id", IntegerType),
      StructField("time2", IntegerType)
    ))

    val t1 = sess.createDataFrame(Seq(
      (100, 1),
      (3, 3),
      (2, 10),
      (0, 2),
      (2, 13)
    ).map(Row.fromTuple(_)).asJava, schema)

    val planner = new SparkPlanner(sess)

    val res = planner.plan("select id + 1, id + 2 from t1;", Map("t1" -> t1))
    val output = res.getDf()
    output.show()
  }

  test("Test const project") {
    val sess = new OpenmldbSession(getSparkSession)
    val sql =  "select 1, 3.5, 'a', date('2024-03-25'), string(int(bigint(1)));"
    val res = sess.sql(sql)
    res.show()
    val sparkDf = sess.sparksql(sql)

    assert(SparkUtil.approximateDfEqual(sparkDf, res.getSparkDf(), false))
  }
}
