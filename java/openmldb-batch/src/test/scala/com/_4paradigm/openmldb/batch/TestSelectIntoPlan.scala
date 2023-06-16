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

import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StructField, StructType}
import org.apache.spark.sql.{AnalysisException, Row}

import scala.collection.JavaConverters.seqAsJavaListConverter

class TestSelectIntoPlan extends SparkTestSuite {
  test("Test Plan Select Into") {
    val sess = getSparkSession

    val schema = StructType(Seq(
      StructField("id", IntegerType),
      StructField("time2", LongType),
      StructField("amt", DoubleType)
    ))

    val t1 = sess.createDataFrame(Seq(
      (0, 1L, 1.0),
      (3, 3L, 3.0),
      (2, 10L, 4.0),
      (0, 2L, 2.0),
      (2, 13L, 3.0)
    ).map(Row.fromTuple(_)).asJava, schema)

    val planner = new SparkPlanner(sess)
    val filePath = "file:///tmp/select_into_test"
    val res = planner.plan(s"select id from t1 into outfile '$filePath' " +
      "options(format='csv', foo='bar', mode='overwrite');", Map("t1" -> t1))
    res.getDf().show()

    // read data is disordered, so only check the count
    // we saved with header==true, so we should read with header.
    val df = getSparkSession.read.option("header", "true").schema("id int").csv(filePath)
    df.show()
    assert(df.count() == t1.count())

    try {
      // writing in default mode 'errorifexsits' will get exception, cuz filePath contains data
      planner.plan(s"select id from t1 into outfile '$filePath' " +
        "options(format='csv', foo='bar');", Map("t1" -> t1))
    } catch {
      case e: AnalysisException => println("It should catch this: " + e.toString)
    }

    // only save one file by coalesce option
    planner.plan(s"select id from t1 into outfile '$filePath' " +
      "options(format='csv', foo='bar', coalesce=1, mode='overwrite');", Map("t1" -> t1))
  }
}
