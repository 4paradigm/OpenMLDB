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

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StructField, StructType}

import scala.collection.JavaConverters.seqAsJavaListConverter


class TestLimitPlan extends SparkTestSuite {

  test("Test groupBy and limit") {
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

    val res = planner.plan("select id, max(time2), min(amt) from t1 group by id limit 2;", Map("t1" -> t1))
    val output = res.getDf()
    assert(output.count()==2);

    val res2 = planner.plan("select id, max(time2), min(amt) from t1 group by id limit 1;", Map("t1" -> t1))
    val output2 = res2.getDf()
    assert(output2.count()==1);

    val res3 = planner.plan("select id, max(time2), min(amt) from t1 group by id limit 0;", Map("t1" -> t1))
    val output3 = res3.getDf()
    assert(output3.count()==0);
  }

  test("Test project and limit") {
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

    val res = planner.plan("select id, time2 + 10 from t1 limit 2;", Map("t1" -> t1))
    val output = res.getDf()
    assert(output.count()==2);

    val res2 = planner.plan("select id, time2 + 10 from t1 limit 1;", Map("t1" -> t1))
    val output2 = res2.getDf()
    assert(output2.count()==1);

    val res3 = planner.plan("select id, time2 + 10 from t1 limit 0;", Map("t1" -> t1))
    val output3 = res3.getDf()
    assert(output3.count()==0);
  }

  test("Test simple project and limit") {
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

    val res = planner.plan("select id, time2 from t1 limit 2;", Map("t1" -> t1))
    val output = res.getDf()
    assert(output.count()==2);

    val res2 = planner.plan("select id, time2 from t1 limit 1;", Map("t1" -> t1))
    val output2 = res2.getDf()
    assert(output2.count()==1);

    val res3 = planner.plan("select id, time2 from t1 limit 0;", Map("t1" -> t1))
    val output3 = res3.getDf()
    assert(output3.count()==0);
  }

}
