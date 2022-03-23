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
import com._4paradigm.openmldb.batch.end2end.DataUtil
import com._4paradigm.openmldb.batch.utils.SparkUtil

class TestUnsafeJoin extends SparkTestSuite {

  override def customizedBefore(): Unit = {
    val spark = getSparkSession
    spark.conf.set("spark.openmldb.unsaferow.opt", true)
  }

  def testSql(sqlText: String) {
    val spark = getSparkSession
    val sess = new OpenmldbSession(spark)

    val t1 = DataUtil.getTestDf(spark)
    val t2 = DataUtil.getTestDf(spark)
    sess.registerTable("t1", t1)
    sess.registerTable("t2", t2)
    t1.createOrReplaceTempView("t1")
    t2.createOrReplaceTempView("t2")

    val outputDf = sess.sql(sqlText)
    val sparksqlOutputDf = sess.sparksql(sqlText)
    assert(SparkUtil.approximateDfEqual(outputDf.getSparkDf(), sparksqlOutputDf, false))
  }

  test("Test unsafe join") {
    // Test different join condictions
    testSql("SELECT t1.id as t1_id, t2.id as t2_id, t1.name FROM t1 LEFT JOIN t2 ON t1.id = t2.id")
    testSql("SELECT t1.id as t1_id, t2.id as t2_id, t1.name FROM t1 LEFT JOIN t2 ON t1.id != t2.id")
    testSql("SELECT t1.id as t1_id, t2.id as t2_id, t1.name FROM t1 LEFT JOIN t2 ON t1.id > t2.id")
    testSql("SELECT t1.id as t1_id, t2.id as t2_id, t1.name FROM t1 LEFT JOIN t2 ON t1.id >= t2.id")
    testSql("SELECT t1.id as t1_id, t2.id as t2_id, t1.name FROM t1 LEFT JOIN t2 ON t1.id < t2.id")
    testSql("SELECT t1.id as t1_id, t2.id as t2_id, t1.name FROM t1 LEFT JOIN t2 ON t1.id <= t2.id")

    // Test join with constant values
    testSql("SELECT t1.id as t1_id, t2.id as t2_id, t1.name FROM t1 LEFT JOIN t2 ON t1.id = 1")
    testSql("SELECT t1.id as t1_id, t2.id as t2_id, t1.name FROM t1 LEFT JOIN t2 ON t1.id != 1")
    testSql("SELECT t1.id as t1_id, t2.id as t2_id, t1.name FROM t1 LEFT JOIN t2 ON t1.id > 1")
    testSql("SELECT t1.id as t1_id, t2.id as t2_id, t1.name FROM t1 LEFT JOIN t2 ON t1.id >= 1")
    testSql("SELECT t1.id as t1_id, t2.id as t2_id, t1.name FROM t1 LEFT JOIN t2 ON 1 > t1.id")
    testSql("SELECT t1.id as t1_id, t2.id as t2_id, t1.name FROM t1 LEFT JOIN t2 ON t1.id <= 1.0")

    // Test with multiple conditions
    testSql("SELECT t1.id as t1_id, t2.id as t2_id FROM t1 LEFT JOIN t2 ON t1.id = t2.id and 100 > t2.id")
    testSql("SELECT t1.id as t1_id, t2.id as t2_id, t1.name FROM t1 LEFT JOIN t2" +
      " ON t1.id >= 1 and t2.id > 1")
    testSql("SELECT t1.id as t1_id, t2.id as t2_id, t1.name FROM t1 LEFT JOIN t2" +
      " ON t1.id >= 1 or t2.id > 1")
    testSql("SELECT t1.id as t1_id, t2.id as t2_id, t1.name FROM t1 LEFT JOIN t2" +
      " ON t1.id >= 1 or t2.id > 1 and t1.id = t2.id or t1.id < 10 and t2.id > 0.1 or t2.id = 2")
  }

  override def customizedAfter(): Unit = {
    val spark = getSparkSession
    spark.conf.set("spark.openmldb.unsaferow.opt", false)
  }

}
