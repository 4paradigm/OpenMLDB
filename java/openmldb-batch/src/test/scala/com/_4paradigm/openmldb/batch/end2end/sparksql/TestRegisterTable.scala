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

package com._4paradigm.openmldb.batch.end2end.sparksql

import com._4paradigm.openmldb.batch.SparkTestSuite
import com._4paradigm.openmldb.batch.api.OpenmldbSession
import com._4paradigm.openmldb.batch.end2end.DataUtil
import com._4paradigm.openmldb.batch.utils.SparkUtil

class TestRegisterTable extends SparkTestSuite {

  test("Test register table for OpenMLDB session and Spark catalog") {
    val spark = getSparkSession
    val sess = new OpenmldbSession(spark)

    val df = DataUtil.getTestDf(spark)
    sess.registerTable("t1", df)

    val sqlText = "select * from t1"
    val sparksqlOutputDf = spark.sql(sqlText)
    val outputDf = sess.sql(sqlText)
    assert(SparkUtil.approximateDfEqual(outputDf.getSparkDf(), sparksqlOutputDf, false))
  }

}
