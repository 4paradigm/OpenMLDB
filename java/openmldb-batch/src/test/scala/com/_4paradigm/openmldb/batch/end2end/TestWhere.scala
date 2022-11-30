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

package com._4paradigm.openmldb.batch.end2end

import com._4paradigm.openmldb.batch.SparkTestSuite
import com._4paradigm.openmldb.batch.api.OpenmldbSession
import com._4paradigm.openmldb.batch.utils.SparkUtil

class TestWhere extends SparkTestSuite {

  test("Test end2end where expression") {
    val spark = getSparkSession
    val sess = new OpenmldbSession(spark)

    val df = DataUtil.getTestDf(spark)

    sess.registerTable("t1", df)
    df.createOrReplaceTempView("t1")

    val sql = "SELECT id + 10 AS id_10, name FROM t1 WHERE id + 5 > trans_time + 4"

    val outputDf = sess.sql(sql)
    val sparkOutputDf = sess.sparksql(sql)
    assert(SparkUtil.approximateDfEqual(outputDf.getSparkDf(), sparkOutputDf, false))
  }

}
