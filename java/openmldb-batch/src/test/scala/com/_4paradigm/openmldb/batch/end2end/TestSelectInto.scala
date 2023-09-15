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
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}


class TestSelectInto extends SparkTestSuite {

  test("Test end2end select into") {
    val spark = getSparkSession
    val sess = new OpenmldbSession(spark)

    val csvFilePath = "file:///tmp/openmldb_output/"
    val sqlText = s"SELECT 1 INTO OUTFILE '$csvFilePath' OPTIONS (mode='overwrite')"
    sess.sql(sqlText)

    val csvDf = spark.read.option("header", true).csv(csvFilePath)
    assert(csvDf.isEmpty)
    assert(csvDf.schema.size == 1)
    assert(csvDf.schema.fields(0).name.equals("1"))
  }

}
