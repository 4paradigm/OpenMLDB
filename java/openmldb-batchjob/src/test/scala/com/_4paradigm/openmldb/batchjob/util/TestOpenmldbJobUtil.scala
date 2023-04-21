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

package com._4paradigm.openmldb.batchjob.util

import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite


class TestOpenmldbJobUtil extends FunSuite {

  test("Test getSqlFromFile") {
    val spark = SparkSession.builder().master("local").getOrCreate()
    val sql = "SELECT col1 FROM db1.t1"

    val outputSql = OpenmldbJobUtil.getSqlFromFile(spark, sql)
    assert(outputSql.equals(sql))
  }

  test("Test getSqlFromFile for sql file") {
    val spark = SparkSession.builder().master("local").getOrCreate()

    val sqlFilePath = getClass.getClassLoader.getResource("sql-test.sql").getPath
    val outputSql = OpenmldbJobUtil.getSqlFromFile(spark, sqlFilePath)

    val expectedSql = "SELECT 100"
    assert(outputSql.equals(expectedSql))
  }

}
