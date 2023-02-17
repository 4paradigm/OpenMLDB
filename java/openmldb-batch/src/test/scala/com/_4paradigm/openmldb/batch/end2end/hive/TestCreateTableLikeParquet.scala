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

package com._4paradigm.openmldb.batch.end2end.hive

import com._4paradigm.openmldb.batch.api.OpenmldbSession
import org.apache.spark.sql.SparkSession
import org.scalatest.{FunSuite, Ignore}

@Ignore
class TestCreateTableLikeParquet extends FunSuite {

  test("Test CREATE TABLE LIKE PARQUET ") {

    val zkHost = "localhost:2181"
    val zkPath = "/openmldb"

    val spark = SparkSession.builder()
      .master("local")
      .config("openmldb.zk.cluster", zkHost)
      .config("openmldb.zk.root.path", zkPath)
      .getOrCreate()

    val sess = new OpenmldbSession(spark)

    val sql = "CREATE TABLE db1.t1 LIKE Parquet 'file:///tmp/test.parquet'"

    sess.sql(sql)
  }

}
