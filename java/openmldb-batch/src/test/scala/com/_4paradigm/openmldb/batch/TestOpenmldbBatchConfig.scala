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

import com._4paradigm.openmldb.batch.api.OpenmldbSession
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite


class TestOpenmldbBatchConfig extends FunSuite {

  test("Test make config") {
    val sess = SparkSession.builder()
        .config("spark.openmldb.groupby.partitions", 100)
        .config("spark.openmldb.test.print", value=true)
        .master("local[1]")
        .getOrCreate()
    val config = OpenmldbBatchConfig.fromSparkSession(sess)
    assert(config.groupbyPartitions == 100)
    assert(config.print)
    sess.close()
  }

  test("Test make config from dict") {
    val dict = Map(
      "openmldb.groupby.partitions" -> 100,
      "openmldb.test.print" -> true
    )
    val config = OpenmldbBatchConfig.fromDict(dict)
    assert(config.groupbyPartitions == 100)
    assert(config.print)
  }

  test("Test config of openmldb.sparksql") {
    val dict = Map(
      "openmldb.sparksql" -> true
    )
    val config = OpenmldbBatchConfig.fromDict(dict)
    assert(config.enableSparksql)

    val spark = SparkSession.builder()
      .master("local")
      .config("openmldb.sparksql", true)
      .getOrCreate()
    val sess = new OpenmldbSession(spark)

    sess.sql("SELECT int(10.0)").show()
  }

}
