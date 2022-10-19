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

package com._4paradigm.openmldb.batch.tools

import org.apache.spark.sql.SparkSession
import com._4paradigm.openmldb.batch.api.OpenmldbSession
import com.google.gson.JsonParser
import org.apache.commons.io.FileUtils
import org.slf4j.LoggerFactory
import java.io.File

/**
 * The main class to run OpenMLDB SQL and show result which is useful to test.
 */
object RunOpenmldbSqlWithJson {
  private val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {

    if (args.length < 1) {
      logger.error("Require one parameter of Json file")
      return
    }

    // Get json string or json file
    val jsonStr = if (args(0).endsWith(".json")) {
      FileUtils.readFileToString(new File(args(0)), "UTF-8")
    } else {
      args(0)
    }
    logger.info("Get JSON string: " + jsonStr)
    /* Example json format.
    {
      "tables": [
        {"t1": "file:///tmp/parquet/"},
      ],
      "sql": "select 10",
      "config": [
        {"spark.openmldb.debug.print_physical_plan": "true"}
      ]
    }
    */

    val parser = new JsonParser
    val jsonElement = parser.parse(jsonStr).getAsJsonObject

    if (!jsonElement.has("sql")) {
      logger.error("Sql is not set in JSON, exit now")
      return
    }

    // Read config
    val sparkBuilder = SparkSession.builder()
    val configsJson = jsonElement.getAsJsonArray("config")
    if (configsJson != null) {
      for (i <- 0 until configsJson.size) {
        val configJson = configsJson.get(i).getAsJsonObject()
        configJson.entrySet().forEach(map => {
          sparkBuilder.config(map.getKey, map.getValue.getAsString())
        })
      }
    }

    val spark = sparkBuilder.getOrCreate()
    val sess = new OpenmldbSession(spark)

    // Read tables
    val tablesJson = jsonElement.getAsJsonArray("tables")
    if (tablesJson != null) {
      for (i <- 0 until tablesJson.size) {
        val tableJson = tablesJson.get(i).getAsJsonObject()
        tableJson.entrySet().forEach(map => {
          sess.registerTable(map.getKey, spark.read.parquet(map.getValue.getAsString()))
        })
      }
    }

    // Read SQL
    val sql = jsonElement.get("sql").getAsString()

    // Run SQL and show
    sess.sql(sql).show()
  }

}
