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

import com._4paradigm.openmldb.batch.api.OpenmldbSession
import org.apache.spark.SparkFiles
import org.apache.spark.sql.SparkSession
import scala.reflect.io.File

object OpenmldbJobUtil {

  def checkArgumentSize(args: Array[String], expectSize: Int): Unit = {
    if (args.length != expectSize) {
      throw new Exception(s"Require args size ${expectSize} but get args: ${args.mkString(",")}")
    }
  }

  def getSqlFromFile(spark: SparkSession, sqlFilePath: String): String = {
    if (!sqlFilePath.split("/").last.startsWith("sql-")) {
      // If it starts with "sql-" it is sql file, otherwise it is sql text
      return sqlFilePath
    }

    val sparkMaster = spark.conf.get("spark.master")
    val sparkDeployMode = spark.conf.get("spark.submit.deployMode", "client")

    val actualSqlFilePath = if (sparkMaster.equalsIgnoreCase("yarn") &&
      sparkDeployMode.equalsIgnoreCase("cluster")) {
      sqlFilePath.split("/").last
    } else {
      sqlFilePath
    }

    if (!File(actualSqlFilePath).exists) {
      throw new Exception("SQL file does not exist in " + actualSqlFilePath)
    }

    scala.io.Source.fromFile(actualSqlFilePath).mkString.trim
  }

  def runOpenmldbSql(spark: SparkSession, sqlFilePath: String): Unit = {
    val sqlText = OpenmldbJobUtil.getSqlFromFile(spark, sqlFilePath)

    val sess = new OpenmldbSession(spark)
    sess.sql(sqlText)
    sess.close()
  }

}
