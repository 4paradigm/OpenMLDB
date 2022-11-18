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

package com._4paradigm.openmldb.batchjob

import com._4paradigm.openmldb.batchjob.util.OpenmldbJobUtil
import org.apache.spark.sql.SparkSession

object ExportOfflineData {

  def main(args: Array[String]): Unit = {
    // sql, enable hive
    OpenmldbJobUtil.checkArgumentSize(args, 2)
    exportOfflineData(args(0), args(1))
  }

  def exportOfflineData(sqlFilePath: String, enableHive: String): Unit = {
    // offline load may read from hive, must create hive catalog session first
    // CAN NOT create one more hive session after a normal session
    val builder = SparkSession.builder()
    if (enableHive.equalsIgnoreCase("true")) {
      builder.enableHiveSupport()
    }
    val spark = builder.getOrCreate()
    OpenmldbJobUtil.runOpenmldbSql(spark, sqlFilePath)
  }

}
