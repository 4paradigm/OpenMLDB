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

object ImportOnlineData {

  def main(args: Array[String]): Unit = {
    OpenmldbJobUtil.checkArgumentSize(args, 1)
    importOnlineData(args(0))
  }

  def importOnlineData(sqlFilePath: String): Unit = {  
    val builder = SparkSession.builder().config("openmldb.loaddata.mode", "online")
    val spark = builder.getOrCreate()
    OpenmldbJobUtil.runOpenmldbSql(spark, sqlFilePath)
  }

}
