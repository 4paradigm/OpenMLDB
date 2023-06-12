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

import com._4paradigm.openmldb.batch.api.OpenmldbSession
import com._4paradigm.openmldb.batchjob.util.OpenmldbJobUtil
import org.apache.spark.SparkFiles
import org.apache.spark.sql.SparkSession

object RunBatchSql {

  def main(args: Array[String]): Unit = {
    OpenmldbJobUtil.checkArgumentSize(args, 1)
    runBatchSql(args(0))
  }

  def runBatchSql(sqlFilePath: String): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    val sqlText = OpenmldbJobUtil.getSqlFromFile(spark, sqlFilePath)

    val sess = new OpenmldbSession(spark)
    // offline sync and send result back to taskmanager
    // no show to print, save result by http in all modes?
    sess.sql(sqlText).sendResult()
    sess.close()
  }

}
