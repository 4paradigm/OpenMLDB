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
import org.apache.spark.sql.SparkSession

object ImportOnlineData {

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      throw new Exception(s"Require args: sql but get args: ${args.mkString(",")}")
    }

    importOnlineData(args(0))
  }

  def importOnlineData(sql: String): Unit = {
    val sess = new OpenmldbSession(SparkSession.builder().config("openmldb.loaddata.mode", "online").getOrCreate())
    sess.sql(sql)
    sess.close()
  }

}
