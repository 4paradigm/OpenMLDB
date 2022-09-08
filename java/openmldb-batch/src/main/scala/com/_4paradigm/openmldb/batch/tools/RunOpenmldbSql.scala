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

/**
 * The main class to run OpenMLDB SQL and show result which is useful to test.
 */
object RunOpenmldbSql {

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      println("Require one parameter of SQL")
      return
    }

    val spark = SparkSession.builder().getOrCreate()
    val sess = new OpenmldbSession(spark)
    sess.sql(args(0)).show()
  }

}
