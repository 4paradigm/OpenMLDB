/*
 * SlowRunSuite.scala
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

package com._4paradigm.fesql.spark.sql

import java.io.{File, IOException}
import java.nio.file.Files

import com._4paradigm.fesql.spark.FeSQLConfig
import org.apache.commons.io.FileUtils

class SlowRunSuite extends SQLBaseSuite {

  private var cacheDir: String = _

  override def customizedBefore() {
    cacheDir = Files.createTempDirectory("slow_hdfs_cache").toAbsolutePath.toString
    val conf = getSparkSession.conf
    conf.set("spark.fesql.slowRunCacheDir", cacheDir)
    conf.set("spark.default.parallelism", 1)
  }

  override def customizedAfter() {
    deleteCacheDir(cacheDir)
  }

  private def deleteCacheDir(dirName: String): Unit = {
    try {
      if (new File(dirName).isDirectory) {
        FileUtils.deleteDirectory(new File(dirName))
      }
    } catch {
      case e: IOException => e.printStackTrace()
    }
  }

  testCase("cases/query/fz_sql.yaml", "4")
}
