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
