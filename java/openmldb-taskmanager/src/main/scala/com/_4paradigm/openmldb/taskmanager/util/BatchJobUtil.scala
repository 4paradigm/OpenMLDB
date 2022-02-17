package com._4paradigm.openmldb.taskmanager.util

import java.io.File
import java.io.IOException

object BatchJobUtil {

  /**
   * Get the default batch jor from presupposed directories.
   *
   * @return the path of openmldb-batchjob jar
   */
  def findLocalBatchJobJar(): String = {
    var jarPath = findBatchJobJar("../lib/")
    if (jarPath == null) {
      jarPath = findBatchJobJar("./openmldb-batchjob/target/")
      if (jarPath == null) {
        throw new IOException("Fail to find default batch job jar")
      }
    }

    jarPath
  }

  /**
   * Find the openmldb-batchjob jar from specified directory.
   *
   * @param libDirectory the directory to check
   * @return
   */
  def findBatchJobJar(libDirectory: String): String = {
    val libDirectoryFile = new File(libDirectory)

    if (libDirectoryFile != null && libDirectoryFile.listFiles != null) {
      val fileList  = libDirectoryFile.listFiles.filter(_.isFile)
      for (file <- fileList) {
        if (file.getName.startsWith("openmldb-batchjob") && file.getName.endsWith(".jar")
          && !file.getName.contains("javadoc") && !file.getName.contains("sources")) {
          return file.getAbsolutePath
        }
      }
    }

    null
  }

}
