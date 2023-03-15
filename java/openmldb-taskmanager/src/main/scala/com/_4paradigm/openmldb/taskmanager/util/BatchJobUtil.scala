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

package com._4paradigm.openmldb.taskmanager.util

import org.slf4j.LoggerFactory

import java.io.File
import java.io.IOException

object BatchJobUtil {

  private val logger = LoggerFactory.getLogger(this.getClass)

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
        // Add the test resource path to run unit tests with test jar
        val resourcesDirectory = new File("openmldb-taskmanager/src/test/resources/")
        jarPath = findBatchJobJar(resourcesDirectory.getAbsolutePath)
        if (jarPath == null) {
          throw new IOException("Fail to find default batch job jar")
        }
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

  def findOpenmldbBatchJar(libDirectory: String): String = {
    val libDirectoryFile = new File(libDirectory)

    if (libDirectoryFile != null && libDirectoryFile.listFiles != null) {
      val fileList  = libDirectoryFile.listFiles.filter(_.isFile)
      for (file <- fileList) {
        if (file.getName.startsWith("openmldb-batch") && file.getName.endsWith(".jar")
          && !file.getName.contains("javadoc") && !file.getName.contains("sources")) {
          return file.getAbsolutePath
        }
      }
    }

    null
  }

}
