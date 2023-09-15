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

import com._4paradigm.openmldb.taskmanager.config.TaskManagerConfig
import org.apache.commons.io.IOUtils
import org.slf4j.LoggerFactory
import java.nio.file.Paths
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

object VersionUtil {

  private val logger = LoggerFactory.getLogger(this.getClass)

  def getBatchVersion(): String = {
    val sparkJarsPath = Paths.get(TaskManagerConfig.getSparkHome, "jars").toString
    val batchJarPath = BatchJobUtil.findOpenmldbBatchJar(sparkJarsPath)
    if (batchJarPath == null) {
      logger.error("Fail to find batch jar file and the version is unknown")
      return "unknown"
    }

    // Use Java command to get version from jar file
    val ps = Runtime.getRuntime.exec(Array[String]("java", "-cp", batchJarPath,
      "com._4paradigm.openmldb.batch.utils.VersionCli"))
    ps.waitFor
    val inputStream = ps.getInputStream
    val bytes = new Array[Byte](inputStream.available)
    inputStream.read(bytes, 0, bytes.length)
    new String(bytes)
  }

}
