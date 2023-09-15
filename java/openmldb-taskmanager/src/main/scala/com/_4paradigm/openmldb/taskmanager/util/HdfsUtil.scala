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
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.slf4j.LoggerFactory

object HdfsUtil {

  private val logger = LoggerFactory.getLogger(this.getClass)

  def deleteHdfsDir(path: String): Unit = {

    val conf = new Configuration()
    conf.addResource(new Path(TaskManagerConfig.getHadoopConfDir, "core-site.xml"))
    conf.addResource(new Path(TaskManagerConfig.getHadoopConfDir, "hdfs-site.xml"))

    val fs = FileSystem.get(conf)

    val pathToDelete = new Path(path)

    if (fs.exists(pathToDelete)) {
      fs.delete(pathToDelete, true);
      logger.info("File deleted successfully: " + path)
    } else {
      logger.warn("File does not exist: " + path)
    }

    fs.close()

  }

}
