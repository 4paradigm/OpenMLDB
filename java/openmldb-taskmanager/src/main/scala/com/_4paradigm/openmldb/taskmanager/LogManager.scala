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

package com._4paradigm.openmldb.taskmanager

import com._4paradigm.openmldb.taskmanager.config.TaskManagerConfig
import org.slf4j.LoggerFactory

import java.io.File
import java.nio.file.Paths

object LogManager {
  private val logger = LoggerFactory.getLogger(this.getClass)

  def getJobLogFile(id: Int): File = {
    Paths.get(TaskManagerConfig.getJobLogPath, s"job_${id}.log").toFile
  }

  def getJobErrorLogFile(id: Int): File = {
    Paths.get(TaskManagerConfig.getJobLogPath, s"job_${id}_error.log").toFile
  }

  def getFileContent(inputFile: File): String = {
    val source = scala.io.Source.fromFile(inputFile)
    val content = try source.mkString finally source.close()
    content
  }

  def getJobLog(id: Int): String = {
    getFileContent(getJobLogFile(id))
  }

  def getJobErrorLog(id: Int): String = {
    getFileContent(getJobErrorLogFile(id))
  }

}
