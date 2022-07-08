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

import org.apache.commons.io.IOUtils
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

object VersionUtil {

  def getVersion(): String = {

    // Read local git properties file
    val stream = this.getClass.getClassLoader.getResourceAsStream("git.properties")
    if (stream == null) {
      throw new Exception("Fail to get version from file of git.properties")
    }
    val gitVersionStrList = IOUtils.readLines(stream, "UTF-8")

    // Only get build version and git commit abbrev
    var version = ""
    var gitCommit = ""
    for (line <- gitVersionStrList) {
      if (line.startsWith("git.build.version=")) {
        version = line.split("=")(1)
      }
      if (line.startsWith("git.commit.id.abbrev=")) {
        gitCommit = line.split("=")(1)
      }
    }

    s"$version-$gitCommit"
  }

}
