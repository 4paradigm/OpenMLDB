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

package com._4paradigm.openmldb.taskmanager.k8s

/**
 * The util class to convert Kubernetes YAML data.
 */
object K8sYamlUtil {

  /**
   * Convert list of string to single line string.
   *
   * @param arguments
   * @return
   */
  def generateArgumentsString(arguments: List[String]): String = {
    var argumentString = "["

    for (argument <- arguments) {
      argumentString += s""" "$argument", """.trim
    }

    if (argumentString.endsWith(",")) {
      argumentString = argumentString.dropRight(1)
    }

    argumentString += "]"

    argumentString
  }

  /**
   * Convert map of string and string to single line string.
   *
   * @param sparkConf
   * @return
   */
  def generateSparkConfString(sparkConf: Map[String, String]): String = {
    var sparkConfString = "{"

    sparkConf.foreach { case (key, value) =>

      sparkConfString += s""" $key: "$value", """.trim
    }

    if (sparkConfString.endsWith(",")) {
      sparkConfString = sparkConfString.dropRight(1)
    }

    sparkConfString += "}"

    sparkConfString
  }


}
