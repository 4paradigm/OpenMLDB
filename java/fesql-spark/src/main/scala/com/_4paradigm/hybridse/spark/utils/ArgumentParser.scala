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

package com._4paradigm.hybridse.spark.utils

import org.slf4j.LoggerFactory

class ArgumentParser(args: Array[String]) {
  private val logger = LoggerFactory.getLogger(this.getClass)

  private var idx = 0
  private var curKey: String = _

  def parseArgs(fn: PartialFunction[String, Unit]): Unit = {
    while (idx < args.length) {
      curKey = args(idx)
      try {
        if (fn.isDefinedAt(curKey)) {
          fn.apply(curKey)
        }
      } catch {
        case e: Exception =>
          logger.error(s"Parse argument $curKey failed: ${e.getMessage}")
      }
      idx += 1
    }
  }

  def parsePair(): (String, String) = {
    val value = parseValue()
    val splitPos = value.indexOf("=")
    if (splitPos < 0) {
      throw new IllegalArgumentException(
        s"Illegal value for $curKey: $value")
    }
    val (k, v) = (value.substring(0, splitPos), value.substring(splitPos + 1))
    k -> v
  }

  def parseValue(): String = {
    idx += 1
    if (idx >= args.length) {
      throw new IllegalArgumentException(
        s"Argument index out of bound for $curKey")
    }
    args(idx)
  }

  def parseInt(): Int = {
    parseValue().toInt
  }
}
