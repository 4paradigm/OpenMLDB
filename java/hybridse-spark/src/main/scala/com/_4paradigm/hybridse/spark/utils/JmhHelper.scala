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

import org.openjdk.jmh.annotations.Mode
import org.openjdk.jmh.runner.options.{ChainedOptionsBuilder, OptionsBuilder, TimeValue}

object JmhHelper {

  def getJmhOptionFromArgs(className: String, args: Array[String]): ChainedOptionsBuilder = {
    val options = new OptionsBuilder()
      .include(className)

    val parser = new ArgumentParser(args)
    parser.parseArgs {
      case "--jmh.batchSize" => options.measurementBatchSize(parser.parseInt())
      case "--jmh.iterations" => options.measurementIterations(parser.parseInt())
      case "--jmh.time" => options.measurementTime(TimeValue.fromString(parser.parseValue()))
      case "--jmh.mode" => options.mode(Mode.valueOf(parser.parseValue()))
      case "--jmh.threads" => options.threads(parser.parseInt())
      case "--jmh.warmupBatchSize" => options.warmupBatchSize(parser.parseInt())
      case "--jmh.warmupIterations" => options.warmupIterations(parser.parseInt())
      case "--jmh.warmupTime" => options.warmupTime(TimeValue.fromString(parser.parseValue()))
      case _ =>  // pass
    }
    options
  }
}
