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

package com._4paradigm.openmldb.batch.tools

import java.io.{File, FileOutputStream, FileWriter, OutputStreamWriter}

import com._4paradigm.openmldb.batch.OpenmldbBatchConfig
import com._4paradigm.openmldb.batch.utils.{ArgumentParser, JmhHelper}
import com._4paradigm.openmldb.batch.window.WindowSampleSupport
import com._4paradigm.openmldb.batch.window.WindowSampleSupport.SampleExecutor
import org.openjdk.jmh.runner.Runner

import scala.io.Source


class BenchmarkWindowSampleImpl {

  var loopMode = false
  var runOnce = false
  var resetWindow = false
  var samplePath = ""
  var sampleIdx = 0
  var executor: SampleExecutor = _

  def run(clazz: Class[_], args: Array[String]): Unit = {
    parseArgs(args)
    if (runOnce) {
      initExecutor()
      runWindowSample()
    } else if (loopMode) {
      initExecutor()
      var cnt = 0
      while (true) {
        runWindowSample()
        cnt += 1
        if (cnt % 1000 == 0) {
          println(s"Run window sample $cnt times")
        }
      }
    } else {
      saveArgs(args)
      val options = JmhHelper.getJmhOptionFromArgs(clazz.getSimpleName, args).build()
      val runner = new Runner(options)
      runner.run()
    }
  }

  def parseArgs(args: Array[String]): Unit = {
    val parser = new ArgumentParser(args)
    parser.parseArgs {
      case "--loop" => loopMode = true
      case "--once" => runOnce = true
      case "--samplePath" => samplePath = parser.parseValue()
      case "--sampleIdx" => sampleIdx = parser.parseInt()
      case "--resetWindow" => resetWindow = true
      case _ =>
    }
    if (samplePath.isEmpty) {
      throw new IllegalArgumentException("sample path is empty")
    }
  }

  def saveArgs(args: Array[String]): Unit = {
    var writer: OutputStreamWriter = null
    try {
      val filePath = "./window_sample_bm_args.log"
      writer = new OutputStreamWriter(new FileOutputStream(filePath), "UTF-8")
      writer.write(args.mkString("\n"))
    } finally {
      if (writer != null) {
        writer.close()
      }
    }
  }

  def loadArgs(): Array[String] = {
    Source.fromFile("./window_sample_bm_args.log").mkString.split("\n")
  }

  // @Setup
  def initExecutor(): Unit = {
    executor = WindowSampleSupport.recover(new OpenmldbBatchConfig, samplePath, sampleIdx)
  }

  // @Benchmark
  def runWindowSample(): Unit = {
    if (resetWindow) {
      executor.setWindow(executor.javaData)
    }
    val output = executor.run()
    output.delete()
  }
}
