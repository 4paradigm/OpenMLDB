package com._4paradigm.fesql.spark.tools

import java.io.{File, FileWriter}

import com._4paradigm.fesql.spark.FeSQLConfig
import com._4paradigm.fesql.spark.nodes.window.WindowSampleSupport
import com._4paradigm.fesql.spark.nodes.window.WindowSampleSupport.SampleExecutor
import com._4paradigm.fesql.spark.utils.{ArgumentParser, JmhHelper}
import org.openjdk.jmh.runner.Runner

import scala.io.Source


class BenchmarkWindowSampleImpl {

  var loopMode = false
  var runOnce = false
  var resetWindow = false
  var samplePath = "file:///Users/wrongtest/Downloads/1610512051444"
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
      case "--resetWindow" => resetWindow = true
    }
    if (samplePath.isEmpty) {
      throw new IllegalArgumentException("sample path is empty")
    }
  }

  def saveArgs(args: Array[String]): Unit = {
    val writer = new FileWriter(new File("./window_sample_bm_args.log"))
    writer.write(args.mkString("\n"))
    writer.close()
  }

  def loadArgs(): Array[String] = {
    Source.fromFile("./window_sample_bm_args.log").mkString.split("\n")
  }

  // @Setup
  def initExecutor(): Unit = {
    executor = WindowSampleSupport.recover(new FeSQLConfig, samplePath)
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
