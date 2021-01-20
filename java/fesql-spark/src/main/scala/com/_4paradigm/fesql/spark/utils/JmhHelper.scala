package com._4paradigm.fesql.spark.utils

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
