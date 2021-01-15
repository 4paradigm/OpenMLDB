package com._4paradigm.fesql.spark.nodes.window

import org.apache.spark.sql.Row

trait WindowHook {
  def preCompute(computer: WindowComputer, curRow: Row): Unit = {}

  def postCompute(computer: WindowComputer, curRow: Row): Unit = {}

  def preBufferOnly(computer: WindowComputer, curRow: Row): Unit = {}

  def postBufferOnly(computer: WindowComputer, curRow: Row): Unit = {}
}
