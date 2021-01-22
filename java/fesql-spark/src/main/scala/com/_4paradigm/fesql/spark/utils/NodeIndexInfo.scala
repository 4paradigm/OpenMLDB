package com._4paradigm.fesql.spark.utils

object NodeIndexType extends Enumeration {
  type NodeIndexType = Value
  val SourceConcatJoinNode = Value(1)
  val InternalConcatJoinNode = Value(2)
  val InternalComputeNode = Value(3)
  val DestNode = Value(4)

  def checkExists(nodeIndexType: Int) = this.values.exists(_ == nodeIndexType)

  def showAll = this.values.foreach(println)
}

// Record the index info for physical node
case class NodeIndexInfo(indexColumnName: String, var nodeIndexType: NodeIndexType.NodeIndexType)
