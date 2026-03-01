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

package com._4paradigm.openmldb.batch.utils

import java.io.File
import com._4paradigm.hybridse.node.JoinType
import com._4paradigm.hybridse.vm.{PhysicalDataProviderNode, PhysicalJoinNode, PhysicalOpNode,
  PhysicalOpType, PhysicalProjectNode, PhysicalRenameNode, PhysicalWindowAggrerationNode, ProjectType}
import guru.nidi.graphviz.engine.{Format, Graphviz}
import guru.nidi.graphviz.model.Factory.{mutGraph, mutNode}
import guru.nidi.graphviz.model.MutableNode

import scala.collection.mutable


object GraphvizUtil {

  def drawPhysicalPlan(root: PhysicalOpNode, outputPath: String): Unit = {

    val outputGraphNode = getGraphNode(root)
    val graph = mutGraph("HybridsePhysicalPlan").setDirected(true).add(outputGraphNode)
    // More API in https://github.com/nidi3/graphviz-java
    Graphviz.fromGraph(graph).render(Format.PNG).toFile(new File(outputPath))
  }

  def getGraphNode(root: PhysicalOpNode): MutableNode = {
    val children = mutable.ArrayBuffer[MutableNode]()

    for (i <- 0 until root.GetProducerCnt().toInt) {
      children += getGraphNode(root.GetProducer(i))
    }

    visitPhysicalOp(root, children.toArray)
  }

  def visitPhysicalOp(node: PhysicalOpNode, children: Array[MutableNode]): MutableNode = {
    val opType = node.GetOpType()

    // Get more readable physical node
    val readableNodeName = opType match {
      case PhysicalOpType.kPhysicalOpDataProvider =>
        val dataProviderNode = PhysicalDataProviderNode.CastFrom(node)
        "DataProvider(" + dataProviderNode.GetName() + ")"
      case PhysicalOpType.kPhysicalOpSimpleProject => "SimpleProject"
      case PhysicalOpType.kPhysicalOpConstProject => "ConstProject"
      case PhysicalOpType.kPhysicalOpProject =>
        val projectNode = PhysicalProjectNode.CastFrom(node)
        projectNode.getProject_type_ match {
          case ProjectType.kTableProject => "TableProject"
          case ProjectType.kWindowAggregation =>
            val windowAggNode = PhysicalWindowAggrerationNode.CastFrom(node)
            "WindowAgg(" + windowAggNode.getWindow_.getName_ + ")"
          case ProjectType.kGroupAggregation => "GroupAgg"
          case _ => opType.toString
        }
      case PhysicalOpType.kPhysicalOpGroupBy => "GroupBy"
      case PhysicalOpType.kPhysicalOpJoin =>
        val joinNode = PhysicalJoinNode.CastFrom(node)
        joinNode.join().join_type() match {
          case JoinType.kJoinTypeLeft => "LeftJoin"
          case JoinType.kJoinTypeLast => "LastJoin"
          case JoinType.kJoinTypeConcat => "ConcatJoin"
          case _ => opType.toString
        }
      case PhysicalOpType.kPhysicalOpLimit => "Limit"
      case PhysicalOpType.kPhysicalOpRename =>
        val reanameNode = PhysicalRenameNode.CastFrom(node)
        "Rename(" + reanameNode.getName_ + ")"
      case PhysicalOpType.kPhysicalOpFilter => "Filter"
      case _ => opType.toString
    }

    // No need to use cache because graphviz will merge node with the same name
    val nodeNameWithId = "[%s]%s".format(node.GetNodeId().toString, readableNodeName)
    var graphNode = mutNode(nodeNameWithId)

    for (child <- children) {
      graphNode = graphNode.addLink(child)
    }

    graphNode
  }

}
