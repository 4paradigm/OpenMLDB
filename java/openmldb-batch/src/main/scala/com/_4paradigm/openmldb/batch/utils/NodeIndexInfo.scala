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

object NodeIndexType extends Enumeration {
  type NodeIndexType = Value
  val SourceConcatJoinNode = Value(1)
  val InternalConcatJoinNode = Value(2)
  val InternalComputeNode = Value(3)
  val DestNode = Value(4)

  def checkExists(nodeIndexType: Int): Boolean = this.values.exists(_ == nodeIndexType)

  def showAll: Unit = this.values.foreach(println)
}

// Record the index info for physical node
case class NodeIndexInfo(indexColumnName: String, var nodeIndexType: NodeIndexType.NodeIndexType)
