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

package com._4paradigm.openmldb.batch.nodes

import com._4paradigm.openmldb.batch.PlanContext
import com._4paradigm.hybridse.vm.PhysicalSetOperationNode
import com._4paradigm.hybridse.node.SetOperationType
import com._4paradigm.openmldb.batch.SparkInstance
import org.slf4j.LoggerFactory
import com._4paradigm.hybridse.sdk.HybridSeException

// UNION [ ALL | DISTINCT ] : YES
// EXCEPT                   : NO
// INTERSECT                : NO
object SetOperationPlan {
  private val logger = LoggerFactory.getLogger(this.getClass)

  def gen(
      ctx: PlanContext,
      node: PhysicalSetOperationNode,
      inputs: Array[SparkInstance]
  ): SparkInstance = {
    val setType = node.getSet_type_()
    if (setType != SetOperationType.UNION) {
      throw new HybridSeException(s"Set Operation type $setType not supported")
    }

    if (inputs.size < 2) {
      throw new HybridSeException(s"Set Operation requires input size >= 2")
    }

    val unionAll = inputs
      .map(inst => inst.getDf())
      .reduceLeft({ (acc, df) =>
        {
          acc.union(df)
        }
      })

    val outputDf = if (node.getDistinct_()) {
      unionAll.distinct()
    } else {
      unionAll
    }

    SparkInstance.createConsideringIndex(ctx, node.GetNodeId(), outputDf)
  }
}
