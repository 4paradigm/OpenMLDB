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

package com._4paradigm.hybridsql.spark.nodes

import com._4paradigm.hybridse.common.HybridSEException
import com._4paradigm.hybridse.vm.PhysicalDataProviderNode
import com._4paradigm.hybridsql.spark.{PlanContext, SparkInstance}


object DataProviderPlan {

  def gen(ctx: PlanContext, node: PhysicalDataProviderNode, inputs: Seq[SparkInstance]): SparkInstance = {
    val tableName = node.GetName()
    val df = ctx.getDataFrame(tableName).getOrElse {
      throw new HybridSEException(s"Input table $tableName not found")
    }

    // If limit has been set
    val outputDf = if (node.GetLimitCnt() > 0) df.limit(node.GetLimitCnt()) else df

    SparkInstance.createConsideringIndex(ctx, node.GetNodeId(), outputDf)
  }

}
