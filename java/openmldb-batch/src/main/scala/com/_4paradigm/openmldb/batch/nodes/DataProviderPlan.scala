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

import com._4paradigm.hybridse.sdk.HybridSeException
import com._4paradigm.hybridse.vm.PhysicalDataProviderNode
import com._4paradigm.openmldb.batch.{PlanContext, SparkInstance}


object DataProviderPlan {

  def gen(ctx: PlanContext, node: PhysicalDataProviderNode, inputs: Seq[SparkInstance]): SparkInstance = {
    val tableName = node.GetName()
    val dbName = node.GetDb()

    val df = ctx.getDataFrame(dbName, tableName).getOrElse {
      throw new HybridSeException(s"Input table $tableName from database $dbName not found")
    }

    // If limit has been set
    val outputDf = if (node.GetLimitCntValue() >= 0) df.limit(node.GetLimitCntValue()) else df

    SparkInstance.createConsideringIndex(ctx, node.GetNodeId(), outputDf)
  }

}
