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

package com._4paradigm.openmldb.batch

import com._4paradigm.hybridse.sdk.HybridSeException
import com._4paradigm.hybridse.vm.PhysicalOpNode
import com._4paradigm.openmldb.batch.utils.{NodeIndexType, SparkColumnUtil, SparkUtil}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType


class SparkInstance {

  private var physicalOpNode: PhysicalOpNode = _

  private var df: DataFrame = _

  // TODO: Keep the rdd optimization

  // The dataframe with index column, which may has one more column than the original dataframe
  private var dfWithIndex: DataFrame = _

  private var schema: StructType = _

  private var schemaWithIndex: StructType = _

  def this(df: DataFrame, physicalOpNode: PhysicalOpNode) = {
    this()
    this.df = df
    this.schema = df.schema
    this.physicalOpNode = physicalOpNode
  }

  def this(df: DataFrame, dfWithIndex: DataFrame, physicalOpNode: PhysicalOpNode) {
    this()
    this.df = df
    this.schema = df.schema
    this.dfWithIndex = dfWithIndex
    this.schemaWithIndex = dfWithIndex.schema
    this.physicalOpNode = physicalOpNode
  }

  def this(df: DataFrame, physicalOpNode: PhysicalOpNode, hasIndex: Boolean) {
    this()
    if (hasIndex) {
      this.dfWithIndex = df
      this.schemaWithIndex = dfWithIndex.schema
    } else {
      this.df = df
      this.schema = df.schema
    }
    this.physicalOpNode = physicalOpNode
  }

  def getDf(): DataFrame = {
    if (df == null && dfWithIndex != null) {
      // Only has dfWithIndex and remove the index column to return "original" df
      dfWithIndex.drop(SparkColumnUtil.getColumnFromIndex(dfWithIndex, dfWithIndex.schema.size-1))
    }
    df
  }

  def getSchema: StructType = {
    assert(schema != null)
    schema
  }

  def getDfWithIndex: DataFrame = {
    assert(dfWithIndex != null)
    dfWithIndex
  }

  def getSchemaWithIndex: StructType = {
    assert(schemaWithIndex != null)
    schemaWithIndex
  }

  // Consider node index info to get Spark DataFrame
  def getDfConsideringIndex(ctx: PlanContext, parentNodeId: Long): DataFrame = {
    if (ctx.hasIndexInfo(parentNodeId)) {
      val nodeIndexType = ctx.getIndexInfo(parentNodeId).nodeIndexType

      nodeIndexType match {
        case NodeIndexType.SourceConcatJoinNode => getDfWithIndex
        case NodeIndexType.InternalConcatJoinNode => getDfWithIndex
        case NodeIndexType.InternalComputeNode => getDfWithIndex
        case NodeIndexType.DestNode => getDf()
        case _ => throw new HybridSeException("Handle unsupported node index type: %s".format(nodeIndexType))
      }
    } else {
      getDf()
    }
  }

  def explain(): Unit= {
    this.physicalOpNode.Print()
  }

}

/** The wrapper of Spark dataframe and rdd.
 *
 * This is useful if want to avoid converting from dataframe to rdd in internal integration.
 */
object SparkInstance {
  def fromDataFrame(df: DataFrame, physicalOpNode: PhysicalOpNode): SparkInstance = {
    new SparkInstance(df, physicalOpNode)
  }

  def fromDfWithIndex(dfWithIndex: DataFrame, physicalOpNode: PhysicalOpNode): SparkInstance = {
    new SparkInstance(dfWithIndex, physicalOpNode, true)
  }

  def fromDfAndIndexedDf(df: DataFrame, dfWithIndex: DataFrame, physicalOpNode: PhysicalOpNode): SparkInstance = {
    new SparkInstance(df, dfWithIndex, physicalOpNode)
  }

  // Consider node index info to create SparkInstance
  def createConsideringIndex(ctx: PlanContext, nodeId: Long, sparkDf: DataFrame,
                             physicalOpNode: PhysicalOpNode): SparkInstance = {
    if (ctx.hasIndexInfo(nodeId)) {
      val nodeIndexType = ctx.getIndexInfo(nodeId).nodeIndexType
      nodeIndexType match {
        case NodeIndexType.SourceConcatJoinNode => SparkInstance.fromDataFrame(sparkDf, physicalOpNode)
        case NodeIndexType.InternalConcatJoinNode => SparkInstance.fromDfWithIndex(sparkDf, physicalOpNode)
        case NodeIndexType.InternalComputeNode => SparkInstance.fromDfWithIndex(sparkDf, physicalOpNode)
        case NodeIndexType.DestNode => {
          val outputDfWithIndex = SparkUtil.addIndexColumn(ctx.getSparkSession,
            sparkDf, ctx.getIndexInfo(nodeId).indexColumnName, ctx.getConf.addIndexColumnMethod)
          SparkInstance.fromDfAndIndexedDf(sparkDf, outputDfWithIndex, physicalOpNode)
        }
        case _ => throw new HybridSeException("Handle unsupported node index type: %s".format(nodeIndexType))
      }
    } else {
      SparkInstance.fromDataFrame(sparkDf, physicalOpNode)
    }
  }

  // Check if we have accepted the data with index column and should output the df with index column
  def keepIndexColumn(ctx: PlanContext, nodeId: Long): Boolean = {
    if (ctx.hasIndexInfo(nodeId)) {
      val nodeIndexType = ctx.getIndexInfo(nodeId).nodeIndexType

      nodeIndexType match {
        case NodeIndexType.SourceConcatJoinNode => false
        case NodeIndexType.InternalConcatJoinNode => true
        case NodeIndexType.InternalComputeNode => true
        // Notice that the dest node will not accept df with index and only append index column after computing
        case NodeIndexType.DestNode => false
        case _ => throw new HybridSeException("Handle unsupported node index type: %s".format(nodeIndexType))
      }
    } else {
      false
    }
  }

}
