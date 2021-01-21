package com._4paradigm.fesql.spark

import com._4paradigm.fesql.spark.utils.SparkUtil
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.DataFrame


class SparkInstance {

  private var df: DataFrame = _

  // The dataframe with index column, which may has one more column than the original dataframe
  private var dfWithIndex: DataFrame = _

  private var schema: StructType = _

  private var schemaWithIndex: StructType = _

  def this(df: DataFrame) = {
    this()
    this.df = df
    this.schema = df.schema
  }

  def this(df: DataFrame, dfWithIndex: DataFrame) {
    this()
    this.df = df
    this.schema = df.schema
    this.dfWithIndex = dfWithIndex
    this.schemaWithIndex = dfWithIndex.schema
  }

  def this(df: DataFrame, hasIndex: Boolean) {
    this()
    if (hasIndex) {
      this.dfWithIndex = df
      this.schemaWithIndex = dfWithIndex.schema
    } else {
      this.df = df
      this.schema = df.schema
    }
  }

  def getDf(): DataFrame = {
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
  def getSparkDfConsideringIndex(ctx: PlanContext, parentNodeId: Long): DataFrame = {
    if (ctx.hasIndexInfo(parentNodeId)) {
      if (ctx.getIndexInfo(parentNodeId).shouldAddIndexColumn) {
        // If parent node is ConcatJoin's lowest common input node, read the normal df
        df
      } else {
        // If parent node has index flag, normally return the one with index
        dfWithIndex
      }
    } else {
      // If parent node do not have index flag, return the normal df
      df
    }
  }

}


object SparkInstance {
  def fromDataFrame(df: DataFrame): SparkInstance = {
    new SparkInstance(df)
  }

  def fromDfWithIndex(dfWithIndex: DataFrame): SparkInstance = {
    new SparkInstance(dfWithIndex, true)
  }

  def fromDfAndIndexedDf(df: DataFrame, dfWithIndex: DataFrame): SparkInstance = {
    new SparkInstance(df, dfWithIndex)
  }

  // Consider node index info to create SparkInstance
  def createWithNodeIndexInfo(ctx: PlanContext, nodeId: Long, sparkDf: DataFrame): SparkInstance = {
    if (ctx.hasIndexInfo(nodeId)) {
      val nodeIndexInfo = ctx.getIndexInfo(nodeId)
      if (nodeIndexInfo.shouldAddIndexColumn) {
        // Add the new index column
        val outputDfWithIndex = SparkUtil.addIndexColumn(ctx.getSparkSession, sparkDf, nodeIndexInfo.indexColumnName)
        SparkInstance.fromDfAndIndexedDf(sparkDf, outputDfWithIndex)
      } else {
        // Do not add new column and return the dataframe as dfWithIndex
        SparkInstance.fromDfWithIndex(sparkDf)
      }
    } else {
      // Return the normal dataframe
      SparkInstance.fromDataFrame(sparkDf)
    }
  }

}
