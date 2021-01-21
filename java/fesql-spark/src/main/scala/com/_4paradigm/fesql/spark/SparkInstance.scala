package com._4paradigm.fesql.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}


class SparkInstance {

  private var df: DataFrame = _

  // The dataframe with index column, which may has one more column than the original dataframe
  private var dfWithIndex: DataFrame = _

  private var rdd: RDD[Row] = _

  private var schema: StructType = _

  def this(df: DataFrame) = {
    this()
    this.df = df
    this.schema = df.schema
  }

  def this(df: DataFrame, hasIndex: Boolean) {
    this()
    if (hasIndex) {
      this.dfWithIndex = df
    } else {
      this.df = df
    }
  }

  def this(schema: StructType, rdd: RDD[Row]) = {
    this()
    this.schema = schema
    this.rdd = rdd
  }

  def getRDD: RDD[Row] = {
    if (rdd == null) {
      assert(df != null)
      rdd = df.rdd
    }
    rdd
  }

  def getDf(sess: SparkSession): DataFrame = {
    if (df == null) {
      assert(rdd != null)
      df = sess.createDataFrame(rdd, schema)
    }
    df
  }

  def getSchema: StructType = {
    schema
  }

  def getDfWithIndex: DataFrame = {
    dfWithIndex
  }

}


object SparkInstance {
  def fromDataFrame(df: DataFrame): SparkInstance = {
    new SparkInstance(df)
  }

  def fromRDD(schema: StructType, rdd: RDD[Row]): SparkInstance = {
    new SparkInstance(schema, rdd)
  }

  def fromDfWithIndex(dfWithIndex: DataFrame): SparkInstance = {
    new SparkInstance(dfWithIndex, true)
  }

}
