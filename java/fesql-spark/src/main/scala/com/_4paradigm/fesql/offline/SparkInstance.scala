package com._4paradigm.fesql.offline

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}


class SparkInstance {

  private var df: DataFrame = _

  private var rdd: RDD[Row] = _

  private var schema: StructType = _

  def this(df: DataFrame) = {
    this()
    this.df = df
    this.schema = df.schema
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
}


object SparkInstance {
  def fromDataFrame(df: DataFrame): SparkInstance = {
    new SparkInstance(df)
  }

  def fromRDD(schema: StructType, rdd: RDD[Row]): SparkInstance = {
    new SparkInstance(schema, rdd)
  }
}
