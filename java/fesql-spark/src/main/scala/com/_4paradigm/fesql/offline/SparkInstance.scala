package com._4paradigm.fesql.offline

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}


class SparkInstance {

  private var df: DataFrame = _

  private var rdd: RDD[Row] = _

  private var schema: StructType = _

  private var name: String = _

  def this(name: String, df: DataFrame) = {
    this()
    this.name = name
    this.df = df
    this.schema = df.schema
  }

  def this(name: String, schema: StructType, rdd: RDD[Row]) = {
    this()
    this.name = name
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

  def getName: String = {
    name
  }

}


object SparkInstance {
  def fromDataFrame(name: String, df: DataFrame): SparkInstance = {
    new SparkInstance(name, df)
  }

  def fromRDD(name: String, schema: StructType, rdd: RDD[Row]): SparkInstance = {
    new SparkInstance(name, schema, rdd)
  }
}
