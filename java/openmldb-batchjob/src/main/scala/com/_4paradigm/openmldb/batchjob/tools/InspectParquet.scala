package com._4paradigm.openmldb.batchjob.tools

import org.apache.spark.sql.SparkSession

object InspectParquet {

  def main(args: Array[String]): Unit = {
    val parquetPath = args(0)

    inspectParquet(parquetPath)
  }

  def inspectParquet(parquetPath: String): Unit = {
    val spark = SparkSession.builder()//.master("local[*]")
      .getOrCreate()
    val df = spark.read.parquet(parquetPath).cache()

    val schema = df.schema
    println("Schema: " + schema)

    println("Count of rows: " + df.count())

    println("Show data:")
    df.show()

    //println("Summary of data:")
    //df.summary().show()

    spark.close()
  }

}
