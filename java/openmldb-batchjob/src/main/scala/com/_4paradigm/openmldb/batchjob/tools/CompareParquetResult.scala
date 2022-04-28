package com._4paradigm.openmldb.batchjob.tools

import org.apache.spark.sql.{SaveMode, SparkSession}

object CompareParquetResult {

  def main(args: Array[String]): Unit = {
    val parquetPath1 = args(0)
    val parquetPath2 = args(1)

    compareTwoParquet(parquetPath1, parquetPath2)
  }

  /**
   * Only check for distinct rows. Can not handle duplicated rows.
   *
   * @param parquetPath1
   * @param parquetPath2
   */
  def compareTwoParquet(parquetPath1: String, parquetPath2: String): Unit = {
    val spark = SparkSession.builder()//.master("local[*]")
      .getOrCreate()
    val df1 = spark.read.parquet(parquetPath1).cache()
    val df2 = spark.read.parquet(parquetPath2).cache()

    val df1Count = df1.count()
    val df2Count = df2.count()
    if (df1Count != df2Count) {
      println("Two dataframes are not equal")
      println(s"Df1 count: $df1Count, Df2 count: $df2Count")
    }

    val isEmpty1 = df1.except(df2).isEmpty
    val isEmpty2 = df2.except(df1).isEmpty

    if (isEmpty1 && isEmpty2) {
      println("Two dataframes are equal")
    } else {
      println("Two dataframes are not equal")
      println(s"df1 - df2 is empty: $isEmpty1, df2 - df1 is empty: $isEmpty2")
    }

    spark.close()
  }

}
