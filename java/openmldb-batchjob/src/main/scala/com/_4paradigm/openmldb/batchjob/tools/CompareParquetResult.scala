package com._4paradigm.openmldb.batchjob.tools

import org.apache.spark.sql.SparkSession

object CompareParquetResult {

  def main(args: Array[String]): Unit = {
    val parquetPath1 = args(1)
    val parquetPath2 = args(2)

    compareTwoParquet(parquetPath1, parquetPath2)
  }

  /**
   * Only check for distinct rows. Can not handle duplicated rows.
   *
   * @param parquetPath1
   * @param parquetPath2
   */
  def compareTwoParquet(parquetPath1: String, parquetPath2: String): Unit = {
    // Read parquet files
    val spark = SparkSession.builder().getOrCreate()
    val df1 = spark.read.parquet(parquetPath1).cache()
    val df2 = spark.read.parquet(parquetPath2).cache()

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
