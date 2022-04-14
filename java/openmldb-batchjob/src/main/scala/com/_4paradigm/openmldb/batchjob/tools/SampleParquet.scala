package com._4paradigm.openmldb.batchjob.tools

import org.apache.spark.sql.{SaveMode, SparkSession}

object SampleParquet {

  def main(args: Array[String]): Unit = {
    val parquetPath = args(1)
    val outputPath = args(2)

    sampleParquet(parquetPath, outputPath)
  }

  def sampleParquet(parquetPath: String, outputPath: String, keepRows: Long = 100): Unit = {
    // Read parquet files
    val spark = SparkSession.builder().getOrCreate()
    val df = spark.read.parquet(parquetPath).cache()

    val rowCount = df.count()
    val newKeepRows = if (rowCount > keepRows) keepRows else rowCount
    val outputDf = df.sample(1.01 * newKeepRows / rowCount).limit (keepRows.toInt)

    outputDf.write.mode(SaveMode.Overwrite).parquet(outputPath)

    spark.close()
  }

}
