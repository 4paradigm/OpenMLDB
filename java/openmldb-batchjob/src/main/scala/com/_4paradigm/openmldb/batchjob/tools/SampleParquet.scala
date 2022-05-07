package com._4paradigm.openmldb.batchjob.tools

import org.apache.spark.sql.{SaveMode, SparkSession}

object SampleParquet {

  def main(args: Array[String]): Unit = {
    val parquetPath = args(0)
    val outputPath = args(1)
    val keepRows = args(2).toInt

    sampleParquet(parquetPath, outputPath, keepRows)
  }

  def sampleParquet(parquetPath: String, outputPath: String, keepRows: Int): Unit = {
    val spark = SparkSession.builder()//.master("local[*]")
      .getOrCreate()
    val df = spark.read.parquet(parquetPath).cache()

    val rowCount = df.count()
    val newKeepRows = if (rowCount > keepRows) keepRows else rowCount
    val outputDf = df.sample(1.01 * newKeepRows / rowCount).limit(keepRows)

    outputDf.write.mode(SaveMode.Overwrite).parquet(outputPath)

    spark.close()
  }

}
