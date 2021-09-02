package com._4paradigm.openmldb.batch.utils

import com._4paradigm.openmldb.batch.SparkTestSuite
import com._4paradigm.openmldb.batch.utils.SkewDataFrameUtils.{genAddColumnsDf, genDistributionDf, genUnionDf}
import com._4paradigm.openmldb.batch.utils.SparkUtil.approximateDfEqual
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

import scala.collection.mutable

class TestSkewDataFrameUtils extends SparkTestSuite {

  val data = Seq(
    Row(550, 5),
    Row(550, 4),
    Row(550, 3),
    Row(50, 2),
    Row(50, 1),
    Row(50, 0)
  )

  val schema = StructType(List(
    StructField("col0", IntegerType),
    StructField("col1", IntegerType)))

  val quantile = 2
  val repartitionColIndex: mutable.ArrayBuffer[Int] = mutable.ArrayBuffer(0)
  val percentileColIndex = 1
  val partitionColName = "_PARTITION_"
  val expandColName = "_EXPAND_"
  val partColName = "_PART_"

  test("Test genDistributionDf") {
    val spark = getSparkSession
    val inputDf = spark.createDataFrame(spark.sparkContext.makeRDD(data), schema)
    val resultDf = genDistributionDf(inputDf, quantile, repartitionColIndex, percentileColIndex, partitionColName)

    val compareData = Seq(
      Row(550, 4),
      Row(50, 1)
    )

    val compareSchema = StructType(List(
      StructField(partitionColName, IntegerType),
      StructField("percentile_1", IntegerType)))

    val compareDf = spark.createDataFrame(spark.sparkContext.makeRDD(compareData), compareSchema)

    assert(approximateDfEqual(resultDf, compareDf, false))
  }

  test("Test genAddColumnsDf") {
    val spark = getSparkSession
    val inputDf = spark.createDataFrame(spark.sparkContext.makeRDD(data), schema)
    val distributionDf = genDistributionDf(inputDf, quantile, repartitionColIndex, percentileColIndex, partitionColName)
    val resultDf = genAddColumnsDf(inputDf, distributionDf, quantile, repartitionColIndex,
      percentileColIndex, partColName, expandColName)

    val compareData = Seq(
      Row(550, 5, 1, 1),
      Row(550, 4, 2, 2),
      Row(550, 3, 2, 2),
      Row(50, 2, 1, 1),
      Row(50, 1, 2, 2),
      Row(50, 0, 2, 2)
    )

    val compareSchema = StructType(List(
      StructField("col0", IntegerType),
      StructField("col1", IntegerType),
      StructField(partColName, IntegerType),
      StructField(expandColName, IntegerType)
    ))

    val compareDf = spark.createDataFrame(spark.sparkContext.makeRDD(compareData), compareSchema)

    assert(approximateDfEqual(resultDf, compareDf, false))
  }

  test("Test genUnionDf") {
    val spark = getSparkSession
    val inputDf = spark.createDataFrame(spark.sparkContext.makeRDD(data), schema)
    val distributionDf = genDistributionDf(inputDf, quantile, repartitionColIndex, percentileColIndex, partitionColName)
    val addColumnDf = genAddColumnsDf(inputDf, distributionDf, quantile, repartitionColIndex,
      percentileColIndex, partColName, expandColName)
    val resultDf = genUnionDf(addColumnDf, quantile, partColName, expandColName)

    val compareData = Seq(
      Row(50, 1, 1, 2),
      Row(50, 0, 1, 2),
      Row(550, 4, 1, 2),
      Row(550, 3, 1, 2),
      Row(550, 5, 1, 1),
      Row(550, 4, 2, 2),
      Row(550, 3, 2, 2),
      Row(50, 2, 1, 1),
      Row(50, 1, 2, 2),
      Row(50, 0, 2, 2)
    )

    val compareSchema = StructType(List(
      StructField("col0", IntegerType),
      StructField("col1", IntegerType),
      StructField(partColName, IntegerType),
      StructField(expandColName, IntegerType)
    ))

    val compareDf = spark.createDataFrame(spark.sparkContext.makeRDD(compareData), compareSchema)

    assert(approximateDfEqual(resultDf, compareDf, false))
  }

}
