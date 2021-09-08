/*
 * Copyright 2021 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com._4paradigm.openmldb.batch.utils

import com._4paradigm.openmldb.batch.SparkTestSuite
import com._4paradigm.openmldb.batch.utils.SkewDataFrameUtils.{genAddColumnsDf, genDistributionDf, genUnionDf}
import com._4paradigm.openmldb.batch.utils.SparkUtil.approximateDfEqual
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{BooleanType, IntegerType, StructField, StructType}

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
  val partitionKeyColName = "_PARTITION_KEY_"
  val originalPartIdColName = "_ORIGINAL_PART_ID_"
  val partIdColName = "_PART_ID_"
  val greaterFlagColName = "_GREATER_FLAG_"
  val countColName = "_COUNT_"

  test("Test genDistributionDf") {
    val spark = getSparkSession
    val inputDf = spark.createDataFrame(spark.sparkContext.makeRDD(data), schema)
    val resultDf = genDistributionDf(inputDf, quantile, repartitionColIndex, percentileColIndex,
      partitionKeyColName, greaterFlagColName, countColName)

    val compareData = Seq(
      Row(550, 4, true, 3),
      Row(50, 1, true, 3)
    )

    val compareSchema = StructType(List(
      StructField(partitionKeyColName, IntegerType),
      StructField("percentile_1", IntegerType),
      StructField(greaterFlagColName, BooleanType),
      StructField(countColName, IntegerType)))

    val compareDf = spark.createDataFrame(spark.sparkContext.makeRDD(compareData), compareSchema)

    assert(approximateDfEqual(resultDf, compareDf, false))
  }

  test("Test genAddColumnsDf") {
    val spark = getSparkSession
    val inputDf = spark.createDataFrame(spark.sparkContext.makeRDD(data), schema)
    val distributionDf = genDistributionDf(inputDf, quantile, repartitionColIndex, percentileColIndex,
      partitionKeyColName, greaterFlagColName, countColName)
    val resultDf = genAddColumnsDf(inputDf, distributionDf, quantile, repartitionColIndex,
      percentileColIndex, partIdColName, originalPartIdColName, countColName)

    val compareData = Seq(
      Row(550, 5, 2, 2),
      Row(550, 4, 1, 1),
      Row(550, 3, 1, 1),
      Row(50, 2, 2, 2),
      Row(50, 1, 1, 1),
      Row(50, 0, 1, 1)
    )

    val compareSchema = StructType(List(
      StructField("col0", IntegerType),
      StructField("col1", IntegerType),
      StructField(partIdColName, IntegerType),
      StructField(originalPartIdColName, IntegerType)
    ))

    val compareDf = spark.createDataFrame(spark.sparkContext.makeRDD(compareData), compareSchema)

    assert(approximateDfEqual(resultDf, compareDf, false))
  }

  test("Test genUnionDf") {
    val spark = getSparkSession
    val inputDf = spark.createDataFrame(spark.sparkContext.makeRDD(data), schema)
    val distributionDf = genDistributionDf(inputDf, quantile, repartitionColIndex, percentileColIndex,
      partitionKeyColName, greaterFlagColName, countColName)
    val addColumnDf = genAddColumnsDf(inputDf, distributionDf, quantile, repartitionColIndex,
      percentileColIndex, partIdColName, originalPartIdColName, countColName)
    val resultDf = genUnionDf(addColumnDf, quantile, partIdColName, originalPartIdColName, 0, 0, 0)

    val compareData = Seq(
      Row(50, 1, 2, 1),
      Row(50, 0, 2, 1),
      Row(550, 4, 2, 1),
      Row(550, 3, 2, 1),
      Row(550, 5, 2, 2),
      Row(550, 4, 1, 1),
      Row(550, 3, 1, 1),
      Row(50, 2, 2, 2),
      Row(50, 1, 1, 1),
      Row(50, 0, 1, 1)
    )

    val compareSchema = StructType(List(
      StructField("col0", IntegerType),
      StructField("col1", IntegerType),
      StructField(partIdColName, IntegerType),
      StructField(originalPartIdColName, IntegerType)
    ))

    val compareDf = spark.createDataFrame(spark.sparkContext.makeRDD(compareData), compareSchema)

    assert(approximateDfEqual(resultDf, compareDf, false))
  }

}
