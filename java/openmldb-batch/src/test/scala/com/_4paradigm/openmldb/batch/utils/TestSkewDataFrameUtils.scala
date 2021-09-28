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
    Row(50, 0))

  val schema = StructType(List(
    StructField("col0", IntegerType),
    StructField("col1", IntegerType)))

  val quantile = 3
  val repartitionColIndex: mutable.ArrayBuffer[Int] = mutable.ArrayBuffer(0)
  val orderByColIndex = 1
  val approxRatio = 0.05
  val partitionKeyColName = "PARTITION_KEY"
  val expandedRowColName = "EXPANDED_ROW"
  val partIdColName = "PART_ID"
  val distinctCountColName = "DISTINCT_COUNT"

  test("Test genDistributionDf") {
    val spark = getSparkSession
    val inputDf = spark.createDataFrame(spark.sparkContext.makeRDD(data), schema)

    // Test genDistributionDf which add distinctCount Column
    val resultDf1 = genDistributionDf(inputDf, quantile, repartitionColIndex, orderByColIndex,
      partitionKeyColName, distinctCountColName, approxRatio)

    val compareData1 = Seq(
      Row(550, 3, 4, 3),
      Row(50, 0, 1, 3)
    )

    val compareSchema1 = StructType(List(
      StructField(partitionKeyColName, IntegerType),
      StructField("PERCENTILE_1", IntegerType),
      StructField("PERCENTILE_2", IntegerType),
      StructField(distinctCountColName, IntegerType)))

    val compareDf1 = spark.createDataFrame(spark.sparkContext.makeRDD(compareData1), compareSchema1)

    assert(approximateDfEqual(resultDf1, compareDf1, false))

    // Test genDistributionDf which don't add distinctCount Column
    val resultDf2 = genDistributionDf(inputDf, quantile, repartitionColIndex, orderByColIndex,
      partitionKeyColName)

    val compareData2 = Seq(
      Row(550, 3, 4),
      Row(50, 0, 1)
    )

    val compareSchema2 = StructType(List(
      StructField(partitionKeyColName, IntegerType),
      StructField("PERCENTILE_1", IntegerType),
      StructField("PERCENTILE_2", IntegerType)))

    val compareDf2 = spark.createDataFrame(spark.sparkContext.makeRDD(compareData2), compareSchema2)

    assert(approximateDfEqual(resultDf2, compareDf2, false))
  }

  test("Test genAddColumnsDf") {
    val spark = getSparkSession
    val inputDf = spark.createDataFrame(spark.sparkContext.makeRDD(data), schema)
    val distributionDf = genDistributionDf(inputDf, quantile, repartitionColIndex, orderByColIndex,
      partitionKeyColName)
    val resultDf = genAddColumnsDf(inputDf, distributionDf, quantile, repartitionColIndex,
      orderByColIndex, partitionKeyColName, partIdColName, expandedRowColName)

    val compareData = Seq(
      Row(550, 3, 1, false),
      Row(550, 4, 2, false),
      Row(550, 5, 3, false),
      Row(50, 0, 1, false),
      Row(50, 1, 2, false),
      Row(50, 2, 3, false)
    )

    val compareSchema = StructType(List(
      StructField("col0", IntegerType),
      StructField("col1", IntegerType),
      StructField(partIdColName, IntegerType),
      StructField(expandedRowColName, BooleanType)
    ))

    val compareDf = spark.createDataFrame(spark.sparkContext.makeRDD(compareData), compareSchema)

    assert(approximateDfEqual(resultDf, compareDf, false))
  }

  test("Test genUnionDf") {
    val spark = getSparkSession
    val inputDf = spark.createDataFrame(spark.sparkContext.makeRDD(data), schema)
    val compareSchema = StructType(List(
      StructField("col0", IntegerType),
      StructField("col1", IntegerType),
      StructField(partIdColName, IntegerType),
      StructField(expandedRowColName, BooleanType)
    ))

    // Do not open the optimization for expanding data
    val distributionDf1 = genDistributionDf(inputDf, quantile, repartitionColIndex, orderByColIndex,
      partitionKeyColName)
    val addColumnDf1 = genAddColumnsDf(inputDf, distributionDf1, quantile, repartitionColIndex,
      orderByColIndex, partitionKeyColName, partIdColName, expandedRowColName)
    val resultDf1 = genUnionDf(addColumnDf1, quantile, partIdColName, expandedRowColName)

    val compareData1 = Seq(
      Row(50, 0, 1, false),
      Row(50, 0, 2, true),
      Row(50, 1, 2, false),
      Row(50, 0, 3, true),
      Row(50, 1, 3, true),
      Row(50, 2, 3, false),
      Row(550, 3, 1, false),
      Row(550, 3, 2, true),
      Row(550, 4, 2, false),
      Row(550, 3, 3, true),
      Row(550, 4, 3, true),
      Row(550, 5, 3, false)
    )

    val compareDf1 = spark.createDataFrame(spark.sparkContext.makeRDD(compareData1), compareSchema)

    assert(approximateDfEqual(resultDf1, compareDf1, false))

    // Open the optimization for expanding data
    var distributionDf2 = genDistributionDf(inputDf, quantile, repartitionColIndex, orderByColIndex,
      partitionKeyColName, distinctCountColName, approxRatio)
    distributionDf2 = distributionDf2.drop(distinctCountColName)
    val addColumnDf2 = genAddColumnsDf(inputDf, distributionDf2, quantile, repartitionColIndex,
      orderByColIndex, partitionKeyColName, partIdColName, expandedRowColName)
    val resultDf2 = genUnionDf(addColumnDf2, quantile, partIdColName, expandedRowColName, 1, 1)

    val compareData2 = Seq(
      Row(50, 0, 1, false),
      Row(50, 0, 2, true),
      Row(50, 1, 2, false),
      Row(50, 1, 3, true),
      Row(50, 2, 3, false),
      Row(550, 3, 1, false),
      Row(550, 3, 2, true),
      Row(550, 4, 2, false),
      Row(550, 4, 3, true),
      Row(550, 5, 3, false)
    )

    val compareDf2 = spark.createDataFrame(spark.sparkContext.makeRDD(compareData2), compareSchema)

    assert(approximateDfEqual(resultDf2, compareDf2, false))
  }

}
