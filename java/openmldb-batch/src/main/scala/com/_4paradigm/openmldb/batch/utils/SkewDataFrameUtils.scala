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

import com._4paradigm.openmldb.batch.udf.PercentileApprox.percentileApprox
import org.apache.spark.sql.functions.{count, countDistinct, lit, when}
import org.apache.spark.sql.{Column, DataFrame}

import scala.collection.mutable

object SkewDataFrameUtils {
  def genDistributionDf(inputDf: DataFrame, quantile: Int, repartitionColIndex: mutable.ArrayBuffer[Int],
                        percentileColIndex: Int, partitionColName: String, greaterFlagColName: String,
                        countColName: String): DataFrame = {

    // TODO: Support multiple repartition keys
    val groupByCol = SparkColumnUtil.getColumnFromIndex(inputDf, repartitionColIndex(0))
    val percentileCol = SparkColumnUtil.getColumnFromIndex(inputDf, percentileColIndex)

    // Add percentile_approx column
    val columns = mutable.ArrayBuffer[Column]()
    val factor = 1.0 / quantile.toDouble
    for (i <- 1 until quantile) {
      val ratio = i * factor
      columns += percentileApprox(percentileCol, lit(ratio)).as(s"percentile_${i}")
    }

    columns += when(countDistinct(percentileCol) < lit(quantile), false)
      .otherwise(true).as(greaterFlagColName)
    columns += count(percentileCol).as(countColName)

    inputDf.groupBy(groupByCol.as(partitionColName)).agg(columns.head, columns.tail: _*)
  }

  def genAddColumnsDf(inputDf: DataFrame, distributionDf: DataFrame, quantile: Int,
                      repartitionColIndex: mutable.ArrayBuffer[Int], percentileColIndex: Int,
                      partColName: String, expandColName: String, countColName: String): DataFrame = {

    // The count column is useless
    val distributionDropCountDf = distributionDf.drop(countColName)

    // Input dataframe left join distribution dataframe
    // TODO: Support multiple repartition keys
    val inputDfJoinCol = SparkColumnUtil.getColumnFromIndex(inputDf, repartitionColIndex(0))
    val distributionDfJoinCol = SparkColumnUtil.getColumnFromIndex(distributionDropCountDf, 0)

    var joinDf = inputDf.join(distributionDropCountDf, inputDfJoinCol === distributionDfJoinCol, "left")

    // Select * and case when(...) from joinDf
    val inputDfPercentileCol = SparkColumnUtil.getColumnFromIndex(inputDf, percentileColIndex)

    var part: Column = null

    for (i <- 1 to quantile) {
      if (i == 1) {
        part = when(inputDfPercentileCol <= joinDf(s"percentile_${i}"), i)
      } else if (i != quantile) {
        part = part.when(inputDfPercentileCol <= joinDf(s"percentile_${i}"), i)
      } else {
        part = part.otherwise(quantile)
      }
    }
    joinDf = joinDf.withColumn(partColName, part).withColumn(expandColName, part)

    // Drop _PARTITION_, _GREATER_FLAG_, percentile_*
    // PS: When quantile is 2, the num of percentile_* columns is 1
    for (i <- 0 until 2 + (quantile - 1)) {
      joinDf = joinDf.drop(SparkColumnUtil.getColumnFromIndex(joinDf, inputDf.schema.length))
    }

    joinDf
  }

  def genUnionDf(addColumnsDf: DataFrame, quantile: Int, partColName: String,
                 expandColName: String, minCount: Long, rowsWindowSize: Long, rowsRangeWindowSize: Long): DataFrame = {

    // True By default
    var isClibing = true

    // PS: minCout / quantile is the min rowsWindowSize
    if (rowsRangeWindowSize == 0 && rowsWindowSize > 0 && minCount / quantile > rowsWindowSize) {
      isClibing = false
    }
    // Filter expandWindowColumns and union
    var unionDf = addColumnsDf
    for (i <- 2 to quantile) {
      if (isClibing) {
        val temDf = addColumnsDf.withColumn(partColName, lit(i))
        for (explode <- 1 until i) {
          unionDf = unionDf.union(temDf.filter(expandColName + s" = ${explode}"))
        }
      }
      else {
        // When the min rowsWindowSize > rowsWindowSize, means that only need one next part of quantile
        val temDf = addColumnsDf.withColumn(partColName, lit(i))
        unionDf = unionDf.union(temDf.filter(expandColName + s" = ${i - 1}"))
      }
    }

    unionDf
  }
}
