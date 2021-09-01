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
import org.apache.spark.sql.functions.{lit, when}
import org.apache.spark.sql.{Column, DataFrame}

import scala.collection.mutable

object SkewDataFrameUtils {
  def genDistributionDf(inputDf: DataFrame, quantile: Int, repartitionColIndex: mutable.ArrayBuffer[Int],
                        percentileColIndex: Int, partitionColName: String): DataFrame = {

    // TODO: Support multiple repartition keys
    val groupByCol = SparkColumnUtil.getColumnFromIndex(inputDf, repartitionColIndex(0))
    val percentileCol = SparkColumnUtil.getColumnFromIndex(inputDf, percentileColIndex)

    // Add percentile_approx column
    val percentileApproxCols = mutable.ArrayBuffer[Column]()
    val factor = 1.0 / quantile.toDouble
    for (i <- 1 until quantile) {
      val ratio = i * factor
      percentileApproxCols += percentileApprox(percentileCol, lit(ratio)).as(s"percentlie_${i}")
    }

    inputDf.groupBy(groupByCol.as(partitionColName)).agg(percentileApproxCols.head, percentileApproxCols.tail: _*)
  }

  def genPercentileTagDf(inputDf: DataFrame, distributionDf: DataFrame, quantile: Int,
                         repartitionColIndex: mutable.ArrayBuffer[Int], percentileColIndex: Int, partColName: String, expandColName: String): DataFrame = {

    // Input dataframe left join distribution dataframe
    // TODO: Support multiple repartition keys
    val inputDfJoinCol = SparkColumnUtil.getColumnFromIndex(inputDf, repartitionColIndex(0))
    val distributionDfJoinCol = SparkColumnUtil.getColumnFromIndex(distributionDf, 0)

    var joinDf = inputDf.join(distributionDf, inputDfJoinCol === distributionDfJoinCol, "left")

    // Select * and case when(...) from joinDf
    val inputDfPercentileCol = SparkColumnUtil.getColumnFromIndex(inputDf, percentileColIndex)

    var part: Column = null
    for (i <- 1 to quantile) {
      if (i != quantile) {
        part = when(inputDfPercentileCol <= joinDf(s"percentlie_${i}"), quantile - i + 1)
      } else {
        part = part.otherwise(quantile)
      }
    }
    joinDf = joinDf.withColumn(partColName, part).withColumn(expandColName, part)

    // Drop percentile_*, _PARTITION_
    // PS: When quantile is 2, the num of percentile_* columns is 1
    for (i <- 0 until (quantile - 1) + 1) {
      joinDf = joinDf.drop(SparkColumnUtil.getColumnFromIndex(joinDf, inputDf.schema.length))
    }

    joinDf
  }

  def genExplodeDataDf(addColumnsDf: DataFrame, quantile: Int,
                       partColName: String, expandColName: String): DataFrame = {

    // True By default
    val isClibing = true

    // Filter expandWindowColumns and union
    var unionDf = addColumnsDf
    for (i <- 1 until quantile) {
      if (isClibing) {
        val temDf = addColumnsDf.withColumn(partColName, lit(i))
        for (explode <- i + 1 to quantile) {
          unionDf = unionDf.union(temDf.filter(expandColName + s" = ${explode}"))
        }
      }
      else {
        // TODO: Support the num of window is precise
      }
    }

    unionDf
  }
}
