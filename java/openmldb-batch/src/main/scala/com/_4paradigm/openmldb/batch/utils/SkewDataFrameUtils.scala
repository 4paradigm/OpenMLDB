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
import org.apache.spark.sql.functions.{approx_count_distinct, lit, when}
import org.apache.spark.sql.{Column, DataFrame}

import scala.collection.mutable

object SkewDataFrameUtils {
  def genDistributionDf(inputDf: DataFrame, quantile: Int, repartitionColIndex: mutable.ArrayBuffer[Int],
                        percentileColIndex: Int, partitionColName: String, distinctCountColName: String,
                        approxRatio: Double): DataFrame = {

    // TODO: Support multiple repartition keys
    val groupByCol = SparkColumnUtil.getColumnFromIndex(inputDf, repartitionColIndex(0))
    val percentileCol = SparkColumnUtil.getColumnFromIndex(inputDf, percentileColIndex)

    // Add percentile_approx column
    val columns = mutable.ArrayBuffer[Column]()
    val factor = 1.0 / quantile.toDouble
    for (i <- 1 until quantile) {
      val ratio = i * factor
      columns += percentileApprox(percentileCol, lit(ratio)).as(s"PERCENTILE_${i}")
    }

    columns += approx_count_distinct(percentileCol, approxRatio).as(distinctCountColName)

    inputDf.groupBy(groupByCol.as(partitionColName)).agg(columns.head, columns.tail: _*)
  }

  def genAddColumnsDf(inputDf: DataFrame, distributionDf: DataFrame, quantile: Int,
                      repartitionColIndex: mutable.ArrayBuffer[Int], percentileColIndex: Int,
                      partIdColName: String, expandedRowColName: String, distinctCountColName: String): DataFrame = {

    // The Count column is useless
    val distributionDropColumnDf = distributionDf.drop(distinctCountColName)

    // Input dataframe left join distribution dataframe
    // TODO: Support multiple repartition keys
    val inputDfJoinCol = SparkColumnUtil.getColumnFromIndex(inputDf, repartitionColIndex(0))
    val distributionDfJoinCol = SparkColumnUtil.getColumnFromIndex(distributionDropColumnDf, 0)

    var joinDf = inputDf.join(distributionDropColumnDf.hint("broadcast"),
      inputDfJoinCol === distributionDfJoinCol, "left")

    // Select * and case when(...) from joinDf
    val inputDfPercentileCol = SparkColumnUtil.getColumnFromIndex(inputDf, percentileColIndex)

    var part: Column = null

    for (i <- 1 to quantile) {
      if (i == 1) {
        part = when(inputDfPercentileCol <= joinDf(s"PERCENTILE_${i}"), i)
      } else if (i != quantile) {
        part = part.when(inputDfPercentileCol <= joinDf(s"PERCENTILE_${i}"), i)
      } else {
        part = part.otherwise(quantile)
      }
    }
    joinDf = joinDf.withColumn(partIdColName, part).withColumn(expandedRowColName, lit(false))

    // Drop "PARTITION_KEY" column and PERCENTILE_* columns
    // PS: When quantile is 2, the num of PERCENTILE_* columns is 1
    for (_ <- 0 until 1 + (quantile - 1)) {
      joinDf = joinDf.drop(SparkColumnUtil.getColumnFromIndex(joinDf, inputDf.schema.length))
    }

    joinDf
  }

  def genUnionDf(addColumnsDf: DataFrame, quantile: Int, partIdColName: String, expandedRowColName: String,
                 rowsWindowSize: Long, rowsRangeWindowSize: Long, minBlockSize: Double): DataFrame = {
    // True By default
    var expandAllData = true

    // When compute rows, open the optimization
    if(rowsRangeWindowSize == 0) {
      expandAllData = false
    }

    // Filter expandWindowColumns and union
    var unionDf = addColumnsDf
    if (expandAllData) {
      for (i <- 2 to quantile) {
        val filterStr = partIdColName +  s" < ${i}"
        unionDf = unionDf.union(
          addColumnsDf.filter(filterStr)
          .withColumn(partIdColName, lit(i)).withColumn(expandedRowColName, lit(true))
        )
      }
    } else {
      // The Optimization for computing rows
      val blockNum = math.ceil(rowsWindowSize / minBlockSize).toInt

      for (i <- 2 to quantile) {
        val filterStr = partIdColName +  s" >= ${i-blockNum}" + " and " + partIdColName +  s" < ${i}"
        unionDf = unionDf.union(
          addColumnsDf.filter(filterStr)
            .withColumn(partIdColName, lit(i)).withColumn(expandedRowColName, lit(true))
        )
      }
    }

    unionDf
  }
}
