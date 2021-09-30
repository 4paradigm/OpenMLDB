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
                        orderByColIndex: Int, partitionColName: String, distinctCountColName: String,
                        approxRatio: Double): DataFrame = {

    // TODO: Support multiple repartition keys
    val groupByCol = SparkColumnUtil.getColumnFromIndex(inputDf, repartitionColIndex(0))
    // TODO: Support multiple orderBy keys
    val percentileCol = SparkColumnUtil.getColumnFromIndex(inputDf, orderByColIndex)

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

  def genDistributionDf(inputDf: DataFrame, quantile: Int, repartitionColIndex: mutable.ArrayBuffer[Int],
                        orderByColIndex: Int, partitionColName: String): DataFrame = {

    // TODO: Support multiple repartition keys
    val groupByCol = SparkColumnUtil.getColumnFromIndex(inputDf, repartitionColIndex(0))
    // TODO: Support multiple orderBy keys
    val percentileCol = SparkColumnUtil.getColumnFromIndex(inputDf, orderByColIndex)

    // Add percentile_approx column
    val columns = mutable.ArrayBuffer[Column]()
    val factor = 1.0 / quantile.toDouble
    for (i <- 1 until quantile) {
      val ratio = i * factor
      columns += percentileApprox(percentileCol, lit(ratio)).as(s"PERCENTILE_${i}")
    }

    inputDf.groupBy(groupByCol.as(partitionColName)).agg(columns.head, columns.tail: _*)
  }

  def genAddColumnsDf(inputDf: DataFrame, distributionDf: DataFrame, quantile: Int,
                      repartitionColIndex: mutable.ArrayBuffer[Int], orderByColIndex: Int,
                      partitionKeyColName: String, partIdColName: String, expandedRowColName: String): DataFrame = {

    // Input dataframe left join distribution dataframe
    // TODO: Support multiple repartition keys
    val inputDfJoinCol = SparkColumnUtil.getColumnFromIndex(inputDf, repartitionColIndex(0))
    val distributionDfJoinCol = distributionDf(partitionKeyColName)

    var joinDf = inputDf.join(distributionDf, inputDfJoinCol === distributionDfJoinCol, "left")

    // TODO: Support multiple orderBy keys
    // Select * and case when(...) from joinDf
    val inputDfPercentileCol = SparkColumnUtil.getColumnFromIndex(inputDf, orderByColIndex)

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
                 rowsWindowSize: Long, minBlockSize: Double): DataFrame = {

    // Filter expandWindowColumns and union
    var unionDf = addColumnsDf

    val blockNum = math.ceil(rowsWindowSize / minBlockSize).toInt

    for (i <- 2 to quantile) {
      val filterStr = partIdColName + s" >= ${i - blockNum}" + " and " + partIdColName + s" < ${i}"
      unionDf = unionDf.union(
        addColumnsDf.filter(filterStr)
          .withColumn(partIdColName, lit(i)).withColumn(expandedRowColName, lit(true))
      )
    }

    unionDf
  }

  def genUnionDf(addColumnsDf: DataFrame, quantile: Int, partIdColName: String,
                 expandedRowColName: String): DataFrame = {

    // Filter expandWindowColumns and union
    var unionDf = addColumnsDf

    for (i <- 2 to quantile) {
      val filterStr = partIdColName + s" < ${i}"
      unionDf = unionDf.union(
        addColumnsDf.filter(filterStr)
          .withColumn(partIdColName, lit(i)).withColumn(expandedRowColName, lit(true))
      )
    }
    unionDf
  }
}
