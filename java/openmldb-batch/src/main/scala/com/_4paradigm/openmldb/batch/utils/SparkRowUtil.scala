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

import com._4paradigm.hybridse.sdk.HybridSeException
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataType, DateType, IntegerType, LongType, ShortType, TimestampType}

import scala.collection.mutable

object SparkRowUtil {

  def getLongFromIndex(keyIdx: Int, sparkType: DataType, row: Row): Long = {
    sparkType match {
      case ShortType => row.getShort(keyIdx).toLong
      case IntegerType => row.getInt(keyIdx).toLong
      case LongType => row.getLong(keyIdx)
      case TimestampType => row.getTimestamp(keyIdx).getTime
      case DateType => row.getDate(keyIdx).getTime
      case _ =>
        throw new HybridSeException(s"Illegal window key type: $sparkType")
    }
  }

  def maxRows(iterator: Iterator[Row], groupByColIndex: Int, orderByColIndex: Int, orderByColType: DataType)
  : mutable.ArrayBuffer[Row] = {
    val resultRows = new mutable.ArrayBuffer[Row]()
    var lastRowPartitionKey: Long = Long.MinValue

    var maxValue: Long = Long.MinValue
    var maxRow: Row = null
    var first = true

    while (iterator.hasNext) {
      val row = iterator.next()
      val currentPartitionKey = row.getLong(groupByColIndex)
      // Determine whether it is in the same partition
      if (lastRowPartitionKey != Long.MinValue && currentPartitionKey != lastRowPartitionKey) {
        // Add the row
        resultRows += maxRow
        first = true
      }
      val value = if (row.isNullAt(orderByColIndex)) {
        Long.MinValue
      } else {
        SparkRowUtil.getLongFromIndex(orderByColIndex, orderByColType, row)
      }
      if (first || value > maxValue) {
        maxRow = row
        maxValue = value
        first = false
      }
      lastRowPartitionKey = currentPartitionKey
    }

    // Add the row in last partition
    if (maxRow != null) {
      resultRows += maxRow
    }

    resultRows
  }

  def minRows(iterator: Iterator[Row], groupByColIndex: Int, orderByColIndex: Int, orderByColType: DataType)
  : mutable.ArrayBuffer[Row] = {
    val resultRows = new mutable.ArrayBuffer[Row]()
    var lastRowPartitionKey: Long = Long.MaxValue

    var minValue: Long = Long.MaxValue
    var minRow: Row = null
    var first = true

    while (iterator.hasNext) {
      val row = iterator.next()
      val currentPartitionKey = row.getLong(groupByColIndex)
      // Determine whether it is in the same partition
      if (lastRowPartitionKey != Long.MaxValue && currentPartitionKey != lastRowPartitionKey) {
        // Add the row
        resultRows += minRow
        first = true
      }
      val value = if (row.isNullAt(orderByColIndex)) {
        Long.MaxValue
      } else {
        SparkRowUtil.getLongFromIndex(orderByColIndex, orderByColType, row)
      }
      if (first || value < minValue) {
        minRow = row
        minValue = value
        first = false
      }
      lastRowPartitionKey = currentPartitionKey
    }

    // Add the row in last partition
    if (minRow != null) {
      resultRows += minRow
    }

    resultRows
  }
}
