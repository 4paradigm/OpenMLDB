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
import com._4paradigm.openmldb.batch.utils.SparkRowUtil.createOrderKeyExtractor
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ByteType, DateType, IntegerType, LongType, ShortType, TimestampType}
import org.scalatest.FunSuite

import java.sql.{Date, Timestamp}

class TestSparkRowUtil extends FunSuite {
  test("Test createOrderKeyExtractor") {
    val shortRow = Row.apply(1.toShort)
    assert(createOrderKeyExtractor(0, ShortType, nullable = false).apply(shortRow) == 1L)

    val intRow = Row.apply(1)
    assert(createOrderKeyExtractor(0, IntegerType, nullable = false).apply(intRow) == 1L)

    val longRow = Row.apply(1L)
    assert(createOrderKeyExtractor(0, LongType, nullable = false).apply(longRow) == 1L)

    val timestamp = Timestamp.valueOf("2000-01-01 0:0:0")
    val timestampRow = Row.apply(timestamp)
    assert(createOrderKeyExtractor(0, TimestampType, nullable = false).apply(timestampRow) == timestamp.getTime)

    val date = Date.valueOf("2000-01-01")
    val dateRow = Row.apply(date)
    assert(createOrderKeyExtractor(0, DateType, nullable = false).apply(dateRow) == date.getTime)

    val byteRow = Row.apply(1.toByte)
    assertThrows[HybridSeException](createOrderKeyExtractor(0, ByteType, nullable = false).apply(byteRow))
  }
}
