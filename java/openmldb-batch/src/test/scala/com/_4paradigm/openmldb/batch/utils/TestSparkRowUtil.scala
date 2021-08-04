package com._4paradigm.openmldb.batch.utils

import com._4paradigm.hybridse.sdk.HybridSeException
import com._4paradigm.openmldb.batch.utils.SparkRowUtil.createOrderKeyExtractor
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ByteType, DateType, IntegerType, LongType, ShortType, TimestampType}
import org.scalatest.FunSuite

import java.sql.{Date, Timestamp}

class TestSparkRowUtil extends FunSuite{
  test("Test createOrderKeyExtractor") {
    val shortRow = Row.apply(1.toShort)
    assert(createOrderKeyExtractor(0,ShortType,false).apply(shortRow) == 1L)

    val intRow = Row.apply(1)
    assert(createOrderKeyExtractor(0,IntegerType,false).apply(intRow) == 1L)

    val longRow = Row.apply(1L)
    assert(createOrderKeyExtractor(0,LongType,false).apply(longRow) == 1L)

    val timestamp = Timestamp.valueOf("2000-01-01 0:0:0")
    val timestampRow = Row.apply(timestamp)
    assert(createOrderKeyExtractor(0,TimestampType,false).apply(timestampRow) == timestamp.getTime)

    val date = Date.valueOf("2000-01-01")
    val dateRow = Row.apply(date)
    assert(createOrderKeyExtractor(0,DateType,false).apply(dateRow) == date.getTime)

    val byteRow = Row.apply(1.toByte)
    assertThrows[HybridSeException](createOrderKeyExtractor(0,ByteType,false).apply(byteRow))
  }
}
