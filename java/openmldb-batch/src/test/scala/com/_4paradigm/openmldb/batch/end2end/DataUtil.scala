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

package com._4paradigm.openmldb.batch.end2end

import org.apache.spark.sql.types.{BooleanType, DateType, DoubleType, FloatType, IntegerType, LongType, ShortType,
  StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import java.sql.{Date, Timestamp}

object DataUtil {

  def getStringDf(spark: SparkSession): DataFrame = {
    val data = Seq(
      Row(1, "abc", 100)
    )
    val schema = StructType(List(
      StructField("int_col", IntegerType),
      StructField("str_col", StringType),
      StructField("int_col2", IntegerType)
    ))
    spark.createDataFrame(spark.sparkContext.makeRDD(data), schema)
  }

  def getTestDf(spark: SparkSession): DataFrame = {
    val data = Seq(
      Row(1, "tom", 100L, 1),
      Row(2, "tom", 200L, 2),
      Row(3, "tom", 300L, 3),
      Row(4, "amy", 400L, 4),
      Row(5, "amy", 500L, 5))
    val schema = StructType(List(
      StructField("id", IntegerType),
      StructField("name", StringType),
      StructField("trans_amount", LongType),
      StructField("trans_time", IntegerType)))
    spark.createDataFrame(spark.sparkContext.makeRDD(data), schema)
  }

  def getAllTypesDfWithNull(spark: SparkSession): DataFrame = {
    val data = Seq(
      Row(true, 1.toShort, 1, 1L, 1.0F, 1.0D, new Timestamp(1), new Date(1), "string1"),
      Row(null, null, null, null, null, null, null, null, null))
    val schema = StructType(List(
      StructField("bool_col", BooleanType),
      StructField("short_col", ShortType),
      StructField("int_col", IntegerType),
      StructField("long_col", LongType),
      StructField("float_col", FloatType),
      StructField("double_col", DoubleType),
      StructField("timestamp_col", TimestampType),
      StructField("date_col", DateType),
      StructField("string_col", StringType)
    ))
    spark.createDataFrame(spark.sparkContext.makeRDD(data), schema)
  }

}
