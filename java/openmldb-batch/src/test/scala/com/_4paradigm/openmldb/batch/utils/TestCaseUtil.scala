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

import com._4paradigm.openmldb.batch.utils.CaseUtil.{getYamlSchemaString, getYamlTypeString}
import org.apache.spark.sql.types.{
  BooleanType, ByteType, DateType, DoubleType, FloatType, IntegerType,
  LongType, ShortType, StringType, StructField, StructType, TimestampType
}
import org.scalatest.FunSuite


class TestCaseUtil extends FunSuite {

  test("Test getYamlSchemaString") {
    val schema1 = StructType(Seq(
      StructField("BooleanType", BooleanType),
      StructField("ShortType", ShortType),
      StructField("IntegerType", IntegerType),
      StructField("LongType", LongType),
      StructField("FloatType", FloatType),
      StructField("DoubleType", DoubleType),
      StructField("DateType", DateType),
      StructField("Timestamp", TimestampType),
      StructField("StringType", StringType)
    ))

    val result = getYamlSchemaString(schema1)

    val expectOutput =""""BooleanType:bool","ShortType:int16","IntegerType:int32",""" +
        """"LongType:int64","FloatType:float","DoubleType:double","DateType:date",""" +
        """"Timestamp:timestamp","StringType:string""""

    assert(result == expectOutput)

    val schema2 = StructType(Seq(
      StructField("ByteType", ByteType)
    ))

    assertThrows[IllegalArgumentException] {
      getYamlSchemaString(schema2)
    }
  }

  test("Test getYamlTypeString") {
    assert(getYamlTypeString(BooleanType) == "bool")
    assert(getYamlTypeString(ShortType) == "int16")
    assert(getYamlTypeString(IntegerType) == "int32")
    assert(getYamlTypeString(LongType) == "int64")
    assert(getYamlTypeString(FloatType) == "float")
    assert(getYamlTypeString(DoubleType) == "double")
    assert(getYamlTypeString(DateType) == "date")
    assert(getYamlTypeString(TimestampType) == "timestamp")
    assert(getYamlTypeString(StringType) == "string")
    assertThrows[IllegalArgumentException] {
      getYamlTypeString(ByteType)
    }
  }
}
