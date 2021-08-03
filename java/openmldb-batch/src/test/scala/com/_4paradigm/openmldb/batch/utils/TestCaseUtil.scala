package com._4paradigm.openmldb.batch.utils

import com._4paradigm.openmldb.batch.utils.CaseUtil.{getYamlSchemaString, getYamlTypeString}
import org.apache.spark.sql.types.{BooleanType, ByteType, DateType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, StructField, StructType, TimestampType}
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

    val expectOutput = "\"BooleanType:bool\",\"ShortType:int16\",\"IntegerType:int32\",\"LongType:int64\",\"FloatType:float\"," +
      "\"DoubleType:double\",\"DateType:date\",\"Timestamp:timestamp\",\"StringType:string\""

    assert(result == expectOutput)

    val schema2 = StructType(Seq(
      StructField("ByteType", ByteType)
    ))

    assertThrows[IllegalArgumentException]{
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
    assertThrows[IllegalArgumentException]{getYamlTypeString(ByteType)}

  }
}
