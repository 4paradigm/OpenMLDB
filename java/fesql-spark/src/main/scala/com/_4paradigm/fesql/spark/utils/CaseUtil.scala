package com._4paradigm.fesql.spark.utils

import org.apache.spark.sql.types._

object CaseUtil {

  def getYamlSchemaString(schema: StructType): String = {
    schema.map(field => "\"" + field.name + ":" + getYamlTypeString(field.dataType) + "\"").mkString(",")
  }

  def getYamlTypeString(dtype: DataType): String = {
    dtype match {
      case BooleanType => "bool"
      case ShortType => "int16"
      case IntegerType => "int32"
      case LongType => "int64"
      case FloatType => "float"
      case DoubleType => "double"
      case DateType => "date"
      case TimestampType => "timestamp"
      case StringType => "string"
      case _ => throw new IllegalArgumentException(s"Unsupported spark type $dtype")
    }
  }


}
