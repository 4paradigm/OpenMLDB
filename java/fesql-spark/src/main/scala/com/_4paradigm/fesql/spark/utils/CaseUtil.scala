/*
 * CaseUtil.scala
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
