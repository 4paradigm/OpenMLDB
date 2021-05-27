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

package com._4paradigm.hybridsql.spark.utils

import com._4paradigm.hybridse.common.HybridSEException
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._


object SparkRowUtil {

  def createOrderKeyExtractor(keyIdx: Int, sparkType: DataType, nullable: Boolean): Row => Long = {
    sparkType match {
      case ShortType => row: Row => row.getShort(keyIdx).toLong
      case IntegerType => row: Row => row.getInt(keyIdx).toLong
      case LongType => row: Row => row.getLong(keyIdx)
      case TimestampType => row: Row => row.getTimestamp(keyIdx).getTime
      case DateType => row: Row=>row.getDate(keyIdx).getTime
      case _ =>
        throw new HybridSEException(s"Illegal window key type: $sparkType")
    }
  }
}
