package com._4paradigm.fesql.offline.utils

import com._4paradigm.fesql.offline.FeSQLException
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
        throw new FeSQLException(s"Illegal window key type: $sparkType")
    }
  }
}
