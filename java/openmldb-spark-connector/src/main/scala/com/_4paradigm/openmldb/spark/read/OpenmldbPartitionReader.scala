package com._4paradigm.openmldb.spark.read

import com._4paradigm.openmldb.sdk.{Schema, SdkOption}
import com._4paradigm.openmldb.sdk.impl.SqlClusterExecutor
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.unsafe.types.UTF8String

import java.sql.Types

class OpenmldbPartitionReader(config: OpenmldbReadConfig) extends PartitionReader[InternalRow] {

  val option = new SdkOption
  option.setZkCluster(config.zkCluster)
  option.setZkPath(config.zkPath)
  option.setIsLight(true)
  val executor = new SqlClusterExecutor(option)
  val dbName: String = config.dbName
  val tableName: String = config.tableName

  val schema: Schema = executor.getTableSchema(dbName, tableName)
  executor.executeSQL(dbName, "SET @@execute_mode='online'")
  val resultSet = executor.executeSQL(dbName, s"SELECT * FROM ${tableName}")

  override def next(): Boolean = {
    resultSet.next()
  }

  override def get(): InternalRow = {
    val rowData = 0.until(schema.getColumnList.size()).map(index => {
      val columnSqlType = schema.getColumnList.get(index).getSqlType
      val columnValue = columnSqlType match {
        case Types.BOOLEAN => resultSet.getBoolean(index + 1)
        case Types.SMALLINT => resultSet.getShort(index + 1)
        case Types.INTEGER => resultSet.getInt(index + 1)
        case Types.BIGINT => resultSet.getLong(index + 1)
        case Types.FLOAT => resultSet.getFloat(index + 1)
        case Types.DOUBLE => resultSet.getDouble(index + 1)
        // Convert java string to UnsafeRow string
        case Types.VARCHAR => UTF8String.fromString(resultSet.getString(index + 1))
        // TODO: Convert date to UnsafeRow int
        case Types.DATE => resultSet.getDate(index + 1).getTime.asInstanceOf[Int]
        case Types.TIMESTAMP => resultSet.getTimestamp(index + 1).getTime
        case _ => throw new Exception(s"Unsupported sql type ${columnSqlType}")
      }

      if (columnSqlType == null) {
        // TODO: Should not pass null to InternalRow
        columnValue
      } else {
        columnValue
      }
    })

    InternalRow(rowData.toList:_*)
  }

  override def close(): Unit = {
    executor.close()
  }
}
