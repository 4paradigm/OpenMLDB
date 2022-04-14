package com._4paradigm.openmldb.batchjob.tools

import com._4paradigm.openmldb.batch.utils.DataTypeUtil
import com._4paradigm.openmldb.sdk.SdkOption
import com._4paradigm.openmldb.sdk.impl.SqlClusterExecutor
import org.apache.spark.sql.SparkSession

object CreateTableFromParquet {

  def main(args: Array[String]): Unit = {

    val openmldbZk = "127.0.0.1:2181"
    val openmldbZkPath = "/openmldb"
    val dbName = "db1"
    val tableName = "t1"
    val parquetPath = "file:///tmp/parquet_path/"

    createTableFromParquet(openmldbZk, openmldbZkPath, dbName, tableName, parquetPath)
  }

  def createTableFromParquet(openmldbZk: String, openmldbZkPath: String, dbName: String, tableName: String,
                             parquetPath: String): Unit = {

    // Read parquet files
    val spark = SparkSession.builder().getOrCreate()
    val df = spark.read.parquet(parquetPath)
    val schema = df.schema

    // Connect with OpenMLDB
    val option = new SdkOption
    option.setZkCluster(openmldbZk)
    option.setZkPath(openmldbZkPath)
    val sqlExecutor = new SqlClusterExecutor(option);

    // Generate SQL
    val createDbSql = s"CREATE DATABASE $dbName"
    val useDbSql = s"USE $dbName"

    var createTableSql = s"CREATE TABLE $tableName ("
    schema.map(structField => {
      val colName = structField.name
      val colType = DataTypeUtil.sparkTypeToString(structField.dataType)
      createTableSql += s"$colName $colType, "
    })
    createTableSql = createTableSql.subSequence(0, createTableSql.size - 2) + ")"

    val loadDataSql = s"LOAD DATA INFILE '$parquetPath' INTO TABLE $tableName OPTIONS (format='parquet')"

    // Run SQL
    val state = sqlExecutor.getStatement();
    try {
      state.execute(createDbSql)
      state.execute(useDbSql)
      state.execute(createTableSql)
      state.execute(loadDataSql)
    } catch {
      case e: Exception => e.printStackTrace();
    } finally {
      state.close()
    }

    val tables = sqlExecutor.getTableNames(dbName)
    tables.forEach(tableName => {
      println(s"Table name: $tableName")
    })

    spark.close()
  }

}
