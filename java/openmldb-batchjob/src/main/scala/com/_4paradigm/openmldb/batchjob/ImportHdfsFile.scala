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

package com._4paradigm.openmldb.batchjob

import org.apache.iceberg.PartitionSpec
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.hive.HiveCatalog
import org.apache.iceberg.spark.SparkSchemaUtil
import org.apache.spark.sql.SparkSession

object ImportHdfsFile {

  def main(args: Array[String]): Unit = {
    if (args.length < 5) {
      throw new Exception(s"Number of arguments is wrong, args: ${args.mkString(",")}")
    }

    val metastoreEndpoint = args(0)
    val fileType = args(1)
    val filePath = args(2)
    val dbName = args(3)
    val tableName = args(4)

    importHdfsFile(metastoreEndpoint, fileType, filePath, dbName, tableName)
  }

  def importHdfsFile(metastoreEndpoint: String, fileType: String, filePath: String, dbName: String, tableName: String): Unit = {

    val icebergCatalogName = "spark_catalog"

    val spark = SparkSession.builder()
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
      .config("spark.sql.catalog.spark_catalog.type", "hive")
      .config("hive.metastore.uris",  metastoreEndpoint)
      .enableHiveSupport()
      .getOrCreate()

    // TODO: Support file type like CSV

    val df = spark.read.parquet(filePath)
    df.createOrReplaceTempView(tableName)

    // Create iceberg table
    val conf = spark.sessionState.newHadoopConf()
    val catalog = new HiveCatalog(conf)
    val icebergSchema = SparkSchemaUtil.schemaForTable(spark, tableName)
    val partitionSpec = PartitionSpec.builderFor(icebergSchema).build()
    val tableIdentifier = TableIdentifier.of(dbName, tableName)

    val icebergTable = catalog.createTable(tableIdentifier, icebergSchema, partitionSpec)
    print(icebergTable)

    // Append data
    val icebergTableName = s"$icebergCatalogName.$dbName.$tableName"
    spark.table(tableName).writeTo(icebergTableName).append()

    spark.close()

  }

}
