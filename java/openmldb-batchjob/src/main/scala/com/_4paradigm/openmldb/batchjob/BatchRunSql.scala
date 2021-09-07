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
import org.slf4j.LoggerFactory

object BatchRunSql {

  private val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    if (args.length < 4) {
      throw new Exception(s"Number of arguments is wrong, args: ${args.mkString(",")}")
    }

    val metastoreEndpoint = args(0)
    val sql = args(1)
    val dbName = args(2)
    val outputTableName = args(3)

    batchRunSql(metastoreEndpoint, sql, dbName, outputTableName)
  }

  def batchRunSql(metastoreEndpoint: String, sql: String, dbName: String, tableName: String): Unit = {

    val icebergCatalogName = "spark_catalog"

    val spark = SparkSession.builder()
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
      .config("spark.sql.catalog.spark_catalog.type", "hive")
      .config("hive.metastore.uris",  metastoreEndpoint)
      .enableHiveSupport()
      .getOrCreate()

    spark.catalog.listDatabases().collect().flatMap(db => {
      spark.catalog.listTables(db.name).collect().map(table => {
        val fullyQualifiedName = s"${db.name}.${table.name}"
        logger.info(s"Register table $fullyQualifiedName as ${table.name}")
        spark.table(fullyQualifiedName).createOrReplaceTempView(table.name)
      })
    })

    val df = spark.sql(sql)
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
