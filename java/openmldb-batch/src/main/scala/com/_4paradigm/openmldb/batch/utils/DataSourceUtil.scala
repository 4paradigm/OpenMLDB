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

import java.util
import com._4paradigm.hybridse.`type`.TypeOuterClass.{ColumnDef, Database, TableDef}
import com._4paradigm.hybridse.node.ConstNode
import com._4paradigm.hybridse.sdk.UnsupportedHybridSeException
import com._4paradigm.hybridse.vm.{PhysicalLoadDataNode, PhysicalOpNode, PhysicalSelectIntoNode}
import com._4paradigm.openmldb.batch.api.OpenmldbSession
import com._4paradigm.openmldb.proto
import com._4paradigm.openmldb.proto.Common
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.functions.{col, first}
import org.apache.spark.sql.types.{BooleanType, DataType, DateType, DoubleType, FloatType, IntegerType, LongType,
  ShortType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, DataFrameReader, Row, SparkSession}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`
import scala.collection.mutable

// util for any data source(defined by format & options)
object DataSourceUtil {
  private val logger = LoggerFactory.getLogger(this.getClass)

  def autoLoad(openmldbSession: OpenmldbSession, file: String, format: String, options: Map[String, String],
               columns: util.List[Common.ColumnDesc]): DataFrame = {
    autoLoad(openmldbSession, file, List.empty[String], format, options, columns, "")
  }

  def autoLoad(openmldbSession: OpenmldbSession, file: String, format: String, options: Map[String, String],
               columns: util.List[Common.ColumnDesc], loadDataSql: String): DataFrame = {
    autoLoad(openmldbSession, file, List.empty[String], format, options, columns, loadDataSql)
  }

  // otherwise isCatalog
  // hdfs files are csv or parquet
  def isFile(format: String): Boolean = {
    format.toLowerCase.equals("csv") || format.toLowerCase.equals("parquet")
  }

  def isCatalog(format: String): Boolean = {
    !isFile(format)
  }

  private def checkSchemaIgnoreNullable(actual: StructType, expect: StructType): Boolean = {
    actual.zip(expect).forall{case (a, b) => (a.name, a.dataType) == (b.name, b.dataType)}
  }

  // Load df from file **and** symbol paths, they should in the same format and options.
  // Decide which load method to use by arg `format`, DO NOT pass `hive://a.b` with format `csv`,
  // the format should be `hive`.
  // Use `parseOptions` in LoadData/SelectInto to get the right format(filePath & option `format`).
  // valid pattern:
  //   1. catalog: discard other options, format supports hive(just schema.table),
  //      custom catalog(<catalog_name>.scham.table, e.g.iceberg)
  //   2. file: local file or hdfs file, format supports csv & parquet, other options take effect
  // We use OpenmldbSession for running sparksql in hiveLoad. If in 4pd Spark distribution, SparkSession.sql
  // will do openmldbSql first, and if DISABLE_OPENMLDB_FALLBACK, we can't use sparksql.
  def autoLoad(openmldbSession: OpenmldbSession, file: String, symbolPaths: List[String], format: String,
               options: Map[String, String], columns: util.List[Common.ColumnDesc], loadDataSql: String = "")
    : DataFrame = {
    val fmt = format.toLowerCase
    if (isCatalog(fmt)) {
      logger.info(s"load data from catalog table, format $fmt, paths: $file $symbolPaths")
      if (file.isEmpty) {
        // no file, read all symbol paths
        var outputDf: DataFrame = null
        symbolPaths.zipWithIndex.foreach { case (path, index) =>
          if (index == 0) {
            outputDf = catalogLoad(openmldbSession, path, columns, loadDataSql)
          } else {
            outputDf = outputDf.union(catalogLoad(openmldbSession, path, columns, loadDataSql))
          }
        }
        outputDf
      } else {
        var outputDf = catalogLoad(openmldbSession, file, columns, loadDataSql)
        for (path: String <- symbolPaths) {
          outputDf = outputDf.union(catalogLoad(openmldbSession, path, columns, loadDataSql))
        }
        outputDf
      }
    } else {
      logger.info("load data from file {} & {} reader[format {}, options {}]", file, symbolPaths, fmt, options)

      if (file.isEmpty) {
        var outputDf: DataFrame = null
        symbolPaths.zipWithIndex.foreach { case (path, index) =>
          if (index == 0) {
            outputDf = autoFileLoad(openmldbSession, path, fmt, options, columns, loadDataSql)
          } else {
            outputDf = outputDf.union(autoFileLoad(openmldbSession, path, fmt, options, columns,
              loadDataSql))
          }
        }
        outputDf
      } else {
        var outputDf = autoFileLoad(openmldbSession, file, fmt, options, columns, loadDataSql)
        for (path: String <- symbolPaths) {
          outputDf = outputDf.union(autoFileLoad(openmldbSession, path, fmt, options, columns,
            loadDataSql))
        }
        outputDf
      }
    }
  }

  // We want df with oriSchema, but if the file format is csv:
  // 1. we support two format of timestamp
  // 2. spark read may change the df schema to all nullable
  // So we should fix it.
  private def autoFileLoad(openmldbSession: OpenmldbSession, file: String, format: String,
    options: Map[String, String], columns: util.List[Common.ColumnDesc], loadDataSql: String): DataFrame = {
    require(format.equals("csv") || format.equals("parquet"), s"unsupported format $format")
    val reader = openmldbSession.getSparkSession.read.options(options)

    val (oriSchema, readSchema, tsCols) = HybridseUtil.extractOriginAndReadSchema(columns)
    var df = if (format.equals("parquet")) {
      // When reading Parquet files, all columns are automatically converted to be nullable for compatibility reasons.
      // ref https://spark.apache.org/docs/3.2.1/sql-data-sources-parquet.html
      val df = if (loadDataSql != null && loadDataSql.nonEmpty) {
        reader.format(format).load(file).createOrReplaceTempView("file")
        openmldbSession.sparksql(loadDataSql)
      } else {
        reader.format(format).load(file)
      }

      require(checkSchemaIgnoreNullable(df.schema, oriSchema),
        s"schema mismatch(ignore nullable), loaded ${df.schema}!= table $oriSchema, check $file")
      // reset nullable property
      df.sqlContext.createDataFrame(df.rdd, oriSchema)
    } else {
      // csv should auto detect the timestamp format
      reader.format(format)
      // use string to read, then infer the format by the first non-null value of the ts column
      val longTsCols = HybridseUtil.parseLongTsCols(reader, readSchema, tsCols, file)
      logger.info(s"read schema: $readSchema, file $file")
      var df = reader.schema(readSchema).load(file)
      if (longTsCols.nonEmpty) {
        // convert long type to timestamp type
        for (tsCol <- longTsCols) {
          logger.debug(s"cast $tsCol to timestamp")
          df = df.withColumn(tsCol, (col(tsCol) / 1000).cast("timestamp"))
        }
      }

      if (loadDataSql != null && loadDataSql.nonEmpty) {
        df.createOrReplaceTempView("file")
        df = openmldbSession.sparksql(loadDataSql)
      }

      if (logger.isDebugEnabled()) {
        logger.debug(s"read dataframe schema: ${df.schema}, count: ${df.count()}")
        df.show(10)
      }

      // if we read non-streaming files, the df schema fields will be set as all nullable.
      // so we need to set it right
      if (!df.schema.equals(oriSchema)) {
        logger.info(s"df schema: ${df.schema}, reset schema")
        df.sqlContext.createDataFrame(df.rdd, oriSchema)
      } else{
        df
      }
    }

    require(df.schema == oriSchema, s"schema mismatch, loaded ${df.schema} != table $oriSchema, check $file")
    df
  }

  // path can have prefix or not, we should remove it if exists
  def catalogDest(path: String): String = {
    path.split("://").last
  }

  private def catalogLoad(openmldbSession: OpenmldbSession, file: String, columns: util.List[Common.ColumnDesc],
                       loadDataSql: String = ""): DataFrame = {
    if (logger.isDebugEnabled()) {
      logger.debug("session catalog {}", openmldbSession.getSparkSession.sessionState.catalog)
      openmldbSession.sparksql("show tables").show()
    }
    // use sparksql to read catalog, no need to try openmldbsql and then fallback to sparksql
    val df = if (loadDataSql != null && loadDataSql.nonEmpty) {
      logger.debug("Try to execute custom SQL for catalog: " + loadDataSql)
      openmldbSession.sparksql(loadDataSql)
    } else {
      openmldbSession.sparksql(s"SELECT * FROM ${catalogDest(file)}")
    }
    if (logger.isDebugEnabled()) {
      logger.debug(s"read dataframe schema: ${df.schema}, count: ${df.count()}")
      df.show(10)
    }

    if (columns != null) {
      val (oriSchema, readSchema, tsCols) = HybridseUtil.extractOriginAndReadSchema(columns)

      require(checkSchemaIgnoreNullable(df.schema, oriSchema), //df.schema == oriSchema, hive table always nullable?
        s"schema mismatch(ignore nullable), loaded hive ${df.schema}!= table $oriSchema, check $file")

      if (!df.schema.equals(oriSchema)) {
        logger.info(s"df schema: ${df.schema}, reset schema")
        df.sqlContext.createDataFrame(df.rdd, oriSchema)
      } else{
        df
      }
    } else {
      df
    }

  }
}
