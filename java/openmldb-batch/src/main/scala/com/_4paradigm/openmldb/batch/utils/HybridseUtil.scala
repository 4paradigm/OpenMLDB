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


object HybridseUtil {
  private val logger = LoggerFactory.getLogger(this.getClass)

  def getOutputSchemaSlices(node: PhysicalOpNode, enableUnsafeRowOpt: Boolean): Array[StructType] = {
    if (enableUnsafeRowOpt) {
      // If enabling UnsafeRowOpt, return row with one slice
      val columnDefs = node.GetOutputSchema()
      Array(getSparkSchema(columnDefs))
    } else {
      (0 until node.GetOutputSchemaSourceSize().toInt).map(i => {
        val columnDefs = node.GetOutputSchemaSource(i).GetSchema()
        getSparkSchema(columnDefs)
      }).toArray
    }
  }

  def getDatabases(tableMap: mutable.Map[String, mutable.Map[String, DataFrame]]): List[Database] = {
    tableMap.map { case (dbName, tableDfMap) =>
      getDatabase(dbName, tableDfMap.toMap)
    }.toList
  }

  def getDatabase(databaseName: String, dict: Map[String, DataFrame]): Database = {
    val databaseBuilder = Database.newBuilder()
    databaseBuilder.setName(databaseName)
    dict.foreach { case (name, df) =>
      databaseBuilder.addTables(getTableDef(name, df))
    }
    databaseBuilder.build()
  }

  def getTableDef(tableName: String, dataFrame: DataFrame): TableDef = {
    val tblBulder = TableDef.newBuilder()
    dataFrame.schema.foreach(field => {
      tblBulder.addColumns(ColumnDef.newBuilder()
        .setName(field.name)
        .setIsNotNull(!field.nullable)
        .setType(DataTypeUtil.sparkTypeToHybridseProtoType(field.dataType))
        .build()
      )
    })
    tblBulder.setName(tableName)
    tblBulder.build()
  }

  def getHybridseSchema(structType: StructType): java.util.List[ColumnDef] = {
    val list = new util.ArrayList[ColumnDef]()
    structType.foreach(field => {
      list.add(ColumnDef.newBuilder()
        .setName(field.name)
        .setIsNotNull(!field.nullable)
        .setType(DataTypeUtil.sparkTypeToHybridseProtoType(field.dataType)).build())
    })
    list
  }

  def getSparkSchema(columns: java.util.List[ColumnDef]): StructType = {
    StructType(columns.asScala.map(col => {
      StructField(col.getName, DataTypeUtil.hybridseProtoTypeToSparkType(col.getType), !col.getIsNotNull)
    }))
  }

  def createGroupKeyComparator(keyIdxs: Array[Int]): (Row, Row) => Boolean = {

    if (keyIdxs.length == 1) {
      val idx = keyIdxs(0)
      (row1, row2) => {
        row1.get(idx) != row2.get(idx)
      }
    } else {
      (row1, row2) => {
        keyIdxs.exists(i => row1.get(i) != row2.get(i))
      }
    }
  }

  def createComparator(idx: Int, dataType: DataType, row1: UnsafeRow, row2: UnsafeRow): Boolean = {
    dataType match {
      case ShortType => row1.getShort(idx) != row2.getShort(idx)
      case IntegerType => row1.getInt(idx) != row2.getInt(idx)
      case LongType => row1.getLong(idx) != row2.getLong(idx)
      case FloatType => row1.getFloat(idx) != row2.getFloat(idx)
      case DoubleType => row1.getDouble(idx) != row2.getDouble(idx)
      case BooleanType => row1.getBoolean(idx) != row2.getBoolean(idx)
      case TimestampType => row1.getLong(idx) != row2.getLong(idx)
      // TODO(tobe): check for date type
      case DateType => row1.getLong(idx) != row2.getLong(idx)
      case StringType => !row1.getString(idx).equals(row2.getString(idx))
    }
  }

  def createUnsafeGroupKeyComparator(keyIdxs: Array[Int], dataTypes: Array[DataType]):
    (UnsafeRow, UnsafeRow) => Boolean = {
    // TODO(tobe): check for different data types

    if (keyIdxs.length == 1) {
      val idx = keyIdxs(0)
      val dataType = dataTypes(0)
      (row1, row2) => createComparator(idx, dataType, row1, row2)
    } else {
      (row1, row2) => {
        keyIdxs.exists(i => {
          val dataType = dataTypes(i)
          createComparator(i, dataType, row1, row2)
        })
      }
    }

  }

  def parseOption(node: ConstNode, default: String, f: (ConstNode, String) => String): String = {
    f(node, default)
  }

  def getBoolOrDefault(node: ConstNode, default: String): String = {
    if (node != null) {
      node.GetBool().toString
    } else {
      default
    }
  }

  def getIntOrDefault(node: ConstNode, default: String): String = {
    if (node != null) {
      node.GetInt().toString
    } else {
      default
    }
  }

  def getIntOrNone(node: ConstNode): Option[Int] = {
    if (node != null) {
      Option(node.GetInt())
    } else {
      None
    }
  }

  def updateOptionsMap(options: mutable.Map[String, String], node: ConstNode, name: String, getValue: ConstNode =>
    String): Unit = {
    if (node != null) {
      options += (name -> getValue(node))
    }
  }

  def getStringOrDefault(node: ConstNode, default: String): String = {
    if (node != null) {
      node.GetStr()
    } else {
      default
    }
  }

  def getBool(node: ConstNode): String = {
    node.GetBool().toString
  }

  def getStr(node: ConstNode): String = {
    node.GetStr()
  }

  def getOptionFromNode[T](node: T, name: String): ConstNode = {
    node match {
      case node1: PhysicalSelectIntoNode => node1.GetOption(name)
      case node1: PhysicalLoadDataNode => node1.GetOption(name)
      case _ => throw new UnsupportedHybridSeException(s"${node.getClass} doesn't support GetOption method")
    }
  }

  // 'file' may change the option 'format':
  // If file starts with 'hive', format is hive, not the detail format in hive
  // If file starts with 'file'/'hdfs', format is the file format
  // result: format, options(spark write/read options), mode is common, if more options, set them to extra map
  def parseOptions[T](file: String, node: T): (String, Map[String, String], String, Map[String, String]) = {
    // load data: read format, select into: write format
    val format = if (file.toLowerCase().startsWith("hive://")) {
      "hive"
    } else {
      parseOption(getOptionFromNode(node, "format"), "csv", getStringOrDefault).toLowerCase
    }

    // load data: read options, select into: write options
    // parquet/hive format doesn't support any option now, consistent with write options(empty) when deep copy
    val options: mutable.Map[String, String] = mutable.Map()
    if (format.equals("csv")){
      // default values: https://spark.apache.org/docs/3.2.1/sql-data-sources-csv.html
      // delimiter -> sep: ,(the same with spark3 default sep)
      // header: true(different with spark)
      // null_value -> nullValue: null(different with spark)
      // quote: `"`(the same with spark3 default quote)
      options += ("header" -> "true")
      options += ("nullValue" -> "null")
      updateOptionsMap(options, getOptionFromNode(node, "delimiter"), "sep", getStr)
      updateOptionsMap(options, getOptionFromNode(node, "header"), "header", getBool)
      updateOptionsMap(options, getOptionFromNode(node, "null_value"), "nullValue", getStr)
      updateOptionsMap(options, getOptionFromNode(node, "quote"), "quote", getStr)
    }
    // load data: write mode(load data may write to offline storage or online storage, needs mode too)
    // select into: write mode
    val modeStr = parseOption(getOptionFromNode(node, "mode"), "error_if_exists", getStringOrDefault).toLowerCase
    val mode = modeStr match {
      case "error_if_exists" => "errorifexists"
      // append/overwrite, stay the same
      case "append" | "overwrite" => modeStr
      case _ => throw new UnsupportedHybridSeException(s"unsupported write mode $modeStr")
    }

    // extra options for some special case
    // only for PhysicalLoadDataNode
    var extraOptions: mutable.Map[String, String] = mutable.Map()
    extraOptions += ("deep_copy" -> parseOption(getOptionFromNode(node, "deep_copy"), "true", getBoolOrDefault))

    // only for select into, "" means N/A
    extraOptions += ("coalesce" -> parseOption(getOptionFromNode(node, "coalesce"), "0", getIntOrDefault))

    extraOptions += ("writer_type") -> parseOption(getOptionFromNode(node, "writer_type"), "single", getStringOrDefault)
    (format, options.toMap, mode, extraOptions.toMap)
  }

  // result 'readSchema' & 'tsCols' is only for csv format, may not be used
  def extractOriginAndReadSchema(columns: util.List[Common.ColumnDesc]): (StructType, StructType, List[String]) = {
    var oriSchema = new StructType
    var readSchema = new StructType
    val tsCols = mutable.ArrayBuffer[String]()
    columns.foreach(col => {
      var ty = col.getDataType
      oriSchema = oriSchema.add(col.getName, SparkRowUtil.protoTypeToScalaType(ty), !col
        .getNotNull)
      if (ty.equals(proto.Type.DataType.kTimestamp)) {
        tsCols += col.getName
        // use string to parse ts column, to avoid getting null(parse wrong format), can't distinguish between the
        // parsed null and the real `null`.
        ty = proto.Type.DataType.kString
      }
      readSchema = readSchema.add(col.getName, SparkRowUtil.protoTypeToScalaType(ty), !col
        .getNotNull)
    }
    )
    logger.debug(s"table schema $oriSchema, may use read schema $readSchema")
    (oriSchema, readSchema, tsCols.toList)
  }

  def parseLongTsCols(reader: DataFrameReader, readSchema: StructType, tsCols: List[String], file: String)
  : List[String] = {
    val longTsCols = mutable.ArrayBuffer[String]()
    if (tsCols.nonEmpty) {
      // normal timestamp format is TimestampType(Y-M-D H:M:S...)
      // and we support one more timestamp format LongType(ms)
      // read one row to auto detect the format, if int64, use LongType to read file, then convert it to TimestampType
      // P.S. don't use inferSchema, cuz we just need to read the first non-null row, not all
      val df = reader.schema(readSchema).load(file)
      // check timestamp cols
      for (col <- tsCols) {
        val i = readSchema.fieldIndex(col)
        var ty: DataType = LongType
        try {
          // value is string, try to parse to long
          df.select(first(df.col(col), ignoreNulls = true)).first().getString(0).toLong
          longTsCols.append(col)
        } catch {
          case e: Any =>
            logger.debug(s"col '$col' parse long failed, use TimestampType to read", e)
            ty = TimestampType
        }

        val newField = StructField(readSchema.fields(i).name, ty, readSchema.fields(i).nullable)
        readSchema.fields(i) = newField
      }
    }
    longTsCols.toList
  }

  def checkSchemaIgnoreNullable(actual: StructType, expect: StructType): Boolean = {
    actual.zip(expect).forall{case (a, b) => (a.name, a.dataType) == (b.name, b.dataType)}
  }

  def autoLoad(openmldbSession: OpenmldbSession, file: String, format: String, options: Map[String, String],
               columns: util.List[Common.ColumnDesc]): DataFrame = {
    autoLoad(openmldbSession, file, List.empty[String], format, options, columns)
  }

  // Load df from file **and** symbol paths, they should in the same format and options.
  // Decide which load method to use by arg `format`, DO NOT pass `hive://a.b` with format `csv`,
  // the format should be `hive`.
  // Use `parseOptions` in LoadData/SelectInto to get the right format(filePath & option `format`).
  // valid pattern:
  //   1. hive path, format must be hive, discard other options
  //   2. file/hdfs path, format supports csv & parquet, other options take effect
  // We use OpenmldbSession for running sparksql in hiveLoad. If in 4pd Spark distribution, SparkSession.sql
  // will do openmldbSql first, and if DISABLE_OPENMLDB_FALLBACK, we can't use sparksql.
  def autoLoad(openmldbSession: OpenmldbSession, file: String, symbolPaths: List[String], format: String,
               options: Map[String, String], columns: util.List[Common.ColumnDesc]): DataFrame = {
    val fmt = format.toLowerCase
    if (fmt.equals("hive")) {
      logger.info(s"load data from hive table $file & $symbolPaths")
      if (file.isEmpty) {
        var outputDf: DataFrame = null
        symbolPaths.zipWithIndex.foreach { case (path, index) =>
          if (index == 0) {
            outputDf = HybridseUtil.hiveLoad(openmldbSession, path, columns);
          } else {
            outputDf = outputDf.union(HybridseUtil.hiveLoad(openmldbSession, path, columns))
          }
        }
        outputDf
      } else {
        var outputDf = HybridseUtil.hiveLoad(openmldbSession, file, columns)
        for (path: String <- symbolPaths) {
          outputDf = outputDf.union(HybridseUtil.hiveLoad(openmldbSession, path, columns))
        }
        outputDf
      }
    } else {
      logger.info("load data from file {} & {} reader[format {}, options {}]", file, symbolPaths, fmt, options)

      if (file.isEmpty) {
        var outputDf: DataFrame = null
        symbolPaths.zipWithIndex.foreach { case (path, index) =>
          if (index == 0) {
            outputDf = HybridseUtil.autoFileLoad(openmldbSession, path, fmt, options, columns);
          } else {
            outputDf = outputDf.union(HybridseUtil.autoFileLoad(openmldbSession, path, fmt, options, columns))
          }
        }
        outputDf
      } else {
        var outputDf = HybridseUtil.autoFileLoad(openmldbSession, file, fmt, options, columns)
        for (path: String <- symbolPaths) {
          outputDf = outputDf.union(HybridseUtil.autoFileLoad(openmldbSession, path, fmt, options, columns))
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
    options: Map[String, String], columns: util.List[Common.ColumnDesc]): DataFrame = {
    require(format.equals("csv") || format.equals("parquet"))
    val reader = openmldbSession.getSparkSession.read.options(options)

    val (oriSchema, readSchema, tsCols) = HybridseUtil.extractOriginAndReadSchema(columns)
    var df = if (format.equals("parquet")) {
      // When reading Parquet files, all columns are automatically converted to be nullable for compatibility reasons.
      // ref https://spark.apache.org/docs/3.2.1/sql-data-sources-parquet.html
      val df = reader.format(format).load(file)
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

  def hiveDest(path: String): String = {
    require(path.toLowerCase.startsWith("hive://"))
    // hive://<table_pattern>
    val tableStartPos = 7
    path.substring(tableStartPos)
  }

  private def hiveLoad(openmldbSession: OpenmldbSession, file: String, columns: util.List[Common.ColumnDesc]):
    DataFrame = {
    if (logger.isDebugEnabled()) {
      logger.debug("session catalog {}", openmldbSession.getSparkSession.sessionState.catalog)
      openmldbSession.sparksql("show tables").show()
    }
    // use sparksql to read hive, no need to try openmldbsql and then fallback to sparksql
    val df = openmldbSession.sparksql(s"SELECT * FROM ${hiveDest(file)}")
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
