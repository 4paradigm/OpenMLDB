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

import com._4paradigm.hybridse.sdk.HybridSeException
import com._4paradigm.hybridse.node.JoinType
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.functions.{col, lit, monotonically_increasing_id}
import org.apache.spark.sql.types.{LongType, StructType}
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.slf4j.LoggerFactory
import scala.collection.mutable.ArrayBuffer


object SparkUtil {

  private val logger = LoggerFactory.getLogger(this.getClass)

  // Check if we can use native last join optimization
  def supportNativeLastJoin(joinType: JoinType, hasOrderby: Boolean): Boolean = {
    if (hasOrderby) {
      logger.info("Has order by column and do not support native last join")
      false
    } else if (joinType != JoinType.kJoinTypeLast && joinType != JoinType.kJoinTypeConcat) {
      logger.info("Join type is neighter last join or concat join and do not support native last join")
      false
    } else {
      try {
        org.apache.spark.sql.catalyst.plans.JoinType("last")
        logger.info("Use custom Spark distribution and support native last join")
        true
      } catch {
        case _: IllegalArgumentException => {
          logger.info("Do not support native last join and use original last join")
          false
        }
      }
    }
  }

  // Add the index column for Spark DataFrame
  def addIndexColumn(spark: SparkSession, df: DataFrame, indexColName: String, method: String): DataFrame = {
    logger.info("Add the indexColName(%s) to Spark DataFrame(%s)".format(indexColName, df.toString()))

    method.toLowerCase() match {
      case "zipwithuniqueid" | "zip_withunique_id" => addColumnByZipWithUniqueId(spark, df, indexColName)
      case "zipwithindex" | "zip_with_index" => addColumnByZipWithIndex(spark, df, indexColName)
      case "monotonicallyincreasingid" | "monotonically_increasing_id" =>
        addColumnByMonotonicallyIncreasingId(spark, df, indexColName)
      case _ => throw new HybridSeException("Unsupported add index column method: " + method)
    }

  }

  def addColumnByZipWithUniqueId(spark: SparkSession, df: DataFrame, indexColName: String = null): DataFrame = {
    logger.info("Use zipWithUniqueId to generate index column")
    val indexedRDD = df.rdd.zipWithUniqueId().map {
      case (row, id) => Row.fromSeq(row.toSeq :+ id)
    }
    spark.createDataFrame(indexedRDD, df.schema.add(indexColName, LongType))
  }

  def addColumnByZipWithIndex(spark: SparkSession, df: DataFrame, indexColName: String = null): DataFrame = {
    logger.info("Use zipWithIndex to generate index column")
    val indexedRDD = df.rdd.zipWithIndex().map {
      case (row, id) => Row.fromSeq(row.toSeq :+ id)
    }
    spark.createDataFrame(indexedRDD, df.schema.add(indexColName, LongType))
  }

  def addColumnByMonotonicallyIncreasingId(spark: SparkSession,
                                           df: DataFrame, indexColName: String = null): DataFrame = {
    logger.info("Use monotonicallyIncreasingId to generate index column")
    df.withColumn(indexColName, monotonically_increasing_id())
  }

  def checkSchemaIgnoreNullable(schema1: StructType, schema2: StructType): Boolean = {
    // Check field size
    if (schema1.fields.size != schema2.fields.size) {
      logger.warn("Scheme size not match, schema1: %d, schema2: %d".format(schema1.fields.size, schema2.fields.size))
      return false
    }

    // Check field name and type, but not nullable
    val fieldSize = schema1.fields.size
    for (i <- 0 until fieldSize) {
      val field1 = schema1.fields(i)
      val field2 = schema2.fields(i)
      if (field1.name != field2.name || field1.dataType != field2.dataType) {
        logger.warn("Schema name or type not match, filed(%s %s) and field(%s %s)"
          .format(field1.dataType, field1.name, field2.dataType, field2.name))
        return false
      }
    }

    true
  }

  /** Check if the dataframes are equal or not for small dataset.
   *
   * @param df1
   * @param df2
   * @return
   */
  def smallDfEqual(df1: DataFrame, df2: DataFrame): Boolean = {
    if (df1.schema != df2.schema) {
      return false
    }
    df1.collect().sameElements(df2.collect())
  }

  /** Approximately check if the dataframes are equal or not.
   *
   * Notice that this can not handle the dataframes which have duplicated rows.
   *
   * @param df1
   * @param df2
   * @return
   */
  def approximateDfEqual(df1: DataFrame, df2: DataFrame, checkSchema: Boolean = true): Boolean = {
    if (checkSchema) {
      if (df1.schema != df2.schema) {
        return false
      }
    }
    df1.except(df2).isEmpty && df2.except(df1).isEmpty
  }

  /** Use Java reflect to call private method to convert RDD[InternalRow] to DataFrame.
   *
   * @param spark
   * @param internalRowRdd
   * @param schema
   * @return
   */
  def rddInternalRowToDf(spark: SparkSession, internalRowRdd: RDD[InternalRow], schema: StructType): DataFrame = {
    val sparkSessionClass = Class.forName("org.apache.spark.sql.SparkSession")
    val internalCreateDataFrameMethod = sparkSessionClass
      .getDeclaredMethod(s"internalCreateDataFrame",
        classOf[RDD[InternalRow]],
        classOf[StructType], classOf[Boolean])
    internalCreateDataFrameMethod.invoke(spark, internalRowRdd, schema, false: java.lang.Boolean)
      .asInstanceOf[DataFrame]
  }

  def disableSparkLog(): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF);
    Logger.getLogger("akka").setLevel(Level.OFF);
  }

}
