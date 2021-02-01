package com._4paradigm.fesql.spark.utils

import com._4paradigm.fesql.common.FesqlException
import com._4paradigm.fesql.node.JoinType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.slf4j.LoggerFactory
import org.apache.spark.sql.functions.monotonically_increasing_id

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
      case "monotonicallyincreasingid" | "monotonically_increasing_id" => addColumnByMonotonicallyIncreasingId(spark, df, indexColName)
      case _ => throw new FesqlException("Unsupported add index column method: " + method)
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

  def addColumnByMonotonicallyIncreasingId(spark: SparkSession, df: DataFrame, indexColName: String = null): DataFrame = {
    logger.info("Use monotonicallyIncreasingId to generate index column")
    df.withColumn(indexColName, monotonically_increasing_id())
  }

}
