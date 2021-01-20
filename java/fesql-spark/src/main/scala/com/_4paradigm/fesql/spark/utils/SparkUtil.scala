package com._4paradigm.fesql.spark.utils

import com._4paradigm.fesql.node.JoinType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.slf4j.LoggerFactory


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
  def addIndexColumn(spark: SparkSession, df: DataFrame, indexColName: String = null): DataFrame = {
    val indexedRDD = df.rdd.zipWithIndex().map {
      case (row, id) => Row.fromSeq(row.toSeq :+ id)
    }
    spark.createDataFrame(indexedRDD, df.schema.add(indexColName, LongType))
  }

}
