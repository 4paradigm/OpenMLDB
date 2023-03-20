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
package com._4paradigm.openmldb.batch.nodes

import com._4paradigm.hybridse.node.CreateTableLikeClause.LikeKind
import com._4paradigm.hybridse.sdk.UnsupportedHybridSeException
import com._4paradigm.hybridse.vm.PhysicalCreateTableNode
import com._4paradigm.openmldb.batch.utils.{HybridseUtil, OpenmldbTableUtil}
import com._4paradigm.openmldb.batch.{PlanContext, SparkInstance}
import org.slf4j.LoggerFactory

object CreateTablePlan {
  private val logger = LoggerFactory.getLogger(this.getClass)

  def gen(ctx: PlanContext, node: PhysicalCreateTableNode): SparkInstance = {

    val tableName = node.getData_.GetTableName()

    val dbName = if (!node.getData_.GetDatabase().equals("")) {
      node.getData_.GetDatabase()
    } else {
      logger.info(s"Use the default db name: ${ctx.getConf.defaultDb}")
      ctx.getConf.defaultDb
    }

    val likeKind = node.getData_.GetLikeKind()
    if (node.getData_.getLike_clause_ == null) {
      throw new UnsupportedHybridSeException(s"Only support CREATE TABLE LIKE for offline")
    }

    val df = likeKind match {
      case LikeKind.HIVE =>
        val hivePath = node.getData_.GetLikePath()
        HybridseUtil.autoLoad(ctx.getOpenmldbSession, hivePath, "hive", Map[String, String](), null)
      case LikeKind.PARQUET =>
        val parquetPath = node.getData_.GetLikePath()
        ctx.getSparkSession.read.parquet(parquetPath)
      case _ => throw new UnsupportedHybridSeException(s"The LikeKind type $likeKind is not supported")
    }

    OpenmldbTableUtil.createOpenmldbTableFromDf(ctx.getOpenmldbSession, df, dbName, tableName)

    val spark = ctx.getSparkSession
    SparkInstance.fromDataFrame(spark.emptyDataFrame)
  }
}
