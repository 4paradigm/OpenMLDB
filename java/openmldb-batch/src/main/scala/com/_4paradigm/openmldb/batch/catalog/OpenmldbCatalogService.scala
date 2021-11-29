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

package com._4paradigm.openmldb.batch.catalog

import com._4paradigm.hybridse.LibraryLoader
import com._4paradigm.openmldb.{SQLRouterOptions, Status, VectorString, sql_router_sdk}

import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

class OpenmldbCatalogService(val zkCluster: String, val zkPath: String) {

  {
    LibraryLoader.loadLibrary("sql_jsdk")
  }

  val sqlOpt = new SQLRouterOptions
  sqlOpt.setZk_cluster(zkCluster)
  sqlOpt.setZk_path(zkPath)

  val sqlRouter = sql_router_sdk.NewClusterSQLRouter(sqlOpt)
  sqlOpt.delete()
  if (sqlRouter == null) {
    throw new Exception("fail to create sql executor")
  }

  def getDatabases(): List[String] = {
    val databases = new VectorString()
    val status = new Status

    sqlRouter.ShowDB(databases, status)
    if (status.getCode == 0) {
      databases.toList
    } else {
      null
    }

  }

  def getTables(dbName: String): List[String] = {
    val tables = new VectorString()
    val status = new Status

    sqlRouter.ShowDbTables(dbName, tables, status)
    if (status.getCode == 0) {
      tables.toList
    } else {
      null
    }
  }

}
