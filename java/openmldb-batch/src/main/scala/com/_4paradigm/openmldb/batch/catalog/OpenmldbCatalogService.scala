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

import com._4paradigm.openmldb.proto.NS
import com._4paradigm.openmldb.sdk.impl.SqlClusterExecutor
import com._4paradigm.openmldb.sdk.SdkOption
import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.mutable

class OpenmldbCatalogService(val zkCluster: String, val zkPath: String) {

  val option = new SdkOption
  option.setZkCluster(zkCluster)
  option.setZkPath(zkPath)

  val sqlExecutor = new SqlClusterExecutor(option)

  def getDatabases: Array[String] = {
    sqlExecutor.showDatabases().asScala.toArray
  }

  def getTableNames(db: String): Array[String] = {
    sqlExecutor.getTableNames(db).asScala.toArray
  }

  def getTableInfos(db: String): Array[NS.TableInfo] = {
    // TODO: Optimize to get all table info once(actually getting from the cache would not take so long)
    //  ref GetTables(db)
    val tableNames = sqlExecutor.getTableNames(db)

    val tableInfos = new mutable.ArrayBuffer[NS.TableInfo](tableNames.size())
    tableNames.forEach(tableName =>
      tableInfos.append(sqlExecutor.getTableInfo(db, tableName))
    )

    tableInfos.toArray
  }

  def getTableInfo(db: String, table: String): NS.TableInfo = {
    sqlExecutor.getTableInfo(db, table)
  }

  def updateOfflineTableInfo(info: NS.TableInfo): Boolean = {
    sqlExecutor.updateOfflineTableInfo(info)
  }

}
