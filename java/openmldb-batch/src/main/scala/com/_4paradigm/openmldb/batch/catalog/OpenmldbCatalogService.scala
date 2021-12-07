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

import com._4paradigm.openmldb.sdk.impl.SqlClusterExecutor
import com._4paradigm.openmldb.sdk.SdkOption

class OpenmldbCatalogService(val zkCluster: String, val zkPath: String) {

  val option = new SdkOption
  option.setZkPath(zkCluster)
  option.setZkCluster(zkPath)

  val sqlExecutor = new SqlClusterExecutor(option)

  def getDatabases(): java.util.List[String] = {
    sqlExecutor.showDatabases()
  }

  def getTableNames(db: String): java.util.List[String] = {
    sqlExecutor.getTableNames(db)
  }



}
