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

import com._4paradigm.openmldb.batch.api.OpenmldbSession
import org.apache.spark.sql.DataFrame

object OpenmldbTableUtil {

  /***
   * Create OpenMLDB Table From Spark DataFrame.
   *
   * @param sess
   * @param df
   * @param dbName
   * @param tableName
   */
  def createOpenmldbTableFromDf(sess: OpenmldbSession, df: DataFrame, dbName: String, tableName: String): Unit = {

    if (sess.openmldbCatalogService != null) {
      val sqlExecutor = sess.openmldbCatalogService.sqlExecutor

      val schema = df.schema

      var createTableSql = s"CREATE TABLE $tableName ("
      schema.map(structField => {
        val colName = structField.name
        val colType = DataTypeUtil.sparkTypeToString(structField.dataType)
        createTableSql += s"$colName $colType, "
      })
      createTableSql = createTableSql.subSequence(0, createTableSql.size - 2) + ")"

      sqlExecutor.executeDDL(dbName, createTableSql)
    } else {
      throw new Exception("openmldbCatalogService is null and check openmldb zk config")
    }

  }

}
