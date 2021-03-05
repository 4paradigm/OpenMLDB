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

package com._4paradigm.fesql.spark

import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
  * Helper class to convert schema.
  */
object SchemaUtil {

  val logger = LoggerFactory.getLogger(this.getClass)

  /**
    * Create new DataFrame to rename the columns with duplicated names.
    *
    * @param df
    * @return
    */
  def renameDuplicateColumns(df: DataFrame): DataFrame = {

    val colNames = df.schema.fieldNames.toSeq

    // Check if DataFrame has duplicated columns or not
    if (checkColumnNameDuplication(colNames, caseSensitive = false)) {
      logger.info(s"Got duplicated column names of $colNames")

      val existColumns = mutable.HashSet[String]()
      val newColumnNames = mutable.ArrayBuffer[String]()

      for (colName <- colNames) {
        // Generate the unique column name
        var newColName = colName
        var index = 1
        var shouldRename = false

        while (existColumns.contains(newColName.toLowerCase)) {
          newColName = colName + "_" + index
          index += 1
          shouldRename = true
        }

        if (shouldRename) {
          logger.info(s"Rename the column from: $colName to: $newColName")
        }

        existColumns += newColName.toLowerCase
        newColumnNames.append(newColName)
      }

      // Return the new DataFrame with unique column names
      logger.info(s"Finally rename the columns from: $colNames to: $newColumnNames")
      df.toDF(newColumnNames:_*)
    } else {
      df
    }
  }

  /**
    * Check if it has duplicate column names or not.
    *
    * @param columnNames
    * @param caseSensitive
    * @return
    */
  def checkColumnNameDuplication(columnNames: Seq[String], caseSensitive: Boolean = true): Boolean = {
    val names = if (caseSensitive) columnNames else columnNames.map(_.toLowerCase)
    names.distinct.length != names.length
  }

}
