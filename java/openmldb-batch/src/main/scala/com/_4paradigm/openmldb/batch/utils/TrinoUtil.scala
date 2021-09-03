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

import java.sql.ResultSet


object TrinoUtil {

  def printResultSet(resultSet: ResultSet, num: Int = 20): Unit = {
    val metaData = resultSet.getMetaData()

    // Print header
    for (i <- 1 to metaData.getColumnCount) {
      System.out.print(" | ")
      // Notice that the index starts from 1 instead of 0
      System.out.print(metaData.getColumnName(i))
    }
    System.out.print(" | ")

    // Print rows
    val columnCount = metaData.getColumnCount
    var rowCnt = 0
    val s = StringBuilder.newBuilder
    while (rowCnt < num && resultSet.next() ) {

      s.clear()
      if (rowCnt == 0) {
        s.append("| ")
        for (i <- 1 to columnCount) {
          val name = metaData.getColumnName(i)
          s.append(name)
          s.append("| ")
        }
        s.append("\n")
      }
      rowCnt += 1
      s.append("| ")
      for (i <- 1 to columnCount) {
        if (i > 1) {
          s.append(" | ")
        }
        s.append(resultSet.getString(i))
      }
      s.append(" |")
      System.out.println(s)
    }

  }

}
