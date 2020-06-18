package com._4paradigm.fesql.offline

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