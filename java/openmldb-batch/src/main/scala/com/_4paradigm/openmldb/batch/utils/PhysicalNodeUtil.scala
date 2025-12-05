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

import com._4paradigm.hybridse.node.{OrderByNode, OrderExpression}
import com._4paradigm.hybridse.sdk.HybridSeException
import com._4paradigm.hybridse.vm.{PhysicalOpNode, PhysicalSortNode, PhysicalWindowAggrerationNode}
import org.apache.spark.sql.{Column, DataFrame}

import scala.collection.mutable


object PhysicalNodeUtil {

  /** Access the WindowAggNode and generate the list of Spark Column for repartition.
   *
   * This function will take the properties of WindowAggNode
   * and resolve to Spark Columns which will be use for repartition.
   */
  def getRepartitionColumns(windowAggNode: PhysicalWindowAggrerationNode, inputDf: DataFrame)
  : mutable.ArrayBuffer[Column] = {
    val windowOp = windowAggNode.window()
    val repartitionExprs = windowOp.partition().keys()

    val repartitionCols = mutable.ArrayBuffer[Column]()
    for (i <- 0 until repartitionExprs.GetChildNum()) {
      val expr = repartitionExprs.GetChild(i)
      val colIdx = SparkColumnUtil.resolveColumnIndex(expr, windowAggNode.GetProducer(0))
      repartitionCols += SparkColumnUtil.getColumnFromIndex(inputDf, colIdx)
    }

    repartitionCols
  }

  /** Like getRepartitionColumns but return the list of names of the columns.
   *
   */
  def getRepartitionColumnNames(windowAggNode: PhysicalWindowAggrerationNode, inputDf: DataFrame)
  : mutable.ArrayBuffer[String] = {
    val windowOp = windowAggNode.window()
    val repartitionExprs = windowOp.partition().keys()

    val repartitionColNames = mutable.ArrayBuffer[String]()
    for (i <- 0 until repartitionExprs.GetChildNum()) {
      val expr = repartitionExprs.GetChild(i)
      val colIdx = SparkColumnUtil.resolveColumnIndex(expr, windowAggNode.GetProducer(0))
      repartitionColNames += inputDf.schema.apply(colIdx).name
    }

    repartitionColNames
  }

  /** Like getRepartitionColumnNames but return the list of indexes of the columns.
   *
   */
  def getRepartitionColumnIndexes(windowAggNode: PhysicalWindowAggrerationNode, inputDf: DataFrame)
  : mutable.ArrayBuffer[Int] = {
    val windowOp = windowAggNode.window()
    val repartitionExprs = windowOp.partition().keys()

    val repartitionColIndexes = mutable.ArrayBuffer[Int]()
    for (i <- 0 until repartitionExprs.GetChildNum()) {
      val expr = repartitionExprs.GetChild(i)
      val colIdx = SparkColumnUtil.resolveColumnIndex(expr, windowAggNode.GetProducer(0))
      repartitionColIndexes += colIdx
    }

    repartitionColIndexes
  }

  def getOrderbyColumns(windowAggNode: PhysicalWindowAggrerationNode, inputDf: DataFrame)
  : mutable.ArrayBuffer[Column] = {
    val windowOp = windowAggNode.window()

    val orderByCols = mutable.ArrayBuffer[Column]()
    val orders = windowOp.sort().orders()
    if (orders == null) {
      return orderByCols
    }
    val ordersExprListNode = orders.getOrder_expressions_

    for (i <- 0 until ordersExprListNode.GetChildNum()) {
      val orderExpr = orders.GetOrderExpression(i)
      orderByCols += getSparkOrderByColumn(orderExpr, windowAggNode, inputDf)
    }

    orderByCols
  }

  def getOrderbyColumns(sortByNode: PhysicalSortNode, inputDf: DataFrame): mutable.ArrayBuffer[Column] = {
    val orders = sortByNode.sort().orders()
    val ordersExprListNode = orders.getOrder_expressions_
    val orderByCols = mutable.ArrayBuffer[Column]()
    for (i <- 0 until ordersExprListNode.GetChildNum()) {
      val orderExpr = orders.GetOrderExpression(i)
      orderByCols += getSparkOrderByColumn(orderExpr, sortByNode, inputDf)
    }

    orderByCols
  }

  def getSparkOrderByColumn(orderExpr: OrderExpression, physicalNode: PhysicalOpNode, inputDf: DataFrame): Column = {
    val colIdx = SparkColumnUtil.resolveOrderColumnIndex(orderExpr, physicalNode.GetProducer(0))
    val column = SparkColumnUtil.getColumnFromIndex(inputDf, colIdx)
    if (orderExpr.is_asc()) {
      column.asc
    } else {
      column.desc
    }
  }


  /** Like getOrderbyColumns but return the column name of the orderby key.
   *
   * Notice that we only support one orderby column
   *
   */
  def getOrderbyColumnName(windowAggNode: PhysicalWindowAggrerationNode, inputDf: DataFrame): String = {
    val windowOp = windowAggNode.window()

    val orders = windowOp.sort().orders()
    if (orders == null) {
      // WINDOW without ORDER BY
      return "";
    }
    val orderExprListNode = orders.getOrder_expressions_

    if (orderExprListNode.GetChildNum() <= 0) {
      throw new HybridSeException("Can not get orderby column name from dataframe: " + inputDf)
    } else if (orderExprListNode.GetChildNum() > 1) {
      throw new HybridSeException("Can more than one orderby keys: " + orderExprListNode.GetChildNum())
    } else {
      val orderExpr = orders.GetOrderExpression(0)
      val colIdx = SparkColumnUtil.resolveOrderColumnIndex(orderExpr, windowAggNode.GetProducer(0))
      inputDf.schema.apply(colIdx).name
    }
  }

  /** Like getOrderbyColumns but return the index of the orderby column.
   *
   */
  def getOrderbyColumnIndex(windowAggNode: PhysicalWindowAggrerationNode, inputDf: DataFrame): Int = {
    val windowOp = windowAggNode.window()

    val orders = windowOp.sort().orders()
    if (orders == null) {
      // WINDOW without ORDER BY
      return -1;
    }
    val orderExprListNode = orders.getOrder_expressions_

    if (orderExprListNode.GetChildNum() <= 0) {
      throw new HybridSeException("Can not get orderby column name from dataframe: " + inputDf)
    } else if (orderExprListNode.GetChildNum() > 1) {
      throw new HybridSeException("Can more than one orderby keys: " + orderExprListNode.GetChildNum())
    } else {
      val orderExpr = orders.GetOrderExpression(0)
      val colIdx = SparkColumnUtil.resolveOrderColumnIndex(orderExpr, windowAggNode.GetProducer(0))
      colIdx
    }
  }

}
