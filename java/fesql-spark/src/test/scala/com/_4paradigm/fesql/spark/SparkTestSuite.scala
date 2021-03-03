/*
 * java/fesql-spark/src/test/scala/com/_4paradigm/fesql/spark/SparkTestSuite.scala
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

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, FunSuite}


abstract class SparkTestSuite extends FunSuite with BeforeAndAfter {

  private val tlsSparkSession = new ThreadLocal[SparkSession]()

  def customizedBefore(): Unit = {}
  def customizedAfter(): Unit = {}

  before {
    val sess = SparkSession.builder().master("local[4]").getOrCreate()
    tlsSparkSession.set(sess)
    customizedBefore()
  }

  after {
    try {
      customizedAfter()
    } catch {
      case e: Exception => e.printStackTrace()
    }
    val sess = tlsSparkSession.get()
    if (sess != null) {
      try {
        sess.close()
      } catch {
        case e: Exception => e.printStackTrace()
      }
    }
    tlsSparkSession.set(null)
  }

  def getSparkSession: SparkSession = {
    tlsSparkSession.get()
  }
}
