package com._4paradigm.fesql.spark

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, FunSuite}


abstract class SparkTestSuite extends FunSuite with BeforeAndAfter {

  private val tlsSparkSession = new ThreadLocal[SparkSession]()

  def customizedBefore(): Unit = {}
  def customizedAfter(): Unit = {}

  before {
    val sess = SparkSession.builder().master("local").getOrCreate()
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
