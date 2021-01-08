package com._4paradigm.fesql.spark

import com._4paradigm.fesql.spark.element.FesqlConfig
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, FunSuite}


abstract class SparkTestSuite extends FunSuite with BeforeAndAfter {

  private val tlsSparkSession = new ThreadLocal[SparkSession]()

  before {
    val sess = SparkSession.builder().master("local").getOrCreate()
    tlsSparkSession.set(sess)
  }

  after {
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
