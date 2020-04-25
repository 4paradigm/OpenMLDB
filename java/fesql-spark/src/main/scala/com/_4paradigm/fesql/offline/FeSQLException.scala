package com._4paradigm.fesql.offline

class FeSQLException(msg: String, cause: Throwable) extends Exception {

  def this(msg: String) = {
    this(msg, null)
  }

  override def getCause: Throwable = cause

  override def getMessage: String = msg
}
