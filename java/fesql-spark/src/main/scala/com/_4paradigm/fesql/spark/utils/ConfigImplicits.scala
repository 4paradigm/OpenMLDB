package com._4paradigm.fesql.spark.utils


trait ConfigImplicits[T] {
  def parse(value: Any): T
}

object ConfigImplicits {


}
