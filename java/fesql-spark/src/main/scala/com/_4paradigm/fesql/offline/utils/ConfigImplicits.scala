package com._4paradigm.fesql.offline.utils


trait ConfigImplicits[T] {
  def parse(value: Any): T
}

object ConfigImplicits {


}
