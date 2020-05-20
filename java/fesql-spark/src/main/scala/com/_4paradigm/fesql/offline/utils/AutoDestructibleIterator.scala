package com._4paradigm.fesql.offline.utils


class AutoDestructibleIterator[T](iter: Iterator[T], destructor: => Unit) extends Iterator[T] {

  private var _destructed = false

  override def hasNext: Boolean = {
    val flag = iter.hasNext
    if (!flag && !_destructed) {
      destructor
      _destructed = true
    }
    flag
  }

  override def next(): T = {
    iter.next()
  }
}

object AutoDestructibleIterator {
  def apply[T](iter: Iterator[T])(destructor: => Unit): AutoDestructibleIterator[T] = {
    new AutoDestructibleIterator[T](iter, destructor)
  }
}

