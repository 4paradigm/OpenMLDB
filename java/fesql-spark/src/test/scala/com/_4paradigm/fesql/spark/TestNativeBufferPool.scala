package com._4paradigm.fesql.spark

import org.scalatest.FunSuite

class TestNativeBufferPool extends FunSuite {

  test("Test native buffer pool") {
    val pool = new NativeBufferPool

    val buffer = pool.getBuffer(4096)
    assert(buffer.getSize >= 4096)
    pool.releaseBuffer(buffer)

    val buffer2 = pool.getBuffer(4096)
    assert(buffer2.id == buffer.id)

    pool.releaseBuffer(buffer2)

    val buffer3 = pool.getBuffer(8192)
    assert(buffer3.getSize >= 8192)
    pool.releaseBuffer(buffer3)

    pool.freeAll()
  }

}
