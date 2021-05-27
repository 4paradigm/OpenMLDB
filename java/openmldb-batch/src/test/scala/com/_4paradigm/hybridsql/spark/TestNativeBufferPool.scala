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

package com._4paradigm.hybridsql.spark

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
