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

package com._4paradigm.openmldb.batch

import java.nio.ByteBuffer

import sun.nio.ch.DirectBuffer

import scala.collection.mutable


/**
  * This class is not thread-safe
  */
class NativeBufferPool {

  private val freeBuffers = mutable.ArrayBuffer[indexedBuffer]()

  private val allocated = mutable.HashMap[Long, indexedBuffer]()

  def getBuffer(bytes: Int): indexedBuffer = {
    if (freeBuffers.isEmpty) {
      allocateNew(bytes)
    } else {
      val buf = freeBuffers.remove(freeBuffers.length - 1)
      if (buf.getSize >= bytes) {
        buf
      } else {
        allocated.remove(buf.id)
        buf.free()

        allocateNew(bytes)
      }
    }
  }

  def releaseBuffer(buffer: indexedBuffer): Unit = {
    freeBuffers += buffer
  }

  private def allocateNew(bytes: Int): indexedBuffer = {
    val buf = ByteBuffer.allocateDirect(bytes)
    val id = allocated.size
    val managed = indexedBuffer(id, buf)
    allocated += id.toLong -> managed
    managed
  }


  def freeAll(): Unit = {
    allocated.values.foreach(buf => buf.free())
    allocated.clear()
  }

  case class indexedBuffer(id: Long, buffer: ByteBuffer) {
    def getSize: Int = buffer.capacity()

    def free(): Unit = {
      buffer.asInstanceOf[DirectBuffer].cleaner().clean()
    }
  }
}
