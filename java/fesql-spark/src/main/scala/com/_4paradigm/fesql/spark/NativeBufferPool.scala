package com._4paradigm.fesql.spark

import java.nio.ByteBuffer

import sun.nio.ch.DirectBuffer

import scala.collection.mutable


/**
  * This class is not thread-safe
  */
class NativeBufferPool {

  private val freeBuffers = mutable.ArrayBuffer[IndexedBuffer]()

  private val allocated = mutable.HashMap[Long, IndexedBuffer]()

  def getBuffer(bytes: Int): IndexedBuffer = {
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

  def releaseBuffer(buffer: IndexedBuffer): Unit = {
    freeBuffers += buffer
  }

  private def allocateNew(bytes: Int): IndexedBuffer = {
    val buf = ByteBuffer.allocateDirect(bytes)
    val id = allocated.size
    val managed = IndexedBuffer(id, buf)
    allocated += id.toLong -> managed
    managed
  }


  def freeAll(): Unit = {
    allocated.values.foreach(buf => buf.free())
    allocated.clear()
  }

  case class IndexedBuffer(id: Long, buffer: ByteBuffer) {
    def getSize: Int = buffer.capacity()

    def free(): Unit = {
      buffer.asInstanceOf[DirectBuffer].cleaner().clean()
    }
  }
}
