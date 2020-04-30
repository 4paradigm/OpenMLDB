package com._4paradigm.fesql.offline

import java.nio.ByteBuffer

import sun.nio.ch.DirectBuffer

import scala.collection.mutable


/**
  * This class is not thread-safe
  */
class NativeBufferPool {

  private val freeBuffers = mutable.ArrayBuffer[ManagedBuffer]()

  private val allocated = mutable.HashMap[Long, ManagedBuffer]()

  def getBuffer(bytes: Int): ManagedBuffer = {
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

  def releaseBuffer(buffer: ManagedBuffer): Unit = {
    freeBuffers += buffer
  }

  private def allocateNew(bytes: Int): ManagedBuffer = {
    val buf = ByteBuffer.allocateDirect(bytes)
    val id = allocated.size
    val managed = ManagedBuffer(id, buf)
    allocated += id.toLong -> managed
    managed
  }


  def freeAll(): Unit = {
    allocated.values.foreach(buf => buf.free())
    allocated.clear()
  }

  case class ManagedBuffer(id: Long, buffer: ByteBuffer) {
    def getSize: Int = buffer.capacity()

    def free(): Unit = {
      buffer.asInstanceOf[DirectBuffer].cleaner().clean()
    }
  }
}
