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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.nio.ByteBuffer

import com._4paradigm.hybridse.sdk.SerializableByteBuffer
import org.scalatest.FunSuite

class TestSerializableByteBuffer extends FunSuite {

  test("Test serializableBuffer") {
    val buf = ByteBuffer.allocate(1024)
    buf.putDouble(3.14)
    buf.putInt(42)

    val ser = new SerializableByteBuffer(buf)

    val os = new ByteArrayOutputStream()
    val objOStream = new ObjectOutputStream(os)
    objOStream.writeObject(ser)
    objOStream.close()

    val is = new ByteArrayInputStream(os.toByteArray())
    val objISteam = new ObjectInputStream(is)

    val ser2 = objISteam.readObject().asInstanceOf[SerializableByteBuffer]
    assert(ser2.getBuffer().getDouble() == 3.14)
    assert(ser2.getBuffer().getInt() == 42)
  }

  test("Test serializableDirectBuffer") {
    val buf = ByteBuffer.allocateDirect(1024)
    buf.putDouble(3.14)
    buf.putInt(42)

    val ser = new SerializableByteBuffer(buf)

    val os = new ByteArrayOutputStream()
    val objOStream = new ObjectOutputStream(os)
    objOStream.writeObject(ser)
    objOStream.close()

    val is = new ByteArrayInputStream(os.toByteArray())
    val objISteam = new ObjectInputStream(is)

    val ser2 = objISteam.readObject().asInstanceOf[SerializableByteBuffer]
    assert(ser2.getBuffer().isDirect())
    assert(ser2.getBuffer().getDouble() == 3.14)
    assert(ser2.getBuffer().getInt() == 42)
  }

}
