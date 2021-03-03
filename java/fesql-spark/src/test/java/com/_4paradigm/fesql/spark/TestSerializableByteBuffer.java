/*
 * java/fesql-spark/src/test/java/com/_4paradigm/fesql/spark/TestSerializableByteBuffer.java
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

package com._4paradigm.fesql.spark;

import com._4paradigm.fesql.common.SerializableByteBuffer;
import org.junit.Test;

import java.io.*;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestSerializableByteBuffer {

    @Test
    public void testSerializableBuffer() throws IOException, ClassNotFoundException {
        ByteBuffer buf = ByteBuffer.allocate(1024);
        buf.putDouble(3.14);
        buf.putInt(42);

        SerializableByteBuffer ser = new SerializableByteBuffer(buf);

        ByteArrayOutputStream os = new ByteArrayOutputStream();
        ObjectOutputStream objOStream = new ObjectOutputStream(os);
        objOStream.writeObject(ser);
        objOStream.close();

        ByteArrayInputStream is = new ByteArrayInputStream(os.toByteArray());
        ObjectInputStream objISteam = new ObjectInputStream(is);

        SerializableByteBuffer ser2 = (SerializableByteBuffer) objISteam.readObject();
        assertEquals(ser2.getBuffer().getDouble(), 3.14, 1e-20);
        assertEquals(ser2.getBuffer().getInt(), 42);
    }

    @Test
    public void testSerializableDirectBuffer() throws IOException, ClassNotFoundException {
        ByteBuffer buf = ByteBuffer.allocateDirect(1024);
        buf.putDouble(3.14);
        buf.putInt(42);

        SerializableByteBuffer ser = new SerializableByteBuffer(buf);

        ByteArrayOutputStream os = new ByteArrayOutputStream();
        ObjectOutputStream objOStream = new ObjectOutputStream(os);
        objOStream.writeObject(ser);
        objOStream.close();

        ByteArrayInputStream is = new ByteArrayInputStream(os.toByteArray());
        ObjectInputStream objISteam = new ObjectInputStream(is);

        SerializableByteBuffer ser2 = (SerializableByteBuffer) objISteam.readObject();
        assertTrue(ser2.getBuffer().isDirect());
        assertEquals(ser2.getBuffer().getDouble(), 3.14, 1e-20);
        assertEquals(ser2.getBuffer().getInt(), 42);
    }
}
