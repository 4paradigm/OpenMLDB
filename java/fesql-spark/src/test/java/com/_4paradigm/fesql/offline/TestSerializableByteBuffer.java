package com._4paradigm.fesql.offline;

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
