package com._4paradigm.fesql.offline;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;


public class SerializableByteBuffer implements Serializable {

    transient private ByteBuffer buffer;

    static private final int MAGIC_END_TAG = 42;

    public SerializableByteBuffer() {}
    public SerializableByteBuffer(ByteBuffer buffer) { this.buffer = buffer; }

    public ByteBuffer getBuffer() {
        return buffer;
    }


    private void writeObject(java.io.ObjectOutputStream out) throws IOException {
        if (buffer == null) {
            throw new IOException("No backed buffer");
        }
        out.defaultWriteObject();
        out.write(buffer.capacity());
        out.writeBoolean(buffer.isDirect());
        if (buffer.hasArray()) {
            out.write(buffer.array());
        } else {
            byte[] bytes = new byte[buffer.capacity()];
            buffer.get(bytes, 0, bytes.length);
            out.write(bytes);
        }
        out.write(MAGIC_END_TAG);
    }

    private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        int capacity = in.readInt();
        boolean isDirect = in.readBoolean();
        if (isDirect) {
            buffer = ByteBuffer.allocateDirect(capacity);
        } else {
            buffer = ByteBuffer.allocate(capacity);
        }
        byte[] bytes;
        if (buffer.hasArray()) {
            bytes = buffer.array();
        } else {
            bytes = new byte[capacity];
        }
        int readLen = in.read(bytes, 0, capacity);
        if (readLen < capacity) {
            throw new IOException("Byte buffer stream corrupt, expect buffer bytes: "
                    + capacity + " but read only " + readLen);
        }
        if (!buffer.hasArray()) {
            buffer.put(bytes, 0, capacity);
        }
        int endTag = in.readInt();
        if (endTag != MAGIC_END_TAG) {
            throw new IOException("Byte buffer stream corrupt");
        }
    }
}
