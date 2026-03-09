/*
 * Copyright 2021 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com._4paradigm.hybridse.sdk;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;


/**
 * Serializable ByteBuffer.
 *
 * <p>By default, ByteBuffer instances are not serializable, this class implemented
 * serializable wrapper for byte buffer to communicate serialize buffer content.
 */
public class SerializableByteBuffer implements Serializable {

    private transient ByteBuffer buffer;

    private static final int MAGIC_END_TAG = 42;

    public SerializableByteBuffer() {}

    public SerializableByteBuffer(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    /**
     * Return ByteBuffer.
     */
    public ByteBuffer getBuffer() {
        return buffer;
    }


    /**
     * Serialization method to save the ByteBuffer.
     *
     * @serialData The length of the ByteBuffer type ID (int),
     *             followed by ByteBuffer isDirect flag (boolean)
     *             followed by buffer array
     */
    private void writeObject(java.io.ObjectOutputStream out) throws IOException {
        if (buffer == null) {
            throw new IOException("No backed buffer");
        }
        out.defaultWriteObject();
        out.writeInt(buffer.capacity());
        out.writeBoolean(buffer.isDirect());
        if (buffer.hasArray()) {
            out.write(buffer.array(), 0, buffer.capacity());
        } else {
            byte[] bytes = new byte[buffer.capacity()];
            ByteBuffer view = buffer.duplicate();
            view.rewind();
            view.get(bytes, 0, bytes.length);
            out.write(bytes);
        }
        out.writeInt(MAGIC_END_TAG);
    }


    /**
     * Serialization method to load the ByteBuffer.
     *
     * @throws IOException throw when fail to read by DataInputStream
     * @throws ClassNotFoundException throw when class not found
     */
    private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();

        // object stream is backed by block stream, thus read bytes
        // operations should be buffered to ensure exact bytes are read
        DataInputStream wrappedInStream = new DataInputStream(in);

        int capacity = wrappedInStream.readInt();
        boolean isDirect = wrappedInStream.readBoolean();
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

        try {
            wrappedInStream.readFully(bytes, 0, capacity);
        } catch (IOException e) {
            throw new IOException("Byte buffer stream corrupt, " + "expect buffer bytes: " + capacity, e);
        }
        if (!buffer.hasArray()) { // maybe direct
            buffer.put(bytes, 0, capacity);
            buffer.rewind();
        }
        int endTag = wrappedInStream.readInt();
        if (endTag != MAGIC_END_TAG) {
            throw new IOException("Byte buffer stream corrupt");
        }
    }
}
