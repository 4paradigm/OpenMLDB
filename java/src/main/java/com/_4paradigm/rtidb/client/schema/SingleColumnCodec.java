package com._4paradigm.rtidb.client.schema;

import com.google.protobuf.ByteString;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class SingleColumnCodec {

    /**
     * encode part
     */
    public static ByteBuffer Append(boolean data) {
        ByteBuffer buffer = ByteBuffer.allocate(1).order(ByteOrder.LITTLE_ENDIAN);
        if (data) {
            buffer.put((byte) 1);
        } else {
            buffer.put((byte) 0);
        }
        buffer.rewind();
        return buffer;
    }

    public static ByteBuffer Append(short data) {
        ByteBuffer buffer = ByteBuffer.allocate(2).order(ByteOrder.LITTLE_ENDIAN);
        buffer.putShort(data);
        buffer.rewind();
        return buffer;
    }

    public static ByteBuffer Append(int data) {
        ByteBuffer buffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
        buffer.putInt(data);
        buffer.rewind();
        return buffer;
    }

    public static ByteBuffer Append(long data) {
        ByteBuffer buffer = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);
        buffer.putLong(data);
        buffer.rewind();
        return buffer;
    }

    public static ByteBuffer Append(float data) {
        ByteBuffer buffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
        buffer.putFloat(data);
        buffer.rewind();
        return buffer;
    }

    public static ByteBuffer Append(double data) {
        ByteBuffer buffer = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);
        buffer.putDouble(data);
        buffer.rewind();
        return buffer;
    }

    /**
     * decode part
     */
    public static boolean GetBool(ByteString bs) {
        ByteBuffer buffer = bs.asReadOnlyByteBuffer();
        int val = buffer.get();
        if (val == 0) {
            return false;
        } else {
            return true;
        }
    }

    public static short GetShort(ByteString bs) {
        ByteBuffer buffer = bs.asReadOnlyByteBuffer();
        if (buffer.order() == ByteOrder.BIG_ENDIAN) {
            buffer = buffer.order(ByteOrder.LITTLE_ENDIAN);
        }
        return buffer.getShort();
    }

    public static int GetInt(ByteString bs) {
        ByteBuffer buffer = bs.asReadOnlyByteBuffer();
        if (buffer.order() == ByteOrder.BIG_ENDIAN) {
            buffer = buffer.order(ByteOrder.LITTLE_ENDIAN);
        }
        return buffer.getInt();
    }

    public static long GetLong(ByteString bs) {
        ByteBuffer buffer = bs.asReadOnlyByteBuffer();
        if (buffer.order() == ByteOrder.BIG_ENDIAN) {
            buffer = buffer.order(ByteOrder.LITTLE_ENDIAN);
        }
        return buffer.getLong();
    }

    public static float GetFloat(ByteString bs) {
        ByteBuffer buffer = bs.asReadOnlyByteBuffer();
        if (buffer.order() == ByteOrder.BIG_ENDIAN) {
            buffer = buffer.order(ByteOrder.LITTLE_ENDIAN);
        }
        return buffer.getFloat();
    }

    public static double GetDouble(ByteString bs) {
        ByteBuffer buffer = bs.asReadOnlyByteBuffer();
        if (buffer.order() == ByteOrder.BIG_ENDIAN) {
            buffer = buffer.order(ByteOrder.LITTLE_ENDIAN);
        }
        return buffer.getDouble();
    }
}
