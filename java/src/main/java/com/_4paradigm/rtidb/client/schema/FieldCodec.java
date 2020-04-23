package com._4paradigm.rtidb.client.schema;

import com._4paradigm.rtidb.client.TabletException;
import com._4paradigm.rtidb.client.type.DataType;
import com.google.protobuf.ByteBufferNoCopy;
import com.google.protobuf.ByteString;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class FieldCodec {
    /**
     * encode part
     */
    public static ByteBuffer convert(boolean data) {
        ByteBuffer buffer = ByteBuffer.allocate(1).order(ByteOrder.LITTLE_ENDIAN);
        if (data) {
            buffer.put((byte) 1);
        } else {
            buffer.put((byte) 0);
        }
        buffer.rewind();
        return buffer;
    }

    public static ByteBuffer convert(short data) {
        ByteBuffer buffer = ByteBuffer.allocate(2).order(ByteOrder.LITTLE_ENDIAN);
        buffer.putShort(data);
        buffer.rewind();
        return buffer;
    }

    public static ByteBuffer convert(int data) {
        ByteBuffer buffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
        buffer.putInt(data);
        buffer.rewind();
        return buffer;
    }

    public static ByteBuffer convert(long data) {
        ByteBuffer buffer = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);
        buffer.putLong(data);
        buffer.rewind();
        return buffer;
    }

    public static ByteBuffer convert(float data) {
        ByteBuffer buffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
        buffer.putFloat(data);
        buffer.rewind();
        return buffer;
    }

    public static ByteBuffer convert(double data) {
        ByteBuffer buffer = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);
        buffer.putDouble(data);
        buffer.rewind();
        return buffer;
    }

    public static ByteBuffer convert(String data) {
        ByteBuffer buffer = ByteBuffer.allocate(data.length()).order(ByteOrder.LITTLE_ENDIAN);
        buffer.put(data.getBytes(RowCodecCommon.CHARSET));
        buffer.rewind();
        return buffer;
    }

    public static ByteBuffer convert(DataType dataType, Object data) throws TabletException {
        if (dataType == null) {
            throw new TabletException("dataType is null");
        }
        if (data == null) {
            return null;
        }
        switch (dataType) {
            case Bool:
                return convert((Boolean) data);
            case SmallInt:
                return convert((Short) data);
            case Int:
                return convert((Integer) data);
            case BigInt:
            case Timestamp:
                return convert((Long) data);
            case Float:
                return convert((Float) data);
            case Double:
                return convert((Double) data);
            case String:
            case Varchar:
            case Blob:
                return convert((String) data);
            default:
                throw new TabletException("unsupported data type");
        }
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

    public static String GetString(ByteString bs) {
        ByteBuffer buffer = bs.asReadOnlyByteBuffer();
        if (buffer.order() == ByteOrder.BIG_ENDIAN) {
            buffer = buffer.order(ByteOrder.LITTLE_ENDIAN);
        }
        return ByteBufferNoCopy.wrap(buffer).toString(RowCodecCommon.CHARSET);
    }

}
