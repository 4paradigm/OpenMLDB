package com._4paradigm.rtidb.client.schema;

import com._4paradigm.rtidb.type.Type;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

public class RowCodecCommon {

    public static final Charset CHARSET = Charset.forName("utf-8");
    public static final int VERSION_LENGTH = 2;
    public static final int SIZE_LENGTH = 4;
    public static final int HEADER_LENGTH = VERSION_LENGTH + SIZE_LENGTH;
    public static final long UINT8_MAX = (1 << 8) - 1;
    public static final long UINT16_MAX = (1 << 16) - 1;
    public static final long UINT24_MAX = (1 << 24) - 1;
    public static final long UINT32_MAX = (1 << 32) - 1;

    public static final Map<Type.DataType, Integer> TYPE_SIZE_MAP = new HashMap<>();
    static {
        TYPE_SIZE_MAP.put(Type.DataType.kBool, 1);
        TYPE_SIZE_MAP.put(Type.DataType.kInt16, 2);
        TYPE_SIZE_MAP.put(Type.DataType.kInt32, 4);
        TYPE_SIZE_MAP.put(Type.DataType.kFloat, 4);
        TYPE_SIZE_MAP.put(Type.DataType.kInt64, 8);
        TYPE_SIZE_MAP.put(Type.DataType.kTimestamp, 8);
        TYPE_SIZE_MAP.put(Type.DataType.kDouble, 8);
    }

    public static int getBitMapSize(int size) {
        int tmp = 0;
        if (((size) & 0x07) > 0) {
            tmp = 1;
        }
        return (size >> 3) + tmp;
    }

    public static int getAddrLength(int size) {
        if (size <= UINT8_MAX) {
            return 1;
        } else if (size <= UINT16_MAX) {
            return 2;
        } else if (size <= UINT24_MAX) {
            return 3;
        } else {
            return 4;
        }
    }
}
