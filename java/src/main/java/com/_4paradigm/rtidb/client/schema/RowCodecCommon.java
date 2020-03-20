package com._4paradigm.rtidb.client.schema;


import com._4paradigm.rtidb.client.TabletException;
import com._4paradigm.rtidb.client.ha.RTIDBClientConfig;
import com._4paradigm.rtidb.client.type.DataType;
import com._4paradigm.rtidb.client.type.IndexType;
import com._4paradigm.rtidb.common.Common;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
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
    public static final long DEFAULT_LONG = 1L;

    public static final Map<DataType, Integer> TYPE_SIZE_MAP = new HashMap<>();
    static {
        TYPE_SIZE_MAP.put(DataType.Bool, 1);
        TYPE_SIZE_MAP.put(DataType.SmallInt, 2);
        TYPE_SIZE_MAP.put(DataType.Int, 4);
        TYPE_SIZE_MAP.put(DataType.Float, 4);
        TYPE_SIZE_MAP.put(DataType.BigInt, 8);
        TYPE_SIZE_MAP.put(DataType.Timestamp, 8);
        TYPE_SIZE_MAP.put(DataType.Double, 8);
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

    public static int calStrLength(Map<String, Object> row, List<ColumnDesc> schema) throws TabletException {
        int strLength = 0;
        for (int i = 0; i < schema.size(); i++) {
            ColumnDesc columnDesc = schema.get(i);
            if (columnDesc.getDataType().equals(DataType.Varchar)) {
                if (!columnDesc.isNotNull() && row.get(columnDesc.getName()) == null) {
                    continue;
                } else if (columnDesc.isNotNull()
                        && row.containsKey(columnDesc.getName())
                        && row.get(columnDesc.getName()) == null) {
                    throw new TabletException("col " + columnDesc.getName() + " should not be null");
                }
                strLength += ((String) row.get(columnDesc.getName())).length();
            }
        }
        return strLength;
    }

    public static String getPrimaryKey(Map<String, Object> row, List<Common.ColumnKey> columnKeyList, List<ColumnDesc> schema) {
        String pkColName = "";
        for (int i = 0; i < columnKeyList.size(); i++) {
            Common.ColumnKey columnKey = columnKeyList.get(i);
            if (columnKey.hasIndexType() &&
                    columnKey.getIndexType() == IndexType.valueFrom(IndexType.PrimaryKey)) {
                pkColName = columnKey.getIndexName();
            }
        }
        String pk = "";
        for (int i = 0; i < schema.size(); i++) {
            ColumnDesc columnDesc = schema.get(i);
            if (columnDesc.getName().equals(pkColName)) {
                if (row.get(columnDesc.getName()) == null) {
                    pk = RTIDBClientConfig.NULL_STRING;
                } else {
                    pk = String.valueOf(row.get(columnDesc.getName()));
                }
                if (pk.isEmpty()) {
                    pk = RTIDBClientConfig.EMPTY_STRING;
                }
            }
        }
        return pk;
    }
}
