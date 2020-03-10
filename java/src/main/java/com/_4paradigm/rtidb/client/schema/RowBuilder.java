package com._4paradigm.rtidb.client.schema;

import com._4paradigm.rtidb.client.TabletException;
import com._4paradigm.rtidb.client.ha.RTIDBClientConfig;
import com._4paradigm.rtidb.client.type.DataType;
import com._4paradigm.rtidb.client.type.IndexType;
import com._4paradigm.rtidb.common.Common;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

public class RowBuilder {

    private final static Logger logger = LoggerFactory.getLogger(RowBuilder.class);

    private ByteBuffer buf;
    private int size = 0;
    private int cnt = 0;
    List<ColumnDesc> schema = new ArrayList<>();
    private int str_field_cnt = 0;
    private int str_field_start_offset = 0;
    private int str_addr_length = 0;
    private int str_offset = 0;
    private List<Integer> offset_vec = new ArrayList<>();

    public RowBuilder(List<ColumnDesc> schema) {
        str_field_start_offset = RowCodecCommon.HEADER_LENGTH + RowCodecCommon.getBitMapSize(schema.size());
        this.schema = schema;
        for (int idx = 0; idx < schema.size(); idx++) {
            ColumnDesc column = schema.get(idx);
            if (column.getDataType() == DataType.Varchar) {
                offset_vec.add(str_field_cnt);
                str_field_cnt++;
            } else {
                if (RowCodecCommon.TYPE_SIZE_MAP.get(column.getDataType()) == null) {
                    logger.warn("type is not supported");
                } else {
                    offset_vec.add(str_field_start_offset);
                    str_field_start_offset += RowCodecCommon.TYPE_SIZE_MAP.get(column.getDataType());
                }
            }
        }
    }

    public int calTotalLength(int string_length) throws TabletException {
        if (schema.size() == 0) {
            return 0;
        }
        long total_length = str_field_start_offset + string_length;
        if (total_length + str_field_cnt <= RowCodecCommon.UINT8_MAX) {
            total_length += str_field_cnt;
        } else if (total_length + str_field_cnt * 2 <= RowCodecCommon.UINT16_MAX) {
            total_length += str_field_cnt * 2;
        } else if (total_length + str_field_cnt * 3 <= RowCodecCommon.UINT24_MAX) {
            total_length += str_field_cnt * 3;
        } else if (total_length + str_field_cnt * 4 <= RowCodecCommon.UINT32_MAX) {
            total_length += str_field_cnt * 4;
        }
        if (total_length > Integer.MAX_VALUE) {
            throw new TabletException("total length is bigger than integer max value");
        }
        return (int) total_length;
    }

    public ByteBuffer setBuffer(ByteBuffer buffer, int size) {
        if (buffer == null || size == 0 ||
                size < str_field_start_offset + str_field_cnt) {
            return null;
        }
        if (buffer.order() == ByteOrder.BIG_ENDIAN) {
            buffer = buffer.order(ByteOrder.LITTLE_ENDIAN);
        }
//        buffer = ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);
        this.size = size;
        buffer.put((byte) 1); // FVersion
        buffer.put((byte) 1); // SVersion
        buffer.putInt(size); // size
        this.buf = buffer;
        str_addr_length = RowCodecCommon.getAddrLength(size);
        str_offset = str_field_start_offset + str_addr_length * str_field_cnt;
        return this.buf;
    }

    private boolean check(DataType type) {
        if (cnt >= schema.size()) {
            return false;
        }
        ColumnDesc column = schema.get(cnt);
        if (column.getDataType() != type) {
            return false;
        }
        if (column.getDataType() != DataType.Varchar) {
            if (RowCodecCommon.TYPE_SIZE_MAP.get(column.getDataType()) == null) {
                return false;
            }
        }
        return true;
    }

    public boolean appendNULL() {
        int index = RowCodecCommon.HEADER_LENGTH + (cnt >> 3);
        byte bt = buf.get(index);
        buf.put(index, (byte) (bt | (1 << (cnt & 0x07))));
        ColumnDesc column = schema.get(cnt);
        if (column.getDataType() == DataType.Varchar) {
            index = str_field_start_offset + str_addr_length * offset_vec.get(cnt);
            buf.position(index);
            if (str_addr_length == 1) {
                buf.put((byte) (str_offset & 0xFF));
            } else if (str_addr_length == 2) {
                buf.putShort((short) (str_offset & 0xFFFF));
            } else if (str_addr_length == 3) {
                buf.put((byte) (str_offset >> 16));
                buf.put((byte) ((str_offset & 0xFF00) >> 8));
                buf.put((byte) (str_offset & 0x00FF));
            } else {
                buf.putInt(str_offset);
            }
        }
        cnt++;
        return true;
    }

    public boolean appendBool(boolean val) {
        if (!check(DataType.Bool)) {
            return false;
        }
        buf.position(offset_vec.get(cnt));
        if (val) {
            buf.put((byte) 1);
        } else {
            buf.put((byte) 0);
        }
        cnt++;
        return true;
    }

    public boolean appendInt32(int val) {
        if (!check(DataType.Int)) {
            return false;
        }
        buf.position(offset_vec.get(cnt));
        buf.putInt(val);
        cnt++;
        return true;
    }

    public boolean appendInt16(short val) {
        if (!check(DataType.SmallInt)) {
            return false;
        }
        buf.position(offset_vec.get(cnt));
        buf.putShort(val);
        cnt++;
        return true;
    }

    public boolean appendTimestamp(long val) {
        if (!check(DataType.Timestamp)) {
            return false;
        }
        buf.position(offset_vec.get(cnt));
        buf.putLong(val);
        cnt++;
        return true;
    }

    public boolean appendInt64(long val) {
        if (!check(DataType.BigInt)) {
            return false;
        }
        buf.position(offset_vec.get(cnt));
        buf.putLong(val);
        cnt++;
        return true;
    }

    public boolean appendFloat(float val) {
        if (!check(DataType.Float)) {
            return false;
        }
        buf.position(offset_vec.get(cnt));
        buf.putFloat(val);
        cnt++;
        return true;
    }

    public boolean appendDouble(double val) {
        if (!check(DataType.Double)) {
            return false;
        }
        buf.position(offset_vec.get(cnt));
        buf.putDouble(val);
        cnt++;
        return true;
    }

    public boolean appendString(String val) {
        int length = val.length();
        if (val == null || !check(DataType.Varchar)) {
            return false;
        }
        if (str_offset + length > size) {
            return false;
        }
        int index = str_field_start_offset + str_addr_length * offset_vec.get(cnt);
        buf.position(index);
        if (str_addr_length == 1) {
            buf.put((byte) (str_offset & 0xFF));
        } else if (str_addr_length == 2) {
            buf.putShort((short) (str_offset & 0xFFFF));
        } else if (str_addr_length == 3) {
            buf.put((byte) (str_offset >> 16));
            buf.put((byte) ((str_offset & 0xFF00) >> 8));
            buf.put((byte) (str_offset & 0x00FF));
        } else {
            buf.putInt(str_offset);
        }
        if (length != 0) {
            buf.position(str_offset);
            buf.put(val.getBytes(RowCodecCommon.CHARSET), 0, length);
        }
        str_offset += length;
        cnt++;
        return true;
    }

    public static ByteBuffer putEncode(Object[] row, List<ColumnDesc> schema) throws TabletException {
        int strLength = calStrLength(row, schema);
        RowBuilder builder = new RowBuilder(schema);
        int size = builder.calTotalLength(strLength);
        ByteBuffer buffer = ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);
        buffer = builder.setBuffer(buffer, size);
        for (int i = 0; i < schema.size(); i++) {
            ColumnDesc columnDesc = schema.get(i);
            if (columnDesc.isNotNull() && row[i] == null) {
                throw new TabletException("col " + columnDesc.getName() + " should not be null");
            } else if (row[i] == null) {
                builder.appendNULL();
                continue;
            }
            switch (columnDesc.getDataType()) {
                case Varchar:
                    boolean ok = builder.appendString((String) row[i]);
                    if (!ok) {
                        throw new TabletException("append string error");
                    }
                    break;
                case Bool:
                    ok = builder.appendBool((Boolean) row[i]);
                    if (!ok) {
                        throw new TabletException("append boolean error");
                    }
                    break;
                case SmallInt:
                    ok = builder.appendInt16((Short) row[i]);
                    if (!ok) {
                        throw new TabletException("append smallInt error");
                    }
                    break;
                case Int:
                    ok = builder.appendInt32((Integer) row[i]);
                    if (!ok) {
                        throw new TabletException("append int error");
                    }
                    break;
                case Timestamp:
                    ok = builder.appendTimestamp((Long) row[i]);
                    if (!ok) {
                        throw new TabletException("append timestamp error");
                    }
                    break;
                case BigInt:
                    ok = builder.appendInt64((Long) row[i]);
                    if (!ok) {
                        throw new TabletException("append bigInt error");
                    }
                    break;
                case Float:
                    ok = builder.appendFloat((Float) row[i]);
                    if (!ok) {
                        throw new TabletException("append float error");
                    }
                    break;
                case Double:
                    ok = builder.appendDouble((Double) row[i]);
                    if (!ok) {
                        throw new TabletException("append double error");
                    }
                    break;
                default:
                    throw new TabletException("unsupported data type");
            }
        }
        return buffer;
    }

    private static int calStrLength(Object[] row, List<ColumnDesc> schema) {
        int strLength = 0;
        for (int i = 0; i < schema.size(); i++) {
            ColumnDesc columnDesc = schema.get(i);
            if (columnDesc.getDataType().equals(DataType.Varchar)) {
                if (!columnDesc.isNotNull() && row[i] == null) {
                    continue;
                }
                strLength += ((String) row[i]).length();
            }
        }
        return strLength;
    }

    public static String getPrimaryKey(Object[] row, List<Common.ColumnKey> columnKeyList, List<ColumnDesc> schema) {
        String pkColName = "";
        for (int i = 0; i < columnKeyList.size(); i++) {
            Common.ColumnKey columnKey = columnKeyList.get(i);
            if (columnKey.hasIndexType() &&
                    columnKey.getIndexType() == IndexType.valueFrom(IndexType.kPrimaryKey)) {
                pkColName = columnKey.getIndexName();
            }
        }
        String pk = "";
        for (int i = 0; i < schema.size(); i++) {
            ColumnDesc columnDesc = schema.get(i);
            if (columnDesc.getName().equals(pkColName)) {
                if (row[i] == null) {
                    pk = RTIDBClientConfig.NULL_STRING;
                } else {
                    pk = String.valueOf(row[i]);
                }
                if (pk.isEmpty()) {
                    pk = RTIDBClientConfig.EMPTY_STRING;
                }
            }
        }
        return pk;
    }
}
