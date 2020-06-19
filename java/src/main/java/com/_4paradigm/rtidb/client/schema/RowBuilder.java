package com._4paradigm.rtidb.client.schema;

import com._4paradigm.rtidb.client.TabletException;
import com._4paradigm.rtidb.client.type.DataType;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class RowBuilder {

    private final static Logger logger = LoggerFactory.getLogger(RowBuilder.class);

    private ByteBuffer buf;
    private int size = 0;
    private int cnt = 0;
    List<ColumnDesc> schema = new ArrayList<>();
    private int strFieldCnt = 0;
    private int strFieldStartOffset = 0;
    private int strAddrLength = 0;
    private int strOffset = 0;
    private List<Integer> offsetVec = new ArrayList<>();

    public RowBuilder(List<ColumnDesc> schema) {
        strFieldStartOffset = RowCodecCommon.HEADER_LENGTH + RowCodecCommon.getBitMapSize(schema.size());
        this.schema = schema;
        for (int idx = 0; idx < schema.size(); idx++) {
            ColumnDesc column = schema.get(idx);
            if (column.getDataType() == DataType.Varchar || column.getDataType() == DataType.String) {
                offsetVec.add(strFieldCnt);
                strFieldCnt++;
            } else {
                if (RowCodecCommon.TYPE_SIZE_MAP.get(column.getDataType()) == null) {
                    logger.warn("type is not supported");
                } else {
                    offsetVec.add(strFieldStartOffset);
                    strFieldStartOffset += RowCodecCommon.TYPE_SIZE_MAP.get(column.getDataType());
                }
            }
        }
    }

    public int calTotalLength(int string_length) throws TabletException {
        if (schema.size() == 0) {
            return 0;
        }
        long totalLength = strFieldStartOffset + string_length;
        if (totalLength + strFieldCnt <= RowCodecCommon.UINT8_MAX) {
            totalLength += strFieldCnt;
        } else if (totalLength + strFieldCnt * 2 <= RowCodecCommon.UINT16_MAX) {
            totalLength += strFieldCnt * 2;
        } else if (totalLength + strFieldCnt * 3 <= RowCodecCommon.UINT24_MAX) {
            totalLength += strFieldCnt * 3;
        } else if (totalLength + strFieldCnt * 4 <= RowCodecCommon.UINT32_MAX) {
            totalLength += strFieldCnt * 4;
        }
        if (totalLength > Integer.MAX_VALUE) {
            throw new TabletException("total length is bigger than integer max value");
        }
        return (int) totalLength;
    }

    public ByteBuffer setBuffer(ByteBuffer buffer, int size) {
        if (buffer == null || size == 0 ||
                size < strFieldStartOffset + strFieldCnt) {
            return null;
        }
        if (buffer.order() == ByteOrder.BIG_ENDIAN) {
            buffer = buffer.order(ByteOrder.LITTLE_ENDIAN);
        }
        this.size = size;
        buffer.put((byte) 1); // FVersion
        buffer.put((byte) 1); // SVersion
        buffer.putInt(size); // size
        this.buf = buffer;
        strAddrLength = RowCodecCommon.getAddrLength(size);
        strOffset = strFieldStartOffset + strAddrLength * strFieldCnt;
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
        if (column.getDataType() != DataType.Varchar && column.getDataType() != DataType.String) {
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
        if (column.getDataType() == DataType.Varchar || column.getDataType() == DataType.String) {
            index = strFieldStartOffset + strAddrLength * offsetVec.get(cnt);
            buf.position(index);
            if (strAddrLength == 1) {
                buf.put((byte) (strOffset & 0xFF));
            } else if (strAddrLength == 2) {
                buf.putShort((short) (strOffset & 0xFFFF));
            } else if (strAddrLength == 3) {
                buf.put((byte) (strOffset >> 16));
                buf.put((byte) ((strOffset & 0xFF00) >> 8));
                buf.put((byte) (strOffset & 0x00FF));
            } else {
                buf.putInt(strOffset);
            }
        }
        cnt++;
        return true;
    }

    public boolean appendBool(boolean val) {
        if (!check(DataType.Bool)) {
            return false;
        }
        buf.position(offsetVec.get(cnt));
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
        buf.position(offsetVec.get(cnt));
        buf.putInt(val);
        cnt++;
        return true;
    }

    public boolean appendInt16(short val) {
        if (!check(DataType.SmallInt)) {
            return false;
        }
        buf.position(offsetVec.get(cnt));
        buf.putShort(val);
        cnt++;
        return true;
    }

    public boolean appendTimestamp(long val) {
        if (!check(DataType.Timestamp)) {
            return false;
        }
        buf.position(offsetVec.get(cnt));
        buf.putLong(val);
        cnt++;
        return true;
    }

    public boolean appendInt64(long val) {
        if (!check(DataType.BigInt)) {
            return false;
        }
        buf.position(offsetVec.get(cnt));
        buf.putLong(val);
        cnt++;
        return true;
    }

    public boolean appendBlob(long val) {
        if (!check(DataType.Blob)) {
            return false;
        }
        buf.position(offsetVec.get(cnt));
        buf.putLong(val);
        cnt++;
        return true;
    }

    public boolean appendFloat(float val) {
        if (!check(DataType.Float)) {
            return false;
        }
        buf.position(offsetVec.get(cnt));
        buf.putFloat(val);
        cnt++;
        return true;
    }

    public boolean appendDouble(double val) {
        if (!check(DataType.Double)) {
            return false;
        }
        buf.position(offsetVec.get(cnt));
        buf.putDouble(val);
        cnt++;
        return true;
    }

    public boolean appendDate(Date date) {
        int year = date.getYear();
        int month = date.getMonth();
        int day = date.getDate();
        int data = year << 16;
        data = data | (month << 8);
        data = data | day;
        buf.position(offsetVec.get(cnt));
        buf.putInt(data);
        cnt++;
        return true;
    }

    public boolean appendString(String val) {
        int length = val.length();
        if (val == null || (!check(DataType.Varchar) && !check(DataType.String))) {
            return false;
        }
        if (strOffset + length > size) {
            return false;
        }
        int index = strFieldStartOffset + strAddrLength * offsetVec.get(cnt);
        buf.position(index);
        if (strAddrLength == 1) {
            buf.put((byte) (strOffset & 0xFF));
        } else if (strAddrLength == 2) {
            buf.putShort((short) (strOffset & 0xFFFF));
        } else if (strAddrLength == 3) {
            buf.put((byte) (strOffset >> 16));
            buf.put((byte) ((strOffset & 0xFF00) >> 8));
            buf.put((byte) (strOffset & 0x00FF));
        } else {
            buf.putInt(strOffset);
        }
        if (length != 0) {
            buf.position(strOffset);
            buf.put(val.getBytes(RowCodecCommon.CHARSET), 0, length);
        }
        strOffset += length;
        cnt++;
        return true;
    }

    public static ByteBuffer encode(Object[] row, List<ColumnDesc> schema) throws TabletException {
        if (row == null || row.length == 0 || schema == null || schema.size() == 0 || row.length != schema.size()) {
            throw new TabletException("input error");
        }
        int strLength = RowCodecCommon.calStrLength(row, schema);
        RowBuilder builder = new RowBuilder(schema);
        int size = builder.calTotalLength(strLength);
        ByteBuffer buffer = ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);
        buffer = builder.setBuffer(buffer, size);
        for (int i = 0; i < schema.size(); i++) {
            ColumnDesc columnDesc = schema.get(i);
            Object column = row[i];
            if (columnDesc.isNotNull()
                    && column == null) {
                throw new TabletException("col " + columnDesc.getName() + " should not be null");
            } else if (column == null) {
                builder.appendNULL();
                continue;
            }
            boolean ok = false;
            switch (columnDesc.getDataType()) {
                case Varchar:
                case String:
                    ok = builder.appendString((String) column);
                    break;
                case Bool:
                    ok = builder.appendBool((Boolean) column);
                    break;
                case SmallInt:
                    ok = builder.appendInt16((Short) column);
                    break;
                case Int:
                    ok = builder.appendInt32((Integer) column);
                    break;
                case Timestamp:
                    if (column instanceof DateTime) {
                        ok = builder.appendTimestamp(((DateTime) column).getMillis());
                    }else if (column instanceof Timestamp) {
                        ok = builder.appendTimestamp(((Timestamp) column).getTime());
                    }else {
                        ok = builder.appendTimestamp((Long) column);
                    }
                    break;
                case Blob:
                    ok = builder.appendBlob((long) column);
                    break;
                case BigInt:
                    ok = builder.appendInt64((Long) column);
                    break;
                case Float:
                    ok = builder.appendFloat((Float) column);
                    break;
                case Date:
                    ok = builder.appendDate((Date)column);
                    break;
                case Double:
                    ok = builder.appendDouble((Double) column);
                    break;
                default:
                    throw new TabletException("unsupported data type");
            }
            if (!ok) {
                throw new TabletException("append " + columnDesc.getDataType().toString() + " error");
            }
        }
        return buffer;
    }

    public static ByteBuffer encode(Map<String, Object> row, List<ColumnDesc> schema) throws TabletException {
        if (row == null || row.size() == 0 || schema == null || schema.size() == 0 || row.size() != schema.size()) {
            throw new TabletException("input error");
        }
        int strLength = RowCodecCommon.calStrLength(row, schema);
        RowBuilder builder = new RowBuilder(schema);
        int size = builder.calTotalLength(strLength);
        ByteBuffer buffer = ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);
        buffer = builder.setBuffer(buffer, size);
        for (int i = 0; i < schema.size(); i++) {
            ColumnDesc columnDesc = schema.get(i);
            Object column = row.get(columnDesc.getName());
            if (columnDesc.isNotNull()
                && column == null) {
                throw new TabletException("col " + columnDesc.getName() + " should not be null");
            } else if (column == null) {
                builder.appendNULL();
                continue;
            }
            boolean ok = false;
            switch (columnDesc.getDataType()) {
                case String:
                case Varchar:
                    ok = builder.appendString((String) column);
                    break;
                case Bool:
                    ok = builder.appendBool((Boolean) column);
                    break;
                case SmallInt:
                    ok = builder.appendInt16((Short) column);
                    break;
                case Int:
                    ok = builder.appendInt32((Integer) column);
                    break;
                case Timestamp:
                    if (column instanceof DateTime) {
                        ok = builder.appendTimestamp(((DateTime) column).getMillis());
                    }else if (column instanceof Timestamp) {
                        ok = builder.appendTimestamp(((Timestamp) column).getTime());
                    }else {
                        ok = builder.appendTimestamp((Long) column);
                    }
                    break;
                case Blob:
                    ok = builder.appendBlob((Long) column);
                    break;
                case BigInt:
                    ok = builder.appendInt64((Long) column);
                    break;
                case Float:
                    ok = builder.appendFloat((Float) column);
                    break;
                case Date:
                    ok = builder.appendDate((Date)column);
                    break;
                case Double:
                    ok = builder.appendDouble((Double) column);
                    break;
                default:
                    throw new TabletException("unsupported data type");
            }
            if (!ok) {
                throw new TabletException("append " + columnDesc.getDataType().toString() + " error");
            }
        }
        return buffer;
    }

    public static ByteBuffer encode(Map<String, Object> row, List<ColumnDesc> schema, Map<String, Long> blobKeys) throws TabletException {
        if (row == null || row.size() == 0 || schema == null || schema.size() == 0 || row.size() != schema.size()) {
            throw new TabletException("input error");
        }
        int strLength = RowCodecCommon.calStrLength(row, schema);
        RowBuilder builder = new RowBuilder(schema);
        int size = builder.calTotalLength(strLength);
        ByteBuffer buffer = ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);
        buffer = builder.setBuffer(buffer, size);
        for (int i = 0; i < schema.size(); i++) {
            ColumnDesc columnDesc = schema.get(i);
            Object column = row.get(columnDesc.getName());
            if (columnDesc.isNotNull()
                    && column == null) {
                throw new TabletException("col " + columnDesc.getName() + " should not be null");
            } else if (column == null) {
                builder.appendNULL();
                continue;
            }
            boolean ok = false;
            switch (columnDesc.getDataType()) {
                case String:
                case Varchar:
                    ok = builder.appendString((String) column);
                    break;
                case Bool:
                    ok = builder.appendBool((Boolean) column);
                    break;
                case SmallInt:
                    ok = builder.appendInt16((Short) column);
                    break;
                case Int:
                    ok = builder.appendInt32((Integer) column);
                    break;
                case Timestamp:
                    if (column instanceof DateTime) {
                        ok = builder.appendTimestamp(((DateTime) column).getMillis());
                    }else if (column instanceof Timestamp) {
                        ok = builder.appendTimestamp(((Timestamp) column).getTime());
                    }else {
                        ok = builder.appendTimestamp((Long) column);
                    }
                    break;
                case Blob:
                    Long key = blobKeys.get(columnDesc.getName());
                    if (key == null) {
                        ok = false;
                        break;
                    }
                    ok = builder.appendBlob(key);
                    break;
                case BigInt:
                    ok = builder.appendInt64((Long) column);
                    break;
                case Float:
                    ok = builder.appendFloat((Float) column);
                    break;
                case Date:
                    ok = builder.appendDate((Date)column);
                    break;
                case Double:
                    ok = builder.appendDouble((Double) column);
                    break;
                default:
                    throw new TabletException("unsupported data type");
            }
            if (!ok) {
                throw new TabletException("append " + columnDesc.getDataType().toString() + " error");
            }
        }
        return buffer;
    }
}
