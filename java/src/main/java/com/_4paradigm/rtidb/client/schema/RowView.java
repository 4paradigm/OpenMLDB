package com._4paradigm.rtidb.client.schema;

import com._4paradigm.rtidb.client.TabletException;
import com._4paradigm.rtidb.client.type.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Date;
import java.util.ArrayList;
import java.util.List;

public class RowView {

    private final static Logger logger = LoggerFactory.getLogger(RowView.class);
    private ByteBuffer row = null;
    private int size = 0;
    List<ColumnDesc> schema = new ArrayList<>();
    private int stringFieldCnt = 0;
    private int strFieldStartOffset = 0;
    private int strAddrLength = 0;
    private List<Integer> offsetVec = new ArrayList<>();
    private boolean isValid = false;


    public RowView(List<ColumnDesc> schema) {
        this.schema = schema;
        this.isValid = true;
        if (schema.size() == 0) {
            isValid = false;
            return;
        }
        init();
    }

    public RowView(List<ColumnDesc> schema, ByteBuffer row, int size) {
        this.schema = schema;
        this.isValid = true;
        this.size = size;
        if (row.order() == ByteOrder.BIG_ENDIAN) {
            row = row.order(ByteOrder.LITTLE_ENDIAN);
        }
        this.row = row;
        if (schema.size() == 0) {
            isValid = false;
            return;
        }
        if (init()) {
            reset(row, size);
        }
    }

    private boolean init() {
        strFieldStartOffset = RowCodecCommon.HEADER_LENGTH + RowCodecCommon.getBitMapSize(schema.size());
        for (int idx = 0; idx < schema.size(); idx++) {
            ColumnDesc column = schema.get(idx);
            if (column.getDataType() == DataType.Varchar || column.getDataType() == DataType.String) {
                offsetVec.add(stringFieldCnt);
                stringFieldCnt++;
            } else {
                if (RowCodecCommon.TYPE_SIZE_MAP.get(column.getDataType()) == null) {
                    isValid = false;
                    logger.warn("type is not supported");
                    return false;
                } else {
                    offsetVec.add(strFieldStartOffset);
                    strFieldStartOffset += RowCodecCommon.TYPE_SIZE_MAP.get(column.getDataType());
                }
            }
        }
        return true;
    }

    public boolean reset(ByteBuffer row, int size) {
        if (schema.size() == 0 || row == null || size <= RowCodecCommon.HEADER_LENGTH ||
                row.getInt(RowCodecCommon.VERSION_LENGTH) != size) {
            isValid = false;
            return false;
        }
        if (row.order() == ByteOrder.BIG_ENDIAN) {
            row = row.order(ByteOrder.LITTLE_ENDIAN);
        }
        this.row = row;
        this.size = size;
        strAddrLength = RowCodecCommon.getAddrLength(size);
        return true;
    }

    private boolean reset(ByteBuffer row) {
        if (schema.size() == 0 || row == null) {
            isValid = false;
            return false;
        }
        if (row.order() == ByteOrder.BIG_ENDIAN) {
            row = row.order(ByteOrder.LITTLE_ENDIAN);
        }
        this.row = row;
        this.size = row.getInt(RowCodecCommon.VERSION_LENGTH);
        if (this.size < RowCodecCommon.HEADER_LENGTH) {
            isValid = false;
            return false;
        }
        strAddrLength = RowCodecCommon.getAddrLength(size);
        return true;
    }

    private boolean checkValid(int idx, DataType type) {
        if (row == null || !isValid) {
            return false;
        }
        if (idx >= schema.size()) {
            return false;
        }
        ColumnDesc column = schema.get(idx);
        if (column.getDataType() != type) {
            return false;
        }
        return true;
    }

    public boolean isNull(int idx) {
        return isNull(row, idx);
    }

    public boolean isNull(ByteBuffer row, int idx) {
        if (row.order() == ByteOrder.BIG_ENDIAN) {
            row = row.order(ByteOrder.LITTLE_ENDIAN);
        }
        int ptr = RowCodecCommon.HEADER_LENGTH + (idx >> 3);
        byte bt = row.get(ptr);
        int ret = bt & (1 << (idx & 0x07));
        return (ret > 0) ? true : false;
    }

    public static int getSize(ByteBuffer row) {
        return row.getInt(RowCodecCommon.VERSION_LENGTH);
    }

    public Boolean getBool(int idx) throws TabletException {
        return (Boolean) getValue(row, idx, DataType.Bool);
    }

    public Integer getInt32(int idx) throws TabletException {
        return (Integer) getValue(row, idx, DataType.Int);
    }

    public Long getTimestamp(int idx) throws TabletException {
        return (Long) getValue(row, idx, DataType.Timestamp);
    }

    public Long getInt64(int idx) throws TabletException {
        return (Long) getValue(row, idx, DataType.BigInt);
    }

    public Short getInt16(int idx) throws TabletException {
        return (Short) getValue(row, idx, DataType.SmallInt);
    }

    public Float getFloat(int idx) throws TabletException {
        return (Float) getValue(row, idx, DataType.Float);
    }

    public Double getDouble(int idx) throws TabletException {
        return (Double) getValue(row, idx, DataType.Double);
    }

    public Object getIntegersNum(ByteBuffer row, int idx, DataType type) throws TabletException {
        switch (type) {
            case SmallInt: {
                return (Short) getValue(row, idx, type);
            }
            case Int: {
                return (Integer) getValue(row, idx, type);
            }
            case Timestamp:
            case BigInt: {
                return (Long) getValue(row, idx, type);
            }
            default:
                throw new TabletException("unsupported data type");
        }
    }

    public Object getValue(int idx, DataType type) throws TabletException {
        return getValue(this.row, idx, type);
    }

    public Object getValue(ByteBuffer row, int idx) throws TabletException {
        if (schema.size() == 0 || row == null || idx >= schema.size()) {
            throw new TabletException("input mistake");
        }
        if (row.order() == ByteOrder.BIG_ENDIAN) {
            row = row.order(ByteOrder.LITTLE_ENDIAN);
        }
        ColumnDesc column = schema.get(idx);
        int size = getSize(row);
        if (size <= RowCodecCommon.HEADER_LENGTH) {
            throw new TabletException("row size is not bigger than header length");
        }
        if (isNull(row, idx)) {
            return null;
        }
        Object val = null;
        int offset = offsetVec.get(idx);
        switch (column.getDataType()) {
            case Bool: {
                int v = row.get(offset);
                if (v == 1) {
                    val = true;
                } else {
                    val = false;
                }
                break;
            }
            case SmallInt:
                val = row.getShort(offset);
                break;
            case Int:
                val = row.getInt(offset);
                break;
            case Timestamp:
            case BigInt:
                val = row.getLong(offset);
                break;
            case Float:
                val = row.getFloat(offset);
                break;
            case Double:
                val = row.getDouble(offset);
                break;
            case Date:
                int date = row.getInt(offset);
                int day = date & 0x0000000FF;
                date = date >> 8;
                int month = date & 0x0000FF;
                int year = date >> 8;
                val = new Date(year, month, day);
                break;
            case Varchar:
                int field_offset = offset;
                int next_str_field_offset = 0;
                if (field_offset < stringFieldCnt - 1) {
                    next_str_field_offset = field_offset + 1;
                }
                return getStrField(row, field_offset, next_str_field_offset,
                        strFieldStartOffset, RowCodecCommon.getAddrLength(size));
            default:
                throw new TabletException("unsupported data type");
        }
        return val;
    }

    public Object getValue(ByteBuffer row, int idx, DataType type) throws TabletException {
        if (schema.size() == 0 || row == null || idx >= schema.size()) {
            throw new TabletException("input mistake");
        }
        if (row.order() == ByteOrder.BIG_ENDIAN) {
            row = row.order(ByteOrder.LITTLE_ENDIAN);
        }
        ColumnDesc column = schema.get(idx);
        if (column.getDataType() != type) {
            throw new TabletException("data type mismatch");
        }
        int size = getSize(row);
        if (size <= RowCodecCommon.HEADER_LENGTH) {
            throw new TabletException("row size is not bigger than header length");
        }
        if (isNull(row, idx)) {
            return null;
        }
        Object val = null;
        int offset = offsetVec.get(idx);
        switch (type) {
            case Bool: {
                int v = row.get(offset);
                if (v == 1) {
                    val = true;
                } else {
                    val = false;
                }
                break;
            }
            case SmallInt:
                val = row.getShort(offset);
                break;
            case Int:
                val = row.getInt(offset);
                break;
            case Timestamp:
            case BigInt:
                val = row.getLong(offset);
                break;
            case Float:
                val = row.getFloat(offset);
                break;
            case Double:
                val = row.getDouble(offset);
                break;
            case Date:
                int date = row.getInt(offset);
                int day = date & 0x0000000FF;
                date = date >> 8;
                int month = date & 0x0000FF;
                int year = date >> 8;
                val = new Date(year, month, day);
                break;
            case String:
            case Varchar:
                int fieldOffset = offset;
                int nextStrFieldOffset = 0;
                if (fieldOffset < stringFieldCnt - 1) {
                    nextStrFieldOffset = fieldOffset + 1;
                }
                return getStrField(row, fieldOffset, nextStrFieldOffset,
                        strFieldStartOffset, RowCodecCommon.getAddrLength(size));
            default:
                throw new TabletException("unsupported data type");
        }
        return val;
    }

    public String getString(int idx) throws TabletException {
        return (String) getValue(row, idx, DataType.Varchar);
    }

    public String getStrField(ByteBuffer row, int fieldOffset,
                              int nextStrFieldOffset, int strStartOffset,
                              int addrSpace) throws TabletException {
        if (row == null) {
            throw new TabletException("row is null");
        }
        if (row.order() == ByteOrder.BIG_ENDIAN) {
            row = row.order(ByteOrder.LITTLE_ENDIAN);
        }
        int rowWithOffset = strStartOffset;
        int strOffset = 0;
        int nextStrOffset = 0;
        switch (addrSpace) {
            case 1: {
                strOffset = row.get(rowWithOffset + fieldOffset * addrSpace) & 0xFF;
                if (nextStrFieldOffset > 0) {
                    nextStrOffset = row.get(rowWithOffset + nextStrFieldOffset * addrSpace) & 0xFF;
                }
                break;
            }
            case 2: {
                strOffset = row.getShort(rowWithOffset + fieldOffset * addrSpace) & 0xFFFF;
                if (nextStrFieldOffset > 0) {
                    nextStrOffset = row.getShort(rowWithOffset + nextStrFieldOffset * addrSpace) & 0xFFFF;
                }
                break;
            }
            case 3: {
                int curRowWithOffset = rowWithOffset + fieldOffset * addrSpace;
                strOffset = row.get(curRowWithOffset) & 0xFF;
                strOffset = (strOffset << 8) + (row.get((curRowWithOffset + 1)) & 0xFF);
                strOffset = (strOffset << 8) + (row.get((curRowWithOffset + 2)) & 0xFF);
                if (nextStrFieldOffset > 0) {
                    int nextRowWithOffset = rowWithOffset + nextStrFieldOffset * addrSpace;
                    nextStrOffset = row.get((nextRowWithOffset)) & 0xFF;
                    nextStrOffset = (nextStrOffset << 8) + (row.get(nextRowWithOffset + 1) & 0xFF);
                    nextStrOffset = (nextStrOffset << 8) + (row.get(nextRowWithOffset + 2) & 0xFF);
                }
                break;
            }
            case 4: {
                strOffset = row.getInt(rowWithOffset + fieldOffset * addrSpace);
                if (nextStrFieldOffset > 0) {
                    nextStrOffset = row.getInt(rowWithOffset + nextStrFieldOffset * addrSpace);
                }
                break;
            }
            default: {
                throw new TabletException("addrSpace mistakes");
            }
        }
        int len;
        if (nextStrFieldOffset <= 0) {
            int totalLength = row.getInt(RowCodecCommon.VERSION_LENGTH);
            len = totalLength - strOffset;
        } else {
            len = nextStrOffset - strOffset;
        }
        byte[] arr = new byte[len];
        row.position(strOffset);
        row.get(arr);
        return new String(arr, RowCodecCommon.CHARSET);
    }
}
