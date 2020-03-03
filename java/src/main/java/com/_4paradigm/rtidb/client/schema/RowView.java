package com._4paradigm.rtidb.client.schema;

import com._4paradigm.rtidb.client.TabletException;
import com._4paradigm.rtidb.type.Type.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

public class RowView {

    private final static Logger logger = LoggerFactory.getLogger(RowView.class);
    private ByteBuffer row;
    private int size = 0;
    private int cnt = 0;
    List<ColumnDesc> schema = new ArrayList<>();
    private int string_field_cnt = 0;
    private int str_field_start_offset = 0;
    private int str_addr_length = 0;
    private int str_offset = 0;
    private List<Integer> offset_vec = new ArrayList<>();
    private boolean is_valid = false;

    public RowView(List<ColumnDesc> schema) {
        this.schema = schema;
        this.is_valid = true;
        if (schema.size() == 0) {
            is_valid = false;
            return;
        }
        init();
    }

    public RowView(List<ColumnDesc> schema, ByteBuffer row, int size) {
        this.schema = schema;
        this.is_valid = true;
        this.size = size;
        if (row.order() == ByteOrder.BIG_ENDIAN) {
            row = row.order(ByteOrder.LITTLE_ENDIAN);
        }
        this.row = row;
        if (schema.size() == 0) {
            is_valid = false;
            return;
        }
        if (init()) {
            reset(row, size);
        }
    }

    private boolean init() {
        str_field_start_offset = RowCodecUtil.HEADER_LENGTH + RowCodecUtil.getBitMapSize(schema.size());
        for (int idx = 0; idx < schema.size(); idx++) {
            ColumnDesc column = schema.get(idx);
            if (column.getDataType() == DataType.kVarchar) {
                offset_vec.add(string_field_cnt);
                string_field_cnt++;
            } else {
                if (RowCodecUtil.TYPE_SIZE_MAP.get(column.getDataType()) == null) {
                    is_valid = false;
                    logger.warn("type is not supported");
                    return false;
                } else {
                    offset_vec.add(str_field_start_offset);
                    str_field_start_offset += RowCodecUtil.TYPE_SIZE_MAP.get(column.getDataType());
                }
            }
        }
        return true;
    }

    private boolean reset(ByteBuffer row, int size) {
        if (schema.size() == 0 || row == null || size <= RowCodecUtil.HEADER_LENGTH ||
                row.get(RowCodecUtil.VERSION_LENGTH) != size) {
            is_valid = false;
            return false;
        }
        if (row.order() == ByteOrder.BIG_ENDIAN) {
            row = row.order(ByteOrder.LITTLE_ENDIAN);
        }
        this.row = row;
        this.size = size;
        str_addr_length = RowCodecUtil.getAddrLength(size);
        return true;
    }

    private boolean reset(ByteBuffer row) {
        if (schema.size() == 0 || row == null) {
            is_valid = false;
            return false;
        }
        if (row.order() == ByteOrder.BIG_ENDIAN) {
            row = row.order(ByteOrder.LITTLE_ENDIAN);
        }
        this.row = row;
        this.size = row.get(RowCodecUtil.VERSION_LENGTH);
        if (this.size < RowCodecUtil.HEADER_LENGTH) {
            is_valid = false;
            return false;
        }
        str_addr_length = RowCodecUtil.getAddrLength(size);
        return true;
    }

    private boolean checkValid(int idx, DataType type) {
        if (row == null || !is_valid) {
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

    private boolean isNull(ByteBuffer row, int idx) {
        if (row.order() == ByteOrder.BIG_ENDIAN) {
            row = row.order(ByteOrder.LITTLE_ENDIAN);
        }
        int ptr = RowCodecUtil.HEADER_LENGTH + (idx >> 3);
        byte bt = row.get(ptr);
        int ret = bt & (1 << (idx & 0x07));
        return (ret > 0) ? true : false;
    }

    public static int getSize(ByteBuffer row) {
        return row.getInt(RowCodecUtil.VERSION_LENGTH);
    }

    public Boolean getBool(int idx) throws TabletException {
        return (Boolean) getValue(row, idx, DataType.kBool);
    }

    public Integer getInt32(int idx) throws TabletException {
        return (Integer) getValue(row, idx, DataType.kInt32);
    }

    public Long getTimestamp(int idx) throws TabletException {
        return (Long) getValue(row, idx, DataType.kTimestamp);
    }

    public Long getInt64(int idx) throws TabletException {
        return (Long) getValue(row, idx, DataType.kInt64);
    }

    public Short getInt16(int idx) throws TabletException {
        return (Short) getValue(row, idx, DataType.kInt16);
    }

    public Float getFloat(int idx) throws TabletException {
        return (Float) getValue(row, idx, DataType.kFloat);
    }

    public Double getDouble(int idx) throws TabletException {
        return (Double) getValue(row, idx, DataType.kDouble);
    }

    public Object getInteger(ByteBuffer row, int idx, DataType type) throws TabletException {
        switch (type) {
            case kInt16: {
                return (Short) getValue(row, idx, type);
            }
            case kInt32: {
                return (Integer) getValue(row, idx, type);
            }
            case kTimestamp:
            case kInt64: {
                return (Long) getValue(row, idx, type);
            }
            default:
                throw new TabletException("unsupported data type");
        }
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
        if (getSize(row) <= RowCodecUtil.HEADER_LENGTH) {
            throw new TabletException("row size is not bigger than header length");
        }
        if (isNull(row, idx)) {
            return null;
        }
        Object val = null;
        int offset = offset_vec.get(idx);
        switch (type) {
            case kBool: {
                int v = row.get(offset);
                if (v == 1) {
                    val = true;
                } else {
                    val = false;
                }
                break;
            }
            case kInt16:
                val = row.getShort(offset);
                break;
            case kInt32:
                val = row.getInt(offset);
                break;
            case kTimestamp:
            case kInt64:
                val = row.getLong(offset);
                break;
            case kFloat:
                val = row.getFloat(offset);
                break;
            case kDouble:
                val = row.getDouble(offset);
                break;
            default:
                throw new TabletException("unsupported data type");
        }
        return val;
    }

    public String getValue(ByteBuffer row, int idx, int[] length) throws TabletException{
        if (schema.size() == 0 || row == null || length == null || idx >= schema.size()) {
            throw new TabletException("input mistake");
        }
        if (row.order() == ByteOrder.BIG_ENDIAN) {
            row = row.order(ByteOrder.LITTLE_ENDIAN);
        }
        ColumnDesc column = schema.get(idx);
        if (column.getDataType() != DataType.kVarchar) {
            throw new TabletException("data type mismatch");
        }
        int size = getSize(row);
        if (size <= RowCodecUtil.HEADER_LENGTH) {
            throw new TabletException("row size is not bigger than header length");
        }
        if (isNull(row, idx)) {
            return null;
        }
        int field_offset = offset_vec.get(idx);
        int next_str_field_offset = 0;
        if (offset_vec.get(idx) < string_field_cnt - 1) {
            next_str_field_offset = field_offset + 1;
        }
        return getStrField(row, field_offset, next_str_field_offset,
                str_field_start_offset, RowCodecUtil.getAddrLength(size), length);
    }

    public String getString(int idx, int[] length) throws TabletException {
        if (!checkValid(idx, DataType.kVarchar)) {
            throw new TabletException("checkValid false");
        }
        if (isNull(row, idx)) {
            return null;
        }
        int field_offset = offset_vec.get(idx);
        int next_str_field_offset = 0;
        if (field_offset < string_field_cnt - 1) {
            next_str_field_offset = field_offset + 1;
        }
        return getStrField(row, field_offset, next_str_field_offset,
                str_field_start_offset, str_addr_length, length);
    }

    public String getStrField(ByteBuffer row, int field_offset,
                              int next_str_field_offset, int str_start_offset,
                              int addr_space, int[] size) throws TabletException {
        if (row == null || size == null) {
            return null;
        }
        if (row.order() == ByteOrder.BIG_ENDIAN) {
            row = row.order(ByteOrder.LITTLE_ENDIAN);
        }
        int row_with_offset = str_start_offset;
        int str_offset = 0;
        int next_str_offset = 0;
        switch (addr_space) {
            case 1: {
                str_offset = row.get(row_with_offset + field_offset * addr_space);
                if (next_str_field_offset > 0) {
                    next_str_offset = row.get(row_with_offset + next_str_field_offset * addr_space);
                }
                break;
            }
            case 2: {
                str_offset = row.getShort(row_with_offset + field_offset * addr_space);
                if (next_str_field_offset > 0) {
                    next_str_offset = row.getShort(row_with_offset + next_str_field_offset * addr_space);
                }
                break;
            }
            case 3: {
                int cur_row_with_offset = row_with_offset + field_offset * addr_space;
                str_offset = row.get(cur_row_with_offset);
                str_offset = (str_offset << 8) + row.get((cur_row_with_offset + 1));
                str_offset = (str_offset << 8) + row.get((cur_row_with_offset + 2));
                if (next_str_field_offset > 0) {
                    int next_row_with_offset = row_with_offset + next_str_field_offset * addr_space;
                    next_str_offset = row.get((next_row_with_offset));
                    next_str_offset = (next_str_offset << 8) + row.get(next_row_with_offset + 1);
                    next_str_offset = (next_str_offset << 8) + row.get(next_row_with_offset + 2);
                }
                break;
            }
            case 4: {
                str_offset = row.getInt(row_with_offset + field_offset * addr_space);
                if (next_str_field_offset > 0) {
                    next_str_offset = row.getInt(row_with_offset + next_str_field_offset * addr_space);
                }
                break;
            }
            default: {
                throw new TabletException("addr_space mistakes");
            }
        }
        if (next_str_field_offset <= 0) {
            int total_length = row.getInt(RowCodecUtil.VERSION_LENGTH);
            size[0] = total_length - str_offset;
        } else {
            size[0] = next_str_offset - str_offset;
        }
        byte[] arr = null;
        row.get(arr, str_offset, size[0]);
        return new String(arr, RowCodecUtil.CHARSET);
    }
}
