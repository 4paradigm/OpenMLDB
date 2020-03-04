package com._4paradigm.rtidb.client.schema;

import com._4paradigm.rtidb.type.Type.DataType;
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
        str_field_start_offset = RowCodecUtil.HEADER_LENGTH + RowCodecUtil.getBitMapSize(schema.size());
        this.schema = schema;
        for (int idx = 0; idx < schema.size(); idx++) {
            ColumnDesc column = schema.get(idx);
            if (column.getDataType() == DataType.kVarchar) {
                offset_vec.add(str_field_cnt);
                str_field_cnt++;
            } else {
                if (RowCodecUtil.TYPE_SIZE_MAP.get(column.getDataType()) == null) {
                    logger.warn("type is not supported");
                } else {
                    offset_vec.add(str_field_start_offset);
                    str_field_start_offset += RowCodecUtil.TYPE_SIZE_MAP.get(column.getDataType());
                }
            }
        }
    }

    public int calTotalLength(int string_length) {
        if (schema.size() == 0) {
            return 0;
        }
        int total_length = str_field_start_offset;
        total_length += string_length;
        if (total_length + str_field_cnt <= RowCodecUtil.UINT8_MAX) {
            return total_length + str_field_cnt;
        } else if (total_length + str_field_cnt * 2 <= RowCodecUtil.UINT16_MAX) {
            return total_length + str_field_cnt * 2;
        } else if (total_length + str_field_cnt * 3 <= RowCodecUtil.UINT24_MAX) {
            return total_length + str_field_cnt * 3;
        } else if (total_length + str_field_cnt * 4 <= RowCodecUtil.UINT32_MAX) {
            return total_length + str_field_cnt * 4;
        }
        return 0;
    }

    public boolean setBuffer(ByteBuffer buffer, int size) {
        if (buffer == null || size == 0 ||
                size < str_field_start_offset + str_field_cnt) {
            return false;
        }
        buffer = ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);
        this.size = size;
        buffer.put((byte) 1); // FVersion
        buffer.put((byte) 1); // SVersion
        buffer.putInt(size); // size
        this.buf = buffer;
        str_addr_length = RowCodecUtil.getAddrLength(size);
        str_offset = str_field_start_offset + str_addr_length * str_field_cnt;
        return true;
    }

    private boolean check(DataType type) {
        if (cnt >= schema.size()) {
            return false;
        }
        ColumnDesc column = schema.get(cnt);
        if (column.getDataType() != type) {
            return false;
        }
        if (column.getDataType() != DataType.kVarchar) {
            if (RowCodecUtil.TYPE_SIZE_MAP.get(column.getDataType()) == null) {
                return false;
            }
        }
        return true;
    }

    boolean appendNULL() {
        int index = RowCodecUtil.HEADER_LENGTH + (cnt >> 3);
        byte bt = buf.get(index);
        buf.put(index, (byte) (bt | (1 << (cnt & 0x07))));
        ColumnDesc column = schema.get(cnt);
        if (column.getDataType() == DataType.kVarchar) {
            index = str_field_start_offset + str_addr_length * offset_vec.get(cnt);
            buf.position(index);
            if (str_addr_length == 1) {
                buf.put((byte) str_offset);
            } else if (str_addr_length == 2) {
                buf.putShort((short) str_offset);
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

    boolean appendBool(boolean val) {
        if (!check(DataType.kBool)) {
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

    boolean appendInt32(int val) {
        if (!check(DataType.kInt32)) {
            return false;
        }
        buf.position(offset_vec.get(cnt));
        buf.putInt(val);
        cnt++;
        return true;
    }

    boolean appendInt16(Short val) {
        if (!check(DataType.kInt16)) {
            return false;
        }
        buf.position(offset_vec.get(cnt));
        buf.putShort(val);
        cnt++;
        return true;
    }

    boolean appendTimestamp(long val) {
        if (!check(DataType.kTimestamp)) {
            return false;
        }
        buf.position(offset_vec.get(cnt));
        buf.putLong(val);
        cnt++;
        return true;
    }

    boolean appendInt64(long val) {
        if (!check(DataType.kInt64)) {
            return false;
        }
        buf.position(offset_vec.get(cnt));
        buf.putLong(val);
        cnt++;
        return true;
    }

    boolean AppendFloat(float val) {
        if (!check(DataType.kFloat)) {
            return false;
        }
        buf.position(offset_vec.get(cnt));
        buf.putFloat(val);
        cnt++;
        return true;
    }

    boolean appendDouble(double val) {
        if (!check(DataType.kDouble)) {
            return false;
        }
        buf.position(offset_vec.get(cnt));
        buf.putDouble(val);
        cnt++;
        return true;
    }

    public boolean appendString(String val) {
        int length = val.length();
        if (val == null || !check(DataType.kVarchar)) {
            return false;
        }
        if (str_offset + length > size) {
            return false;
        }
        int index = str_field_start_offset + str_addr_length * offset_vec.get(cnt);
        buf.position(index);
        if (str_addr_length == 1) {
            buf.put((byte) str_offset);
        } else if (str_addr_length == 2) {
            buf.putShort((short) str_offset);
        } else if (str_addr_length == 3) {
            buf.put((byte) (str_offset >> 16));
            buf.put((byte) ((str_offset & 0xFF00) >> 8));
            buf.put((byte) (str_offset & 0x00FF));
        } else {
            buf.putInt(str_offset);
        }
        if (length != 0) {
            buf.put(val.getBytes(RowCodecUtil.CHARSET), str_offset, length);
        }
        str_offset += length;
        cnt++;
        return true;
    }
}
