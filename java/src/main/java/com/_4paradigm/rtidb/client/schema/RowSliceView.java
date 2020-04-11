package com._4paradigm.rtidb.client.schema;

import com._4paradigm.rtidb.client.TabletException;
import com._4paradigm.rtidb.client.type.DataType;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Date;
import java.util.ArrayList;
import java.util.List;

public class RowSliceView {

    private final static byte BOOL_FALSE = 0;
    private final static Logger logger = LoggerFactory.getLogger(RowSliceView.class);
    private List<ColumnDesc> schema;
    private int stringFieldCnt = 0;
    private int strFieldStartOffset = 0;
    private List<Integer> offsetVec;
    private boolean isValid = false;
    public RowSliceView(List<ColumnDesc> schema) {
        this.schema = schema;
        this.isValid = true;
        if (schema.size() == 0) {
            isValid = false;
            return;
        }
        init();
    }

    private boolean init() {
        offsetVec = new ArrayList<>(schema.size());
        strFieldStartOffset = RowCodecCommon.HEADER_LENGTH + RowCodecCommon.getBitMapSize(schema.size());
        for (int idx = 0; idx < schema.size(); idx++) {
            ColumnDesc column = schema.get(idx);
            if (column.getDataType() == DataType.Varchar) {
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
    public void read(ByteBuffer buf, Object[] row, int start, int length) throws TabletException{
        if (buf == null) throw new TabletException("buf is null");
        int size = buf.getInt(RowCodecCommon.VERSION_LENGTH);
        int index = start;
        int strAddrLength = RowCodecCommon.getAddrLength(size);
        for (int i = 0; i < schema.size() && i < length ; i++) {
            if (isNull(buf, i)) continue;
            ColumnDesc column = schema.get(i);
            int offset = offsetVec.get(i);
            switch (column.getDataType()) {
                case Bool:
                    row[index] = buf.get(offset) == BOOL_FALSE ? false: true;
                    break;
                case SmallInt:
                    row[index] = buf.getShort(offset);
                    break;
                case Int:
                    row[index] = buf.getInt(offset);
                    break;
                case BigInt:
                    row[index] = buf.getLong(offset);
                    break;
                case Float:
                    row[index] = buf.getFloat(offset);
                    break;
                case Double:
                    row[index] = buf.getDouble(offset);
                    break;
                case Timestamp:
                    row[index] = new DateTime(buf.getLong(offset));
                    break;
                case Date:
                    int date = buf.getInt(offset);
                    int day = date & 0x0000000FF;
                    date = date >> 8;
                    int month = date & 0x0000FF;
                    int year = date >> 8;
                    row[index] = new Date(year, month, day);
                    break;
                case Varchar:
                    int nextStrFieldOffset = 0;
                    if (offset < stringFieldCnt - 1) {
                        nextStrFieldOffset = offset + 1;
                    }
                    row[index] = getStrField(buf, offset, nextStrFieldOffset,
                            strFieldStartOffset, strAddrLength, size);
                    break;
                default:
                    throw new TabletException("invalid column type");
            }
            index ++;
        }
    }

    public Object[] read(ByteBuffer buf) throws TabletException{
        Object[] row = new Object[schema.size()];
        read(buf, row, 0, row.length);
        return row;
    }

    private boolean isNull(ByteBuffer row, int idx) {
        if (row.order() == ByteOrder.BIG_ENDIAN) {
            row = row.order(ByteOrder.LITTLE_ENDIAN);
        }
        int ptr = RowCodecCommon.HEADER_LENGTH + (idx >> 3);
        byte bt = row.get(ptr);
        int ret = bt & (1 << (idx & 0x07));
        return (ret > 0) ? true : false;
    }

    private String getStrField(ByteBuffer row, int fieldOffset,
                              int nextStrFieldOffset, int strStartOffset,
                              int addrSpace,
                              int size){
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
        }
        int len;
        if (nextStrFieldOffset <= 0) {
            len = size - strOffset;
        } else {
            len = nextStrOffset - strOffset;
        }
        byte[] arr = new byte[len];
        row.position(strOffset);
        row.get(arr, 0, len);
        return new String(arr, RowCodecCommon.CHARSET);
    }
}
