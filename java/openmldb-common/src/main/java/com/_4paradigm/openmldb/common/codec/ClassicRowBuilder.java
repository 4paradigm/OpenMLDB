/*
 * Copyright 2021 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com._4paradigm.openmldb.common.codec;

import com._4paradigm.openmldb.proto.Type.DataType;
import com._4paradigm.openmldb.proto.Common.ColumnDesc;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ClassicRowBuilder implements RowBuilder {

    private ByteBuffer buf;
    private int size = 0;
    private int cnt = 0;
    List<ColumnDesc> schema = new ArrayList<>();
    private int strFieldCnt = 0;
    private int strFieldStartOffset = 0;
    private int strAddrLength = 0;
    private int strOffset = 0;
    private int schemaVersion = 1;
    private List<Integer> offsetVec = new ArrayList<>();
    private List<Integer> strIdx = new ArrayList<>();
    private int curPos = 0;

    public ClassicRowBuilder(List<ColumnDesc> schema) throws Exception {
        calcSchemaOffset(schema);
    }

    public ClassicRowBuilder(List<ColumnDesc> schema, int schemaversion) throws Exception {
        this.schemaVersion = schemaversion;
        calcSchemaOffset(schema);
    }

    private void calcSchemaOffset(List<ColumnDesc> schema) throws Exception {
        strFieldStartOffset = CodecUtil.HEADER_LENGTH + CodecUtil.getBitMapSize(schema.size());
        this.schema = schema;
        for (int idx = 0; idx < schema.size(); idx++) {
            ColumnDesc column = schema.get(idx);
            if (column.getDataType() == DataType.kVarchar || column.getDataType() == DataType.kString) {
                offsetVec.add(strFieldCnt);
                strIdx.add(idx);
                strFieldCnt++;
            } else {
                if (CodecUtil.TYPE_SIZE_MAP.get(column.getDataType()) == null) {
                    throw new Exception("type is not supported");
                } else {
                    offsetVec.add(strFieldStartOffset);
                    strFieldStartOffset += CodecUtil.TYPE_SIZE_MAP.get(column.getDataType());
                }
            }
        }
    }

    public void SetSchemaVersion(int version) {
        this.schemaVersion = version;
    }

    public int calTotalLength(List<Object> row) throws Exception {
        if (row.size() != schema.size()) {
            throw new Exception("row size is not equal schema size");
        }
        int stringLength = 0;
        for (Integer idx : strIdx) {
            Object obj = row.get(idx);
            if (obj == null) {
                continue;
            }
            stringLength += ((String)obj).getBytes(CodecUtil.CHARSET).length;
        }
        return calTotalLength(stringLength);
    }

    public int calTotalLength(int stringLength) throws Exception {
        if (schema.size() == 0) {
            return 0;
        }
        long totalLength = strFieldStartOffset + stringLength;
        if (totalLength + strFieldCnt <= CodecUtil.UINT8_MAX) {
            totalLength += strFieldCnt;
        } else if (totalLength + strFieldCnt * 2 <= CodecUtil.UINT16_MAX) {
            totalLength += strFieldCnt * 2;
        } else if (totalLength + strFieldCnt * 3 <= CodecUtil.UINT24_MAX) {
            totalLength += strFieldCnt * 3;
        } else if (totalLength + strFieldCnt * 4 <= CodecUtil.UINT32_MAX) {
            totalLength += strFieldCnt * 4;
        }
        if (totalLength > Integer.MAX_VALUE) {
            throw new Exception("total length is bigger than integer max value");
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
        buffer.put((byte) schemaVersion); // SVersion
        buffer.putInt(size); // size
        for (int idx = 0; idx < CodecUtil.getBitMapSize(schema.size()); idx++) {
            buffer.put((byte)0xFF);
        }
        this.buf = buffer;
        strAddrLength = CodecUtil.getAddrLength(size);
        strOffset = strFieldStartOffset + strAddrLength * strFieldCnt;
        return this.buf;
    }

    private void setField(int index) {
        int pos = CodecUtil.HEADER_LENGTH + (index >> 3);
        byte bt = buf.get(pos);
        buf.put(pos, (byte) (bt & (~(1 << (cnt & 0x07)))));
    }

    private boolean check(DataType type) {
        return CodecUtil.checkType(schema, cnt, type);
    }

    private void setStrOffset(int strPos) {
        if (strPos >= strFieldCnt) {
            return;
        }
        int index = strFieldStartOffset + strAddrLength * strPos;
        CodecUtil.setStrOffset(buf, index, strOffset, strAddrLength);
    }

    @Override
    public boolean appendNULL() {
        ColumnDesc column = schema.get(cnt);
        if (column.getDataType() == DataType.kVarchar || column.getDataType() == DataType.kString) {
            int strPos = offsetVec.get(cnt);
            setStrOffset(strPos + 1);
        }
        cnt++;
        return true;
    }

    @Override
    public boolean appendBool(boolean val) {
        if (!check(DataType.kBool)) {
            return false;
        }
        setField(cnt);
        buf.position(offsetVec.get(cnt));
        if (val) {
            buf.put((byte) 1);
        } else {
            buf.put((byte) 0);
        }
        cnt++;
        return true;
    }

    @Override
    public boolean appendInt(int val) {
        if (!check(DataType.kInt)) {
            return false;
        }
        setField(cnt);
        buf.position(offsetVec.get(cnt));
        buf.putInt(val);
        cnt++;
        return true;
    }

    @Override
    public boolean appendSmallInt(short val) {
        if (!check(DataType.kSmallInt)) {
            return false;
        }
        setField(cnt);
        buf.position(offsetVec.get(cnt));
        buf.putShort(val);
        cnt++;
        return true;
    }

    @Override
    public boolean appendTimestamp(Timestamp val) {
        if (!check(DataType.kTimestamp)) {
            return false;
        }
        if (val == null) {
            return appendNULL();
        }
        setField(cnt);
        buf.position(offsetVec.get(cnt));
        buf.putLong(val.getTime());
        cnt++;
        return true;
    }

    @Override
    public boolean appendBigInt(long val) {
        if (!check(DataType.kBigInt)) {
            return false;
        }
        setField(cnt);
        buf.position(offsetVec.get(cnt));
        buf.putLong(val);
        cnt++;
        return true;
    }

    @Override
    public boolean appendFloat(float val) {
        if (!check(DataType.kFloat)) {
            return false;
        }
        setField(cnt);
        buf.position(offsetVec.get(cnt));
        buf.putFloat(val);
        cnt++;
        return true;
    }

    @Override
    public boolean appendDouble(double val) {
        if (!check(DataType.kDouble)) {
            return false;
        }
        setField(cnt);
        buf.position(offsetVec.get(cnt));
        buf.putDouble(val);
        cnt++;
        return true;
    }

    @Override
    public boolean appendDate(Date date) {
        if (date == null) {
            return appendNULL();
        }
        int dateInt = CodecUtil.dateToDateInt(date);
        buf.position(offsetVec.get(cnt));
        buf.putInt(dateInt);
        setField(cnt);
        cnt++;
        return true;
    }

    @Override
    public boolean appendString(String val) {
        if (!check(DataType.kVarchar) && !check(DataType.kString)) {
            return false;
        }
        if (val == null) {
            return appendNULL();
        }
        byte[] bytes = val.getBytes(CodecUtil.CHARSET);
        int length = bytes.length;
        if (strOffset + length > size) {
            return false;
        }
        int strPos = offsetVec.get(cnt);
        if (strPos == 0) {
            setStrOffset(strPos);
        }
        if (length != 0) {
            buf.position(strOffset);
            buf.put(val.getBytes(CodecUtil.CHARSET), 0, length);
        }
        strOffset += length;
        setStrOffset(strPos + 1);
        setField(cnt);
        cnt++;
        return true;
    }

    public static ByteBuffer encode(Object[] row, List<ColumnDesc> schema, int schemaVer) throws Exception {
        if (row == null || row.length == 0 || schema == null || schema.size() == 0 || row.length != schema.size()) {
            throw new Exception("input error");
        }
        int strLength = CodecUtil.calStrLength(row, schema);
        ClassicRowBuilder builder = new ClassicRowBuilder(schema, schemaVer);
        int size = builder.calTotalLength(strLength);
        ByteBuffer buffer = ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);
        buffer = builder.setBuffer(buffer, size);
        for (int i = 0; i < schema.size(); i++) {
            ColumnDesc columnDesc = schema.get(i);
            Object column = row[i];
            if (columnDesc.getNotNull() && column == null) {
                throw new Exception("col " + columnDesc.getName() + " should not be null");
            } else if (column == null) {
                builder.appendNULL();
                continue;
            }
            boolean ok = false;
            switch (columnDesc.getDataType()) {
                case kVarchar:
                case kString:
                    ok = builder.appendString((String) column);
                    break;
                case kBool:
                    ok = builder.appendBool((Boolean) column);
                    break;
                case kSmallInt:
                    ok = builder.appendSmallInt((Short) column);
                    break;
                case kInt:
                    ok = builder.appendInt((Integer) column);
                    break;
                case kTimestamp:
                    if (column instanceof Timestamp) {
                        ok = builder.appendTimestamp((Timestamp) column);
                    } else {
                        ok = false;
                    }
                    break;
                case kBigInt:
                    ok = builder.appendBigInt((Long) column);
                    break;
                case kFloat:
                    ok = builder.appendFloat((Float) column);
                    break;
                case kDate:
                    ok = builder.appendDate((Date)column);
                    break;
                case kDouble:
                    ok = builder.appendDouble((Double) column);
                    break;
                default:
                    throw new Exception("unsupported data type");
            }
            if (!ok) {
                throw new Exception("append " + columnDesc.getDataType().toString() + " error");
            }
        }
        return buffer;
    }

    public static ByteBuffer encode(Map<String, Object> row, List<ColumnDesc> schema, Map<String, Long> blobKeys, int schemaVersion) throws Exception {
        if (row == null || row.size() == 0 || schema == null || schema.size() == 0 || row.size() != schema.size()) {
            throw new Exception("input error");
        }
        int strLength = CodecUtil.calStrLength(row, schema);
        ClassicRowBuilder builder = new ClassicRowBuilder(schema, schemaVersion);
        int size = builder.calTotalLength(strLength);
        ByteBuffer buffer = ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);
        buffer = builder.setBuffer(buffer, size);
        for (int i = 0; i < schema.size(); i++) {
            ColumnDesc columnDesc = schema.get(i);
            Object column = row.get(columnDesc.getName());
            if (columnDesc.getNotNull()
                    && column == null) {
                throw new Exception("col " + columnDesc.getName() + " should not be null");
            } else if (column == null) {
                builder.appendNULL();
                continue;
            }
            boolean ok = false;
            switch (columnDesc.getDataType()) {
                case kString:
                case kVarchar:
                    ok = builder.appendString((String) column);
                    break;
                case kBool:
                    ok = builder.appendBool((Boolean) column);
                    break;
                case kSmallInt:
                    ok = builder.appendSmallInt((Short) column);
                    break;
                case kInt:
                    ok = builder.appendInt((Integer) column);
                    break;
                case kTimestamp:
                    if (column instanceof Timestamp) {
                        ok = builder.appendTimestamp((Timestamp)column);
                    }else {
                        ok = false;
                    }
                    break;
                case kBigInt:
                    ok = builder.appendBigInt((Long) column);
                    break;
                case kFloat:
                    ok = builder.appendFloat((Float) column);
                    break;
                case kDate:
                    ok = builder.appendDate((Date)column);
                    break;
                case kDouble:
                    ok = builder.appendDouble((Double) column);
                    break;
                default:
                    throw new Exception("unsupported data type");
            }
            if (!ok) {
                throw new Exception("append " + columnDesc.getDataType().toString() + " error");
            }
        }
        return buffer;
    }

    @Override
    public boolean setNULL(int idx) {
        if (idx != curPos) {
            return false;
        }
        curPos++;
        return appendNULL();
    }

    @Override
    public boolean setBool(int idx, boolean val) {
        if (idx != curPos) {
            return false;
        }
        curPos++;
        return appendBool(val);
    }

    @Override
    public boolean setInt(int idx, int val) {
        if (idx != curPos) {
            return false;
        }
        curPos++;
        return appendInt(val);
    }

    @Override
    public boolean setSmallInt(int idx, short val) {
        if (idx != curPos) {
            return false;
        }
        curPos++;
        return appendSmallInt(val);
    }

    @Override
    public boolean setTimestamp(int idx, Timestamp val) {
        if (idx != curPos) {
            return false;
        }
        curPos++;
        return appendTimestamp(val);
    }

    @Override
    public boolean setBigInt(int idx, long val) {
        if (idx != curPos) {
            return false;
        }
        curPos++;
        return appendBigInt(val);
    }

    @Override
    public boolean setFloat(int idx, float val){
        if (idx != curPos) {
            return false;
        }
        curPos++;
        return appendFloat(val);
    }

    @Override
    public boolean setDouble(int idx, double val) {
        if (idx != curPos) {
            return false;
        }
        curPos++;
        return appendDouble(val);
    }

    @Override
    public boolean setDate(int idx, Date date) {
        if (idx != curPos) {
            return false;
        }
        curPos++;
        return appendDate(date);
    }

    @Override
    public boolean setString(int idx, String val) {
        if (idx != curPos) {
            return false;
        }
        curPos++;
        return appendString(val);
    }

    @Override
    public boolean build() {
        return true;
    }

    @Override
    public ByteBuffer getValue() {
        return buf;
    }
}

