package com._4paradigm.openmldb.common.codec;

import com._4paradigm.openmldb.proto.Common;
import com._4paradigm.openmldb.proto.Type;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Date;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FlexibleRowBuilder implements RowBuilder {

    private List<Common.ColumnDesc> schema = new ArrayList<>();
    private int schemaVersion = 1;
    private int strFieldCnt = 0;
    private int strFieldStartOffset = 0;
    private int baseFieldStartOffset = 0;
    private List<Integer> offsetVec = new ArrayList<>();

    private int curPos = 0;
    private ByteBitMap nullBitmap;
    private ByteBitMap settedValue;
    private ByteBuffer baseFieldBuf;
    private ByteBuffer strAddrBuf;
    private int strAddrSize = 1;
    private ByteArrayDataOutput stringWriter = ByteStreams.newDataOutput();
    private int settedStrCnt = 0;
    private ByteBuffer result;
    private int strTotalLen = 0;
    private int strAddrLen = 0;

    public FlexibleRowBuilder(List<Common.ColumnDesc> schema) throws Exception {
        this(schema, 1);
    }

    public FlexibleRowBuilder(List<Common.ColumnDesc> schema, int schemaversion) throws Exception {
        this.schemaVersion = schemaversion;
        baseFieldStartOffset = CodecUtil.HEADER_LENGTH + CodecUtil.getBitMapSize(schema.size());
        this.schema = schema;
        calcSchemaOffset(schema);
        nullBitmap = new ByteBitMap(schema.size());
        settedValue = new ByteBitMap(schema.size());
        baseFieldBuf = ByteBuffer.allocate(strFieldStartOffset - baseFieldStartOffset).order(ByteOrder.LITTLE_ENDIAN);
        strAddrLen = strAddrSize * strFieldCnt;
        strAddrBuf = ByteBuffer.allocate(strAddrLen).order(ByteOrder.LITTLE_ENDIAN);
    }

    private void calcSchemaOffset(List<Common.ColumnDesc> schema) throws Exception {
        int baseOffset = 0;
        for (int idx = 0; idx < schema.size(); idx++) {
            Common.ColumnDesc column = schema.get(idx);
            if (column.getDataType() == Type.DataType.kVarchar || column.getDataType() == Type.DataType.kString) {
                offsetVec.add(strFieldCnt);
                strFieldCnt++;
            } else {
                if (CodecUtil.TYPE_SIZE_MAP.get(column.getDataType()) == null) {
                    throw new Exception("type is not supported");
                } else {
                    offsetVec.add(baseOffset);
                    baseOffset += CodecUtil.TYPE_SIZE_MAP.get(column.getDataType());
                }
            }
        }
        strFieldStartOffset = baseFieldStartOffset + baseOffset;
    }

    private boolean checkType(int pos, Type.DataType type) {
        return CodecUtil.checkType(schema, pos, type);
    }

    private int getOffset(int idx) {
        return offsetVec.get(idx);
    }

    private void setStrOffset(int strPos) {
        if (strPos >= strFieldCnt) {
            return;
        }
        int curOffset = strFieldStartOffset + strAddrLen + strTotalLen;
        int curStrAddrSize = CodecUtil.getAddrLength(curOffset);
        if (curStrAddrSize > strAddrSize) {
            strAddrBuf = expandStrLenBuf(curStrAddrSize, strPos);
            strAddrSize = curStrAddrSize;
            curOffset = strFieldStartOffset + strAddrLen + strTotalLen;
        }
        CodecUtil.setStrOffset(strAddrBuf, strPos * strAddrSize, curOffset, strAddrSize);
    }

    private ByteBuffer expandStrLenBuf(int newStrAddrsize, int strPos) {
        int newStrAddBufLen = newStrAddrsize * strFieldCnt;
        ByteBuffer newStrAddrBuf = ByteBuffer.allocate(newStrAddBufLen).order(ByteOrder.LITTLE_ENDIAN);
        for (int i = 0; i < strPos; i++) {
            int strOffset = CodecUtil.getStrOffset(strAddrBuf, i * strAddrSize, strAddrSize);
            strOffset += (newStrAddBufLen - strAddrLen);
            CodecUtil.setStrOffset(newStrAddrBuf, i * newStrAddrsize, strOffset, newStrAddrsize);
        }
        strAddrLen = newStrAddBufLen;
        return newStrAddrBuf;
    }

    public void clear() {
        nullBitmap.clear();
        settedValue.clear();
        settedStrCnt = 0;
        baseFieldBuf.clear();
        strAddrBuf.clear();
        stringWriter = ByteStreams.newDataOutput();
        curPos = 0;
        strTotalLen = 0;
        strAddrSize = 1;
        strAddrLen = strAddrSize * strFieldCnt;
        result = null;
    }

    @Override
    public boolean appendNULL() {
        if (!setNULL(curPos)) {
            return false;
        }
        curPos++;
        return true;
    }

    @Override
    public boolean appendBool(boolean val) {
        if (!setBool(curPos, val)) {
            return false;
        }
        curPos++;
        return true;
    }

    @Override
    public boolean appendInt(int val) {
        if (!setInt(curPos, val)) {
            return false;
        }
        curPos++;
        return true;
    }

    @Override
    public boolean appendSmallInt(short val) {
        if (!setSmallInt(curPos, val)) {
            return false;
        }
        curPos++;
        return true;
    }

    @Override
    public boolean appendTimestamp(long val) {
        if (!setTimestamp(curPos, val)) {
            return false;
        }
        curPos++;
        return true;
    }

    @Override
    public boolean appendBigInt(long val) {
        if (!setBigInt(curPos, val)) {
            return false;
        }
        curPos++;
        return true;
    }

    @Override
    public boolean appendFloat(float val) {
        if (!setFloat(curPos, val)) {
            return false;
        }
        curPos++;
        return true;
    }

    @Override
    public boolean appendDouble(double val) {
        if (!setDouble(curPos, val)) {
            return false;
        }
        curPos++;
        return true;
    }

    @Override
    public boolean appendDate(Date date) {
        if (!setDate(curPos, date)) {
            return false;
        }
        curPos++;
        return true;
    }

    @Override
    public boolean appendString(String val) {
        if (!setString(curPos, val)) {
            return false;
        }
        curPos++;
        return true;
    }

    @Override
    public boolean setNULL(int idx) {
        if (idx >= schema.size()) {
            return false;
        }
        nullBitmap.atPut(idx, true);
        settedValue.atPut(idx, true);
        Type.DataType type = schema.get(idx).getDataType();
        if (type == Type.DataType.kVarchar || type == Type.DataType.kString) {
            int strPos = getOffset(idx);
            setStrOffset(strPos + 1);
            settedStrCnt++;
        }
        return true;
    }

    @Override
    public boolean setBool(int idx, boolean val) {
        if (!checkType(idx, Type.DataType.kBool)) {
            return false;
        }
        settedValue.atPut(idx, true);
        if (val) {
            baseFieldBuf.put(getOffset(idx), (byte)1);
        } else {
            baseFieldBuf.put(getOffset(idx), (byte)0);
        }
        return true;
    }

    @Override
    public boolean setInt(int idx, int val) {
        if (!checkType(idx, Type.DataType.kInt)) {
            return false;
        }
        settedValue.atPut(idx, true);
        baseFieldBuf.putInt(getOffset(idx), val);
        return true;
    }

    @Override
    public boolean setSmallInt(int idx, short val) {
        if (!checkType(idx, Type.DataType.kSmallInt)) {
            return false;
        }
        settedValue.atPut(idx, true);
        baseFieldBuf.putShort(getOffset(idx), val);
        return true;
    }

    @Override
    public boolean setTimestamp(int idx, long val) {
        if (!checkType(idx, Type.DataType.kTimestamp)) {
            return false;
        }
        settedValue.atPut(idx, true);
        baseFieldBuf.putLong(getOffset(idx), val);
        return true;
    }

    @Override
    public boolean setBigInt(int idx, long val) {
        if (!checkType(idx, Type.DataType.kBigInt)) {
            return false;
        }
        settedValue.atPut(idx, true);
        baseFieldBuf.putLong(getOffset(idx), val);
        return true;
    }

    @Override
    public boolean setFloat(int idx, float val) {
        if (!checkType(idx, Type.DataType.kFloat)) {
            return false;
        }
        settedValue.atPut(idx, true);
        baseFieldBuf.putFloat(getOffset(idx), val);
        return true;
    }

    @Override
    public boolean setDouble(int idx, double val) {
        if (!checkType(idx, Type.DataType.kDouble)) {
            return false;
        }
        settedValue.atPut(idx, true);
        baseFieldBuf.putDouble(getOffset(idx), val);
        return true;
    }

    @Override
    public boolean setDate(int idx, Date val) {
        if (!checkType(idx, Type.DataType.kDate)) {
            return false;
        }
        settedValue.atPut(idx, true);
        int dateVal = CodecUtil.dateToDateInt(val);
        baseFieldBuf.putInt(getOffset(idx), dateVal);
        return true;
    }

    @Override
    public boolean setString(int idx, String val) {
        if (!checkType(idx, Type.DataType.kString) && !checkType(idx, Type.DataType.kVarchar)) {
            return false;
        }
        if (settedValue.at(idx)) {
            return false;
        }
        settedValue.atPut(idx, true);
        byte[] bytes = val.getBytes(CodecUtil.CHARSET);
        stringWriter.write(bytes);
        if (settedStrCnt == 0) {
            setStrOffset(settedStrCnt);
        }
        strTotalLen += bytes.length;
        settedStrCnt++;
        setStrOffset(settedStrCnt);

        // TODO:  process disorder set
        return true;
    }

    @Override
    public boolean build() {
        if (!settedValue.allSetted()) {
            return false;
        }
        int totalSize = strFieldStartOffset + strAddrLen + strTotalLen;
        result = ByteBuffer.allocate(totalSize).order(ByteOrder.LITTLE_ENDIAN);;
        result.put((byte)1);                     // FVersion
        result.put((byte)schemaVersion);         // SVersion
        result.putInt(totalSize);                // Size
        result.put(nullBitmap.getBuffer());      // BitMap
        result.put(baseFieldBuf.array());        // Base data type
        result.put(strAddrBuf.array());          // String addr
        result.put(stringWriter.toByteArray());  // String value
        return true;
    }

    @Override
    public ByteBuffer getValue() {
        return result;
    }
}
