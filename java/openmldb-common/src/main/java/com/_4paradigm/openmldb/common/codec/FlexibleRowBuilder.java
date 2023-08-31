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

import com._4paradigm.openmldb.proto.Common;
import com._4paradigm.openmldb.proto.Type;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class FlexibleRowBuilder implements RowBuilder {

    private CodecMetaData metaData;
    private int strFieldStartOffset;

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
    private boolean allocDirect = false;
    // Cache string values in case of out-of-order insertion
    private Map<Integer, String> stringValueCache;
    private int curStrIdx = 0;

    public FlexibleRowBuilder(List<Common.ColumnDesc> schema) throws Exception {
        this(new CodecMetaData(schema, 1, false));
    }

    public FlexibleRowBuilder(CodecMetaData metaData) {
        this(metaData, false);
    }

    public FlexibleRowBuilder(CodecMetaData metaData, boolean allocDirect) {
        this.metaData = metaData;
        nullBitmap = new ByteBitMap(metaData.getSchema().size());
        settedValue = new ByteBitMap(metaData.getSchema().size());
        strFieldStartOffset = metaData.getStrFieldStartOffset();
        baseFieldBuf = ByteBuffer.allocate(strFieldStartOffset - metaData.getBaseFieldStartOffset()).order(ByteOrder.LITTLE_ENDIAN);
        strAddrLen = strAddrSize * metaData.getStrFieldCnt();
        strAddrBuf = ByteBuffer.allocate(strAddrLen).order(ByteOrder.LITTLE_ENDIAN);
        this.allocDirect = allocDirect;
    }

    private boolean checkType(int pos, Type.DataType type) {
        return CodecUtil.checkType(metaData.getSchema(), pos, type);
    }

    private int getOffset(int idx) {
        return metaData.getOffsetList().get(idx);
    }

    private void setStrOffset(int strPos) {
        if (strPos >= metaData.getStrFieldCnt()) {
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
        int newStrAddBufLen = newStrAddrsize * metaData.getStrFieldCnt();
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
        strAddrLen = strAddrSize * metaData.getStrFieldCnt();
        result = null;
        curStrIdx = 0;
        if (stringValueCache != null) {
            stringValueCache.clear();
            stringValueCache = null;
        }
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
    public boolean appendTimestamp(Timestamp val) {
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
        if (idx >= metaData.getSchema().size()) {
            return false;
        }
        Type.DataType type = metaData.getSchema().get(idx).getDataType();
        if (type == Type.DataType.kVarchar || type == Type.DataType.kString) {
            if (idx != metaData.getStrIdxList().get(curStrIdx)) {
                if (stringValueCache == null) {
                    stringValueCache = new TreeMap<>();
                }
                stringValueCache.put(idx, null);
                return true;
            } else {
                int strPos = getOffset(idx);
                setStrOffset(strPos + 1);
                settedStrCnt++;
                curStrIdx++;
            }
        }
        nullBitmap.atPut(idx, true);
        settedValue.atPut(idx, true);
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
    public boolean setTimestamp(int idx, Timestamp val) {
        if (!checkType(idx, Type.DataType.kTimestamp)) {
            return false;
        }
        settedValue.atPut(idx, true);
        baseFieldBuf.putLong(getOffset(idx), val.getTime());
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
        if (idx != metaData.getStrIdxList().get(curStrIdx)) {
            if (stringValueCache == null) {
                stringValueCache = new TreeMap<>();
            }
            stringValueCache.put(idx, val);
        } else {
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
            curStrIdx++;
        }
        return true;
    }

    @Override
    public boolean build() {
        if (stringValueCache != null) {
            for (Map.Entry<Integer, String> kv : stringValueCache.entrySet()) {
                if (kv.getValue() == null) {
                    if (!setNULL(kv.getKey())) {
                        return false;
                    }
                } else {
                    if (!setString(kv.getKey(), kv.getValue())) {
                        return false;
                    }
                }
            }
        }
        if (!settedValue.allSetted()) {
            return false;
        }
        int totalSize = strFieldStartOffset + strAddrLen + strTotalLen;
        if (allocDirect) {
            result = ByteBuffer.allocateDirect(totalSize).order(ByteOrder.LITTLE_ENDIAN);
        } else {
            result = ByteBuffer.allocate(totalSize).order(ByteOrder.LITTLE_ENDIAN);
        }
        result.put((byte)1);                            // FVersion
        result.put((byte)metaData.getSchemaVersion());  // SVersion
        result.putInt(totalSize);                       // Size
        result.put(nullBitmap.getBuffer());             // BitMap
        result.put(baseFieldBuf.array());               // Base data type
        result.put(strAddrBuf.array());                 // String addr
        result.put(stringWriter.toByteArray());         // String value
        return true;
    }

    @Override
    public ByteBuffer getValue() {
        return result;
    }
}
