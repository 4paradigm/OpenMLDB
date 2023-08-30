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

import java.util.ArrayList;
import java.util.List;

public class CodecMetaData {
    private List<Common.ColumnDesc> schema;
    private int schemaVersion = 1;
    List<Integer> offsetVec = new ArrayList<>();
    private int strFieldCnt = 0;
    private int strFieldStartOffset = 0;
    private int baseFieldStartOffset = 0;

    public CodecMetaData(List<Common.ColumnDesc> schema) throws Exception  {
        this(schema, 1, true);
    }

    public CodecMetaData(List<Common.ColumnDesc> schema, boolean addOffsetHeader) throws Exception  {
        this.schema = schema;
        calcSchemaOffset(addOffsetHeader);
    }

    public CodecMetaData(List<Common.ColumnDesc> schema, int schemaVersion, boolean addOffsetHeader) throws Exception {
        this.schema = schema;
        this.schemaVersion = schemaVersion;
        calcSchemaOffset(addOffsetHeader);
    }

    private void calcSchemaOffset(boolean addOffsetHeader) throws Exception {
        if (schema.size() == 0) {
            throw new Exception("schema size is zero");
        }
        baseFieldStartOffset = CodecUtil.HEADER_LENGTH + CodecUtil.getBitMapSize(schema.size());
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
                    if (addOffsetHeader) {
                        offsetVec.add(baseOffset + baseFieldStartOffset);
                    } else {
                        offsetVec.add(baseOffset);
                    }
                    baseOffset += CodecUtil.TYPE_SIZE_MAP.get(column.getDataType());
                }
            }
        }
        strFieldStartOffset = baseFieldStartOffset + baseOffset;
    }

    public List<Common.ColumnDesc> getSchema() {
        return schema;
    }

    public int getSchemaVersion() {
        return schemaVersion;
    }

    public List<Integer> getOffsetVec() {
        return offsetVec;
    }

    public int getStrFieldCnt() {
        return strFieldCnt;
    }

    public int getStrFieldStartOffset() {
        return strFieldStartOffset;
    }

    public int getBaseFieldStartOffset() {
        return baseFieldStartOffset;
    }
}
