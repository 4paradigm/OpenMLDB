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
package com._4paradigm.openmldb.synctool;

import com._4paradigm.openmldb.proto.Common.ColumnDesc;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

import com._4paradigm.openmldb.common.codec.RowView;

import io.netty.buffer.ByteBuf;
import org.apache.commons.lang3.StringEscapeUtils;
import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DataParser {
    private final ByteBuf data;
    private final long count;
    private final RowView rowView;

    public DataParser(ByteBuf data, final long count, List<ColumnDesc> schema) throws Exception {
        this.data = data;
        this.count = count;
        this.rowView = new RowView(schema);
    }

    // spark-readable format, escape string, null is null
    public static String rowToString(RowView rowView) throws Exception {
        StringBuilder sb = new StringBuilder();
        List<ColumnDesc> schema = rowView.getSchema();
        for (int i = 0; i < schema.size(); i++) {
            if (i != 0) {
                sb.append(",");
            }
            ColumnDesc col = schema.get(i);
            Object value = rowView.getValue(i, col.getDataType());
            String appendStr = value==null? "null":value.toString();
            if (value instanceof String) {
                // escape for string
                appendStr = StringEscapeUtils.escapeJson((String) value);
            }
            // TODO(hw): escape for date?
            sb.append(appendStr);
            // case kDate:
            // rowView.getDate(i);
            // case kTimestamp:
            // value = rowView.getTimestamp(i);
        }
        return sb.toString();
    }

    public void writeAll(BufferedWriter bufferedWriter) {
        try {
            long readCount = 0;
            while (readCount < count) {
                int start = data.readerIndex();
                data.readerIndex(start + 2); // skip 2 bytes
                int rowSize = data.readIntLE();
                log.debug("read row size: {}", rowSize);
                // netty is big endian, but we use little endian
                ByteBuffer row = data.nioBuffer(start, rowSize).order(ByteOrder.LITTLE_ENDIAN);
                Preconditions.checkState(rowView.reset(row, rowSize), "reset row view failed");
                data.readerIndex(start + rowSize);
                bufferedWriter.write(rowToString(rowView));
                bufferedWriter.write("\n");
                readCount++;
            }
            Preconditions.checkState(readCount == count,
                    String.format("read count not match, read: %d, expect: %d", readCount, count));
        } catch (IOException e) {
            throw new RuntimeException("data write failed", e);
        } catch (Exception e) {
            throw new RuntimeException("data parse failed", e);
        }
    }
}
