package com._4paradigm.rtidb.client.impl;

import com._4paradigm.rtidb.client.TabletException;
import com._4paradigm.rtidb.client.ha.TableHandler;
import com._4paradigm.rtidb.client.schema.ColumnDesc;
import com._4paradigm.rtidb.client.schema.RowView;
import com._4paradigm.rtidb.ns.NS;
import com._4paradigm.rtidb.utils.Compress;
import com.google.protobuf.ByteString;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;

public class RelationalIterator {

    private ByteString bs;
    private int offset;
    private ByteBuffer bb;
    // no copy
    private ByteBuffer slice;
    private int length;
    private int totalSize;
    private List<ColumnDesc> schema;
    private int count;
    private NS.CompressType compressType = NS.CompressType.kNoCompress;
    private TableHandler th;
    private Map<Integer, ColumnDesc> idxDescMap = new HashMap<>();
    private RowView rowView;

    public RelationalIterator() {
    }

    public RelationalIterator(ByteString bs, TableHandler th, Set<String> colSet) {
        this.bs = bs;
        this.bb = this.bs.asReadOnlyByteBuffer();
        this.offset = 0;
        this.totalSize = this.bs.size();
        next();
        this.schema = th.getSchema();
        this.th = th;

        if (colSet != null && !colSet.isEmpty()) {
            for (int i = 0; i < this.getSchema().size(); i++) {
                ColumnDesc columnDesc = this.getSchema().get(i);
                if (colSet.contains(columnDesc.getName())) {
                    this.idxDescMap.put(i, columnDesc);
                }
            }
        }
        rowView = new RowView(th.getSchema());
    }


    public List<ColumnDesc> getSchema() {
        if (th != null && th.getSchemaMap().size() > 0) {
            return th.getSchemaMap().get(th.getSchema().size() + th.getSchemaMap().size());
        }
        return schema;
    }

    public boolean valid() {
        if (offset <= totalSize && totalSize != 0) {
            return true;
        }
        return false;
    }

    public Map<String, Object> getDecodedValue() throws TabletException {
        if (schema == null) {
            throw new TabletException("get decoded value is not supported");
        }
        if (compressType == NS.CompressType.kSnappy) {
            byte[] data = new byte[slice.remaining()];
            slice.get(data);
            byte[] uncompressed = Compress.snappyUnCompress(data);
            if (uncompressed == null) {
                throw new TabletException("snappy uncompress error");
            }
            rowView.reset(ByteBuffer.wrap(uncompressed),
                    ByteBuffer.wrap(uncompressed).array().length);
            return getInternel(ByteBuffer.wrap(uncompressed));
        } else {
            rowView.reset(slice, bs.size());
            return getInternel(slice);
        }
    }

    public void next() {
//        if (offset + 4 > totalSize) {
//            offset += 4;
//            return;
//        }
//        slice = this.bb.slice().asReadOnlyBuffer().order(ByteOrder.LITTLE_ENDIAN);
//        slice.position(offset);
//        length = slice.getInt();
//        if (length <= 0) {
//            throw new RuntimeException("bad frame data");
//        }
//        offset += (4 + length);
//        slice.limit(offset);

        slice = this.bb.slice().asReadOnlyBuffer().order(ByteOrder.LITTLE_ENDIAN);
        slice.position(0);
        slice.limit(bs.size());
    }

    private Map<String, Object> getInternel(ByteBuffer row) throws TabletException {
        Map<String, Object> map = new HashMap<>();
        if (idxDescMap.isEmpty()) {
            for (int i = 0; i < this.getSchema().size(); i++) {
                ColumnDesc columnDesc = this.getSchema().get(i);
                Object value = rowView.getValue(row, i, columnDesc.getDataType());
                map.put(columnDesc.getName(), value);
            }
        } else {
            Iterator<Map.Entry<Integer, ColumnDesc>> iter = this.idxDescMap.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry<Integer, ColumnDesc> next = iter.next();
                int index = next.getKey();
                ColumnDesc columnDesc = next.getValue();
                Object value = rowView.getValue(row, index, columnDesc.getDataType());
                map.put(columnDesc.getName(), value);
            }
        }
        return map;
    }

}
