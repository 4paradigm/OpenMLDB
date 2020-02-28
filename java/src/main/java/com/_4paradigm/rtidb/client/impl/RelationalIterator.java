package com._4paradigm.rtidb.client.impl;

import com._4paradigm.rtidb.client.TabletException;
import com._4paradigm.rtidb.client.ha.TableHandler;
import com._4paradigm.rtidb.client.schema.ColumnDesc;
import com._4paradigm.rtidb.client.schema.RowCodec;
import com._4paradigm.rtidb.ns.NS;
import com._4paradigm.rtidb.utils.Compress;
import com.google.protobuf.ByteString;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    public RelationalIterator() {
    }

    public RelationalIterator(ByteString bs, TableHandler th) {
        this.bs = bs;
        this.bb = this.bs.asReadOnlyByteBuffer();
        this.offset = 0;
        this.totalSize = this.bs.size();
        next();
        this.schema = th.getSchema();
        this.th = th;
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

    // no copy
    private ByteBuffer getValue() {
        if (compressType == NS.CompressType.kSnappy) {
            byte[] data = new byte[slice.remaining()];
            slice.get(data);
            byte[] uncompressed = Compress.snappyUnCompress(data);
            return ByteBuffer.wrap(uncompressed);
        } else {
            return slice;
        }
    }

    public Map<String, Object> getDecodedValue() throws TabletException {
        if (schema == null) {
            throw new TabletException("get decoded value is not supported");
        }
        Object[] row;
        if (th != null) {
            row = new Object[schema.size() + th.getSchemaMap().size()];
        } else {
            row = new Object[schema.size()];
        }
        return getDecodedValue(row, 0, row.length);
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

    private Map<String, Object> getDecodedValue(Object[] row, int start, int length) throws TabletException {
        Map<String, Object> map = new HashMap<>();
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
            RowCodec.decode(ByteBuffer.wrap(uncompressed), schema, row, 0, length);
        } else {
            RowCodec.decode(slice, schema, row, start, length);
        }
        for (int i = 0; i < schema.size(); i++) {
            map.put(schema.get(i).getName(), row[i]);
        }
        return map;
    }

}
