package com._4paradigm.rtidb.client.impl;

import com._4paradigm.rtidb.client.KvIterator;
import com._4paradigm.rtidb.client.TabletException;
import com._4paradigm.rtidb.client.schema.ColumnDesc;
import com._4paradigm.rtidb.client.schema.RowCodec;
import com._4paradigm.rtidb.client.schema.RowView;
import com._4paradigm.rtidb.ns.NS;
import com.google.protobuf.ByteString;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

public class RowKvIterator implements KvIterator {

    private ByteString bs;
    private int offset;
    private ByteBuffer bb;
    // no copy
    private ByteBuffer slice;
    private int length;
    private int totalSize;
    private List<ColumnDesc> schema;
    private int count;
    private long key;
    private NS.CompressType compressType = NS.CompressType.kNoCompress;
    private RowView rv;
    public RowKvIterator(ByteString bs, List<ColumnDesc> schema, int count) {
        this.bs = bs;
        this.bb = this.bs.asReadOnlyByteBuffer();
        this.offset = 0;
        this.totalSize = this.bs.size();
        this.count = count;
        next();
        this.schema = schema;
        rv = new RowView(schema);
    }

    public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

    public NS.CompressType getCompressType() {
        return compressType;
    }

    public void setCompressType(NS.CompressType compressType) {
        this.compressType = compressType;
    }

    public List<ColumnDesc> getSchema() {
        return schema;
	}

	public boolean valid() {
        if (offset <= totalSize) {
            return true;
        }
        return false;
    }

    public long getKey() {
        return key;
    }

    @Override
    public String getPK() {
        return null;
    }
    // no copy
    public ByteBuffer getValue() {
        return slice;
    }

    public Object[] getDecodedValue() throws TabletException {
        if (schema == null) {
            throw new TabletException("get decoded value is not supported");
        }
        Object[] row = new Object[schema.size()];
        getDecodedValue(row, 0, row.length);
        return row;
    }

    public void next() {
        if (offset + 4 > totalSize) {
            offset += 4;
            return;
        }
        slice = this.bb.slice().asReadOnlyBuffer().order(ByteOrder.LITTLE_ENDIAN);
        slice.position(offset);
        int size = slice.getInt();
        key = slice.getLong();
        length = size - 8;
        if (length < 0) {
            throw new RuntimeException("bad frame data");
        }
        offset += (4 + size);
        slice.limit(offset);
    }

    @Override
    public void getDecodedValue(Object[] row, int start, int length) throws TabletException {
        if (schema == null) {
            throw new TabletException("get decoded value is not supported");
        }
        rv.reset(slice, length);
        int index = start;
        for (int i = 0; i < schema.size() && i < length; i ++) {
            row[index] = rv.getValue(slice, i);
        }
    }
}
