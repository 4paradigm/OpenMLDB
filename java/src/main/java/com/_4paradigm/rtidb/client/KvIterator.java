package com._4paradigm.rtidb.client;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

import com._4paradigm.rtidb.client.schema.ColumnDesc;
import com._4paradigm.rtidb.client.schema.RowCodec;
import com.google.protobuf.ByteString;

public class KvIterator {

    private ByteString bs;
    private int offset;
    private ByteBuffer bb;
    // no copy
    private ByteBuffer slice;
    private int length;
    private long time;
    private int totalSize;
    private List<ColumnDesc> schema;
    public KvIterator(ByteString bs) {
        this.bs = bs;
        this.bb = this.bs.asReadOnlyByteBuffer();
        this.offset = 0;
        this.totalSize = this.bs.size();
        next();
    }
    
    public KvIterator(ByteString bs, List<ColumnDesc> schema) {
        this.bs = bs;
        this.bb = this.bs.asReadOnlyByteBuffer();
        this.offset = 0;
        this.totalSize = this.bs.size();
        next();
        this.schema = schema;
    }

    public boolean valid() {
        if (offset <= totalSize) {
            return true;
        }
        return false;
    }

    public long getKey() {
        return time;
    }

    // no copy
    public ByteBuffer getValue() {
        return slice;
    }
    
    public Object[] getDecodedValue() throws TabletException {
    	if (schema == null) {
    		throw new TabletException("get decoded value is not supported");
    	}
    	return RowCodec.decode(slice, schema);
    }

    public void next() {
        if (offset + 4 > totalSize) {
            offset += 4;
            return;
        }
        slice = this.bb.slice().asReadOnlyBuffer().order(ByteOrder.LITTLE_ENDIAN);
        slice.position(offset);
        int size = slice.getInt();
        time = slice.getLong();
        // calc the data size
        length = size - 8;
        if (length < 0) {
            throw new RuntimeException("bad frame data");
        }
        offset += (4 + size);
        slice.limit(offset);
    }
}
