package com._4paradigm.rtidb.client;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

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

    public KvIterator(ByteString bs) {
        this.bs = bs;
        this.bb = this.bs.asReadOnlyByteBuffer();
        this.offset = 0;
        this.totalSize = this.bs.size();
    }

    public boolean valid() {
        if (offset < totalSize) {
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

    public void next() {
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
