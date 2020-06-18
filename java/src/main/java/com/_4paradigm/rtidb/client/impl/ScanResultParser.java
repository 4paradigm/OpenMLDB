package com._4paradigm.rtidb.client.impl;

import com.google.protobuf.ByteString;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class ScanResultParser {
    private ByteBuffer bb;
    private int offset;
    private long ts;
    private ByteBuffer slice;
    private int totalSize;

    public ScanResultParser(ByteString bs) {
        this.bb = bs.asReadOnlyByteBuffer();
        this.totalSize = bs.size();
        this.offset = 0;
        this.ts = 0;
        parseData();
    }

    public long GetTs() {
        return ts;
    }

    public boolean valid() {
        return offset <= totalSize;
    }

    public ByteBuffer fetchData() {
        ByteBuffer cur_slice = slice;
        parseData();
        return cur_slice;
    }

    private void parseData() {
        if (offset + 4 > totalSize) {
            offset += 4;
            return;
        }
        slice = this.bb.slice().asReadOnlyBuffer().order(ByteOrder.LITTLE_ENDIAN);
        slice.position(offset);
        int size = slice.getInt();
        if (size < 8) {
            throw new RuntimeException("bad frame data");
        }
        ts = slice.getLong();
        offset += (4 + size);
        slice.limit(offset);
    }

}
