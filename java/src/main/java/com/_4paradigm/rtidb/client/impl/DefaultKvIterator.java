package com._4paradigm.rtidb.client.impl;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

import com._4paradigm.rtidb.client.KvIterator;
import com._4paradigm.rtidb.client.TabletException;
import com._4paradigm.rtidb.client.ha.RTIDBClientConfig;
import com._4paradigm.rtidb.client.metrics.TabletMetrics;
import com._4paradigm.rtidb.client.schema.ColumnDesc;
import com._4paradigm.rtidb.client.schema.RowCodec;
import com._4paradigm.rtidb.utils.Compress;
import com.google.protobuf.ByteString;

public class DefaultKvIterator implements KvIterator {

    private ByteString bs;
    private int offset;
    private ByteBuffer bb;
    // no copy
    private ByteBuffer slice;
    private int length;
    private long time;
    private int totalSize;
    private List<ColumnDesc> schema;
    private Long network = 0l;
    private Long decode = 0l;
    private int count;
    private RTIDBClientConfig config = null;
    private String compressType = "kNoCompress";
    public DefaultKvIterator(ByteString bs) {
        this.bs = bs;
        this.bb = this.bs.asReadOnlyByteBuffer();
        this.offset = 0;
        this.totalSize = this.bs.size();
        next();
    }
    
    public DefaultKvIterator(ByteString bs, RTIDBClientConfig config) {
        this(bs);
        this.config = config;
    }
    
    public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

    public String getCompressType() {
        return compressType;
    }

    public void setCompressType(String compressType) {
        this.compressType = compressType;
    }


	public DefaultKvIterator(ByteString bs, List<ColumnDesc> schema) {
        this(bs);
        this.schema = schema;
    }
	
	public DefaultKvIterator(ByteString bs, List<ColumnDesc> schema, RTIDBClientConfig config) {
        this(bs, schema);
        this.config = config;
    }
    
    public DefaultKvIterator(ByteString bs, Long network) {
        this.bs = bs;
        this.bb = this.bs.asReadOnlyByteBuffer();
        this.offset = 0;
        this.totalSize = this.bs.size();
        next();
        this.network = network;
    }
    
    public DefaultKvIterator(ByteString bs, List<ColumnDesc> schema, Long network) {
        this.bs = bs;
        this.bb = this.bs.asReadOnlyByteBuffer();
        this.offset = 0;
        this.totalSize = this.bs.size();
        next();
        this.schema = schema;
        if (network != null) {
            this.network = network;
        }
    }

    public List<ColumnDesc> getSchema() {
		return schema;
	}

	public boolean valid() {
        if (offset <= totalSize) {
            return true;
        }
        if (config != null && config.isMetricsEnabled()) {
            TabletMetrics.getInstance().addScan(decode, network);
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
        Object[] row = new Object[schema.size()];
        getDecodedValue(row, 0, row.length);
        return row;
    }

    public void next() {
        long delta = 0l;
        if (config != null && config.isMetricsEnabled()) {
    	    delta = System.nanoTime();
        }
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
        if (config != null && config.isMetricsEnabled()) {
            decode += System.nanoTime() - delta;
        }
    }

    @Override
    public void getDecodedValue(Object[] row, int start, int length) throws TabletException {
        long delta = 0l;
        if (config != null && config.isMetricsEnabled()) {
            delta = System.nanoTime();
        }
        if (schema == null) {
            throw new TabletException("get decoded value is not supported");
        }
        if (compressType.equals("kSnappy")) {
            byte[] data = new byte[slice.remaining()];
            slice.get(data);
            byte[] uncompressed = Compress.snappyUnCompress(data);
            RowCodec.decode(ByteBuffer.wrap(uncompressed), schema, row, 0, length);
        } else {
            RowCodec.decode(slice, schema, row, start, length);
        }
        if (config != null && config.isMetricsEnabled()) {
            decode += System.nanoTime() - delta;
        }
    }
}
