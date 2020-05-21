package com._4paradigm.rtidb.client.impl;

import com._4paradigm.rtidb.client.KvIterator;
import com._4paradigm.rtidb.client.TabletException;
import com._4paradigm.rtidb.client.ha.RTIDBClientConfig;
import com._4paradigm.rtidb.client.ha.TableHandler;
import com._4paradigm.rtidb.client.schema.ColumnDesc;
import com._4paradigm.rtidb.client.schema.RowCodec;
import com._4paradigm.rtidb.ns.NS;
import com._4paradigm.rtidb.utils.Compress;
import com.google.protobuf.ByteString;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

public class DefaultKvIterator implements KvIterator {

    public class QueryResultData {
        private ByteBuffer bb;
        private int offset;
        private long ts;
        private ByteBuffer slice;
        private int totalSize;

        public QueryResultData(ByteString bs) {
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

    private List<QueryResultData> dataList = new ArrayList<>();
    // no copy
    private ByteBuffer slice = null;
    private long time;
    private List<ColumnDesc> schema;
    private Long network = 0l;
    private int count;
    private RTIDBClientConfig config = null;
    private NS.CompressType compressType = NS.CompressType.kNoCompress;
    private TableHandler th = null;
    private List<Integer> projection;
    private boolean hasProjection = false;
    private BitSet bitSet;
    private int maxIndex;
    public DefaultKvIterator(ByteString bs) {
        this.dataList.add(new QueryResultData(bs));
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

    public NS.CompressType getCompressType() {
        return compressType;
    }

    public void setCompressType(NS.CompressType compressType) {
        this.compressType = compressType;
    }

	public DefaultKvIterator(ByteString bs, List<ColumnDesc> schema) {
        this(bs);
        this.schema = schema;
    }
    public DefaultKvIterator(ByteString bs, List<ColumnDesc> schema, BitSet bitSet, List<Integer> projection, int maxIndex) {
        this(bs);
        this.schema = schema;
        this.projection = projection;
        if (projection != null && projection.size() > 0) {
            hasProjection = true;
            this.bitSet = bitSet;
            this.maxIndex = maxIndex;
        }
    }
	public DefaultKvIterator(ByteString bs, List<ColumnDesc> schema, RTIDBClientConfig config) {
        this(bs, schema);
        this.config = config;
    }
    
    public DefaultKvIterator(ByteString bs, Long network) {
        this(bs);
        this.network = network;
    }

    public DefaultKvIterator(ByteString bs, TableHandler th) {
        this(bs);
        this.schema = th.getSchema();
        this.th = th;
    }

    public DefaultKvIterator(List<ByteString> bsList, TableHandler th) {
        for (ByteString bs : bsList) {
            this.dataList.add(new QueryResultData(bs));
        }
        next();
        this.schema = th.getSchema();
        this.th = th;
    }

    public DefaultKvIterator(ByteString bs, List<ColumnDesc> schema, Long network) {
        this(bs);
        this.schema = schema;
        if (network != null) {
            this.network = network;
        }
    }

    public List<ColumnDesc> getSchema() {
        if (th != null && th.getSchemaMap().size() > 0) {
            return th.getSchemaMap().get(th.getSchema().size() + th.getSchemaMap().size());
        }
        return schema;
	}

	public boolean valid() {
        return slice != null;
    }

    public long getKey() {
        return time;
    }

    @Override
    public String getPK() {
        return null;
    }

    // no copy
    public ByteBuffer getValue() {
        if (compressType == NS.CompressType.kSnappy) {
            byte[] data = new byte[slice.remaining()];
            slice.get(data);
            byte[] uncompressed = Compress.snappyUnCompress(data);
            return ByteBuffer.wrap(uncompressed);
        } else {
            return slice;
        }
    }
    
    public Object[] getDecodedValue() throws TabletException {
        if (schema == null) {
            throw new TabletException("get decoded value is not supported");
        }
        Object[] row;
        if (hasProjection) {
            row = new Object[projection.size()];
            getDecodedValue(row, 0, row.length);
            return row;
        }
        if (th != null) {
            row = new Object[schema.size() + th.getSchemaMap().size()];
        } else {
            row = new Object[schema.size()];
        }
        getDecodedValue(row, 0, row.length);
        return row;
    }

    public void next() {
        int maxTsIndex = -1;
        long ts = -1;
        for (int i = 0; i < dataList.size(); i++) {
            QueryResultData queryResult = dataList.get(i);
            if (queryResult.valid() && queryResult.GetTs() > ts) {
                maxTsIndex = i;
                ts = queryResult.GetTs();
            }
        }
        if (maxTsIndex >= 0) {
            QueryResultData queryResult = dataList.get(maxTsIndex);
            time = queryResult.GetTs();
            slice = queryResult.fetchData();
        } else {
            slice = null;
        }
    }

    @Override
    public void getDecodedValue(Object[] row, int start, int length) throws TabletException {
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
            if (hasProjection) {
                RowCodec.decode(ByteBuffer.wrap(uncompressed), schema, bitSet, projection, maxIndex, row, 0, length);
            }else {
                RowCodec.decode(ByteBuffer.wrap(uncompressed), schema, row, 0, length);
            }
        } else {
            if (hasProjection) {
                RowCodec.decode(slice, schema, bitSet, projection,maxIndex, row, start, length);
            }else {
                RowCodec.decode(slice, schema, row, start, length);
            }
        }
    }
}
