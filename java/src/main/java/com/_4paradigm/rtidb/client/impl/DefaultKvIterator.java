package com._4paradigm.rtidb.client.impl;

import com._4paradigm.rtidb.client.KvIterator;
import com._4paradigm.rtidb.client.TabletException;
import com._4paradigm.rtidb.client.ha.RTIDBClientConfig;
import com._4paradigm.rtidb.client.ha.TableHandler;
import com._4paradigm.rtidb.client.schema.ColumnDesc;
import com._4paradigm.rtidb.client.schema.ProjectionInfo;
import com._4paradigm.rtidb.client.schema.RowCodec;
import com._4paradigm.rtidb.ns.NS;
import com._4paradigm.rtidb.utils.Compress;
import com.google.protobuf.ByteString;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class DefaultKvIterator implements KvIterator {

    private List<ScanResultData> dataList = new ArrayList<>();
    // no copy
    private ByteBuffer slice = null;
    private long time;
    private List<ColumnDesc> schema;
    private Long network = 0l;
    private int count;
    private int iter_count = 0;
    private RTIDBClientConfig config = null;
    private NS.CompressType compressType = NS.CompressType.kNoCompress;
    private TableHandler th = null;
    private ProjectionInfo projectionInfo = null;

    public DefaultKvIterator(ByteString bs) {
        this.dataList.add(new ScanResultData(bs));
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
    public DefaultKvIterator(ByteString bs, List<ColumnDesc> schema, ProjectionInfo projectionInfo) {
        this(bs);
        this.schema = schema;
        this.projectionInfo = projectionInfo;
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

    public DefaultKvIterator(List<ByteString> bsList, TableHandler th, ProjectionInfo projectionInfo) {
        for (ByteString bs : bsList) {
            if (!bs.isEmpty()) {
                this.dataList.add(new ScanResultData(bs));
            }
        }
        this.projectionInfo = projectionInfo;
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
        return slice != null && iter_count <= count;
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
        if (projectionInfo != null) {
            row = new Object[projectionInfo.getProjectionCol().size()];
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
            ScanResultData queryResult = dataList.get(i);
            if (queryResult.valid() && queryResult.GetTs() > ts) {
                maxTsIndex = i;
                ts = queryResult.GetTs();
            }
        }
        if (maxTsIndex >= 0) {
            ScanResultData queryResult = dataList.get(maxTsIndex);
            time = queryResult.GetTs();
            slice = queryResult.fetchData();
        } else {
            slice = null;
        }
        iter_count++;
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
            if (projectionInfo != null) {
                RowCodec.decode(ByteBuffer.wrap(uncompressed), schema, projectionInfo, row, 0, length);
            }else {
                RowCodec.decode(ByteBuffer.wrap(uncompressed), schema, row, 0, length);
            }
        } else {
            if (projectionInfo != null) {
                RowCodec.decode(slice, schema, projectionInfo, row, start, length);
            }else {
                RowCodec.decode(slice, schema, row, start, length);
            }
        }
    }
}
