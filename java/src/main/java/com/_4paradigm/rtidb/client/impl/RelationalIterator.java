package com._4paradigm.rtidb.client.impl;

import com._4paradigm.rtidb.client.TabletException;
import com._4paradigm.rtidb.client.ha.PartitionHandler;
import com._4paradigm.rtidb.client.ha.RTIDBClient;
import com._4paradigm.rtidb.client.ha.TableHandler;
import com._4paradigm.rtidb.client.schema.ColumnDesc;
import com._4paradigm.rtidb.client.schema.RowView;
import com._4paradigm.rtidb.client.type.DataType;
import com._4paradigm.rtidb.client.type.IndexType;
import com._4paradigm.rtidb.common.Common;
import com._4paradigm.rtidb.ns.NS;
import com._4paradigm.rtidb.tablet.Tablet;
import com._4paradigm.rtidb.utils.Compress;
import com.google.protobuf.ByteString;
import rtidb.api.TabletServer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;
import java.util.concurrent.TimeoutException;

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
    private boolean isFinished = false;
    private int pid = 0;
    private String last_pk = null;
    private int key_idx = 0;
    private DataType key_type;
    private RTIDBClient client = null;
    private boolean continue_update = false;
    private boolean batch_query = false;
    private List<String> keys = null;

    public RelationalIterator() {
    }

    public RelationalIterator(RTIDBClient client, TableHandler th, Set<String> colSet) {
        this.offset = 0;
        this.totalSize = 0;
        this.schema = th.getSchema();
        this.th = th;
        this.client = client;
        this.compressType = th.getTableInfo().getCompressType();

        if (colSet != null && !colSet.isEmpty()) {
            for (int i = 0; i < this.getSchema().size(); i++) {
                ColumnDesc columnDesc = this.getSchema().get(i);
                if (colSet.contains(columnDesc.getName())) {
                    this.idxDescMap.put(i, columnDesc);
                }
            }
        }
        this.rowView = new RowView(th.getSchema());
        String idxName = "";
        for (int i = 0; i < th.getTableInfo().getColumnKeyCount(); i++) {
            Common.ColumnKey key = th.getTableInfo().getColumnKey(i);
            if (key.hasIndexType() && key.getIndexType() == IndexType.valueFrom(IndexType.PrimaryKey)) {
                idxName = key.getIndexName();
            }
        }
        for (int i = 0; i < schema.size(); i++) {
            if (schema.get(i).getName().equals(idxName)) {
                key_type = schema.get(i).getDataType();
                key_idx = i;
            }
        }
        continue_update = true;
    }
    public RelationalIterator(ByteString bs, TableHandler th, Set<String> colSet) {
        this.bs = bs;
        this.bb = this.bs.asReadOnlyByteBuffer();
        this.offset = 0;
        this.totalSize = this.bs.size();
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
        next();
    }

    public RelationalIterator(RTIDBClient client, TableHandler th, List<String> queryKeys, Set<String> colSet) {
        this.offset = 0;
        this.totalSize = 0;
        this.schema = th.getSchema();
        this.th = th;
        this.client = client;
        this.compressType = th.getTableInfo().getCompressType();
        this.keys = queryKeys;

        if (colSet != null && !colSet.isEmpty()) {
            for (int i = 0; i < this.getSchema().size(); i++) {
                ColumnDesc columnDesc = this.getSchema().get(i);
                if (colSet.contains(columnDesc.getName())) {
                    this.idxDescMap.put(i, columnDesc);
                }
            }
        }
        rowView = new RowView(th.getSchema());
        String idxName = "";
        for (int i = 0; i < th.getTableInfo().getColumnKeyCount(); i++) {
            Common.ColumnKey key = th.getTableInfo().getColumnKey(i);
            if (key.hasIndexType() && key.getIndexType() == IndexType.valueFrom(IndexType.PrimaryKey)) {
                idxName = key.getIndexName();
            }
        }
        for (int i = 0; i < schema.size(); i++) {
            if (schema.get(i).getName().equals(idxName)) {
                key_type = schema.get(i).getDataType();
                key_idx = i;
            }
        }
        batch_query = true;

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
            return getInternel();
        } else {
            return getInternel();
        }
    }

    public void next() {
        if (continue_update || batch_query) {
            if (offset >= totalSize && !isFinished) {
                try {
                    getData();
                } catch (Exception e) {
                    throw new RuntimeException(e.getMessage());
                }
            }
            if (offset + 4 > totalSize) {
                offset += 4;
                return;
            }
            slice = this.bb.slice().asReadOnlyBuffer().order(ByteOrder.LITTLE_ENDIAN);
            slice.position(offset);
            int length = slice.getInt();
            if (length <= 0) {
                throw new RuntimeException("bad frame data");
            }
            offset += (4 + length);
            slice.limit(offset);
            ByteBuffer value = slice.slice().asReadOnlyBuffer().order(ByteOrder.LITTLE_ENDIAN);
            boolean ok = rowView.reset(value, length);
            if (!ok) {
                throw new RuntimeException("row view reset failed");
            }
        } else {
            slice = this.bb.slice().asReadOnlyBuffer().order(ByteOrder.LITTLE_ENDIAN);
            slice.position(0);
            slice.limit(bs.size());
            rowView.reset(slice, bs.size());
        }
    }

    private Map<String, Object> getInternel() throws TabletException {
        Map<String, Object> map = new HashMap<>();
        if (idxDescMap.isEmpty()) {
            for (int i = 0; i < this.getSchema().size(); i++) {
                ColumnDesc columnDesc = this.getSchema().get(i);
                Object value = rowView.getValue(i, columnDesc.getDataType());
                map.put(columnDesc.getName(), value);
            }
        } else {
            Iterator<Map.Entry<Integer, ColumnDesc>> iter = this.idxDescMap.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry<Integer, ColumnDesc> next = iter.next();
                int index = next.getKey();
                ColumnDesc columnDesc = next.getValue();
                Object value = rowView.getValue(index, columnDesc.getDataType());
                map.put(columnDesc.getName(), value);
            }
        }
        if (batch_query) {
            Object val = rowView.getValue(key_idx, key_type);
            if (keys.contains(val)) {
                keys.remove(val);
            }
        }
        return map;
    }

    private void getData() throws TimeoutException, TabletException {
        if (batch_query) {
            PartitionHandler ph = th.getHandler(0);
            TabletServer ts = ph.getReadHandler(th.getReadStrategy());
            Tablet.BatchQueryRequest.Builder builder = Tablet.BatchQueryRequest.newBuilder();
            builder.setTid(th.getTableInfo().getTid());
            if (keys == null || keys.isEmpty()) {
                throw new TabletException("batch query keys is not provides");
            }
            for (int i = 0; i < keys.size(); i++) {
                String key = keys.get(i);
                builder.addQueryKey(key);
            }
            Tablet.BatchQueryRequest request = builder.build();
            Tablet.BatchQueryResponse response = ts.batchQuery(request);
            if (response != null && response.getCode() == 0) {
                bs = response.getPairs();
                bb = bs.asReadOnlyByteBuffer();
                totalSize = this.bs.size();
                offset = 0;
                if (response.hasIsFinish() && response.getIsFinish()) {
                    isFinished = true;
                }
                return;
            }
            if (response != null) {
                throw new TabletException(response.getCode(), response.getMsg());
            }
            throw new TabletException("rtidb internal server error");
        }
        do {
            if (pid >= th.getPartitions().length) {
                isFinished = true;
                return;
            }
            PartitionHandler ph = th.getHandler(pid);
            TabletServer ts = ph.getReadHandler(th.getReadStrategy());
            Tablet.TraverseRequest.Builder builder = Tablet.TraverseRequest.newBuilder();
            builder.setTid(th.getTableInfo().getTid());
            if (offset != 0) {
                Object val = rowView.getValue(key_idx, key_type);
                last_pk = val.toString();
                builder.setPk(last_pk);
            }
            builder.setLimit(client.getConfig().getTraverseLimit());
            Tablet.TraverseRequest request = builder.build();
            Tablet.TraverseResponse response = ts.traverse(request);
            if (response != null && response.getCode() == 0) {
                bs = response.getPairs();
                bb = bs.asReadOnlyByteBuffer();
                totalSize = this.bs.size();
                offset = 0;
                if (totalSize == 0) {
                    if (response.hasIsFinish() && response.getIsFinish()) {
                        pid++;
                    }
                    continue;
                }
                if (response.hasIsFinish() && response.getIsFinish() || (!response.hasIsFinish() && response.getCount() < client.getConfig().getTraverseLimit())) {
                    pid++;
                    if (pid >= th.getPartitions().length) {
                        isFinished = true;
                    }
                }
                return;
            }
            if (response != null) {
                throw new TabletException(response.getCode(), response.getMsg());
            }
            throw new TabletException("rtidb internal server error");
        } while (true);
    }

}
