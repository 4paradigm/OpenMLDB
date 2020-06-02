package com._4paradigm.rtidb.client.impl;

import com._4paradigm.rtidb.blobserver.OSS;
import com._4paradigm.rtidb.client.ReadOption;
import com._4paradigm.rtidb.client.TabletException;
import com._4paradigm.rtidb.client.ha.PartitionHandler;
import com._4paradigm.rtidb.client.ha.RTIDBClient;
import com._4paradigm.rtidb.client.ha.TableHandler;
import com._4paradigm.rtidb.client.schema.ColumnDesc;
import com._4paradigm.rtidb.client.schema.FieldCodec;
import com._4paradigm.rtidb.client.schema.RowView;
import com._4paradigm.rtidb.client.type.DataType;
import com._4paradigm.rtidb.ns.NS;
import com._4paradigm.rtidb.tablet.Tablet;
import com._4paradigm.rtidb.utils.Compress;
import com.google.protobuf.ByteBufferNoCopy;
import com.google.protobuf.ByteString;
import rtidb.api.TabletServer;
import rtidb.blobserver.BlobServer;

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
    private List<Integer> idxList = new ArrayList<>();
    private RowView rowView;
    private boolean isFinished = false;
    private int pid = 0;
    private ByteString last_pk = null;
    private RTIDBClient client = null;
    private boolean continue_update = false;
    private boolean batch_query = false;
    private long snapshot_id = 0;
    private List<ReadOption> ros = new ArrayList<>();
    private List<Integer> blobIdxList = new ArrayList<>();

    public RelationalIterator(RTIDBClient client, TableHandler th, ReadOption ro) {
        this.offset = 0;
        this.totalSize = 0;
        this.schema = th.getSchema();
        this.th = th;
        this.client = client;
        this.compressType = th.getTableInfo().getCompressType();
        Set<String> colSet = null;
        if (ro != null) {
            ros.add(ro);
            colSet = ro.getColSet();
        }
        if (colSet != null && !colSet.isEmpty()) {
            for (int i = 0; i < this.getSchema().size(); i++) {
                ColumnDesc columnDesc = this.getSchema().get(i);
                if (colSet.contains(columnDesc.getName())) {
                    this.idxList.add(i);
                    if (th.getBlobIdxList().contains(i)) {
                        blobIdxList.add(i);
                    }
                }
            }
        } else {
            if (!th.getBlobIdxList().isEmpty()) {
                blobIdxList = th.getBlobIdxList();
            }
        }
        this.rowView = new RowView(th.getSchema());
        continue_update = true;
        next();
    }

    public RelationalIterator(RTIDBClient client, TableHandler th, List<ReadOption> ros) {
        this.offset = 0;
        this.totalSize = 0;
        this.schema = th.getSchema();
        this.th = th;
        this.client = client;
        this.compressType = th.getTableInfo().getCompressType();
        this.ros = ros;

        Set<String> colSet = ros.get(0).getColSet();
        if (colSet != null && !colSet.isEmpty()) {
            for (int i = 0; i < this.getSchema().size(); i++) {
                ColumnDesc columnDesc = this.getSchema().get(i);
                if (colSet.contains(columnDesc.getName())) {
                    this.idxList.add(i);
                    if (th.getBlobIdxList().contains(i)) {
                        blobIdxList.add(i);
                    }
                }
            }
        } else {
            if (!th.getBlobIdxList().isEmpty()) {
                blobIdxList = th.getBlobIdxList();
            }
        }
        rowView = new RowView(th.getSchema());
        batch_query = true;
        next();
    }


    public List<ColumnDesc> getSchema() {
        if (th != null && th.getSchemaMap().size() > 0) {
            return th.getSchemaMap().get(th.getSchema().size() + th.getSchemaMap().size());
        }
        return schema;
    }

    public int getCount() {
        return count;
    }

    public Map<String, String> getUrlMap() throws TabletException {
        // eg. "/v1/get/" + table_name + "/" + key
        if (blobIdxList.isEmpty()) {
            throw new TabletException("can't get url because no blob column!");
        }
        Map<String, String> map = new HashMap<>();
        StringBuilder sb = new StringBuilder("/v1/get/");
        sb.append(th.getTableInfo().getName());
        sb.append("/");
        String prefix = sb.toString();
        for (int idx : blobIdxList) {
            ColumnDesc columnDesc = th.getSchema().get(idx);
            Object value = rowView.getValue(idx, columnDesc.getDataType());
            map.put(columnDesc.getName(), prefix + ((Long) value).toString());
        }
        return map;
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
        }
    }

    private Map<String, Object> getInternel() throws TabletException {
        Map<String, Object> map = new HashMap<>();
        if (idxList.isEmpty()) {
            for (int i = 0; i < this.getSchema().size(); i++) {
                ColumnDesc columnDesc = this.getSchema().get(i);
                Object value = rowView.getValue(i, columnDesc.getDataType());
                map.put(columnDesc.getName(), value);
            }
        } else {
            for (int idx : idxList) {
                ColumnDesc columnDesc = this.getSchema().get(idx);
                Object value = rowView.getValue(idx, columnDesc.getDataType());
                map.put(columnDesc.getName(), value);
            }
        }
        for (Integer idx : blobIdxList) {
            ColumnDesc colDesc = schema.get(idx);
            Object col = map.get(colDesc.getName());
            if (col == null) {
                continue;
            }
            BlobData blobData = new BlobData(th, (long)col);
            map.put(colDesc.getName(), blobData);
        }
        return map;
    }

    private void getData() throws TimeoutException, TabletException {
        if (batch_query) {
            PartitionHandler ph = th.getHandler(0);
            TabletServer ts = ph.getReadHandler(th.getReadStrategy());
            Tablet.BatchQueryRequest.Builder builder = Tablet.BatchQueryRequest.newBuilder();
            int tid = th.getTableInfo().getTid();
            builder.setTid(tid);

            for (int i = 0; i < ros.size(); i++) {
                Iterator<Map.Entry<String, Object>> it = ros.get(i).getIndex().entrySet().iterator();
                Tablet.ReadOption.Builder roBuilder = Tablet.ReadOption.newBuilder();
                while (it.hasNext()) {
                    Map.Entry<String, Object> next = it.next();
                    String idxName = next.getKey();
                    Object idxValue = next.getValue();
                    {
                        Tablet.Columns.Builder indexBuilder = Tablet.Columns.newBuilder();
                        indexBuilder.addName(idxName);
                        Map<String, DataType> nameTypeMap = th.getNameTypeMap();
                        if (!nameTypeMap.containsKey(idxName)) {
                            throw new TabletException("index name not found with tid " + tid);
                        }
                        DataType dataType = nameTypeMap.get(idxName);
                        ByteBuffer buffer = FieldCodec.convert(dataType, idxValue);
                        if (buffer != null) {
                            indexBuilder.setValue(ByteBufferNoCopy.wrap(buffer));
                        }
                        roBuilder.addIndex(indexBuilder.build());
                    }
                }
                builder.addReadOption(roBuilder.build());
            }
            Tablet.BatchQueryRequest request = builder.build();
            Tablet.BatchQueryResponse response = ts.batchQuery(request);
            if (response != null && response.getCode() == 0) {
                bs = response.getPairs();
                bb = bs.asReadOnlyByteBuffer();
                totalSize = this.bs.size();
                offset = 0;
                count = response.getCount();
                if (response.hasIsFinish() && response.getIsFinish()) {
                    isFinished = true;
                }
                return;
            } else if (response.getCode() != 0) {
                offset = 0;
                totalSize = 0;
                return;
            }
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
                builder.setPkBytes(last_pk);
            }
            if (snapshot_id > 0) {
                builder.setSnapshotId(snapshot_id);
            }
            builder.setLimit(client.getConfig().getTraverseLimit());
            if (!ros.isEmpty()) {
                Map<String, Object> index = ros.get(0).getIndex();
                Tablet.ReadOption.Builder roBuilder = Tablet.ReadOption.newBuilder();
                if (index != null && !index.isEmpty()) {
                    Iterator<Map.Entry<String, Object>> it = index.entrySet().iterator();
                    while (it.hasNext()) {
                        Map.Entry<String, Object> next = it.next();
                        String idxName = next.getKey();
                        Object idxValue = next.getValue();
                        {
                            Tablet.Columns.Builder indexBuilder = Tablet.Columns.newBuilder();
                            indexBuilder.addName(idxName);
                            Map<String, DataType> nameTypeMap = th.getNameTypeMap();
                            if (!nameTypeMap.containsKey(idxName)) {
                                throw new TabletException("index name not found with tid " + th.getTableInfo().getTid());
                            }
                            DataType dataType = nameTypeMap.get(idxName);
                            ByteBuffer buffer = FieldCodec.convert(dataType, idxValue);
                            if (buffer != null) {
                                indexBuilder.setValue(ByteBufferNoCopy.wrap(buffer));
                            }
                            roBuilder.addIndex(indexBuilder.build());
                        }
                    }
                    index.clear();
                }
                builder.setReadOption(roBuilder.build());
            }
            Tablet.TraverseRequest request = builder.build();
            Tablet.TraverseResponse response = ts.traverse(request);
            if (response != null && response.getCode() == 0) {
                bs = response.getPairs();
                bb = bs.asReadOnlyByteBuffer();
                totalSize = this.bs.size();
                count += response.getCount();
                if (response.hasSnapshotId()) {
                    snapshot_id = response.getSnapshotId();
                }
                last_pk = response.getPkBytes();
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
