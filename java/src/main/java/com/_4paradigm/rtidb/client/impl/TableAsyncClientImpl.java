package com._4paradigm.rtidb.client.impl;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import com._4paradigm.rtidb.client.GetFuture;
import com._4paradigm.rtidb.client.PutFuture;
import com._4paradigm.rtidb.client.ScanFuture;
import com._4paradigm.rtidb.client.TableAsyncClient;
import com._4paradigm.rtidb.client.TabletException;
import com._4paradigm.rtidb.client.ha.PartitionHandler;
import com._4paradigm.rtidb.client.ha.RTIDBClient;
import com._4paradigm.rtidb.client.ha.RTIDBClientConfig;
import com._4paradigm.rtidb.client.ha.TableHandler;
import com._4paradigm.rtidb.client.schema.ColumnDesc;
import com._4paradigm.rtidb.client.schema.RowCodec;
import com._4paradigm.rtidb.ns.NS;
import com._4paradigm.rtidb.tablet.Tablet;
import com._4paradigm.rtidb.tablet.Tablet.GetResponse;
import com._4paradigm.rtidb.tablet.Tablet.PutResponse;
import com._4paradigm.rtidb.tablet.Tablet.ScanResponse;
import com._4paradigm.rtidb.utils.Compress;
import com.google.common.base.Charsets;
import com.google.protobuf.ByteBufferNoCopy;

import io.brpc.client.RpcCallback;
import rtidb.api.TabletServer;

public class TableAsyncClientImpl implements TableAsyncClient {

    private RTIDBClient client;


    public TableAsyncClientImpl(RTIDBClient client) {
        this.client = client;
    }

    @Override
    public PutFuture put(int tid, int pid, long time, Object[] row) throws TabletException {
        TableHandler tableHandler = client.getHandler(tid);
        if (tableHandler == null) {
            throw new TabletException("fail to find table with id " + tid);
        }
        if (tableHandler.getPartitions().length <= pid) {
            throw new TabletException("fail to find partition with pid "+ pid +" from table " +tableHandler.getTableInfo().getName());
        }
        List<Tablet.Dimension> dimList = TableClientCommon.fillTabletDimension(row, tableHandler, client.getConfig().isHandleNull());
        ByteBuffer buffer = RowCodec.encode(row, tableHandler.getSchema());
        return put(tid, pid, null, time, dimList, buffer, tableHandler);
    }

    @Override
    public PutFuture put(int tid, int pid, String key, long time, byte[] bytes) throws TabletException {
        TableHandler th = client.getHandler(tid);
        if (th == null) {
            throw new TabletException("fail to find table with id " + tid);
        }
        if (th.getPartitions().length <= pid) {
            throw new TabletException("fail to find partition with pid "+ pid +" from table " +th.getTableInfo().getName());
        }
        return put(tid, pid, key, time, bytes, th);
    }

    @Override
    public PutFuture put(int tid, int pid, String key, long time, String value) throws TabletException {
        return put(tid, pid, key, time, value.getBytes(Charsets.UTF_8));
    }

    @Override
    public PutFuture put(String name, String key, long time, byte[] bytes) throws TabletException {
        TableHandler th = client.getHandler(name);
        if (th == null) {
            throw new TabletException("fail to find table with name " + name);
        }
        if (th.getSchema() != null) {
            throw new TabletException("fail to put the schema table in the way of putting kv table");
        }
        key = validateKey(key);
        int pid = TableClientCommon.computePidByKey(key, th.getPartitions().length);
        return put(th.getTableInfo().getTid(), pid, key, time, bytes, th);
    }

    @Override
    public PutFuture put(String name, long time, Object[] row) throws TabletException {
        return put(name, time, row, null);
    }

    private PutFuture put(String name, long time, Object[] row, List<Tablet.TSDimension> ts) throws TabletException {
        TableHandler th = client.getHandler(name);
        if (th == null) {
            throw new TabletException("no table with name " + name);
        }
        Map<Integer, List<Tablet.Dimension>> mapping = TableClientCommon.fillPartitionTabletDimension(row, th, client.getConfig().isHandleNull());
        ByteBuffer buffer = RowCodec.encode(row, th.getSchema());
        List<Future<PutResponse>> pl = new ArrayList<Future<PutResponse>>();
        Iterator<Map.Entry<Integer, List<Tablet.Dimension>>> it = mapping.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Integer, List<Tablet.Dimension>> entry = it.next();
            Future<PutResponse> response = putForInternal(th.getTableInfo().getTid(), entry.getKey(), null,
                    time, entry.getValue(), ts, buffer, th);
            pl.add(response);
        }
        return PutFuture.wrapper(pl);
    }

    @Override
    public PutFuture put(String name, Object[] row) throws TabletException {
        TableHandler th = client.getHandler(name);
        if (th == null) {
            throw new TabletException("no table with name " + name);
        }
        List<Tablet.TSDimension> tsDimensions = TableClientCommon.parseArrayInput(row, th);
        return put(name, 0, row, tsDimensions);
    }

    @Override
    public PutFuture put(String name, Map<String, Object> row) throws TabletException {
        TableHandler th = client.getHandler(name);
        if (th == null) {
            throw new TabletException("no table with name " + name);
        }
        Object[] arrayRow = new Object[th.getSchema().size()];
        List<Tablet.TSDimension> tsDimensions = new ArrayList<Tablet.TSDimension>();
        TableClientCommon.parseMapInput(row, th, arrayRow, tsDimensions);
        return put(name, 0, arrayRow, tsDimensions);
    }

    @Override
    public PutFuture put(String name, String key, long time, String value) throws TabletException {
        return put(name, key, time, value.getBytes(Charsets.UTF_8));
    }

    @Override
    public GetFuture get(int tid, int pid, String key) throws TabletException {
        return get(tid, pid, key, null, 0l);
    }

    @Override
    public GetFuture get(int tid, int pid, String key, String idxName) throws TabletException {
        return get(tid, pid, key, idxName, 0l);
    }

    @Override
    public GetFuture get(int tid, int pid, String key, long time) throws TabletException {
        return get(tid, pid, key, null, time);
    }

    @Override
    public GetFuture get(int tid, int pid, String key, String idxName, long time) throws TabletException {
        TableHandler th = client.getHandler(tid);
        if (th == null) {
            throw new TabletException("no table with id " + tid);
        }
        return get(tid, pid, key, idxName, time, null, null, 0l, null ,th);
    }

    @Override
    public ScanFuture scan(int tid, int pid, String key, long st, long et) throws TabletException {
        return scan(tid, pid, key, null, st, et);
    }

    @Override
    public ScanFuture scan(int tid, int pid, String key, long st, long et, int limit) throws TabletException {
        return scan(tid, pid, key, null, st, et, limit);
    }

    @Override
    public ScanFuture scan(int tid, int pid, String key, int limit) throws TabletException {
        return scan(tid, pid, key, null, 0, 0, limit);
    }

    @Override
    public ScanFuture scan(int tid, int pid, String key, String idxName, long st, long et) throws TabletException {
        return scan(tid, pid, key, idxName, st, et, 0);
    }

    @Override
    public ScanFuture scan(int tid, int pid, String key, String idxName, int limit) throws TabletException {
        return scan(tid, pid, key, idxName, 0, 0, limit);
    }

    @Override
    public ScanFuture scan(int tid, int pid, String key, String idxName, long st, long et, int limit) throws TabletException {
        TableHandler th = client.getHandler(tid);
        if (th == null) {
            throw new TabletException("no table with id " + tid);
        }
        return scan(tid, pid, key, idxName, st, et, null, limit, th);
    }


    @Override
    public ScanFuture scan(String name, String key, String idxName, long st, long et) throws TabletException {
        return scan(name, key, idxName, st, et, 0);
    }

    @Override
    public ScanFuture scan(String name, String key, String idxName, int limit) throws TabletException {
        return scan(name, key, idxName, 0, 0, limit);
    }

    @Override
    public ScanFuture scan(String name, String key, String idxName, long st, long et, int limit) throws TabletException {
        return scan(name, key, idxName, st, et, null, limit);
    }

    @Override
    public ScanFuture scan(String name, String key, long st, long et) throws TabletException {
        return scan(name, key, null, st, et, null,0);
    }

    @Override
    public ScanFuture scan(String name, String key, int limit) throws TabletException {
        return scan(name, key, null, 0, 0, null, limit);
    }

    @Override
    public ScanFuture scan(String name, String key, long st, long et, int limit) throws TabletException {
        return scan(name, key, null, st, et, null, limit);
    }

    @Override
    public ScanFuture scan(String name, String key, String idxName, long st, long et, String tsName, int limit) throws TabletException {
        TableHandler th = client.getHandler(name);
        if (th == null) {
            throw new TabletException("no table with name " + name);
        }
        key = validateKey(key);
        int pid = TableClientCommon.computePidByKey(key, th.getPartitions().length);
        return scan(th.getTableInfo().getTid(), pid, key, idxName, st, et, tsName, limit, th);
    }

    @Override
    public ScanFuture scan(String name, Object[] keyArr, String idxName, long st, long et, String tsName, int limit) throws TabletException {
        TableHandler th = client.getHandler(name);
        if (th == null) {
            throw new TabletException("no table with name " + name);
        }
        List<String> list = th.getKeyMap().get(idxName);
        if (list == null) {
            throw new TabletException("no index name in table" + idxName);
        }
        if (keyArr.length != list.size()) {
            throw new TabletException("check key number failed");
        }
        String combinedKey = TableClientCommon.getCombinedKey(keyArr, client.getConfig().isHandleNull());
        int pid = TableClientCommon.computePidByKey(combinedKey, th.getPartitions().length);
        return scan(th.getTableInfo().getTid(), pid, combinedKey, idxName, st, et, tsName, limit, th);
    }

    @Override
    public ScanFuture scan(String name, Map<String, Object> keyMap, String idxName, long st, long et, String tsName,
                           int limit) throws TabletException {
        TableHandler th = client.getHandler(name);
        if (th == null) {
            throw new TabletException("no table with name " + name);
        }
        List<String> list = th.getKeyMap().get(idxName);
        if (list == null) {
            throw new TabletException("no index name in table" + idxName);
        }
        String combinedKey = TableClientCommon.getCombinedKey(keyMap, list, client.getConfig().isHandleNull());
        int pid = TableClientCommon.computePidByKey(combinedKey, th.getPartitions().length);
        return scan(th.getTableInfo().getTid(), pid, combinedKey, idxName, st, et, tsName, limit, th);
    }

    @Override
    public GetFuture get(String name, String key, long time) throws TabletException {
        return get(name, key, null, time, null);
    }

    @Override
    public GetFuture get(String name, String key, String idxName, long time) throws TabletException {
        TableHandler th = client.getHandler(name);
        if (th == null) {
            throw new TabletException("no table with name " + name);
        }
        key = validateKey(key);
        int pid = TableClientCommon.computePidByKey(key, th.getPartitions().length);
        return get(th.getTableInfo().getTid(), pid, key, idxName, time, null, null,0l, null, th);
    }

    @Override
    public GetFuture get(String name, String key) throws TabletException {
        return get(name, key, null, 0l);
    }

    @Override
    public GetFuture get(String name, String key, String idxName) throws TabletException {
        return get(name, key, idxName, 0l);
    }

    private PutFuture put(int tid, int pid, String key, long time, byte[] bytes, TableHandler th) throws TabletException{
        return put(tid, pid, key, time, null, ByteBuffer.wrap(bytes), th);
    }

    private PutFuture put(int tid, int pid,
                          String key, long time,
                          List<Tablet.Dimension> ds,
                          ByteBuffer row, TableHandler th) throws TabletException {
        if ((ds == null || ds.isEmpty()) && (key == null || key.isEmpty())) {
            throw new TabletException("key is null or empty");
        }
        long start = System.currentTimeMillis();
        Future<PutResponse> response = putForInternal(tid, pid, key, time, ds, null, row, th);
        return PutFuture.wrapper(response, start, client.getConfig());
    }

    /**
     * validate key
     * rewrite key if rtidb client config set isHandleNull {@code true}
     * @param key
     * @return
     * @throws TabletException if key is null or empty
     */
    private String validateKey(String key) throws TabletException {
        if (key == null || key.isEmpty()) {
            if (client.getConfig().isHandleNull()) {
                key = key == null ? RTIDBClientConfig.NULL_STRING : key.isEmpty() ? RTIDBClientConfig.EMPTY_STRING : key;
            } else {
                throw new TabletException("key is null or empty");
            }
        }
        return key;
    }

    private Future<PutResponse> putForInternal(int tid, int pid, String key, long time,
                                               List<Tablet.Dimension> ds, List<Tablet.TSDimension> ts,
                                               ByteBuffer row, TableHandler th) throws TabletException {
        if ((ds == null || ds.isEmpty()) && (key == null || key.isEmpty())) {
            throw new TabletException("key is null or empty");
        }
        if (time == 0 && (ts == null || ts.isEmpty())) {
            throw new TabletException("ts is null or empty");
        }
        PartitionHandler ph = th.getHandler(pid);
        TabletServer tablet = ph.getLeader();
        if (tablet == null) {
            throw new TabletException("Cannot find available tabletServer with tid " + tid);
        }
        Tablet.PutRequest.Builder builder = Tablet.PutRequest.newBuilder();
        if (ds != null) {
            for (Tablet.Dimension dim : ds) {
                builder.addDimensions(dim);
            }
        }
        builder.setPid(pid);
        builder.setTid(tid);
        if (time != 0) {
            builder.setTime(time);
        }
        if (ts != null) {
            for (Tablet.TSDimension tsDim : ts) {
                builder.addTsDimensions(tsDim);
            }
        }
        if (key != null) {
            builder.setPk(key);
        }
        row.rewind();
        if (th.getTableInfo().hasCompressType() && th.getTableInfo().getCompressType() == NS.CompressType.kSnappy) {
            byte[] data = row.array();
            byte[] compressed = Compress.snappyCompress(data);
            if (compressed == null) {
                throw new TabletException("snappy compress error");
            }
            ByteBuffer buffer = ByteBuffer.wrap(compressed);
            row = buffer;
        }
        builder.setValue(ByteBufferNoCopy.wrap(row.asReadOnlyBuffer()));
        Tablet.PutRequest request = builder.build();
        Future<PutResponse> response = tablet.put(request, putFakeCallback);
        return response;
    }

    private GetFuture get(int tid, int pid, String key, String idxName,
                          long time, String tsName, Tablet.GetType type,
                          long endtime, Tablet.GetType etType,
                          TableHandler th) throws TabletException {
        key = validateKey(key);
        Tablet.GetRequest.Builder builder = Tablet.GetRequest.newBuilder();
        builder.setTid(tid);
        builder.setPid(pid);
        builder.setKey(key);
        builder.setTs(time);
        builder.setEt(endtime);
        if (type != null)  builder.setType(type);
        if (etType != null) builder.setEtType(etType);
        if (idxName != null && !idxName.isEmpty()) {
            builder.setIdxName(idxName);
        }
        if (tsName != null && !tsName.isEmpty()) {
            builder.setTsName(tsName);
        }
        Tablet.GetRequest request = builder.build();
        PartitionHandler ph = th.getHandler(pid);
        TabletServer ts = ph.getReadHandler(th.getReadStrategy());
        if (ts == null) {
            throw new TabletException("Cannot find available tabletServer with tid " + tid);
        }
        Future<Tablet.GetResponse> response = ts.get(request, getFakeCallback);
        return GetFuture.wrappe(response, th, client.getConfig());
    }


    private ScanFuture scan(int tid, int pid, String key, String idxName, long st, long et, String tsName, int limit,
                            TableHandler th) throws TabletException {
        key = validateKey(key);

        Tablet.ScanRequest.Builder builder = Tablet.ScanRequest.newBuilder();
        builder.setPk(key);
        builder.setTid(tid);
        builder.setEt(et);
        builder.setSt(st);
        builder.setPid(pid);
        builder.setLimit(limit);
        if (idxName != null && !idxName.isEmpty()) {
            builder.setIdxName(idxName);
        }
        if (tsName != null && !tsName.isEmpty()) {
            builder.setTsName(tsName);
        }
        if (client.getConfig().isRemoveDuplicateByTime()) {
            builder.setEnableRemoveDuplicatedRecord(true);
        }
        Tablet.ScanRequest request = builder.build();
        Long startTime = System.nanoTime();
        PartitionHandler ph = th.getHandler(pid);
        TabletServer ts = ph.getReadHandler(th.getReadStrategy());
        if (ts == null) {
            throw new TabletException("Cannot find available tabletServer with tid " + tid);
        }
        Future<Tablet.ScanResponse> response = ts.scan(request, scanFakeCallback);
        return ScanFuture.wrappe(response, th, startTime);
    }

    private static RpcCallback<Tablet.PutResponse> putFakeCallback = new RpcCallback<Tablet.PutResponse>() {

        @Override
        public void success(PutResponse response) {
        }

        @Override
        public void fail(Throwable e) {

        }

    };

    private static RpcCallback<Tablet.GetResponse> getFakeCallback = new RpcCallback<Tablet.GetResponse>() {

        @Override
        public void success(GetResponse response) {
        }

        @Override
        public void fail(Throwable e) {
        }

    };

    private static RpcCallback<Tablet.ScanResponse> scanFakeCallback = new RpcCallback<Tablet.ScanResponse>() {

        @Override
        public void success(ScanResponse response) {
        }

        @Override
        public void fail(Throwable e) {
        }

    };

    @Override
    public PutFuture put(String name, long time, Map<String, Object> row) throws TabletException {
        TableHandler th = client.getHandler(name);
        if (th == null) {
            throw new TabletException("no table with name " + name);
        }
        Object[] arrayRow = new Object[th.getSchema().size()];
        for(int i = 0; i < th.getSchema().size(); i++) {
            arrayRow[i] = row.get(th.getSchema().get(i).getName());
        }
        return put(name, time, arrayRow);
    }

    @Override
    public List<ColumnDesc> getSchema(String tname) throws TabletException {
        TableHandler th = client.getHandler(tname);
        if (th == null) {
            throw new TabletException("no table with name " + tname);
        }
        return th.getSchema();
    }

    @Override
    public PutFuture put(int tid, int pid, long time, Map<String, Object> row) throws TabletException {
        TableHandler th = client.getHandler(tid);
        if (th == null) {
            throw new TabletException("fail to find table with id " + tid);
        }
        Object[] arrayRow = new Object[th.getSchema().size()];
        for(int i = 0; i < th.getSchema().size(); i++) {
            arrayRow[i] = row.get(th.getSchema().get(i).getName());
        }
        return put(tid, pid, time, arrayRow);
    }

    @Override
    public List<ColumnDesc> getSchema(int tid) throws TabletException {
        TableHandler th = client.getHandler(tid);
        if (th == null) {
            throw new TabletException("fail to find table with id " + tid);
        }
        return th.getSchema();
    }

    @Override
    public GetFuture get(String name, String key, long time, Tablet.GetType type) throws TabletException {
        return get(name, key, null, time, type);
    }

    @Override
    public GetFuture get(String name, String key, String idxName, long time, Tablet.GetType type) throws TabletException {
        return get(name, key, idxName, time, null, type);
    }

    @Override
    public GetFuture get(String name, String key, String idxName, long time, String tsName, Tablet.GetType type) throws TabletException {
        TableHandler th = client.getHandler(name);
        if (th == null) {
            throw new TabletException("no table with name " + name);
        }

        key = validateKey(key);
        int pid = TableClientCommon.computePidByKey(key, th.getPartitions().length);
        return get(th.getTableInfo().getTid(), pid, key, idxName, time, tsName, type,0l, null, th);
    }

    @Override
    public GetFuture get(String name, Object[] keyArr, String idxName, long time, String tsName, Tablet.GetType type) throws TabletException {
        return get(name, keyArr, idxName, time, tsName, type, 0l, null);
    }

    @Override
    public GetFuture get(int tid, int pid, String key,
                         String idxName, String tsName,
                         long st, Tablet.GetType stType,
                         long et, Tablet.GetType etType) throws TabletException {
        TableHandler th = client.getHandler(tid);
        if (th == null) {
            throw new TabletException("no table with tid " + tid);
        }
        return this.get(tid, pid, key, idxName, st, tsName, stType, et, etType, th);
    }

    @Override
    public GetFuture get(String name, String key,
                         String idxName, long time, String tsName,
                         Tablet.GetType type, long et, Tablet.GetType etType) throws TabletException {
        TableHandler th = client.getHandler(name);
        if (th == null) {
            throw new TabletException("no table with name " + name);
        }
        int pid = TableClientCommon.computePidByKey(key, th.getPartitions().length);
        return this.get(th.getTableInfo().getTid(), pid, key, idxName, time, tsName, type, et, etType, th);
    }

    @Override
    public GetFuture get(String name, Object[] keyArr, String idxName, long time, String tsName,
                         Tablet.GetType type, long et, Tablet.GetType etType) throws TabletException {
        TableHandler th = client.getHandler(name);
        if (th == null) {
            throw new TabletException("no table with name " + name);
        }
        List<String> list = th.getKeyMap().get(idxName);
        if (list == null) {
            throw new TabletException("no index name " + idxName + " in table " + name);
        }
        if (keyArr.length != list.size()) {
            throw new TabletException("check key number failed");
        }
        String combinedKey = TableClientCommon.getCombinedKey(keyArr, client.getConfig().isHandleNull());
        int pid = TableClientCommon.computePidByKey(combinedKey, th.getPartitions().length);
        return get(th.getTableInfo().getTid(), pid, combinedKey, idxName, time, tsName, type, et, etType, th);
    }

    @Override
    public GetFuture get(String name, Map<String, Object> keyMap, String idxName, long time, String tsName,
                         Tablet.GetType type, long et, Tablet.GetType etType) throws TabletException {
        TableHandler th = client.getHandler(name);
        if (th == null) {
            throw new TabletException("no table with name " + name);
        }
        List<String> list = th.getKeyMap().get(idxName);
        if (list == null) {
            throw new TabletException("no index name " + idxName + " in table " + name);
        }
        String combinedKey = TableClientCommon.getCombinedKey(keyMap, list, client.getConfig().isHandleNull());
        int pid = TableClientCommon.computePidByKey(combinedKey, th.getPartitions().length);
        return get(th.getTableInfo().getTid(), pid, combinedKey, idxName, time, tsName, type, et, etType, th);
    }

    @Override
    public GetFuture get(String name, Map<String, Object> keyMap, String idxName, long time, String tsName,
                         Tablet.GetType type) throws TabletException {
        return get(name, keyMap, idxName, time, tsName, type, 0l, null);
    }
}