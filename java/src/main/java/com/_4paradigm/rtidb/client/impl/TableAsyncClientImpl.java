package com._4paradigm.rtidb.client.impl;

import com._4paradigm.rtidb.client.*;
import com._4paradigm.rtidb.client.ha.PartitionHandler;
import com._4paradigm.rtidb.client.ha.RTIDBClient;
import com._4paradigm.rtidb.client.ha.RTIDBClientConfig;
import com._4paradigm.rtidb.client.ha.TableHandler;
import com._4paradigm.rtidb.client.schema.ColumnDesc;
import com._4paradigm.rtidb.client.schema.ProjectionInfo;
import com._4paradigm.rtidb.client.schema.RowBuilder;
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

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.Future;

public class TableAsyncClientImpl implements TableAsyncClient {

    private RTIDBClient client;


    public TableAsyncClientImpl(RTIDBClient client) {
        this.client = client;
    }

    @Override
    public GetFuture get(String name, Map<String, Object> keyMap, long time, GetOption getOption) throws TabletException {
        TableHandler th = client.getHandler(name);
        if (th == null) {
            throw new TabletException("no table with name " + name);
        }
        if (getOption.getIdxName() == null) {
            throw new TabletException("index name is required but null");
        }
        List<String> list = th.getKeyMap().get(getOption.getIdxName());
        if (list == null) {
            throw new TabletException("no index name found ");
        }
        String combinedKey = TableClientCommon.getCombinedKey(keyMap, list, client.getConfig().isHandleNull());
        int pid = TableClientCommon.computePidByKey(combinedKey, th.getPartitions().length);
        return get(pid, combinedKey, time, getOption, th);
    }

    @Override
    public GetFuture get(String name, Object[] keys, long time, GetOption getOption) throws TabletException {
        TableHandler th = client.getHandler(name);
        if (th == null) {
            throw new TabletException("no table with name " + name);
        }
        if (getOption.getIdxName() == null) {
            throw new TabletException("index name is required but null");
        }
        List<String> list = th.getKeyMap().get(getOption.getIdxName());
        if (list == null) {
            throw new TabletException("no index name found");
        }
        if (keys.length != list.size()) {
            throw new TabletException("check key number failed");
        }
        String combinedKey = TableClientCommon.getCombinedKey(keys, client.getConfig().isHandleNull());
        int pid = TableClientCommon.computePidByKey(combinedKey, th.getPartitions().length);
        return get(pid, combinedKey, time, getOption, th);
    }

    @Override
    public GetFuture get(String name, String key, long time, GetOption getOption) throws TabletException {
        TableHandler th = client.getHandler(name);
        if (th == null) {
            throw new TabletException("no table with name " + name);
        }
        key = validateKey(key);
        int pid = TableClientCommon.computePidByKey(key, th.getPartitions().length);
        return get(pid, key, time, getOption, th);
    }

    @Override
    public GetFuture get(String name, String key, long time, Object type) throws TabletException {
        return get(name, key, time, (Tablet.GetType)(type));
    }

    @Override
    public PutFuture put(int tid, int pid, long time, Object[] row) throws TabletException {
        TableHandler tableHandler = client.getHandler(tid);
        if (tableHandler == null) {
            throw new TabletException("fail to find table with id " + tid);
        }
        if (tableHandler.getPartitions().length <= pid) {
            throw new TabletException("fail to find partition with pid " + pid + " from table " + tableHandler.getTableInfo().getName());
        }
        if (row == null) {
            throw new TabletException("putting data is null");
        }
        List<Tablet.Dimension> dimList = TableClientCommon.fillTabletDimension(row, tableHandler, client.getConfig().isHandleNull());
        ByteBuffer buffer = null;
        if (row.length == tableHandler.getSchema().size()) {
            switch (tableHandler.getFormatVersion()) {
                case 1:
                    buffer = RowBuilder.encode(row, tableHandler.getSchema());
                    break;
                default:
                    buffer = RowCodec.encode(row, tableHandler.getSchema());
            }
        } else {
            List<ColumnDesc> columnDescs = tableHandler.getSchemaMap().get(row.length);
            if (columnDescs == null) {
                throw new TabletException("no schema for column count " + row.length);
            }
            int modifyTimes = row.length - tableHandler.getSchema().size();
            if (row.length > tableHandler.getSchema().size() + tableHandler.getSchemaMap().size()) {
                modifyTimes = tableHandler.getSchemaMap().size();
            }
            buffer = RowCodec.encode(row, columnDescs, modifyTimes);
        }
        return put(tid, pid, null, time, dimList, buffer, tableHandler);
    }

    @Override
    public PutFuture put(int tid, int pid, String key, long time, byte[] bytes) throws TabletException {
        TableHandler th = client.getHandler(tid);
        if (th == null) {
            throw new TabletException("fail to find table with id " + tid);
        }
        if (th.getPartitions().length <= pid) {
            throw new TabletException("fail to find partition with pid " + pid + " from table " + th.getTableInfo().getName());
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
        if (!th.getSchema().isEmpty()) {
            throw new TabletException("fail to put the schema table " + th.getTableInfo().getName() + " in the way of putting kv table");
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
        if (row == null) {
            throw new TabletException("putting data is null");
        }
        Map<Integer, List<Tablet.Dimension>> mapping = TableClientCommon.fillPartitionTabletDimension(row, th, client.getConfig().isHandleNull());
        ByteBuffer buffer = null;
        if (row.length == th.getSchema().size()) {
            switch (th.getTableInfo().getFormatVersion()) {
                case 1:
                    buffer = RowBuilder.encode(row, th.getSchema());
                    break;
                default:
                    buffer = RowCodec.encode(row, th.getSchema());
            }
        } else {
            List<ColumnDesc> columnDescs = th.getSchemaMap().get(row.length);
            if (columnDescs == null) {
                throw new TabletException("no schema for column count " + row.length);
            }
            int modifyTimes = row.length - th.getSchema().size();
            if (row.length > th.getSchema().size() + th.getSchemaMap().size()) {
                modifyTimes = th.getSchemaMap().size();
            }
            buffer = RowCodec.encode(row, columnDescs, modifyTimes);
        }
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
        if (row == null) {
            throw new TabletException("putting data is null");
        }
        if (row.length > th.getSchema().size() + th.getSchemaMap().size()) {
            row = Arrays.copyOf(row, th.getSchema().size() + th.getSchemaMap().size());
        }
        long ts = 0;
        List<Tablet.TSDimension> tsDimensions = TableClientCommon.parseArrayInput(row, th);
        if (tsDimensions.size() <= 0) {
            ts = System.currentTimeMillis();
        }
        return put(name, ts, row, tsDimensions);
    }

    @Override
    public PutFuture put(String name, Map<String, Object> row) throws TabletException {
        TableHandler th = client.getHandler(name);
        if (th == null) {
            throw new TabletException("no table with name " + name);
        }
        if (row == null) {
            throw new TabletException("putting data is null");
        }
        List<Tablet.TSDimension> tsDimensions = new ArrayList<Tablet.TSDimension>();
        Object[] arrayRow = null;
        if (row.size() > th.getSchema().size() && th.getSchemaMap().size() > 0) {
            if (row.size() > th.getSchema().size() + th.getSchemaMap().size()) {
                arrayRow = new Object[th.getSchema().size() + th.getSchemaMap().size()];
            } else {
                arrayRow = new Object[row.size()];
            }
        } else {
            arrayRow = new Object[th.getSchema().size()];
        }
        long ts = 0;
        TableClientCommon.parseMapInput(row, th, arrayRow, tsDimensions);
        if (tsDimensions.size() <= 0) {
            ts = System.currentTimeMillis();
        }
        return put(name, ts, arrayRow, tsDimensions);
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
        return get(tid, pid, key, idxName, time, null, null, 0l, null, th);
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
        return scan(tid, pid, key, idxName, st, et, null, limit,0, th);
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
        ScanOption scanOption = new ScanOption();
        scanOption.setLimit(0);
        return scan(name, key, st, et,scanOption);
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
    public ScanFuture scan(String tname, Map<String, Object> keyMap, long st, long et, ScanOption option) throws TabletException {
        if (option.getIdxName() == null) throw new TabletException("idx name is required ");
        TableHandler th = client.getHandler(tname);
        if (th == null) {
            throw new TabletException("no table with name " + tname);
        }
        List<String> list = th.getKeyMap().get(option.getIdxName());
        if (list == null) {
            throw new TabletException("no index name in table" + option.getIdxName());
        }
        String combinedKey = TableClientCommon.getCombinedKey(keyMap, list, client.getConfig().isHandleNull());
        int pid = TableClientCommon.computePidByKey(combinedKey, th.getPartitions().length);
        return scan(th.getTableInfo().getTid(), pid, combinedKey,  st,
                et, th, option);
    }

    @Override
    public ScanFuture scan(String tname, String key, long st, long et, ScanOption option) throws TabletException {
        TableHandler th = client.getHandler(tname);
        if (th == null) {
            throw new TabletException("no table with name " + tname);
        }
        key = validateKey(key);
        int pid = TableClientCommon.computePidByKey(key, th.getPartitions().length);
        return scan(th.getTableInfo().getTid(), pid, key,  st,
                et, th, option);
    }

    @Override
    public ScanFuture scan(String tname, Object[] keyArr, long st, long et, ScanOption option) throws TabletException {
        if (option.getIdxName() == null) throw new TabletException("idx name is required ");
        TableHandler th = client.getHandler(tname);
        if (th == null) {
            throw new TabletException("no table with name " + tname);
        }
        List<String> list = th.getKeyMap().get(option.getIdxName());
        if (list == null) {
            throw new TabletException("no index name in table" + option.getIdxName());
        }
        if (keyArr.length != list.size()) {
            throw new TabletException("check key number failed");
        }
        String combinedKey = TableClientCommon.getCombinedKey(keyArr, client.getConfig().isHandleNull());
        int pid = TableClientCommon.computePidByKey(combinedKey, th.getPartitions().length);
        return scan(th.getTableInfo().getTid(), pid, combinedKey,  st,
                et, th, option);
    }

    @Override
    public ScanFuture scan(String name, String key, String idxName, long st, long et, String tsName, int limit) throws TabletException {
        TableHandler th = client.getHandler(name);
        if (th == null) {
            throw new TabletException("no table with name " + name);
        }
        key = validateKey(key);
        int pid = TableClientCommon.computePidByKey(key, th.getPartitions().length);
        ScanOption scanOption = new ScanOption();
        return scan(th.getTableInfo().getTid(), pid, key, idxName, st, et, tsName, limit, 0,th);
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
        return scan(th.getTableInfo().getTid(), pid, combinedKey, idxName, st, et, tsName, limit, 0, th);
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
        return scan(th.getTableInfo().getTid(), pid, combinedKey, idxName, st, et, tsName, limit,0, th);
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
        return get(th.getTableInfo().getTid(), pid, key, idxName, time, null, null, 0l, null, th);
    }

    @Override
    public GetFuture get(String name, String key) throws TabletException {
        return get(name, key, null, 0l);
    }

    @Override
    public GetFuture get(String name, String key, String idxName) throws TabletException {
        return get(name, key, idxName, 0l);
    }

    private PutFuture put(int tid, int pid, String key, long time, byte[] bytes, TableHandler th) throws TabletException {
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
     *
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
        builder.setFormatVersion(th.getFormatVersion());
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
        Future<PutResponse> response = tablet.put(request, TableClientCommon.putFakeCallback);
        return response;
    }

    private GetFuture get(int pid, String key, long time, GetOption getOption, TableHandler th) throws TabletException {
        key = validateKey(key);
        PartitionHandler ph = th.getHandler(pid);
        TabletServer ts = ph.getReadHandler(th.getReadStrategy());
        if (ts == null) {
            throw new TabletException("Cannot find available tabletServer with tid " + th.getTableInfo().getTid());
        }
        List<ColumnDesc> schema = null;
        Tablet.GetRequest.Builder builder = Tablet.GetRequest.newBuilder();
        builder.setTid(th.getTableInfo().getTid());
        builder.setPid(pid);
        builder.setKey(key);
        builder.setTs(time);
        builder.setEt(getOption.getEt());
        if (getOption.getStType() != null) builder.setType(getOption.getStType());
        if (getOption.getEtType() != null) builder.setEtType(getOption.getEtType());
        if (getOption.getIdxName() != null && !getOption.getIdxName().isEmpty()) {
            builder.setIdxName(getOption.getIdxName());
        }
        if (getOption.getTsName()!= null && !getOption.getTsName().isEmpty()) {
            builder.setTsName(getOption.getTsName());
        }
        if (th.getFormatVersion() == 1 ) {
            if (getOption.getProjection().size() > 0) {
                schema = new ArrayList<>();
                for (String name : getOption.getProjection()) {
                    Integer idx = th.getSchemaPos().get(name);
                    if (idx == null) {
                        throw new TabletException("Cannot find column " + name);
                    }
                    builder.addProjection(idx);
                    schema.add(th.getSchema().get(idx));
                }
            }
            Tablet.GetRequest request = builder.build();
            Future<Tablet.GetResponse> future = ts.get(request, TableClientCommon.getFakeCallback);
            return new GetFuture(future, th, client.getConfig(), schema);
        }else {
            if (getOption.getProjection().size() > 0) {
                List<Integer> projectIdx = new ArrayList<>();
                BitSet bitSet = new BitSet(th.getSchema().size());
                int maxIndex = -1;
                for (String name : getOption.getProjection()) {
                    Integer idx = th.getSchemaPos().get(name);
                    if (idx == null) {
                        throw new TabletException("Cannot find column " + name);
                    }
                    projectIdx.add(idx);
                    if (idx > maxIndex) {
                        maxIndex = idx;
                    }
                    bitSet.set(idx, true);
                }
                Tablet.GetRequest request = builder.build();
                Future<Tablet.GetResponse> future = ts.get(request, TableClientCommon.getFakeCallback);
                return new GetFuture(future, th, projectIdx, bitSet, maxIndex);
            }else {
                Tablet.GetRequest request = builder.build();
                Future<Tablet.GetResponse> future = ts.get(request, TableClientCommon.getFakeCallback);
                return new GetFuture(future, th);
            }
        }
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
        if (type != null) builder.setType(type);
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
        Future<Tablet.GetResponse> response = ts.get(request, TableClientCommon.getFakeCallback);
        return new GetFuture(response, th);
    }


    private ScanFuture scan(int tid, int pid, String key, String idxName, long st, long et, String tsName, int limit, int atLeast,
                            TableHandler th) throws TabletException {
        key = validateKey(key);
        Tablet.ScanRequest.Builder builder = Tablet.ScanRequest.newBuilder();
        builder.setPk(key);
        builder.setTid(tid);
        builder.setEt(et);
        builder.setSt(st);
        builder.setPid(pid);
        builder.setLimit(limit);
        builder.setAtleast(atLeast);
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
        Future<Tablet.ScanResponse> response = ts.scan(request, TableClientCommon.scanFakeCallback);
        return ScanFuture.wrappe(response, th, startTime);
    }


    @Override
    public PutFuture put(String name, long time, Map<String, Object> row) throws TabletException {
        TableHandler th = client.getHandler(name);
        if (th == null) {
            throw new TabletException("no table with name " + name);
        }
        if (row == null) {
            throw new TabletException("putting data is null");
        }
        Object[] arrayRow = null;
        if (row.size() > th.getSchema().size() && th.getSchemaMap().size() > 0) {
            int columnSize = row.size();
            if (row.size() > th.getSchema().size() + th.getSchemaMap().size()) {
                columnSize = th.getSchema().size() + th.getSchemaMap().size();
            }
            arrayRow = new Object[columnSize];
            List<ColumnDesc> schema = th.getSchemaMap().get(columnSize);
            for (int i = 0; i < schema.size(); i++) {
                arrayRow[i] = row.get(schema.get(i).getName());
            }
        } else {
            arrayRow = new Object[th.getSchema().size()];
            for (int i = 0; i < th.getSchema().size(); i++) {
                arrayRow[i] = row.get(th.getSchema().get(i).getName());
            }
        }
        return put(name, time, arrayRow);
    }

    @Override
    public List<ColumnDesc> getSchema(String tname) throws TabletException {
        TableHandler th = client.getHandler(tname);
        if (th == null) {
            throw new TabletException("no table with name " + tname);
        }
        if (th.getSchemaMap().size() == 0) {
            return th.getSchema();
        } else {
            return th.getSchemaMap().get(th.getSchema().size() + th.getSchemaMap().size());
        }
    }

    @Override
    public PutFuture put(int tid, int pid, long time, Map<String, Object> row) throws TabletException {
        TableHandler th = client.getHandler(tid);
        if (th == null) {
            throw new TabletException("fail to find table with id " + tid);
        }
        if (row == null) {
            throw new TabletException("putting data is null");
        }
        Object[] arrayRow = null;
        if (row.size() > th.getSchema().size() && th.getSchemaMap().size() > 0) {
            int columnSize = row.size();
            if (row.size() > th.getSchema().size() + th.getSchemaMap().size()) {
                columnSize = th.getSchema().size() + th.getSchemaMap().size();
            }
            arrayRow = new Object[columnSize];
            List<ColumnDesc> schema = th.getSchemaMap().get(columnSize);
            for (int i = 0; i < schema.size(); i++) {
                arrayRow[i] = row.get(schema.get(i).getName());
            }
        } else {
            arrayRow = new Object[th.getSchema().size()];
            for (int i = 0; i < th.getSchema().size(); i++) {
                arrayRow[i] = row.get(th.getSchema().get(i).getName());
            }
        }
        return put(tid, pid, time, arrayRow);
    }

    @Override
    public List<ColumnDesc> getSchema(int tid) throws TabletException {
        TableHandler th = client.getHandler(tid);
        if (th == null) {
            throw new TabletException("fail to find table with id " + tid);
        }
        if (th.getSchemaMap().size() == 0) {
            return th.getSchema();
        } else {
            return th.getSchemaMap().get(th.getSchema().size() + th.getSchemaMap().size());
        }
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
        return get(th.getTableInfo().getTid(), pid, key, idxName, time, tsName, type, 0l, null, th);
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
        GetOption option = new GetOption();
        option.setTsName(tsName);
        option.setIdxName(idxName);
        option.setEt(et);
        option.setEtType(etType);
        option.setStType(type);
        String combinedKey = TableClientCommon.getCombinedKey(keyArr, client.getConfig().isHandleNull());
        int pid = TableClientCommon.computePidByKey(combinedKey, th.getPartitions().length);
        return get(pid, combinedKey, time, option, th);
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
        GetOption option = new GetOption();
        option.setTsName(tsName);
        option.setIdxName(idxName);
        option.setEt(et);
        option.setEtType(etType);
        option.setStType(type);
        String combinedKey = TableClientCommon.getCombinedKey(keyMap, list, client.getConfig().isHandleNull());
        int pid = TableClientCommon.computePidByKey(combinedKey, th.getPartitions().length);
        return get(pid, combinedKey, time, option, th);
    }

    @Override
    public GetFuture get(String name, Map<String, Object> keyMap, String idxName, long time, String tsName,
                         Tablet.GetType type) throws TabletException {
        return get(name, keyMap, idxName, time, tsName, type, 0l, null);
    }

    private ScanFuture scan(int tid, int pid, String key, long st, long et,TableHandler th, ScanOption option) throws TabletException{
        key = validateKey(key);
        PartitionHandler ph = th.getHandler(pid);
        TabletServer ts = ph.getReadHandler(th.getReadStrategy());
        if (ts == null) {
            throw new TabletException("Cannot find available tabletServer with tid " + tid);
        }
        Tablet.ScanRequest.Builder builder = Tablet.ScanRequest.newBuilder();
        builder.setPk(key);
        builder.setTid(tid);
        builder.setEt(et);
        builder.setSt(st);
        builder.setPid(pid);
        builder.setLimit(option.getLimit());
        builder.setAtleast(option.getAtLeast());
        if (option.getIdxName() != null)
            builder.setIdxName(option.getIdxName());
        if (option.getTsName() != null)
            builder.setTsName(option.getTsName());
        builder.setEnableRemoveDuplicatedRecord(option.isRemoveDuplicateRecordByTime());
        List<ColumnDesc> schema = th.getSchema();
        switch (th.getFormatVersion()) {
            case 1:
            {
                if (option.getProjection().size() > 0) {
                    schema = new ArrayList<>();
                    for (String name : option.getProjection()) {
                        Integer idx = th.getSchemaPos().get(name);
                        if (idx == null) {
                            throw new TabletException("Cannot find column " + name);
                        }
                        builder.addProjection(idx);
                        schema.add(th.getSchema().get(idx));
                    }
                }
                Tablet.ScanRequest request = builder.build();
                Future<Tablet.ScanResponse> response = ts.scan(request, TableClientCommon.scanFakeCallback);
                return new ScanFuture(response, th, schema);
            }
            default:
            {
                Tablet.ScanRequest request = builder.build();
                Future<Tablet.ScanResponse> response = ts.scan(request, TableClientCommon.scanFakeCallback);
                if (option.getProjection().size() > 0) {
                    List<Integer> projectionIdx = new ArrayList<>();
                    int maxIdx = -1;
                    BitSet bitSet = new BitSet(schema.size());
                    for (String name : option.getProjection()) {
                        Integer idx = th.getSchemaPos().get(name);
                        if (idx == null) {
                            throw new TabletException("Cannot find column " + name);
                        }
                        bitSet.set(idx, true);
                        if (idx > maxIdx) {
                            maxIdx = idx;
                        }
                        projectionIdx.add(idx);
                    }
                    ProjectionInfo projectionInfo = new ProjectionInfo(projectionIdx, bitSet, maxIdx);
                    return new ScanFuture(response, th, projectionInfo);
                }
                return new ScanFuture(response, th);
            }

        }
    }
}
