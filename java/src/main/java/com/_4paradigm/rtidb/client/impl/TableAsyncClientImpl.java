package com._4paradigm.rtidb.client.impl;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
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
import com._4paradigm.rtidb.client.ha.TableHandler;
import com._4paradigm.rtidb.client.schema.ColumnDesc;
import com._4paradigm.rtidb.client.schema.RowCodec;
import com._4paradigm.rtidb.tablet.Tablet;
import com._4paradigm.rtidb.tablet.Tablet.GetResponse;
import com._4paradigm.rtidb.tablet.Tablet.PutResponse;
import com._4paradigm.rtidb.tablet.Tablet.ScanResponse;
import com._4paradigm.rtidb.utils.MurmurHash;
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
        ByteBuffer buffer = RowCodec.encode(row, tableHandler.getSchema());
        List<Tablet.Dimension> dimList = new ArrayList<Tablet.Dimension>();
        int index = 0;
        int count = 0;
        for (int i = 0; i < tableHandler.getSchema().size(); i++) {
            if (tableHandler.getSchema().get(i).isAddTsIndex()) {
                if (row[i] == null) {
                    index ++;
                    continue;
                }
                String value = row[i].toString();
                if (value.isEmpty()) {
                    index ++;
                    continue;
                }
                Tablet.Dimension dim = Tablet.Dimension.newBuilder().setIdx(index).setKey(value).build();
                dimList.add(dim);
                index ++;
                count ++;
            }
        }
        if (count == 0) {
            throw new TabletException("no dimension in this row");
        }
        return put(tid, pid, null, time, dimList, buffer, tableHandler.getHandler(pid));
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
        PartitionHandler ph = th.getHandler(pid);
        return put(tid, pid, key, time, bytes, ph);
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
        int pid = (int) (MurmurHash.hash64(key) % th.getPartitions().length);
        if (pid < 0) {
            pid = pid * -1;
        }
        return put(th.getTableInfo().getTid(), pid, key, time, bytes, th.getHandler(pid));
    }
    
    @Override
    public PutFuture put(String name, long time, Object[] row) throws TabletException {
        TableHandler th = client.getHandler(name);
        if (th == null) {
            throw new TabletException("no table with name " + name);
        }
        ByteBuffer buffer = RowCodec.encode(row, th.getSchema());
        Map<Integer, List<Tablet.Dimension>> mapping = new HashMap<Integer, List<Tablet.Dimension>>();
        int index = 0;
        int count = 0;
        for (int i = 0; i < th.getSchema().size(); i++) {
            if (th.getSchema().get(i).isAddTsIndex()) {
                if (row[i] == null) {
                    index++;
                    continue;
                }
                String value = row[i].toString();
                if (value.isEmpty()) {
                    index++;
                    continue;
                }
                int pid = (int) (MurmurHash.hash64(value) % th.getPartitions().length);
                if (pid < 0) {
                    pid = pid * -1;
                }
                Tablet.Dimension dim = Tablet.Dimension.newBuilder().setIdx(index).setKey(value).build();
                List<Tablet.Dimension> dimList = mapping.get(pid);
                if (dimList == null) {
                    dimList = new ArrayList<Tablet.Dimension>();
                    mapping.put(pid, dimList);
                }
                dimList.add(dim);
                index++;
                count++;
            }
        }
        if (count == 0) {
            throw new TabletException("no dimension in this row for table name " + name);
        }
        List<Future<PutResponse>> pl = new ArrayList<Future<PutResponse>>();
        Iterator<Map.Entry<Integer, List<Tablet.Dimension>>> it = mapping.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Integer, List<Tablet.Dimension>> entry = it.next();
            Future<PutResponse> response = putForInternal(th.getTableInfo().getTid(), entry.getKey(), null, 
                             time, entry.getValue(), buffer, 
                             th.getHandler(entry.getKey()));
            pl.add(response);
        }
        return PutFuture.wrapper(pl);
    }
    
    @Override
    public PutFuture put(String name, String key, long time, String value) throws TabletException {
        return put(name, key, time, value.getBytes(Charsets.UTF_8));
    }

    @Override
    public GetFuture get(int tid, int pid, String key) throws TabletException {
        return get(tid, pid, key, null,0l);
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
        return get(tid, pid, key, idxName, time,null,  th);
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
        return scan(tid, pid, key, idxName, st, et, limit, th);
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
        TableHandler th = client.getHandler(name);
        if (th == null) {
            throw new TabletException("no table with name " + name);
        }
        int pid = (int) (MurmurHash.hash64(key) % th.getPartitions().length);
        if (pid < 0) {
            pid = pid * -1;
        }
        return scan(th.getTableInfo().getTid(), pid, key, idxName, st, et, limit, th);
    }

    @Override
    public ScanFuture scan(String name, String key, long st, long et) throws TabletException {
        return scan(name, key, null, st, et, 0);
    }

    @Override
    public ScanFuture scan(String name, String key, int limit) throws TabletException {
        return scan(name, key, null, 0, 0, limit);
    }

    @Override
    public ScanFuture scan(String name, String key, long st, long et, int limit) throws TabletException {
        return scan(name, key, null, st, et, limit);
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
        int pid = (int) (MurmurHash.hash64(key) % th.getPartitions().length);
        if (pid < 0) {
            pid = pid * -1;
        }
        return get(th.getTableInfo().getTid(), pid, key, idxName, time, null, th);
    }

    @Override
    public GetFuture get(String name, String key) throws TabletException {
        return get(name, key, null, 0l);
    }

    @Override
    public GetFuture get(String name, String key, String idxName) throws TabletException {
        return get(name, key, idxName, 0l);
    }

    private PutFuture put(int tid, int pid, String key, long time, byte[] bytes, PartitionHandler ph) throws TabletException{
        return put(tid, pid, key, time, null, ByteBuffer.wrap(bytes), ph);
    }
    
    private PutFuture put(int tid, int pid, 
            String key, long time, 
            List<Tablet.Dimension> ds, 
            ByteBuffer row, PartitionHandler ph) throws TabletException {
        if ((ds == null || ds.isEmpty()) && (key == null || key.isEmpty())) {
            throw new TabletException("key is null or empty");
        }
        long start = System.currentTimeMillis();
        Future<PutResponse> response = putForInternal(tid, pid, key, time, ds, row, ph);
        return PutFuture.wrapper(response, start, client.getConfig());
    }
    
    private Future<PutResponse> putForInternal(int tid, int pid, 
            String key, long time, 
            List<Tablet.Dimension> ds, 
            ByteBuffer row, PartitionHandler ph) throws TabletException {
        if ((ds == null || ds.isEmpty()) && (key == null || key.isEmpty())) {
            throw new TabletException("key is null or empty");
        }
        TabletServer tablet = ph.getLeader();
        Tablet.PutRequest.Builder builder = Tablet.PutRequest.newBuilder();
        if (ds != null) {
            for (Tablet.Dimension dim : ds) {
                builder.addDimensions(dim);
            }
        }
        builder.setPid(pid);
        builder.setTid(tid);
        builder.setTime(time);
        if (key != null) {
            builder.setPk(key);
        }
        row.rewind();
        builder.setValue(ByteBufferNoCopy.wrap(row.asReadOnlyBuffer()));
        Tablet.PutRequest request = builder.build();
        Future<PutResponse> response = tablet.put(request, putFakeCallback);
        return response;
    }
    
    private GetFuture get(int tid, int pid, String key, String idxName, long time, Tablet.GetType  type, TableHandler th) throws TabletException {
        if (key == null || key.isEmpty()) {
            throw new TabletException("key is null or empty");
        }
        Tablet.GetRequest.Builder builder = Tablet.GetRequest.newBuilder();
        builder.setTid(tid);
        builder.setPid(pid);
        builder.setKey(key);
        builder.setTs(time);
        if (type != null)  builder.setType(type);
        if (idxName != null && !idxName.isEmpty()) {
            builder.setIdxName(idxName);
        }
        Tablet.GetRequest request = builder.build();
        Long startTime = System.currentTimeMillis();
        PartitionHandler ph = th.getHandler(pid);
        TabletServer ts = ph.getReadHandler(th.getReadStrategy());
        Future<Tablet.GetResponse> response = ts.get(request, getFakeCallback);
        return GetFuture.wrappe(response, th, startTime, client.getConfig());
    }
    
    
    private ScanFuture scan(int tid, int pid, String key, String idxName, long st, long et, int limit, TableHandler th) throws TabletException {
        if (key == null || key.isEmpty()) {
            throw new TabletException("key is null or empty");
        }
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
        if (client.getConfig().isRemoveDuplicateByTime()) {
            builder.setEnableRemoveDuplicatedRecord(true);
        }
        Tablet.ScanRequest request = builder.build();
        Long startTime = System.nanoTime();
        PartitionHandler ph = th.getHandler(pid);
        TabletServer ts = ph.getReadHandler(th.getReadStrategy());
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
        TableHandler th = client.getHandler(name);
        if (th == null) {
            throw new TabletException("no table with name " + name);
        }
        int pid = (int) (MurmurHash.hash64(key) % th.getPartitions().length);
        if (pid < 0) {
            pid = pid * -1;
        }
        return get(th.getTableInfo().getTid(), pid, key, idxName, time, type, th);
    }
}
