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
import com._4paradigm.rtidb.client.schema.RowCodec;
import com._4paradigm.rtidb.tablet.Tablet;
import com._4paradigm.rtidb.tablet.Tablet.GetResponse;
import com._4paradigm.rtidb.tablet.Tablet.PutResponse;
import com._4paradigm.rtidb.tablet.Tablet.ScanResponse;
import com._4paradigm.utils.MurmurHash;
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
        for (int i = 0; i < tableHandler.getSchema().size(); i++) {
            if (tableHandler.getSchema().get(i).isAddTsIndex()) {
                if (row[i] == null) {
                    throw new TabletException("index " + index + "column is empty");
                }
                String value = row[i].toString();
                if (value.isEmpty()) {
                    throw new TabletException("index" + index + " column is empty");
                }
                Tablet.Dimension dim = Tablet.Dimension.newBuilder().setIdx(index).setKey(value).build();
                dimList.add(dim);
                index++;
            }
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
        for (int i = 0; i < th.getSchema().size(); i++) {
            if (th.getSchema().get(i).isAddTsIndex()) {
                if (row[i] == null) {
                    throw new TabletException("index " + index + "column is empty");
                }
                String value = row[i].toString();
                if (value.isEmpty()) {
                    throw new TabletException("index" + index + " column is empty");
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
            }
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
        return get(tid, pid, key, 0l);
    }

    @Override
    public GetFuture get(int tid, int pid, String key, long time) throws TabletException {
        TableHandler th = client.getHandler(tid);
        if (th == null) {
            throw new TabletException("no table with id " + tid);
        }
        return get(tid, pid, key, time, th);
    }

    @Override
    public ScanFuture scan(int tid, int pid, String key, long st, long et) throws TabletException {
        return scan(tid, pid, key, null, st, et);
    }

    @Override
    public ScanFuture scan(int tid, int pid, String key, String idxName, long st, long et) throws TabletException {
        TableHandler th = client.getHandler(tid);
        if (th == null) {
            throw new TabletException("no table with id " + tid);
        }
        return scan(tid, pid, key, idxName, st, et, th);
    }

    
    @Override
    public ScanFuture scan(String name, String key, String idxName, long st, long et) throws TabletException {
        TableHandler th = client.getHandler(name);
        if (th == null) {
            throw new TabletException("no table with name " + name);
        }
        int pid = (int) (MurmurHash.hash64(key) % th.getPartitions().length);
        if (pid < 0) {
            pid = pid * -1;
        }
        return scan(th.getTableInfo().getTid(), pid, key, idxName, st, et, th);
    }

    @Override
    public ScanFuture scan(String name, String key, long st, long et) throws TabletException {
        TableHandler th = client.getHandler(name);
        if (th == null) {
            throw new TabletException("no table with name " + name);
        }
        int pid = (int) (MurmurHash.hash64(key) % th.getPartitions().length);
        if (pid < 0) {
            pid = pid * -1;
        }
        return scan(th.getTableInfo().getTid(), pid, key, null, st, et, th);
    }

    @Override
    public GetFuture get(String name, String key, long time) throws TabletException {
        TableHandler th = client.getHandler(name);
        if (th == null) {
            throw new TabletException("no table with name " + name);
        }
        int pid = (int) (MurmurHash.hash64(key) % th.getPartitions().length);
        if (pid < 0) {
            pid = pid * -1;
        }
        return get(th.getTableInfo().getTid(), pid, key, time, th);
    }

    @Override
    public GetFuture get(String name, String key) throws TabletException {
        return get(name, key, 0l);
    }
    
    private PutFuture put(int tid, int pid, String key, long time, byte[] bytes, PartitionHandler ph){
        return put(tid, pid, key, time, null, ByteBuffer.wrap(bytes), ph);
    }
    
    private PutFuture put(int tid, int pid, 
            String key, long time, 
            List<Tablet.Dimension> ds, 
            ByteBuffer row, PartitionHandler ph) {
        long start = System.currentTimeMillis();
        Future<PutResponse> response = putForInternal(tid, pid, key, time, ds, row, ph);
        return PutFuture.wrapper(response, start, client.getConfig());
    }
    
    private Future<PutResponse> putForInternal(int tid, int pid, 
            String key, long time, 
            List<Tablet.Dimension> ds, 
            ByteBuffer row, PartitionHandler ph) {
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
    
    private GetFuture get(int tid, int pid, String key, long time, TableHandler th) {
        Tablet.GetRequest request = Tablet.GetRequest.newBuilder().setPid(pid).setTid(tid).setKey(key).setTs(time)
                .build();
        Long startTime = System.currentTimeMillis();
        Future<Tablet.GetResponse> response = th.getHandler(pid).getLeader().get(request, getFakeCallback);
        return GetFuture.wrappe(response, th, startTime, client.getConfig());
    }
    
    
    private ScanFuture scan(int tid, int pid, String key, String idxName, long st, long et, TableHandler th) {
        TabletServer tabletServer = th.getHandler(pid).getLeader();
        Tablet.ScanRequest.Builder builder = Tablet.ScanRequest.newBuilder();
        builder.setPk(key);
        builder.setTid(tid);
        builder.setEt(et);
        builder.setSt(st);
        builder.setPid(pid);
        if (idxName != null && !idxName.isEmpty()) {
            builder.setIdxName(idxName);
        }
        Tablet.ScanRequest request = builder.build();
        Long startTime = System.nanoTime();
        Future<Tablet.ScanResponse> response = tabletServer.scan(request, scanFakeCallback);
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

   
   
    

}
