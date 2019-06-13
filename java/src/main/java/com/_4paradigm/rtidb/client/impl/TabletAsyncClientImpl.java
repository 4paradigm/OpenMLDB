package com._4paradigm.rtidb.client.impl;

import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com._4paradigm.rtidb.client.GetFuture;
import com._4paradigm.rtidb.client.PutFuture;
import com._4paradigm.rtidb.client.ScanFuture;
import com._4paradigm.rtidb.client.TabletAsyncClient;
import com._4paradigm.rtidb.client.ha.PartitionHandler;
import com._4paradigm.rtidb.client.ha.RTIDBClient;
import com._4paradigm.rtidb.client.ha.TableHandler;
import com._4paradigm.rtidb.client.schema.Table;
import com._4paradigm.rtidb.tablet.Tablet;
import com._4paradigm.rtidb.tablet.Tablet.GetResponse;
import com._4paradigm.rtidb.tablet.Tablet.PutResponse;
import com._4paradigm.rtidb.tablet.Tablet.ScanResponse;
import com.google.common.base.Charsets;
import com.google.protobuf.ByteString;

import io.brpc.client.RpcCallback;

public class TabletAsyncClientImpl implements TabletAsyncClient {
    private final static Logger logger = LoggerFactory.getLogger(TabletAsyncClientImpl.class);
    private RTIDBClient client;
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

    public TabletAsyncClientImpl(RTIDBClient client) {
        this.client = client;
    }

    @Override
    public PutFuture put(int tid, int pid, String key, long time, byte[] bytes) {
        PartitionHandler ph = client.getHandler(tid).getHandler(pid);
        return put(tid, pid, key, time, bytes, ph);
    }

    @Override
    public PutFuture put(int tid, int pid, String key, long time, String value) {
        PartitionHandler ph = client.getHandler(tid).getHandler(pid);
        return put(tid, pid, key, time, value.getBytes(Charsets.UTF_8), ph);
    }

    private PutFuture put(int tid, int pid, String key, long time, byte[] bytes, PartitionHandler ph) {
        Tablet.PutRequest request = Tablet.PutRequest.newBuilder().setPid(pid).setPk(key).setTid(tid).setTime(time)
                .setValue(ByteString.copyFrom(bytes)).build();
        Future<Tablet.PutResponse> f = ph.getLeader().put(request, putFakeCallback);
        return PutFuture.wrapper(f);
    }

    @Override
    public GetFuture get(int tid, int pid, String key) {
        return get(tid, pid, key, 0l);
    }

    @Override
    public GetFuture get(int tid, int pid, String key, long time) {
        TableHandler th = client.getHandler(tid);
        return get(tid, pid, key, time, th);
    }

    private GetFuture get(int tid, int pid, String key, long time, TableHandler th) {
        Tablet.GetRequest request = Tablet.GetRequest.newBuilder().setPid(pid).setTid(tid).setKey(key).setTs(time)
                .build();
        // TODO add read strategy
        long startTime = System.nanoTime();
        Future<Tablet.GetResponse> f = th.getHandler(pid).getLeader().get(request, getFakeCallback);
        return GetFuture.wrappe(f, th, client.getConfig());
    }

    @Override
    public ScanFuture scan(int tid, int pid, String key, long st, long et) {
        return scan(tid, pid, key, null, st, et);
    }

    @Override
    public ScanFuture scan(int tid, int pid, String key, String idxName, long st, long et) {
        TableHandler th = client.getHandler(tid);
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
        Future<Tablet.ScanResponse> f = th.getHandler(pid).getLeader().scan(request, scanFakeCallback);
        return ScanFuture.wrappe(f, th);
    }

    @Override
    public PutFuture put(String name, String key, long time, byte[] bytes) {
        // TODO Auto-generated method stub
        return null;
    }

}
