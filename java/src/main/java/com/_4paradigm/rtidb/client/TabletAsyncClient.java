package com._4paradigm.rtidb.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com._4paradigm.pbrpc.AsyncConnection;
import com._4paradigm.pbrpc.AsyncRpcChannel;
import com._4paradigm.pbrpc.FakeRpcController;
import com._4paradigm.rtidb.Tablet;
import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;

public class TabletAsyncClient {
    private final static Logger logger = LoggerFactory.getLogger(TabletAsyncClient.class);
    private final static RpcController ctrl = new FakeRpcController();
    private AsyncConnection asyncConn;
    private AsyncRpcChannel channel;
    private Tablet.TabletServer.Interface iface;
    private String host;
    private int port;
    private int maxFrameLength;
    private int eventLoopThreadCnt;
    
    public TabletAsyncClient(String host, int port, int maxFrameLength,
            int eventLoopThreadCnt) {
        this.host = host;
        this.port = port;
        this.maxFrameLength = maxFrameLength;
        this.eventLoopThreadCnt = eventLoopThreadCnt;
    }

    public void init() throws InterruptedException {
        asyncConn = new AsyncConnection(host, port, maxFrameLength, eventLoopThreadCnt);
        asyncConn.connect();
        channel = new AsyncRpcChannel(asyncConn);
        iface = Tablet.TabletServer.newStub(channel);
    }

    public void put(int tid, String key, long time, byte[] bytes, RpcCallback<Tablet.PutResponse> done) {
        Tablet.PutRequest resquest = Tablet.PutRequest.newBuilder().setPk(key).setTid(tid).setTime(time)
                .setValue(ByteString.copyFrom(bytes)).build();
        iface.put(ctrl, resquest, done);
    }

    public void put(int tid, String key, long time, String value, RpcCallback<Tablet.PutResponse> done) {
        Tablet.PutRequest resquest = Tablet.PutRequest.newBuilder().setPk(key).setTid(tid).setTime(time)
                .setValue(ByteString.copyFrom(value.getBytes())).build();
        iface.put(ctrl, resquest, done);
    }

    public void createTable(String name, int tid, int pid, int ttl, RpcCallback<Tablet.CreateTableResponse> done) {
        Tablet.CreateTableRequest request = Tablet.CreateTableRequest.newBuilder().setName(name).setTid(tid).setPid(pid)
                .setTtl(ttl).build();
        iface.createTable(ctrl, request, done);
    }

    public void scan(int tid, String pk, long st, long et, RpcCallback<Tablet.ScanResponse> done) {
        Tablet.ScanRequest.Builder builder = Tablet.ScanRequest.newBuilder();
        builder.setPk(pk);
        builder.setTid(tid);
        builder.setEt(et);
        builder.setSt(st);
        Tablet.ScanRequest request = builder.build();
        iface.scan(ctrl, request, done);
    }
    
    public void dropTable(int tid, RpcCallback<Tablet.DropTableResponse> done) {
        Tablet.DropTableRequest.Builder builder = Tablet.DropTableRequest.newBuilder();
        builder.setTid(tid);
        Tablet.DropTableRequest request = builder.build();
        iface.dropTable(ctrl, request, done);
    }

    public void close() {
        this.asyncConn.close();
    }
}
