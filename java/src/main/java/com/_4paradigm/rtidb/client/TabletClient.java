package com._4paradigm.rtidb.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com._4paradigm.pbrpc.AsyncConnection;
import com._4paradigm.pbrpc.FakeRpcController;
import com._4paradigm.pbrpc.SyncRpcChannel;
import com._4paradigm.rtidb.Tablet;
import com.google.protobuf.ByteString;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

public class TabletClient {
    private final static Logger logger = LoggerFactory.getLogger(TabletClient.class);
    private final static RpcController ctrl = new FakeRpcController();
    private AsyncConnection asyncConn;
    private SyncRpcChannel channel;
    private Tablet.TabletServer.BlockingInterface iface;
    private String host;
    private int port;

    public TabletClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public void init() throws InterruptedException {
        asyncConn = new AsyncConnection(host, port);
        asyncConn.connect();
        channel = new SyncRpcChannel(asyncConn);
        iface = Tablet.TabletServer.newBlockingStub(channel);
    }

    public boolean put(int tid, String key, long time, byte[] bytes) {
        Tablet.PutRequest resquest = Tablet.PutRequest.newBuilder().setPk(key).setTid(tid).setTime(time)
                .setValue(ByteString.copyFrom(bytes)).build();
        try {

            Tablet.PutResponse response = iface.put(ctrl, resquest);
            if (response.getCode() == 0) {
                return true;
            }
            return false;
        } catch (ServiceException e) {
            logger.error("fail call put", e);
        }
        return false;
    }

    public boolean put(int tid, String key, long time, String value) {
        Tablet.PutRequest resquest = Tablet.PutRequest.newBuilder().setPk(key).setTid(tid).setTime(time)
                .setValue(ByteString.copyFrom(value.getBytes())).build();
        try {
            Tablet.PutResponse response = iface.put(ctrl, resquest);
            if (response.getCode() == 0) {
                return true;
            }
            return false;
        } catch (ServiceException e) {
            logger.error("fail call put", e);
        }
        return false;
    }

    public boolean createTable(String name, int tid, int pid, int ttl) {
        Tablet.CreateTableRequest request = Tablet.CreateTableRequest.newBuilder().setName(name).setTid(tid).setPid(pid)
                .setTtl(ttl).build();
        try {
            Tablet.CreateTableResponse response = iface.createTable(ctrl, request);
            if (response.getCode() == 0) {
                return true;
            }
        } catch (ServiceException e) {
            logger.error("fail to call create", e);
        }
        return false;
    }

    public KvIterator scan(int tid, String pk, long st, long et) {
        Tablet.ScanRequest.Builder builder = Tablet.ScanRequest.newBuilder();
        builder.setPk(pk);
        builder.setTid(tid);
        builder.setEt(et);
        builder.setSt(st);
        Tablet.ScanRequest request = builder.build();
        try {
            Tablet.ScanResponse response = iface.scan(ctrl, request);
            if (response.getCode() == 0) {
                return new KvIterator(response.getPairs());
            }
            return null;
        } catch (ServiceException e) {
            logger.error("fail to scan tablet {}", tid, e);
        }
        return null;
    }

    public void close() {
        this.asyncConn.close();
    }
}
