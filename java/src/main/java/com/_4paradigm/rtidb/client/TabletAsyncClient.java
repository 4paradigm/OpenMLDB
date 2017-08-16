package com._4paradigm.rtidb.client;

import com._4paradigm.rtidb.Tablet;
import com.google.protobuf.RpcCallback;

public interface TabletAsyncClient {

    void put(int tid, int pid, String key, long time, byte[] bytes, RpcCallback<Tablet.PutResponse> done);
    void put(int tid,  int pid,String key, long time, String value, RpcCallback<Tablet.PutResponse> done);
}
