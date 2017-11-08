package com._4paradigm.rtidb.client.impl;

import java.nio.charset.Charset;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com._4paradigm.rtidb.client.GetFuture;
import com._4paradigm.rtidb.client.PutFuture;
import com._4paradigm.rtidb.client.ScanFuture;
import com._4paradigm.rtidb.client.TabletAsyncClient;
import com.google.protobuf.ByteString;

import io.brpc.client.RpcCallback;
import rtidb.api.Tablet;
import rtidb.api.Tablet.GetResponse;
import rtidb.api.Tablet.PutResponse;
import rtidb.api.Tablet.ScanResponse;
import rtidb.api.TabletServer;

public class TabletAsyncClientImpl implements TabletAsyncClient {
    private final static Logger logger = LoggerFactory.getLogger(TabletAsyncClientImpl.class);
    private TabletServer tablet;
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
    
    public TabletAsyncClientImpl(TabletServer tablet) {
        this.tablet = tablet;
    }

	@Override
	public PutFuture put(int tid, int pid, String key, long time, byte[] bytes) {
		Tablet.PutRequest request = Tablet.PutRequest.newBuilder().setPid(pid).setPk(key).setTid(tid).setTime(time)
                .setValue(ByteString.copyFrom(bytes)).build();
		Future<Tablet.PutResponse> f = tablet.put(request, putFakeCallback);
		return PutFuture.wrapper(f);
	}

	@Override
	public PutFuture put(int tid, int pid, String key, long time, String value) {
		return put(tid, pid, key, time, value.getBytes(Charset.forName("utf-8")));
	}

	@Override
	public GetFuture get(int tid, int pid, String key) {
		return get(tid, pid, key, 0l);
	}

	@Override
	public GetFuture get(int tid, int pid, String key, long time) {
		Tablet.GetRequest request = Tablet.GetRequest.newBuilder().setPid(pid).setTid(tid).setKey(key).setTs(time).build();
		Future<Tablet.GetResponse> f = tablet.get(request, getFakeCallback);
		return GetFuture.wrappe(f);
	}

	@Override
	public ScanFuture scan(int tid, int pid, String key, long st, long et) {
		Tablet.ScanRequest.Builder builder = Tablet.ScanRequest.newBuilder();
        builder.setPk(key);
        builder.setTid(tid);
        builder.setEt(et);
        builder.setSt(st);
        builder.setPid(pid);
        Tablet.ScanRequest request = builder.build();
        Future<Tablet.ScanResponse> f =  tablet.scan(request, scanFakeCallback);
		return ScanFuture.wrappe(f);
	}

	
    
}
