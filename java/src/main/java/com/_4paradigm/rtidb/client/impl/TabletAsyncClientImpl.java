package com._4paradigm.rtidb.client.impl;

import java.nio.charset.Charset;
import java.util.List;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com._4paradigm.rtidb.client.GetFuture;
import com._4paradigm.rtidb.client.PutFuture;
import com._4paradigm.rtidb.client.ScanFuture;
import com._4paradigm.rtidb.client.TabletAsyncClient;
import com._4paradigm.rtidb.client.schema.ColumnDesc;
import com._4paradigm.rtidb.client.schema.SchemaCodec;
import com._4paradigm.rtidb.client.schema.Table;
import com.google.protobuf.ByteString;

import io.brpc.client.RpcCallback;
import io.brpc.client.RpcClient;
import io.brpc.client.RpcProxy;
import rtidb.api.Tablet;
import rtidb.api.Tablet.GetResponse;
import rtidb.api.Tablet.PutResponse;
import rtidb.api.Tablet.ScanResponse;
import rtidb.api.TabletServer;

public class TabletAsyncClientImpl implements TabletAsyncClient {
	private final static Logger logger = LoggerFactory.getLogger(TabletAsyncClientImpl.class);
	private TabletServer tablet;
	private RpcClient client;
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

	public TabletAsyncClientImpl(RpcClient rpcClient) {
		this.client = rpcClient;
	}

	public void init() {
		tablet = RpcProxy.getProxy(client, TabletServer.class);
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
		Tablet.GetRequest request = Tablet.GetRequest.newBuilder().setPid(pid).setTid(tid).setKey(key).setTs(time)
				.build();
		Future<Tablet.GetResponse> f = tablet.get(request, getFakeCallback);
		Table table = GTableSchema.getTable(tid, pid, tablet);
		long startTime = System.nanoTime();
		return GetFuture.wrappe(f, table, startTime);
	}

	@Override
	public ScanFuture scan(int tid, int pid, String key, long st, long et) {
		return scan(tid, pid, key, null, st, et);
	}

	@Override
	public ScanFuture scan(int tid, int pid, String key, String idxName, long st, long et) {
		Table table = GTableSchema.getTable(tid, pid, tablet);
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
		Future<Tablet.ScanResponse> f = tablet.scan(request, scanFakeCallback);
		return ScanFuture.wrappe(f, table);
	}



}
