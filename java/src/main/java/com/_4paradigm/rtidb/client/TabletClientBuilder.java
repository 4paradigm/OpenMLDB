package com._4paradigm.rtidb.client;

import com._4paradigm.rtidb.client.impl.TabletAsyncClientImpl;
import com._4paradigm.rtidb.client.impl.TabletSyncClientImpl;

import io.brpc.client.EndPoint;
import io.brpc.client.RpcClient;
import io.brpc.client.RpcClientOptions;
import io.brpc.client.RpcProxy;
import io.brpc.protocol.Options;
import rtidb.api.TabletServer;

public class TabletClientBuilder {

	public static RpcClient buildRpcClient(String ip, int port, int timeout, int ioThreadNum) {
		RpcClientOptions options = new RpcClientOptions();
		options.setIoThreadNum(ioThreadNum);
		options.setMaxTryTimes(1);
		options.setProtocolType(Options.ProtocolType.PROTOCOL_SOFA_PBRPC_VALUE);
		options.setReadTimeoutMillis(timeout);
		EndPoint endpoint = new EndPoint(ip, port);
		RpcClient client = new RpcClient(endpoint, options);
		return client;
	}
	
	public static TabletAsyncClient buildAsyncClient(RpcClient client) {
		TabletServer ts = RpcProxy.getProxy(client, TabletServer.class);
		TabletAsyncClient asyncClient = new TabletAsyncClientImpl(ts);
		return asyncClient;
	}
	
	public static TabletSyncClient buildSyncClient(RpcClient client) {
		TabletServer ts = RpcProxy.getProxy(client, TabletServer.class);
		TabletSyncClient syncClient = new TabletSyncClientImpl(ts);
		return syncClient;
	}
	
}
