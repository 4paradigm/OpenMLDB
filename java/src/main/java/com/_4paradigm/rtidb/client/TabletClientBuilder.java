package com._4paradigm.rtidb.client;

import com._4paradigm.rtidb.client.impl.TabletAsyncClientImpl;
import com._4paradigm.rtidb.client.impl.TabletSyncClientImpl;

import io.brpc.client.EndPoint;
import io.brpc.client.RpcClient;
import io.brpc.client.RpcClientOptions;
import io.brpc.protocol.Options;

public class TabletClientBuilder {

	public static RpcClient buildRpcClient(String ip, int port, int timeout, int ioThreadNum) {
		RpcClientOptions options = new RpcClientOptions();
		options.setIoThreadNum(ioThreadNum);
		options.setMaxTryTimes(1);
		options.setProtocolType(Options.ProtocolType.PROTOCOL_BAIDU_STD_VALUE);
		options.setReadTimeoutMillis(timeout);
		EndPoint endpoint = new EndPoint(ip, port);
		RpcClient client = new RpcClient(endpoint, options);
		return client;
	}
	
	public static TabletAsyncClient buildAsyncClient(RpcClient client) {
		TabletAsyncClientImpl asyncClient = new TabletAsyncClientImpl(client);
		asyncClient.init();
		return asyncClient;
	}
	
	public static TabletSyncClient buildSyncClient(RpcClient client) {
		TabletSyncClientImpl syncClient = new TabletSyncClientImpl(client);
		syncClient.init();
		return syncClient;
	}
	
}
