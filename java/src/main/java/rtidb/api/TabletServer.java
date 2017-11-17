package rtidb.api;

import java.util.concurrent.Future;

import io.brpc.client.RpcCallback;

public interface TabletServer {

	Tablet.PutResponse put(Tablet.PutRequest request);
	Tablet.GetResponse get(Tablet.GetRequest request);
	Tablet.ScanResponse scan(Tablet.ScanRequest request);
	Tablet.CreateTableResponse createTable(Tablet.CreateTableRequest request);
	Tablet.DropTableResponse dropTable(Tablet.DropTableRequest request);
	Tablet.GetTableSchemaResponse getTableSchema(Tablet.GetTableSchemaRequest request);
	Future<Tablet.PutResponse> put(Tablet.PutRequest request, RpcCallback<Tablet.PutResponse> callback);
	Future<Tablet.GetResponse> get(Tablet.GetRequest request, RpcCallback<Tablet.GetResponse> callback);
	Future<Tablet.ScanResponse> scan(Tablet.ScanRequest request, RpcCallback<Tablet.ScanResponse> callback);
}
