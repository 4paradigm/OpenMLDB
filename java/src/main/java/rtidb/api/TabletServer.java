package rtidb.api;

import java.util.concurrent.Future;

import com._4paradigm.rtidb.Tablet;
import com._4paradigm.rtidb.Tablet.CreateTableRequest;
import com._4paradigm.rtidb.Tablet.CreateTableResponse;
import com._4paradigm.rtidb.Tablet.DropTableRequest;
import com._4paradigm.rtidb.Tablet.DropTableResponse;
import com._4paradigm.rtidb.Tablet.GetRequest;
import com._4paradigm.rtidb.Tablet.GetResponse;
import com._4paradigm.rtidb.Tablet.PutRequest;
import com._4paradigm.rtidb.Tablet.PutResponse;
import com._4paradigm.rtidb.Tablet.ScanRequest;
import com._4paradigm.rtidb.Tablet.ScanResponse;

import io.brpc.client.RpcCallback;

public interface TabletServer {

	Tablet.PutResponse put(Tablet.PutRequest request);
	Tablet.GetResponse get(Tablet.GetRequest request);
	Tablet.ScanResponse scan(Tablet.ScanRequest request);
	Tablet.CreateTableResponse createTable(Tablet.CreateTableRequest request);
	Tablet.DropTableResponse dropTable(Tablet.DropTableRequest request);
	
	Future<Tablet.PutResponse> put(Tablet.PutRequest request, RpcCallback<Tablet.PutResponse> callback);
	Future<Tablet.GetResponse> get(Tablet.GetRequest request, RpcCallback<Tablet.GetResponse> callback);
	Future<Tablet.ScanResponse> scan(Tablet.ScanRequest request, RpcCallback<Tablet.ScanResponse> callback);
}
