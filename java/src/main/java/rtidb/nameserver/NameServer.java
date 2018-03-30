package rtidb.nameserver;

import com._4paradigm.rtidb.ns.NS.ChangeLeaderRequest;
import com._4paradigm.rtidb.ns.NS.CreateTableRequest;
import com._4paradigm.rtidb.ns.NS.DropTableRequest;
import com._4paradigm.rtidb.ns.NS.GeneralResponse;
import com._4paradigm.rtidb.ns.NS.RecoverEndpointRequest;
import com._4paradigm.rtidb.ns.NS.ShowTableRequest;
import com._4paradigm.rtidb.ns.NS.ShowTableResponse;

public interface NameServer {
    GeneralResponse createTable(CreateTableRequest request);
    GeneralResponse dropTable(DropTableRequest request);
    ShowTableResponse showTable(ShowTableRequest request);
    GeneralResponse changeLeader(ChangeLeaderRequest request);
    GeneralResponse recoverEndpoint(RecoverEndpointRequest request);
}
