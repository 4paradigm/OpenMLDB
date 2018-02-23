package rtidb.nameserver;

import com._4paradigm.rtidb.ns.NS.CreateTableRequest;
import com._4paradigm.rtidb.ns.NS.DropTableRequest;
import com._4paradigm.rtidb.ns.NS.GeneralResponse;

public interface NameServer {
    GeneralResponse createTable(CreateTableRequest request);
    GeneralResponse dropTable(DropTableRequest request);
}
