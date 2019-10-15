package rtidb.nameserver;

import com._4paradigm.rtidb.ns.NS.*;

public interface NameServer {
    GeneralResponse createTable(CreateTableRequest request);
    GeneralResponse dropTable(DropTableRequest request);
    ShowTableResponse showTable(ShowTableRequest request);
    ShowTabletResponse showTablet(ShowTabletRequest request);
    GeneralResponse changeLeader(ChangeLeaderRequest request);
    GeneralResponse recoverEndpoint(RecoverEndpointRequest request);

    GeneralResponse AddTableField(AddTableFieldRequest request);
}
