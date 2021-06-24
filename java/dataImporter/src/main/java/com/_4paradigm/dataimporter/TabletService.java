package com._4paradigm.dataimporter;


import com._4paradigm.openmldb.api.Tablet;
import com.baidu.brpc.protocol.BrpcMeta;

public interface TabletService {
    // c++ serviceName doesn't contain the package name.
    @BrpcMeta(serviceName = "TabletServer", methodName = "GetTableStatus")
    Tablet.GetTableStatusResponse getTableStatus(Tablet.GetTableStatusRequest request);

    @BrpcMeta(serviceName = "TabletServer", methodName = "GetBulkLoadInfo")
    Tablet.BulkLoadInfoResponse getBulkLoadInfo(Tablet.BulkLoadInfoRequest request);

    @BrpcMeta(serviceName = "TabletServer", methodName = "BulkLoad")
    Tablet.GeneralResponse bulkLoad(Tablet.BulkLoadRequest request);
}
