/*
 * Copyright 2021 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com._4paradigm.openmldb.importer;

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

    // TODO(hw):     @BrpcMeta(serviceName = "example.EchoService", methodName = "Echo")
    //    Future<EchoResponse> echo(EchoRequest request, RpcCallback<EchoResponse> callback);
    //  or need another interface. Using the same RpcClient is ok.
}
