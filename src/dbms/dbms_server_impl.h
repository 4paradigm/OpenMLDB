/*
 * dbms_server_impl.h
 * Copyright (C) 4paradigm.com 2019 wangtaize <wangtaize@4paradigm.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef SRC_DBMS_DBMS_SERVER_IMPL_H_
#define SRC_DBMS_DBMS_SERVER_IMPL_H_

#include <map>
#include <mutex> // NOLINT (build/c++11)
#include <string>
#include "proto/dbms.pb.h"
#include "proto/type.pb.h"
#include "tablet/tablet_internal_sdk.h"

namespace fesql {
namespace dbms {

using ::google::protobuf::Closure;
using ::google::protobuf::RpcController;

typedef std::map<std::string, ::fesql::type::Group> Groups;
typedef std::map<std::string, ::fesql::type::Database> Databases;
typedef std::map<std::string, ::fesql::type::TableDef*> Tables;

class DBMSServerImpl : public DBMSServer {
 public:
    DBMSServerImpl();
    ~DBMSServerImpl();

    void AddGroup(RpcController* ctr, const AddGroupRequest* request,
                  AddGroupResponse* response, Closure* done);

    void AddDatabase(RpcController* ctr, const AddDatabaseRequest* request,
                     AddDatabaseResponse* response, Closure* done) override;

    void IsExistDatabase(RpcController* ctr, const IsExistRequest* request,
                         IsExistResponse* response, Closure* done);
    void AddTable(RpcController* ctr, const AddTableRequest* request,
                  AddTableResponse* response, Closure* done);

    void GetSchema(RpcController* controller, const GetSchemaRequest* request,
                   GetSchemaResponse* response, Closure* done);
    void GetDatabases(RpcController* controller, const GetItemsRequest* request,
                      GetItemsResponse* response, Closure* done);
    void GetTables(RpcController* controller, const GetItemsRequest* request,
                   GetItemsResponse* response, Closure* done);

    void SetTabletEndpoint(const std::string& endpoint) {
        tablet_endpoint_ = endpoint;
    }

 private:
    std::mutex mu_;
    Groups groups_;
    Databases databases_;
    std::string tablet_endpoint_;
    fesql::tablet::TabletInternalSDK* tablet_sdk;
    int32_t tid_;
    void InitTable(type::Database* db,
                   Tables& table);  // NOLINT (runtime/references)
    type::Database* GetDatabase(
        const std::string db_name,
        common::Status& status);  // NOLINT (runtime/references)
};

}  // namespace dbms
}  // namespace fesql
#endif  // SRC_DBMS_DBMS_SERVER_IMPL_H_
