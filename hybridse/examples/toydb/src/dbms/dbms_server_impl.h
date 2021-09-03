/*
 * Copyright 2021 4Paradigm
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

#ifndef EXAMPLES_TOYDB_SRC_DBMS_DBMS_SERVER_IMPL_H_
#define EXAMPLES_TOYDB_SRC_DBMS_DBMS_SERVER_IMPL_H_

#include <map>
#include <mutex>  // NOLINT (build/c++11)
#include <set>
#include <string>
#include "proto/dbms.pb.h"
#include "proto/fe_type.pb.h"
#include "tablet/tablet_internal_sdk.h"

namespace hybridse {
namespace dbms {

using ::google::protobuf::Closure;
using ::google::protobuf::RpcController;

typedef std::map<std::string, ::hybridse::type::Group> Groups;
typedef std::map<std::string, ::hybridse::type::Database> Databases;
typedef std::map<std::string, ::hybridse::type::TableDef*> Tables;

class DBMSServerImpl : public DBMSServer {
 public:
    DBMSServerImpl();
    ~DBMSServerImpl();

    void AddDatabase(RpcController* ctr, const AddDatabaseRequest* request,
                     AddDatabaseResponse* response, Closure* done) override;

    void AddTable(RpcController* ctr, const AddTableRequest* request,
                  AddTableResponse* response, Closure* done);

    void GetSchema(RpcController* controller, const GetSchemaRequest* request,
                   GetSchemaResponse* response, Closure* done);

    void GetDatabases(RpcController* controller,
                      const GetDatabasesRequest* request,
                      GetDatabasesResponse* response, Closure* done);

    void GetTables(RpcController* controller, const GetTablesRequest* request,
                   GetTablesResponse* response, Closure* done);

    void KeepAlive(RpcController* controller, const KeepAliveRequest* request,
                   KeepAliveResponse* response, Closure* done);

    void GetTablet(RpcController* ctrl, const GetTabletRequest* request,
                   GetTabletResponse* response, Closure* done);

 private:
    std::mutex mu_;
    Groups groups_;
    Databases databases_;
    hybridse::tablet::TabletInternalSDK* tablet_sdk;
    int32_t tid_;
    std::set<std::string> tablets_;
    void InitTable(type::Database* db,
                   Tables& table);  // NOLINT (runtime/references)
    type::Database* GetDatabase(
        const std::string db_name,
        common::Status& status);  // NOLINT (runtime/references)
};

}  // namespace dbms
}  // namespace hybridse
#endif  // EXAMPLES_TOYDB_SRC_DBMS_DBMS_SERVER_IMPL_H_
