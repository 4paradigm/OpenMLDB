/*
 * tablet_server_impl.h
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

#ifndef SRC_TABLET_TABLET_SERVER_IMPL_H_
#define SRC_TABLET_TABLET_SERVER_IMPL_H_

#include <map>
#include <memory>
#include <string>
#include "base/spin_lock.h"
#include "brpc/server.h"
#include "proto/tablet.pb.h"
#include "vm/engine.h"
#include "vm/table_mgr.h"

namespace fesql {
namespace tablet {

using ::google::protobuf::Closure;
using ::google::protobuf::RpcController;

// TODO(wtx): opt db tid pid structure

typedef std::map<uint32_t, std::shared_ptr<vm::TableStatus>> Partition;

typedef std::map<uint32_t, std::map<uint32_t, std::shared_ptr<vm::TableStatus>>>
    Table;

typedef std::map<
    std::string,
    std::map<uint32_t, std::map<uint32_t, std::shared_ptr<vm::TableStatus>>>>
    Tables;

typedef std::map<std::string, std::map<std::string, uint32_t>> TableNames;

class TabletServerImpl : public TabletServer, public vm::TableMgr {

 public:
    TabletServerImpl();
    ~TabletServerImpl();

    bool Init();

    void CreateTable(RpcController* ctrl, const CreateTableRequest* request,
                     CreateTableResponse* response, Closure* done);

    void Query(RpcController* ctrl, const QueryRequest* request,
               QueryResponse* response, Closure* done);

    void Insert(RpcController* ctrl, const InsertRequest* request,
                InsertResponse* response, Closure* done);

    void GetTableSchema(RpcController* ctrl,
                        const GetTablesSchemaRequest* request,
                        GetTableSchemaReponse* response, Closure* done);

    std::shared_ptr<vm::TableStatus> GetTableDef(const std::string& db,
                                                 const std::string& name);

    std::shared_ptr<vm::TableStatus> GetTableDef(const std::string& db,
                                                 const uint32_t tid);

 private:

    inline std::shared_ptr<vm::TableStatus> GetTableLocked(
        const std::string& db, uint32_t tid, uint32_t pid);

    std::shared_ptr<vm::TableStatus> GetTableDefUnLocked(const std::string& db,
                                                         uint32_t tid);

    std::shared_ptr<vm::TableStatus> GetTableUnLocked(const std::string& db,
                                                      uint32_t tid,
                                                      uint32_t pid);

    bool AddTableUnLocked(std::shared_ptr<vm::TableStatus>&
                              table);  // NOLINT (runtime/references)

    inline bool AddTableLocked(std::shared_ptr<vm::TableStatus>&
                                   table);  // NOLINT (runtime/references)

 private:
    base::SpinMutex slock_;
    Tables tables_;
    TableNames table_names_;
    std::unique_ptr<vm::Engine> engine_;
};

}  // namespace tablet
}  // namespace fesql
#endif  // SRC_TABLET_TABLET_SERVER_IMPL_H_
