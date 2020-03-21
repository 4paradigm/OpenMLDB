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
#include "proto/dbms.pb.h"
#include "tablet/tablet_catalog.h"
#include "vm/engine.h"
#include "brpc/channel.h"

namespace fesql {
namespace tablet {

using ::google::protobuf::Closure;
using ::google::protobuf::RpcController;

class TabletServerImpl : public TabletServer {
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

 private:
    void KeepAlive();
    inline std::shared_ptr<TabletTableHandler> GetTableLocked(
        const std::string& db, const std::string& name) {
        std::lock_guard<base::SpinMutex> lock(slock_);
        return GetTableUnLocked(db, name);
    }

    inline std::shared_ptr<TabletTableHandler> GetTableUnLocked(
        const std::string& db, const std::string& name) {
        return std::static_pointer_cast<TabletTableHandler>(
            catalog_->GetTable(db, name));
    }

    inline bool AddTableUnLocked(std::shared_ptr<storage::Table>
                                     table) { 
        const type::TableDef& table_def = table->GetTableDef();
        std::shared_ptr<TabletTableHandler> handler(new TabletTableHandler(
            table_def.columns(), table_def.name(), table_def.catalog(),
            table_def.indexes(), table));
        bool ok = handler->Init();
        if (!ok) {
            return false;
        }
        return catalog_->AddTable(handler);
    }

    inline bool AddTableLocked(std::shared_ptr<storage::Table>
                                   table) { 
        std::lock_guard<base::SpinMutex> lock(slock_);
        return AddTableUnLocked(table);
    }

 private:
    base::SpinMutex slock_;
    std::unique_ptr<vm::Engine> engine_;
    std::shared_ptr<TabletCatalog> catalog_;
    brpc::Channel* dbms_ch_;

};

}  // namespace tablet
}  // namespace fesql
#endif  // SRC_TABLET_TABLET_SERVER_IMPL_H_
