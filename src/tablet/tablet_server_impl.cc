/*
 * tablet_server_impl.cc
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

#include "tablet/tablet_server_impl.h"

#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "base/strings.h"

namespace fesql {
namespace tablet {

TabletServerImpl::TabletServerImpl() : slock_(), engine_(), catalog_() {}

TabletServerImpl::~TabletServerImpl() {}

bool TabletServerImpl::Init() {
    catalog_ = std::shared_ptr<TabletCatalog>(new TabletCatalog());
    bool ok = catalog_->Init();
    if (!ok) {
        LOG(WARNING) << "fail to init catalog ";
        return false;
    }
    engine_ = std::move(std::unique_ptr<vm::Engine>(
        new vm::Engine(catalog_)));
    LOG(INFO) << "init tablet ok";
    return true;
}

void TabletServerImpl::CreateTable(RpcController* ctrl,
                                   const CreateTableRequest* request,
                                   CreateTableResponse* response,
                                   Closure* done) {
    brpc::ClosureGuard done_guard(done);
    ::fesql::common::Status* status = response->mutable_status();
    if (request->pids_size() == 0) {
        status->set_code(common::kBadRequest);
        status->set_msg("create table without pid");
        return;
    }
    if (request->tid() <= 0) {
        status->set_code(common::kBadRequest);
        status->set_msg("create table with invalid tid " +
                        std::to_string(request->tid()));
        return;
    }

    for (int32_t i = 0; i < request->pids_size(); ++i) {
        std::shared_ptr<storage::Table> table(new storage::Table(
            request->tid(), request->pids(i), request->table()));
        bool ok = table->Init();
        if (!ok) {
            LOG(WARNING) << "fail to init table storage for table "
                         << request->table().name();
            status->set_code(common::kBadRequest);
            status->set_msg("fail to init table storage");
            return;
        }
        ok = AddTableLocked(table);
        if (!ok) {
            LOG(WARNING) << "table with name " << request->table().name()
                         << " exists";
            status->set_code(common::kTableExists);
            status->set_msg("table exist");
            return;
        }
        //TODO just one partition
        break;
    }
    status->set_code(common::kOk);
    DLOG(INFO) << "create table with name " << request->table().name()
               << " done";
}

void TabletServerImpl::Insert(RpcController* ctrl, const InsertRequest* request,
                              InsertResponse* response, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    ::fesql::common::Status* status = response->mutable_status();
    if (request->db().empty() || request->table().empty()) {
        status->set_code(common::kBadRequest);
        status->set_msg("db or table name is empty");
        return;
    }
    std::shared_ptr<TabletTableHandler> handler = GetTableLocked(request->db(),
            request->table());

    if (!handler) {
        status->set_code(common::kTableNotFound);
        status->set_msg("table is not found");
        return;
    }

    bool ok = handler->GetTable()->Put(request->row().c_str(), 
            request->row().size());
    if (!ok) {
        status->set_code(common::kTablePutFailed);
        status->set_msg("fail to put row");
        LOG(WARNING) << "fail to put data to table " << request->table()
                     << " with key " << request->key();
        return;
    }
    status->set_code(common::kOk);
}

void TabletServerImpl::Query(RpcController* ctrl, const QueryRequest* request,
                             QueryResponse* response, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    common::Status* status = response->mutable_status();
    status->set_code(common::kOk);
    status->set_msg("ok");
    vm::RunSession session;

    {
        base::Status base_status;
        bool ok =
            engine_->Get(request->sql(), request->db(), session, base_status);
        if (!ok) {
            status->set_msg(base_status.msg);
            status->set_code(base_status.code);
            return;
        }
    }

    std::vector<int8_t*> buf;
    int32_t code;
    if (request->is_batch()) {
        code = session.RunBatch(buf, UINT32_MAX);
    } else {
        code = session.Run(buf, UINT32_MAX);
    }

    if (code != 0) {
        LOG(WARNING) << "fail to run sql " << request->sql();
        status->set_code(common::kSQLError);
        status->set_msg("fail to run sql");
        return;
    }
    // TODO(wangtaize) opt the result buf
    std::vector<int8_t*>::iterator it = buf.begin();
    for (; it != buf.end(); ++it) {
        int8_t* ptr = *it;
        response->add_result_set(ptr, *reinterpret_cast<uint32_t*>(ptr + 2));
        free(ptr);
    }
    response->mutable_schema()->CopyFrom(session.GetSchema());
    status->set_code(common::kOk);
}

void TabletServerImpl::GetTableSchema(RpcController* ctrl,
                                      const GetTablesSchemaRequest* request,
                                      GetTableSchemaReponse* response,
                                      Closure* done) {
    brpc::ClosureGuard done_guard(done);
    ::fesql::common::Status* status = response->mutable_status();
    std::shared_ptr<TabletTableHandler> handler = GetTableLocked(request->db(),
            request->name());
    if (!handler) {
        status->set_code(common::kTableNotFound);
        status->set_msg("table is not found");
        return;
    }
    response->mutable_schema()->CopyFrom(handler->GetTable()->GetTableDef());
}

}  // namespace tablet
}  // namespace fesql
