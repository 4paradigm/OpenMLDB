/*
 * client_manager.cc
 * Copyright (C) 4paradigm.com
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

#include "catalog/client_manager.h"
#include <utility>
#include "codec/fe_schema_codec.h"

namespace rtidb {
namespace catalog {

TabletRowHandler::TabletRowHandler(const std::string& db, std::unique_ptr<brpc::Controller> cntl,
                                   std::unique_ptr<::rtidb::api::QueryResponse> response)
    : db_(db),
      name_(),
      status_(::fesql::base::Status::Running()),
      row_(),
      cntl_(std::move(cntl)),
      response_(std::move(response)) {}

TabletRowHandler::TabletRowHandler(::fesql::base::Status status)
    : db_(), name_(), status_(status), row_(), cntl_(), response_() {}

const ::fesql::codec::Row& TabletRowHandler::GetValue() {
    if (!status_.isRunning()) {
        return row_;
    }
    if (!cntl_ || !response_) {
        status_.code = fesql::common::kRpcError;
        return row_;
    }
    DLOG(INFO) << "TabletRowHandler get value by brpc join";
    // TODO(denglong) timeout handle
    brpc::Join(cntl_->call_id());
    if (cntl_->Failed()) {
        status_ = ::fesql::base::Status(::fesql::common::kRpcError, "request error. " + cntl_->ErrorText());
        return row_;
    }
    if (cntl_->response_attachment().size() <= codec::HEADER_LENGTH) {
        status_.code = fesql::common::kSchemaCodecError;
        status_.msg = "response content decode fail";
        return row_;
    }
    uint32_t tmp_size = 0;
    cntl_->response_attachment().copy_to(reinterpret_cast<void*>(&tmp_size), codec::SIZE_LENGTH,
                 codec::VERSION_LENGTH);
    int8_t* out_buf = reinterpret_cast<int8_t*>(malloc(tmp_size));
    cntl_->response_attachment().copy_to(out_buf, tmp_size);
    row_ = fesql::codec::Row(fesql::base::RefCountedSlice::CreateManaged(out_buf, tmp_size));
    status_.code = ::fesql::common::kOk;
    return row_;
}

AsyncTableHandler::AsyncTableHandler(std::unique_ptr<brpc::Controller> cntl,
                                     std::unique_ptr<::rtidb::api::SQLBatchRequestQueryResponse> response)
    : fesql::vm::MemTableHandler("", "", nullptr),
      status_(::fesql::base::Status::Running()),
      cntl_(std::move(cntl)),
      response_(std::move(response)) {}

std::unique_ptr<fesql::vm::RowIterator> AsyncTableHandler::GetIterator() {
    if (status_.isRunning()) {
        SyncRpcResponse();
    }
    if (status_.isOK()) {
        return fesql::vm::MemTableHandler::GetIterator();
    }

    return std::unique_ptr<fesql::vm::RowIterator>();
}
fesql::vm::RowIterator* AsyncTableHandler::GetRawIterator() {
    if (status_.isRunning()) {
        SyncRpcResponse();
    }
    if (status_.isOK()) {
        return fesql::vm::MemTableHandler::GetRawIterator();
    }
    return nullptr;
}
void AsyncTableHandler::SyncRpcResponse() {
    if (!cntl_ || !response_) {
        status_.code = fesql::common::kRpcError;
        status_.msg = "rpc controller or response is null";
        LOG(WARNING) << status_.msg;
        return;
    }
    DLOG(INFO) << "AsyncTableHandler sync data brpc join";
    // TODO(denglong) timeout handle
    brpc::Join(cntl_->call_id());
    if (cntl_->Failed()) {
        status_ = ::fesql::base::Status(::fesql::common::kRpcError, "request error. " + cntl_->ErrorText());
        LOG(WARNING) << status_.msg;
        return;
    }
    int32_t position = 0;
    for (auto row_size : response_->row_sizes()) {
        if (row_size < 0) {
            LOG(WARNING) << "illegal row size field";
            status_ = ::fesql::base::Status(::fesql::common::kResponseError, "illegal row size field.");
            LOG(WARNING) << status_.msg;
            return;
        }
        int8_t* out_buf = reinterpret_cast<int8_t*>(malloc(row_size));
        cntl_->response_attachment().copy_to(out_buf, row_size, position);
        AddRow(fesql::codec::Row(fesql::base::RefCountedSlice::CreateManaged(out_buf, row_size)));
        position += row_size;
    }
    return;
}

AsyncTablesHandler::AsyncTablesHandler()
    : fesql::vm::MemTableHandler("", "", nullptr),
      status_(::fesql::base::Status::Running()),
      rows_cnt_(0),
      posinfos_(),
      handlers_() {}

std::unique_ptr<fesql::vm::RowIterator> AsyncTablesHandler::GetIterator() {
    if (status_.isRunning()) {
        SyncAllTableHandlers();
    }
    if (status_.isOK()) {
        return fesql::vm::MemTableHandler::GetIterator();
    }
    return std::unique_ptr<fesql::vm::RowIterator>();
}
fesql::vm::RowIterator* AsyncTablesHandler::GetRawIterator() {
    if (status_.isRunning()) {
        SyncAllTableHandlers();
    }
    if (status_.isOK()) {
        return fesql::vm::MemTableHandler::GetRawIterator();
    }
    return nullptr;
}
bool AsyncTablesHandler::SyncAllTableHandlers() {
    DLOG(INFO) << "SyncAllTableHandlers";
    Reserve(rows_cnt_);
    for(size_t handler_idx = 0; handler_idx < handlers_.size(); handler_idx++) {
        auto& handler = handlers_[handler_idx];
        auto iter = handler->GetIterator();
        if (!handler->GetStatus().isOK()) {
            status_.msg = "fail to sync table handler " + std::to_string(handler_idx) + ": " + handler->GetStatus().msg;
            status_.code = handler->GetStatus().code;
            return false;
        }
        if (!iter) {
            status_.msg = "fail to sync table hander: iter is null";
            status_.code = fesql::common::kResponseError;
            return false;
        }
        auto& posinfo = posinfos_[handler_idx];
        if (handler->GetCount() != posinfos_[handler_idx].size()) {
            status_.msg = "fail to sync table hander: unexpected rows cnt";
            status_.code = fesql::common::kResponseError;
            return false;
        }
        size_t pos_idx = 0;
        iter->SeekToFirst();
        while(iter->Valid()) {
            SetRow(posinfo[pos_idx], iter->GetValue());
            iter->Next();
            pos_idx++;
        }
    }
    DLOG(INFO) << "SyncAllTableHandlers OK";
    return true;
}

std::shared_ptr<::fesql::vm::RowHandler> TabletAccessor::SubQuery(uint32_t task_id, const std::string& db,
                                                                  const std::string& sql,
                                                                  const ::fesql::codec::Row& row,
                                                                  const bool is_debug) {
    DLOG(INFO) << "SubQuery taskid: " << task_id;
    ::rtidb::api::QueryRequest request;
    request.set_sql(sql);
    request.set_db(db);
    request.set_is_batch(false);
    request.set_task_id(task_id);
    request.set_is_debug(is_debug);
    if (!row.empty()) {
        std::string* input_row = request.mutable_input_row();
        input_row->assign(reinterpret_cast<const char*>(row.buf()), row.size());
    }
    auto client = GetClient();
    if (!client) {
        return std::make_shared<TabletRowHandler>(
            ::fesql::base::Status(::fesql::common::kRpcError, "get client failed"));
    }
    std::unique_ptr<brpc::Controller> cntl(new brpc::Controller);
    std::unique_ptr<::rtidb::api::QueryResponse> response(new ::rtidb::api::QueryResponse);
    if (!client->SubQuery(request, cntl.get(), response.get())) {
        return std::make_shared<TabletRowHandler>(
            ::fesql::base::Status(::fesql::common::kRpcError, "send request failed"));
    }
    return std::make_shared<TabletRowHandler>(db, std::move(cntl), std::move(response));
}

std::shared_ptr<::fesql::vm::TableHandler> TabletAccessor::SubQuery(uint32_t task_id, const std::string& db,
                                                                    const std::string& sql,
                                                                    std::shared_ptr<::fesql::vm::TableHandler> table,
                                                                    const bool is_debug) {
    DLOG(INFO) << "SubQuery batch request, taskid: " << task_id;
    ::rtidb::api::SQLBatchRequestQueryRequest request;
    request.set_sql(sql);
    request.set_db(db);
    request.set_task_id(task_id);
    request.set_is_debug(is_debug);
    auto iter = table->GetIterator();
    if (!iter) {
        return std::make_shared<::fesql::vm::ErrorTableHandler>(::fesql::common::kBadRequest,
                                                                "batch request table is null");
    }
    iter->SeekToFirst();
    while (iter->Valid()) {
        request.add_non_common_rows(iter->GetValue().ToString());
        iter->Next();
    }
    auto client = GetClient();
    if (!client) {
        return std::make_shared<::fesql::vm::ErrorTableHandler>(::fesql::common::kRpcError, "get client failed");
    }
    std::unique_ptr<brpc::Controller> cntl(new brpc::Controller);
    std::unique_ptr<::rtidb::api::SQLBatchRequestQueryResponse> response(
        new ::rtidb::api::SQLBatchRequestQueryResponse);
    bool ok = client->SubBatchRequestQuery(request, cntl.get(), response.get());
    if (!ok || response->code() != ::rtidb::base::kOk) {
        LOG(WARNING) << "fail to query tablet";
        return std::make_shared<::fesql::vm::ErrorTableHandler>(::fesql::common::kRpcError,
                                                                "fail to batch request query");
    }

    return std::make_shared<AsyncTableHandler>(std::move(cntl), std::move(response));
}
std::shared_ptr<fesql::vm::RowHandler> TabletsAccessor::SubQuery(uint32_t task_id, const std::string& db,
                                                                 const std::string& sql, const fesql::codec::Row& row,
                                                                 const bool is_debug) {
    return std::make_shared<::fesql::vm::ErrorRowHandler>(::fesql::common::kRpcError,
                                                            "Unsupport SubQuery");
}
std::shared_ptr<fesql::vm::TableHandler> TabletsAccessor::SubQuery(uint32_t task_id, const std::string& db,
                                                                   const std::string& sql,
                                                                   const std::shared_ptr<fesql::vm::TableHandler> table,
                                                                   const bool is_debug) {
    auto tables_handler = std::make_shared<AsyncTablesHandler>();
    std::vector<std::shared_ptr<fesql::vm::MemTableHandler>> sub_tables;
    for (size_t idx = 0; idx < accessors_.size(); idx++) {
        sub_tables.push_back(std::make_shared<fesql::vm::MemTableHandler>());
    }
    auto iter = table->GetIterator();
    iter->SeekToFirst();
    size_t row_idx = 0;
    while (iter->Valid()) {
        size_t handler_idx = assign_accessor_idxs_[row_idx];
        sub_tables[handler_idx]->AddRow(iter->GetValue());
        iter->Next();
        row_idx++;
    }
    for (size_t idx = 0; idx < accessors_.size(); idx++) {
        tables_handler->AddAsyncRpcHandler(accessors_[idx]->SubQuery(task_id, db, sql, sub_tables[idx], is_debug),
                                           posinfos_[idx]);
    }
    return tables_handler;
}
PartitionClientManager::PartitionClientManager(uint32_t pid, const std::shared_ptr<TabletAccessor>& leader,
                                               const std::vector<std::shared_ptr<TabletAccessor>>& followers)
    : pid_(pid), leader_(leader), followers_(followers), rand_(0xdeadbeef) {}

std::shared_ptr<TabletAccessor> PartitionClientManager::GetFollower() {
    if (!followers_.empty()) {
        uint32_t it = rand_.Next() % followers_.size();
        return followers_[it];
    }
    return std::shared_ptr<TabletAccessor>();
}

TableClientManager::TableClientManager(const TablePartitions& partitions, const ClientManager& client_manager) {
    for (const auto& table_partition : partitions) {
        uint32_t pid = table_partition.pid();
        if (pid > partition_managers_.size()) {
            continue;
        }
        std::shared_ptr<TabletAccessor> leader;
        std::vector<std::shared_ptr<TabletAccessor>> follower;
        for (const auto& meta : table_partition.partition_meta()) {
            if (meta.is_alive()) {
                auto client = client_manager.GetTablet(meta.endpoint());
                if (!client) {
                    continue;
                }
                if (meta.is_leader()) {
                    leader = client;
                } else {
                    follower.push_back(client);
                }
            }
        }
        partition_managers_.push_back(std::make_shared<PartitionClientManager>(pid, leader, follower));
    }
}

TableClientManager::TableClientManager(const ::rtidb::storage::TableSt& table_st, const ClientManager& client_manager) {
    for (const auto& partition_st : *(table_st.GetPartitions())) {
        uint32_t pid = partition_st.GetPid();
        if (pid > partition_managers_.size()) {
            continue;
        }
        std::shared_ptr<TabletAccessor> leader = client_manager.GetTablet(partition_st.GetLeader());
        std::vector<std::shared_ptr<TabletAccessor>> follower;
        for (const auto& endpoint : partition_st.GetFollower()) {
            auto client = client_manager.GetTablet(endpoint);
            if (client) {
                follower.push_back(client);
            }
        }
        partition_managers_.push_back(std::make_shared<PartitionClientManager>(pid, leader, follower));
    }
}

bool TableClientManager::UpdatePartitionClientManager(const ::rtidb::storage::PartitionSt& partition,
                                                      const ClientManager& client_manager) {
    uint32_t pid = partition.GetPid();
    if (pid > partition_managers_.size()) {
        return false;
    }
    auto leader = client_manager.GetTablet(partition.GetLeader());
    if (!leader) {
        return false;
    }
    std::vector<std::shared_ptr<TabletAccessor>> followers;
    for (const auto& endpoint : partition.GetFollower()) {
        auto client = client_manager.GetTablet(endpoint);
        if (!client) {
            return false;
        }
        followers.push_back(client);
    }
    auto partition_manager = std::make_shared<PartitionClientManager>(pid, leader, followers);
    std::atomic_store_explicit(&partition_managers_[pid], partition_manager, std::memory_order_relaxed);
    return true;
}

std::shared_ptr<TabletAccessor> ClientManager::GetTablet(const std::string& name) const {
    std::lock_guard<::rtidb::base::SpinMutex> lock(mu_);
    auto it = clients_.find(name);
    if (it == clients_.end()) {
        return std::shared_ptr<TabletAccessor>();
    }
    return it->second;
}

std::shared_ptr<TabletAccessor> ClientManager::GetTablet() const {
    std::lock_guard<::rtidb::base::SpinMutex> lock(mu_);
    if (clients_.empty()) {
        return std::shared_ptr<TabletAccessor>();
    }
    uint32_t seq = rand_.Uniform(clients_.size());
    uint32_t cnt = 0;
    for (const auto& kv : clients_) {
        if (cnt == seq) {
            return kv.second;
        }
        cnt++;
    }
    return std::shared_ptr<TabletAccessor>();
}

bool ClientManager::UpdateClient(const std::map<std::string, std::string>& endpoint_map) {
    if (endpoint_map.empty()) {
        DLOG(INFO) << "endpoint_map is empty";
        return true;
    }
    std::lock_guard<::rtidb::base::SpinMutex> lock(mu_);
    for (const auto& kv : endpoint_map) {
        auto it = real_endpoint_map_.find(kv.first);
        if (it == real_endpoint_map_.end()) {
            auto wrapper = std::make_shared<TabletAccessor>(kv.first);
            if (!wrapper->UpdateClient(kv.second)) {
                LOG(WARNING) << "add client failed. name " << kv.first << ", endpoint " << kv.second;
                continue;
            }
            LOG(INFO) << "add client. name " << kv.first << ", endpoint " << kv.second;
            clients_.emplace(kv.first, wrapper);
            real_endpoint_map_.emplace(kv.first, kv.second);
            continue;
        }
        if (it->second != kv.second) {
            auto client_it = clients_.find(kv.first);
            LOG(INFO) << "update client " << kv.first << "from " << it->second << " to " << kv.second;
            if (!client_it->second->UpdateClient(kv.second)) {
                LOG(WARNING) << "update client failed. name " << kv.first << ", endpoint " << kv.second;
                continue;
            }
            it->second = kv.second;
        }
    }
    return true;
}

bool ClientManager::UpdateClient(
    const std::map<std::string, std::shared_ptr<::rtidb::client::TabletClient>>& tablet_clients) {
    std::lock_guard<::rtidb::base::SpinMutex> lock(mu_);
    for (const auto& kv : tablet_clients) {
        auto it = real_endpoint_map_.find(kv.first);
        if (it == real_endpoint_map_.end()) {
            auto wrapper = std::make_shared<TabletAccessor>(kv.first, kv.second);
            DLOG(INFO) << "add client. name " << kv.first << ", endpoint " << kv.second->GetRealEndpoint();
            clients_.emplace(kv.first, wrapper);
            real_endpoint_map_.emplace(kv.first, kv.second->GetRealEndpoint());
            continue;
        }
        if (it->second != kv.second->GetRealEndpoint()) {
            auto client_it = clients_.find(kv.first);
            LOG(INFO) << "update client " << kv.first << " from " << it->second << " to "
                      << kv.second->GetRealEndpoint();
            if (!client_it->second->UpdateClient(kv.second)) {
                LOG(WARNING) << "update client failed. name " << kv.first << ", endpoint "
                             << kv.second->GetRealEndpoint();
                continue;
            }
            it->second = kv.second->GetRealEndpoint();
        }
    }
    return true;
}

}  // namespace catalog
}  // namespace rtidb
