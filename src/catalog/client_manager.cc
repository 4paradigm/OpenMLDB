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

#include "catalog/client_manager.h"

#include <utility>

#include "codec/fe_schema_codec.h"
#include "codec/sql_rpc_row_codec.h"

DECLARE_int32(request_timeout_ms);

namespace openmldb {
namespace catalog {

TabletRowHandler::TabletRowHandler(const std::string& db, openmldb::RpcCallback<openmldb::api::QueryResponse>* callback)
    : db_(db), name_(), status_(::hybridse::base::Status::Running()), row_(), callback_(callback) {
    callback_->Ref();
}

TabletRowHandler::TabletRowHandler(::hybridse::base::Status status)
    : db_(), name_(), status_(status), row_(), callback_(nullptr) {}

TabletRowHandler::~TabletRowHandler() {
    if (callback_ != nullptr) {
        callback_->UnRef();
    }
}

const ::hybridse::codec::Row& TabletRowHandler::GetValue() {
    if (!status_.isRunning() || !callback_) {
        return row_;
    }
    auto cntl = callback_->GetController();
    auto response = callback_->GetResponse();
    if (!cntl || !response) {
        status_.code = hybridse::common::kRpcError;
        return row_;
    }
    DLOG(INFO) << "TabletRowHandler get value by brpc join";
    brpc::Join(cntl->call_id());
    if (cntl->Failed()) {
        status_ = ::hybridse::base::Status(::hybridse::common::kRpcError, "request error. " + cntl->ErrorText());
        return row_;
    }
    if (cntl->response_attachment().size() <= codec::HEADER_LENGTH) {
        status_.code = hybridse::common::kSchemaCodecError;
        status_.msg = "response content decode fail";
        return row_;
    }
    row_ = hybridse::codec::Row();
    if (0 != response->byte_size() &&
        !codec::DecodeRpcRow(cntl->response_attachment(), 0, response->byte_size(), response->row_slices(), &row_)) {
        status_.code = hybridse::common::kRpcError;
        status_.msg = "response content decode fail";
        return row_;
    }
    status_.code = ::hybridse::common::kOk;
    return row_;
}

AsyncTableHandler::AsyncTableHandler(openmldb::RpcCallback<openmldb::api::SQLBatchRequestQueryResponse>* callback,
                                     const bool is_common)
    : hybridse::vm::MemTableHandler("", "", nullptr),
      status_(::hybridse::base::Status::Running()),
      callback_(callback),
      request_is_common_(is_common) {
    callback_->Ref();
}

std::unique_ptr<hybridse::vm::RowIterator> AsyncTableHandler::GetIterator() {
    if (status_.isRunning()) {
        SyncRpcResponse();
    }
    if (status_.isOK()) {
        return hybridse::vm::MemTableHandler::GetIterator();
    }

    return std::unique_ptr<hybridse::vm::RowIterator>();
}
hybridse::vm::RowIterator* AsyncTableHandler::GetRawIterator() {
    if (status_.isRunning()) {
        SyncRpcResponse();
    }
    if (status_.isOK()) {
        return hybridse::vm::MemTableHandler::GetRawIterator();
    }
    return nullptr;
}
void AsyncTableHandler::SyncRpcResponse() {
    auto cntl = callback_->GetController();
    auto response = callback_->GetResponse();
    if (!cntl || !response) {
        status_.code = hybridse::common::kRpcError;
        status_.msg = "rpc controller or response is null";
        LOG(WARNING) << status_.msg;
        return;
    }
    brpc::Join(cntl->call_id());
    if (cntl->Failed()) {
        status_ = ::hybridse::base::Status(::hybridse::common::kRpcError, "request error. " + cntl->ErrorText());
        LOG(WARNING) << status_.msg;
        return;
    }
    if (response->code() != 0) {
        status_ = ::hybridse::base::Status(::hybridse::common::kResponseError, "request error. " + response->msg());
        LOG(WARNING) << status_.msg;
        return;
    }

    if (response->row_sizes_size() == 0) {
        status_.code = hybridse::common::kResponseError;
        status_.msg = "response error: rows empty";
        LOG(WARNING) << status_.msg;
        return;
    }
    size_t buf_offset = 0;
    for (int i = 0; i < response->row_sizes_size(); ++i) {
        size_t row_size = response->row_sizes(i);
        hybridse::codec::Row row;
        if (0 != row_size && !codec::DecodeRpcRow(cntl->response_attachment(), buf_offset, row_size,
                                                  response->non_common_slices(), &row)) {
            status_.code = hybridse::common::kResponseError;
            status_.msg = "response error: content decode fail";
            LOG(WARNING) << status_.msg;
            return;
        }
        AddRow(row);
        buf_offset += row_size;
    }
    status_ = hybridse::base::Status::OK();
    return;
}

AsyncTablesHandler::AsyncTablesHandler()
    : hybridse::vm::MemTableHandler("", "", nullptr),
      status_(::hybridse::base::Status::Running()),
      rows_cnt_(0),
      posinfos_(),
      handlers_() {}

std::unique_ptr<hybridse::vm::RowIterator> AsyncTablesHandler::GetIterator() {
    if (status_.isRunning()) {
        SyncAllTableHandlers();
    }
    if (status_.isOK()) {
        return hybridse::vm::MemTableHandler::GetIterator();
    }
    return std::unique_ptr<hybridse::vm::RowIterator>();
}
hybridse::vm::RowIterator* AsyncTablesHandler::GetRawIterator() {
    if (status_.isRunning()) {
        SyncAllTableHandlers();
    }
    if (status_.isOK()) {
        return hybridse::vm::MemTableHandler::GetRawIterator();
    }
    return nullptr;
}
bool AsyncTablesHandler::SyncAllTableHandlers() {
    DLOG(INFO) << "SyncAllTableHandlers rows_cnt_ " << rows_cnt_;
    Resize(rows_cnt_);
    for (size_t handler_idx = 0; handler_idx < handlers_.size(); handler_idx++) {
        auto& handler = handlers_[handler_idx];
        auto iter = handler->GetIterator();
        if (!handler->GetStatus().isOK()) {
            status_.msg = "fail to sync table handler " + std::to_string(handler_idx) + ": " + handler->GetStatus().msg;
            status_.code = handler->GetStatus().code;
            LOG(WARNING) << status_;
            return false;
        }
        if (!iter) {
            status_.msg = "fail to sync table hander: iter is null";
            status_.code = hybridse::common::kResponseError;
            LOG(WARNING) << status_;
            return false;
        }
        auto& posinfo = posinfos_[handler_idx];
        auto handler_count = handler->GetCount();
        if (handler_count != posinfos_[handler_idx].size()) {
            status_.msg = "fail to sync table : rows cnt " + std::to_string(handler_count) +
                          " != " + std::to_string(posinfos_[handler_idx].size());
            status_.code = hybridse::common::kResponseError;
            LOG(WARNING) << status_;
            return false;
        }
        size_t pos_idx = 0;
        iter->SeekToFirst();
        while (iter->Valid()) {
            SetRow(posinfo[pos_idx], iter->GetValue());
            iter->Next();
            pos_idx++;
        }
    }
    status_ = hybridse::base::Status::OK();
    DLOG(INFO) << "SyncAllTableHandlers OK";
    return true;
}

std::shared_ptr<::hybridse::vm::RowHandler> TabletAccessor::SubQuery(uint32_t task_id, const std::string& db,
                                                                     const std::string& sql,
                                                                     const ::hybridse::codec::Row& row,
                                                                     const bool is_procedure, const bool is_debug) {
    DLOG(INFO) << "SubQuery taskid: " << task_id << " is_procedure=" << is_procedure;
    auto client = GetClient();
    if (!client) {
        return std::make_shared<TabletRowHandler>(
            ::hybridse::base::Status(::hybridse::common::kRpcError, "get client failed"));
    }
    ::openmldb::api::QueryRequest request;
    if (is_procedure) {
        request.set_sp_name(sql);
    } else {
        request.set_sql(sql);
    }
    request.set_db(db);
    request.set_is_batch(false);
    request.set_task_id(task_id);
    request.set_is_debug(is_debug);
    request.set_is_procedure(is_procedure);
    auto cntl = std::make_shared<brpc::Controller>();
    if (!row.empty()) {
        auto& io_buf = cntl->request_attachment();
        size_t row_size;
        if (!codec::EncodeRpcRow(row, &io_buf, &row_size)) {
            return std::make_shared<TabletRowHandler>(
                ::hybridse::base::Status(::hybridse::common::kRpcError, "encode row failed"));
        }
        request.set_row_size(row_size);
        request.set_row_slices(row.GetRowPtrCnt());
    }
    auto response = std::make_shared<::openmldb::api::QueryResponse>();
    cntl->set_timeout_ms(FLAGS_request_timeout_ms);
    auto callback = new openmldb::RpcCallback<openmldb::api::QueryResponse>(response, cntl);
    auto row_handler = std::make_shared<TabletRowHandler>(db, callback);
    if (!client->SubQuery(request, callback)) {
        return std::make_shared<TabletRowHandler>(
            ::hybridse::base::Status(::hybridse::common::kRpcError, "send request failed"));
    }
    return row_handler;
}

std::shared_ptr<::hybridse::vm::TableHandler> TabletAccessor::SubQuery(uint32_t task_id, const std::string& db,
                                                                       const std::string& sql,
                                                                       const std::set<size_t>& common_column_indices,
                                                                       const std::vector<::hybridse::codec::Row>& rows,
                                                                       const bool request_is_common,
                                                                       const bool is_procedure, const bool is_debug) {
    DLOG(INFO) << "SubQuery batch request, taskid=" << task_id << ", is_procedure=" << is_procedure;
    auto client = GetClient();
    if (!client) {
        return std::make_shared<hybridse::vm::ErrorTableHandler>(::hybridse::common::kRpcError, "get client failed");
    }
    auto cntl = std::make_shared<brpc::Controller>();
    auto& io_buf = cntl->request_attachment();

    ::openmldb::api::SQLBatchRequestQueryRequest request;
    if (is_procedure) {
        request.set_sp_name(sql);
    } else {
        request.set_sql(sql);
    }
    request.set_is_procedure(is_procedure);
    request.set_db(db);
    request.set_task_id(task_id);
    request.set_is_debug(is_debug);
    for (size_t idx : common_column_indices) {
        request.add_common_column_indices(idx);
    }
    if (request_is_common) {
        if (rows.empty()) {
            request.set_common_slices(0);
        } else {
            size_t common_slice_size = 0;
            if (!codec::EncodeRpcRow(rows[0], &io_buf, &common_slice_size)) {
                return std::make_shared<::hybridse::vm::ErrorTableHandler>(::hybridse::common::kRequestError,
                                                                           "encode common row buf failed");
            }
            request.add_row_sizes(common_slice_size);
            request.set_common_slices(rows[0].GetRowPtrCnt());

            // TODO(baoxinqi): opt request is common, need no uncommon slices
            size_t uncommon_slice_size = 0;
            if (!codec::EncodeRpcRow(rows[0], &io_buf, &uncommon_slice_size)) {
                return std::make_shared<::hybridse::vm::ErrorTableHandler>(::hybridse::common::kRequestError,
                                                                           "encode uncommon row buf failed");
            }
            request.add_row_sizes(uncommon_slice_size);
            request.set_non_common_slices(rows[0].GetRowPtrCnt());
        }
    } else {
        request.set_common_slices(0);
        for (const auto& row : rows) {
            size_t uncommon_slice_size = 0;
            if (!codec::EncodeRpcRow(row, &io_buf, &uncommon_slice_size)) {
                return std::make_shared<::hybridse::vm::ErrorTableHandler>(::hybridse::common::kRequestError,
                                                                           "encode uncommon row buf failed");
            }
            request.add_row_sizes(uncommon_slice_size);
            request.set_non_common_slices(row.GetRowPtrCnt());
        }
    }
    auto response = std::make_shared<::openmldb::api::SQLBatchRequestQueryResponse>();
    cntl->set_timeout_ms(FLAGS_request_timeout_ms);
    auto callback = new openmldb::RpcCallback<openmldb::api::SQLBatchRequestQueryResponse>(response, cntl);
    auto async_table_handler = std::make_shared<AsyncTableHandler>(callback, request_is_common);
    if (!client->SubBatchRequestQuery(request, callback)) {
        LOG(WARNING) << "fail to query tablet";
        return std::make_shared<::hybridse::vm::ErrorTableHandler>(::hybridse::common::kRpcError,
                                                                   "fail to batch request query");
    }
    return async_table_handler;
}

void TabletsAccessor::AddTabletAccessor(std::shared_ptr<Tablet> accessor) {
    if (!accessor) {
        LOG(WARNING) << "Fail to add null tablet accessor";
        return;
    }
    auto iter = name_idx_map_.find(accessor->GetName());
    if (iter == name_idx_map_.cend()) {
        accessors_.push_back(accessor);
        name_idx_map_.insert(std::make_pair(accessor->GetName(), accessors_.size() - 1));
        posinfos_.push_back(std::vector<size_t>({rows_cnt_}));
        assign_accessor_idxs_.push_back(accessors_.size() - 1);
    } else {
        posinfos_[iter->second].push_back(rows_cnt_);
        assign_accessor_idxs_.push_back(iter->second);
    }
    rows_cnt_++;
}

std::shared_ptr<hybridse::vm::RowHandler> TabletsAccessor::SubQuery(uint32_t task_id, const std::string& db,
                                                                    const std::string& sql,
                                                                    const hybridse::codec::Row& row,
                                                                    const bool is_procedure, const bool is_debug) {
    return std::make_shared<::hybridse::vm::ErrorRowHandler>(::hybridse::common::kRpcError,
                                                             "TabletsAccessor Unsupport SubQuery with request");
}

std::shared_ptr<hybridse::vm::TableHandler> TabletsAccessor::SubQuery(uint32_t task_id, const std::string& db,
                                                                      const std::string& sql,
                                                                      const std::set<size_t>& common_column_indices,
                                                                      const std::vector<hybridse::codec::Row>& rows,
                                                                      const bool request_is_common,
                                                                      const bool is_procedure, const bool is_debug) {
    auto tables_handler = std::make_shared<AsyncTablesHandler>();
    std::vector<std::vector<hybridse::vm::Row>> accessors_rows(accessors_.size());
    for (size_t idx = 0; idx < rows.size(); idx++) {
        accessors_rows[assign_accessor_idxs_[idx]].push_back(rows[idx]);
    }
    for (size_t idx = 0; idx < accessors_.size(); idx++) {
        tables_handler->AddAsyncRpcHandler(
            accessors_[idx]->SubQuery(task_id, db, sql, common_column_indices, accessors_rows[idx], request_is_common,
                                      is_procedure, is_debug),
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

TableClientManager::TableClientManager(const ::openmldb::storage::TableSt& table_st,
                                       const ClientManager& client_manager) {
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

void TableClientManager::Show() const {
    DLOG(INFO) << "show client manager ";
    for (size_t id = 0; id < partition_managers_.size(); id++) {
        auto pmg = std::atomic_load_explicit(&partition_managers_[id], std::memory_order_relaxed);
        if (pmg) {
            if (pmg->GetLeader()) {
                DLOG(INFO) << "partition managers (pid, leader) " << id << ", " << pmg->GetLeader()->GetName();
            } else {
                DLOG(INFO) << "partition managers (pid, leader) " << id << ", null leader";
            }
        } else {
            DLOG(INFO) << "partition managers (pid, leader) " << id << ", null mamanger";
        }
    }
}

std::shared_ptr<PartitionClientManager> TableClientManager::GetPartitionClientManager(uint32_t pid) const {
    if (pid < partition_managers_.size()) {
        return std::atomic_load_explicit(&partition_managers_[pid], std::memory_order_relaxed);
    }
    return std::shared_ptr<PartitionClientManager>();
}

bool TableClientManager::UpdatePartitionClientManager(const ::openmldb::storage::PartitionSt& partition,
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

std::shared_ptr<TabletAccessor> TableClientManager::GetTablet(uint32_t pid) const {
    auto partition_manager = GetPartitionClientManager(pid);
    if (partition_manager) {
        return partition_manager->GetLeader();
    }
    return std::shared_ptr<TabletAccessor>();
}

std::vector<std::shared_ptr<TabletAccessor>> TableClientManager::GetTabletFollowers(uint32_t pid) const {
    auto partition_manager = GetPartitionClientManager(pid);
    if (partition_manager) {
        return partition_manager->GetFollowers();
    }
    return {};
}

std::shared_ptr<TabletsAccessor> TableClientManager::GetTablet(std::vector<uint32_t> pids) const {
    auto tablets_accessor = std::make_shared<TabletsAccessor>();
    for (size_t idx = 0; idx < pids.size(); idx++) {
        auto partition_manager = GetPartitionClientManager(pids[idx]);
        if (partition_manager) {
            auto leader = partition_manager->GetLeader();
            if (!leader) {
                LOG(WARNING) << "fail to get TabletsAccessor, null tablet for pid " << pids[idx];
                return std::shared_ptr<TabletsAccessor>();
            }
            tablets_accessor->AddTabletAccessor(partition_manager->GetLeader());
        } else {
            LOG(WARNING) << "fail to get tablet: pid " << pids[idx] << " not exist";
            return std::shared_ptr<TabletsAccessor>();
        }
    }
    return tablets_accessor;
}

std::shared_ptr<TabletAccessor> ClientManager::GetTablet(const std::string& name) const {
    std::lock_guard<::openmldb::base::SpinMutex> lock(mu_);
    auto it = clients_.find(name);
    if (it == clients_.end()) {
        return std::shared_ptr<TabletAccessor>();
    }
    return it->second;
}

std::shared_ptr<TabletAccessor> ClientManager::GetTablet() const {
    std::lock_guard<::openmldb::base::SpinMutex> lock(mu_);
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

std::vector<std::shared_ptr<TabletAccessor>> ClientManager::GetAllTablet() const {
    std::lock_guard<::openmldb::base::SpinMutex> lock(mu_);
    if (clients_.empty()) {
        return {};
    }
    std::vector<std::shared_ptr<TabletAccessor>> allTablet;
    for (const auto& kv : clients_) {
        allTablet.emplace_back(kv.second);
    }
    return allTablet;
}

bool ClientManager::UpdateClient(const std::map<std::string, std::string>& endpoint_map) {
    if (endpoint_map.empty()) {
        DLOG(INFO) << "endpoint_map is empty";
        return true;
    }
    std::lock_guard<::openmldb::base::SpinMutex> lock(mu_);
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
    const std::map<std::string, std::shared_ptr<::openmldb::client::TabletClient>>& tablet_clients) {
    std::lock_guard<::openmldb::base::SpinMutex> lock(mu_);
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
}  // namespace openmldb
