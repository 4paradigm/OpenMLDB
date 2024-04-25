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

#ifndef SRC_CATALOG_CLIENT_MANAGER_H_
#define SRC_CATALOG_CLIENT_MANAGER_H_

#include <map>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "base/random.h"
#include "base/spinlock.h"
#include "client/tablet_client.h"
#include "storage/schema.h"
#include "vm/catalog.h"
#include "vm/mem_catalog.h"

namespace openmldb {
namespace catalog {

using TablePartitions = ::google::protobuf::RepeatedPtrField<::openmldb::nameserver::TablePartition>;

class TabletRowHandler : public ::hybridse::vm::RowHandler {
 public:
    TabletRowHandler(const std::string& db, openmldb::RpcCallback<openmldb::api::QueryResponse>* callback);
    ~TabletRowHandler();
    explicit TabletRowHandler(::hybridse::base::Status status);
    const ::hybridse::vm::Schema* GetSchema() override { return nullptr; }
    const std::string& GetName() override { return name_; }
    const std::string& GetDatabase() override { return db_; }

    ::hybridse::base::Status GetStatus() override { return status_; }
    const ::hybridse::codec::Row& GetValue() override;

 private:
    std::string db_;
    std::string name_;
    ::hybridse::base::Status status_;
    ::hybridse::codec::Row row_;
    openmldb::RpcCallback<openmldb::api::QueryResponse>* callback_;
};

class AsyncTableHandler : public ::hybridse::vm::MemTableHandler {
 public:
    explicit AsyncTableHandler(openmldb::RpcCallback<openmldb::api::SQLBatchRequestQueryResponse>* callback,
                               const bool is_common);
    ~AsyncTableHandler() {
        if (nullptr != callback_) {
            callback_->UnRef();
        }
    }
    const uint64_t GetCount() override {
        if (status_.isRunning()) {
            SyncRpcResponse();
        }
        return hybridse::vm::MemTableHandler::GetCount();
    }
    hybridse::codec::Row At(uint64_t pos) override {
        if (status_.isRunning()) {
            SyncRpcResponse();
        }
        return hybridse::vm::MemTableHandler::At(pos);
    }
    std::unique_ptr<hybridse::vm::RowIterator> GetIterator();
    hybridse::vm::RowIterator* GetRawIterator();
    std::unique_ptr<hybridse::vm::WindowIterator> GetWindowIterator(const std::string& idx_name) {
        return std::unique_ptr<hybridse::vm::WindowIterator>();
    }
    const std::string GetHandlerTypeName() override { return "AsyncTableHandler"; }
    virtual hybridse::base::Status GetStatus() { return status_; }

 private:
    void SyncRpcResponse();
    hybridse::base::Status status_;
    openmldb::RpcCallback<openmldb::api::SQLBatchRequestQueryResponse>* callback_;
    bool request_is_common_;
};

class AsyncTablesHandler : public ::hybridse::vm::MemTableHandler {
 public:
    AsyncTablesHandler();
    ~AsyncTablesHandler() {}
    void AddAsyncRpcHandler(std::shared_ptr<TableHandler> handler, const std::vector<size_t>& pos_info) {
        handlers_.push_back(handler);
        posinfos_.push_back(pos_info);
        rows_cnt_ += pos_info.size();
    }
    const uint64_t GetCount() override {
        if (status_.isRunning()) {
            SyncAllTableHandlers();
        }
        return hybridse::vm::MemTableHandler::GetCount();
    }
    hybridse::codec::Row At(uint64_t pos) override {
        if (status_.isRunning()) {
            SyncAllTableHandlers();
        }
        return hybridse::vm::MemTableHandler::At(pos);
    }
    std::unique_ptr<hybridse::vm::RowIterator> GetIterator();
    hybridse::vm::RowIterator* GetRawIterator();
    std::unique_ptr<hybridse::vm::WindowIterator> GetWindowIterator(const std::string& idx_name) {
        return std::unique_ptr<hybridse::vm::WindowIterator>();
    }
    const std::string GetHandlerTypeName() override { return "AsyncTableHandler"; }
    virtual hybridse::base::Status GetStatus() { return status_; }

 private:
    bool SyncAllTableHandlers();
    hybridse::base::Status status_;
    size_t rows_cnt_;
    std::vector<std::vector<size_t>> posinfos_;
    std::vector<std::shared_ptr<TableHandler>> handlers_;
};

class TabletAccessor : public ::hybridse::vm::Tablet {
 public:
    explicit TabletAccessor(const std::string& name,
                            const openmldb::authn::AuthToken auth_token = openmldb::authn::ServiceToken{"default"})
        : name_(name), tablet_client_(), auth_token_(auth_token) {}

    TabletAccessor(const std::string& name, const std::shared_ptr<::openmldb::client::TabletClient>& client,
                   const openmldb::authn::AuthToken auth_token = openmldb::authn::ServiceToken{"default"})
        : name_(name), tablet_client_(client), auth_token_(auth_token) {}

    std::shared_ptr<::openmldb::client::TabletClient> GetClient() {
        return std::atomic_load_explicit(&tablet_client_, std::memory_order_relaxed);
    }

    bool UpdateClient(const std::string& endpoint) {
        auto client = std::make_shared<::openmldb::client::TabletClient>(name_, endpoint, auth_token_);
        if (client->Init() != 0) {
            return false;
        }
        std::atomic_store_explicit(&tablet_client_, client, std::memory_order_relaxed);
        return true;
    }

    bool UpdateClient(const std::shared_ptr<::openmldb::client::TabletClient>& client) {
        std::atomic_store_explicit(&tablet_client_, client, std::memory_order_relaxed);
        return true;
    }

    std::shared_ptr<::hybridse::vm::RowHandler> SubQuery(uint32_t task_id, const std::string& db,
                                                         const std::string& sql, const ::hybridse::codec::Row& row,
                                                         const bool is_procedure, const bool is_debug) override;

    std::shared_ptr<::hybridse::vm::TableHandler> SubQuery(uint32_t task_id, const std::string& db,
                                                           const std::string& sql,
                                                           const std::set<size_t>& common_column_indices,
                                                           const std::vector<::hybridse::codec::Row>& row,
                                                           const bool request_is_common, const bool is_procedure,
                                                           const bool is_debug) override;
    const std::string& GetName() const { return name_; }

 private:
    std::string name_;
    std::shared_ptr<::openmldb::client::TabletClient> tablet_client_;
    const openmldb::authn::AuthToken auth_token_;
};

class TabletsAccessor : public ::hybridse::vm::Tablet {
 public:
    TabletsAccessor() : name_("TabletsAccessor"), rows_cnt_(0) {}
    ~TabletsAccessor() {}
    const std::string& GetName() const { return name_; }
    void AddTabletAccessor(std::shared_ptr<Tablet> accessor);

    std::shared_ptr<hybridse::vm::RowHandler> SubQuery(uint32_t task_id, const std::string& db, const std::string& sql,
                                                       const hybridse::codec::Row& row, const bool is_procedure,
                                                       const bool is_debug) override;
    std::shared_ptr<hybridse::vm::TableHandler> SubQuery(uint32_t task_id, const std::string& db,
                                                         const std::string& sql,
                                                         const std::set<size_t>& common_column_indices,
                                                         const std::vector<hybridse::codec::Row>& rows,
                                                         const bool request_is_common, const bool is_procedure,
                                                         const bool is_debug);

 private:
    const std::string name_;
    size_t rows_cnt_;
    std::vector<std::shared_ptr<Tablet>> accessors_;
    std::vector<size_t> assign_accessor_idxs_;
    std::vector<std::vector<size_t>> posinfos_;
    std::map<std::string, size_t> name_idx_map_;
};

class PartitionClientManager {
 public:
    PartitionClientManager(uint32_t pid, const std::shared_ptr<TabletAccessor>& leader,
                           const std::vector<std::shared_ptr<TabletAccessor>>& followers);

    inline std::shared_ptr<TabletAccessor> GetLeader() const { return leader_; }

    std::shared_ptr<TabletAccessor> GetFollower();

    const std::vector<std::shared_ptr<TabletAccessor>>& GetFollowers() const { return followers_; }

 private:
    uint32_t pid_;
    std::shared_ptr<TabletAccessor> leader_;
    std::vector<std::shared_ptr<TabletAccessor>> followers_;
    ::openmldb::base::Random rand_;
};

class ClientManager;

class TableClientManager {
 public:
    TableClientManager(const TablePartitions& partitions, const ClientManager& client_manager);

    TableClientManager(const ::openmldb::storage::TableSt& table_st, const ClientManager& client_manager);

    void Show() const;

    std::shared_ptr<PartitionClientManager> GetPartitionClientManager(uint32_t pid) const;

    bool UpdatePartitionClientManager(const ::openmldb::storage::PartitionSt& partition,
                                      const ClientManager& client_manager);

    std::shared_ptr<TabletAccessor> GetTablet(uint32_t pid) const;

    std::vector<std::shared_ptr<TabletAccessor>> GetTabletFollowers(uint32_t pid) const;

    std::shared_ptr<TabletsAccessor> GetTablet(std::vector<uint32_t> pids) const;

 private:
    std::vector<std::shared_ptr<PartitionClientManager>> partition_managers_;
};

class ClientManager {
 public:
    explicit ClientManager(const openmldb::authn::AuthToken auth_token = openmldb::authn::ServiceToken{"default"})
        : real_endpoint_map_(), clients_(), mu_(), rand_(0xdeadbeef), auth_token_(auth_token) {}
    std::shared_ptr<TabletAccessor> GetTablet(const std::string& name) const;
    std::shared_ptr<TabletAccessor> GetTablet() const;
    std::vector<std::shared_ptr<TabletAccessor>> GetAllTablet() const;

    bool UpdateClient(const std::map<std::string, std::string>& real_ep_map);

    bool UpdateClient(const std::map<std::string, std::shared_ptr<::openmldb::client::TabletClient>>& tablet_clients);

 private:
    std::unordered_map<std::string, std::string> real_endpoint_map_;
    std::unordered_map<std::string, std::shared_ptr<TabletAccessor>> clients_;
    mutable ::openmldb::base::SpinMutex mu_;
    mutable ::openmldb::base::Random rand_;
    const openmldb::authn::AuthToken auth_token_;
};

}  // namespace catalog
}  // namespace openmldb
#endif  // SRC_CATALOG_CLIENT_MANAGER_H_
