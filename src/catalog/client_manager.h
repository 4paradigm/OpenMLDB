/*
 * client_manager.h
 * Copyright (C) 4paradigm.com 2020
 * Author denglong
 * Date 2020-09-14
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

#ifndef SRC_CATALOG_CLIENT_MANAGER_H_
#define SRC_CATALOG_CLIENT_MANAGER_H_

#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "base/random.h"
#include "base/spinlock.h"
#include "client/tablet_client.h"
#include "storage/schema.h"
#include "vm/catalog.h"

namespace rtidb {
namespace catalog {

using TablePartitions = ::google::protobuf::RepeatedPtrField<::rtidb::nameserver::TablePartition>;

class TabletRowHandler : public ::fesql::vm::RowHandler {
 public:
    TabletRowHandler(const std::string& db, std::unique_ptr<brpc::Controller> cntl,
                     std::unique_ptr<::rtidb::api::QueryResponse> response);
    explicit TabletRowHandler(::fesql::base::Status status);
    const ::fesql::vm::Schema* GetSchema() override { return nullptr; }
    const std::string& GetName() override { return name_; }
    const std::string& GetDatabase() override { return db_; }

    ::fesql::base::Status GetStatus() override { return status_; }
    const ::fesql::codec::Row& GetValue() override;

 private:
    std::string db_;
    std::string name_;
    ::fesql::base::Status status_;
    std::string buf_;
    ::fesql::codec::Row row_;
    std::unique_ptr<brpc::Controller> cntl_;
    std::unique_ptr<::rtidb::api::QueryResponse> response_;
};

class TabletAccessor : public ::fesql::vm::Tablet {
 public:
    explicit TabletAccessor(const std::string& name) : name_(name), tablet_client_() {}

    TabletAccessor(const std::string& name, const std::shared_ptr<::rtidb::client::TabletClient>& client)
        : name_(name), tablet_client_(client) {}

    std::shared_ptr<::rtidb::client::TabletClient> GetClient() {
        return std::atomic_load_explicit(&tablet_client_, std::memory_order_relaxed);
    }

    bool UpdateClient(const std::string& endpoint) {
        auto client = std::make_shared<::rtidb::client::TabletClient>(name_, endpoint);
        if (client->Init() != 0) {
            return false;
        }
        std::atomic_store_explicit(&tablet_client_, client, std::memory_order_relaxed);
        return true;
    }

    bool UpdateClient(const std::shared_ptr<::rtidb::client::TabletClient>& client) {
        std::atomic_store_explicit(&tablet_client_, client, std::memory_order_relaxed);
        return true;
    }

    std::shared_ptr<::fesql::vm::RowHandler> SubQuery(uint32_t task_id, const std::string& db, const std::string& sql,
                                                      const ::fesql::codec::Row& row) override;

    std::shared_ptr<::fesql::vm::RowHandler> SubQuery(uint32_t task_id, const std::string& db, const std::string& sql,
                                                      const std::vector<::fesql::codec::Row>& row) override;
    const std::string& GetName() const { return name_; }

 private:
    std::string name_;
    std::shared_ptr<::rtidb::client::TabletClient> tablet_client_;
};

class PartitionClientManager {
 public:
    PartitionClientManager(uint32_t pid, const std::shared_ptr<TabletAccessor>& leader,
                           const std::vector<std::shared_ptr<TabletAccessor>>& followers);

    inline std::shared_ptr<TabletAccessor> GetLeader() const { return leader_; }

    std::shared_ptr<TabletAccessor> GetFollower();

 private:
    uint32_t pid_;
    std::shared_ptr<TabletAccessor> leader_;
    std::vector<std::shared_ptr<TabletAccessor>> followers_;
    ::rtidb::base::Random rand_;
};

class ClientManager;

class TableClientManager {
 public:
    TableClientManager(const TablePartitions& partitions, const ClientManager& client_manager);

    TableClientManager(const ::rtidb::storage::TableSt& table_st, const ClientManager& client_manager);

    void Show() const {
        DLOG(INFO) << "show client manager ";
        for (auto id = 0; id < partition_managers_.size(); id++) {
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
    std::shared_ptr<PartitionClientManager> GetPartitionClientManager(uint32_t pid) const {
        Show();
        if (pid < partition_managers_.size()) {
            return std::atomic_load_explicit(&partition_managers_[pid], std::memory_order_relaxed);
        }
        DLOG(INFO) << "GetPartitionClientManager pid " << pid << " pid > partition manager size";
        return std::shared_ptr<PartitionClientManager>();
    }

    bool UpdatePartitionClientManager(const ::rtidb::storage::PartitionSt& partition,
                                      const ClientManager& client_manager);

    std::shared_ptr<TabletAccessor> GetTablet(uint32_t pid) const {
        DLOG(INFO) << "TableClientManager GetTablet pid = " << pid;
        auto partition_manager = GetPartitionClientManager(pid);
        if (partition_manager) {
            return partition_manager->GetLeader();
        }
        DLOG(INFO) << "partition manager is null with pid " << pid;
        return std::shared_ptr<TabletAccessor>();
    }

 private:
    std::vector<std::shared_ptr<PartitionClientManager>> partition_managers_;
};

class ClientManager {
 public:
    ClientManager() : real_endpoint_map_(), clients_(), mu_(), rand_(0xdeadbeef) {}
    std::shared_ptr<TabletAccessor> GetTablet(const std::string& name) const;
    std::shared_ptr<TabletAccessor> GetTablet() const;

    bool UpdateClient(const std::map<std::string, std::string>& real_ep_map);

    bool UpdateClient(const std::map<std::string, std::shared_ptr<::rtidb::client::TabletClient>>& tablet_clients);

 private:
    std::unordered_map<std::string, std::string> real_endpoint_map_;
    std::unordered_map<std::string, std::shared_ptr<TabletAccessor>> clients_;
    mutable ::rtidb::base::SpinMutex mu_;
    mutable ::rtidb::base::Random rand_;
};

}  // namespace catalog
}  // namespace rtidb
#endif  // SRC_CATALOG_CLIENT_MANAGER_H_
