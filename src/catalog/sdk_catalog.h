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

#ifndef SRC_CATALOG_SDK_CATALOG_H_
#define SRC_CATALOG_SDK_CATALOG_H_

#include <map>
#include <memory>
#include <mutex> // NOLINT
#include <string>
#include <utility>
#include <vector>

#include "base/spinlock.h"
#include "catalog/base.h"
#include "catalog/client_manager.h"
#include "client/tablet_client.h"
#include "proto/name_server.pb.h"
#include "vm/catalog.h"

namespace openmldb {
namespace catalog {

class SDKTableHandler : public ::hybridse::vm::TableHandler {
 public:
    SDKTableHandler(const ::openmldb::nameserver::TableInfo& meta, const ClientManager& client_manager);

    bool Init();

    const ::hybridse::vm::Schema* GetSchema() override { return schema_map_.rbegin()->second.get(); }

    const ::hybridse::vm::Schema* GetSchema(int version) {
        auto iter = schema_map_.find(version);
        if (iter == schema_map_.end()) {
            return nullptr;
        }
        return iter->second.get();
    }

    const std::string& GetName() override { return name_; }

    const std::string& GetDatabase() override { return db_; }

    const ::hybridse::vm::Types& GetTypes() override { return types_; }

    const ::hybridse::vm::IndexHint& GetIndex() override { return index_hint_; }

    std::unique_ptr<::hybridse::codec::RowIterator> GetIterator() override {
        return std::unique_ptr<::hybridse::codec::RowIterator>();
    }

    ::hybridse::codec::RowIterator* GetRawIterator() override { return nullptr; }

    std::unique_ptr<::hybridse::codec::WindowIterator> GetWindowIterator(const std::string& idx_name) override {
        return std::unique_ptr<::hybridse::codec::WindowIterator>();
    }

    const uint64_t GetCount() override { return cnt_; }

    ::hybridse::codec::Row At(uint64_t pos) override { return ::hybridse::codec::Row(); }

    std::shared_ptr<::hybridse::vm::PartitionHandler> GetPartition(const std::string& index_name) override {
        return std::shared_ptr<::hybridse::vm::PartitionHandler>();
    }

    const std::string GetHandlerTypeName() override { return "TabletTableHandler"; }

    std::shared_ptr<::hybridse::vm::Tablet> GetTablet(const std::string& index_name, const std::string& pk) override;

    std::shared_ptr<TabletAccessor> GetTablet(uint32_t pid);

    bool GetTablet(std::vector<std::shared_ptr<TabletAccessor>>* tablets);

    inline uint32_t GetTid() const { return meta_.tid(); }

    inline uint32_t GetPartitionNum() const { return meta_.table_partition_size(); }

    inline int32_t GetColumnIndex(const std::string& column) {
        auto it = types_.find(column);
        if (it != types_.end()) {
            return it->second.idx;
        }
        return -1;
    }

 private:
    ::openmldb::nameserver::TableInfo meta_;
    std::map<int, std::shared_ptr<::hybridse::vm::Schema>> schema_map_;
    std::string name_;
    std::string db_;
    ::hybridse::vm::Types types_;
    ::hybridse::vm::IndexHint index_hint_;
    uint64_t cnt_;
    std::shared_ptr<TableClientManager> table_client_manager_;
};

typedef std::map<std::string, std::map<std::string, std::shared_ptr<SDKTableHandler>>> SDKTables;
typedef std::map<std::string, std::shared_ptr<::hybridse::type::Database>> SDKDB;
typedef std::map<std::string, std::map<std::string, std::shared_ptr<::hybridse::sdk::ProcedureInfo>>> Procedures;

class SDKCatalog : public ::hybridse::vm::Catalog {
 public:
    explicit SDKCatalog(std::shared_ptr<ClientManager> client_manager)
        : tables_(), db_(), client_manager_(client_manager), db_sp_map_() {}

    ~SDKCatalog() {}

    bool Init(const std::vector<::openmldb::nameserver::TableInfo>& tables, const Procedures& db_sp_map);

    std::shared_ptr<::hybridse::type::Database> GetDatabase(const std::string& db) override {
        return std::shared_ptr<::hybridse::type::Database>();
    }

    std::shared_ptr<::hybridse::vm::TableHandler> GetTable(const std::string& db,
                                                           const std::string& table_name) override;

    bool IndexSupport() override { return true; }

    std::shared_ptr<TabletAccessor> GetTablet() const;

    std::vector<std::shared_ptr<TabletAccessor>> GetAllTablet() const;

    std::shared_ptr<::hybridse::sdk::ProcedureInfo> GetProcedureInfo(const std::string& db,
                                                                     const std::string& sp_name) override;

    const Procedures& GetProcedures() { return db_sp_map_; }

 private:
    SDKTables tables_;
    SDKDB db_;
    std::shared_ptr<ClientManager> client_manager_;
    Procedures db_sp_map_;
};

}  // namespace catalog
}  // namespace openmldb
#endif  // SRC_CATALOG_SDK_CATALOG_H_
