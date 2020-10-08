/*
 * sdk_catalog.h
 * Copyright (C) 4paradigm.com 2020 wangtaize <wangtaize@4paradigm.com>
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

#ifndef SRC_CATALOG_SDK_CATALOG_H_
#define SRC_CATALOG_SDK_CATALOG_H_

#include <vector>
#include <map>
#include <utility>
#include <string>
#include <memory>
#include <mutex>
#include "base/spinlock.h"
#include "catalog/client_manager.h"
#include "client/tablet_client.h"
#include "proto/name_server.pb.h"
#include "vm/catalog.h"

namespace rtidb {
namespace catalog {

class SDKTableHandler : public ::fesql::vm::TableHandler {
 public:
    SDKTableHandler(const ::rtidb::nameserver::TableInfo& meta,
            const std::map<std::string, std::shared_ptr<::rtidb::client::TabletClient>>& tablet_clients);

    bool Init();

    const ::fesql::vm::Schema* GetSchema() override { return &schema_; }

    const std::string& GetName() override { return name_; }

    const std::string& GetDatabase() override { return db_; }

    const ::fesql::vm::Types& GetTypes() override { return types_; }

    const ::fesql::vm::IndexHint& GetIndex() override { return index_hint_; }

    std::unique_ptr<::fesql::codec::RowIterator> GetIterator() const override {
        return std::move(std::unique_ptr<::fesql::codec::RowIterator>());
    }

    ::fesql::codec::RowIterator* GetRawIterator() const override {
        return nullptr;
    }

    std::unique_ptr<::fesql::codec::WindowIterator> GetWindowIterator(
        const std::string& idx_name) override {
        return std::move(std::unique_ptr<::fesql::codec::WindowIterator>());
    }

    const uint64_t GetCount() override { return cnt_; }

    ::fesql::codec::Row At(uint64_t pos) override {
        return ::fesql::codec::Row();
    }

    std::shared_ptr<::fesql::vm::PartitionHandler> GetPartition(
        const std::string& index_name) override {
        return std::shared_ptr<::fesql::vm::PartitionHandler>();
    }

    const std::string GetHandlerTypeName() override {
        return "TabletTableHandler";
    }

    bool GetTablets(const std::string& index_name, const std::string& pk,
            std::vector<std::shared_ptr<::rtidb::client::TabletClient>>* tablets);

 private:
    inline int32_t GetColumnIndex(const std::string& column) {
        auto it = types_.find(column);
        if (it != types_.end()) {
            return it->second.idx;
        }
        return -1;
    }

 private:
    ::rtidb::nameserver::TableInfo meta_;
    ::fesql::vm::Schema schema_;
    std::string name_;
    std::string db_;
    ::fesql::vm::Types types_;
    ::fesql::vm::IndexList index_list_;
    ::fesql::vm::IndexHint index_hint_;
    uint64_t cnt_;
    TableClientManager table_client_manager_;
    std::vector<std::string> partition_key_;
};

typedef std::map<std::string,
                 std::map<std::string, std::shared_ptr<SDKTableHandler>>>
    SDKTables;
typedef std::map<std::string, std::shared_ptr<::fesql::type::Database>> SDKDB;

class SDKCatalog : public ::fesql::vm::Catalog {
 public:
    SDKCatalog() : table_metas_(), tables_(), db_() {}

    ~SDKCatalog() {}

    bool Init(const std::vector<::rtidb::nameserver::TableInfo>& tables,
        const std::map<std::string, std::shared_ptr<::rtidb::client::TabletClient>>& tablet_clients);

    std::shared_ptr<::fesql::type::Database> GetDatabase(const std::string& db) override {
        return std::shared_ptr<::fesql::type::Database>();
    }

    std::shared_ptr<::fesql::vm::TableHandler> GetTable(
        const std::string& db, const std::string& table_name) override;

    bool IndexSupport() override { return true; }

 private:
    std::vector<::rtidb::nameserver::TableInfo> table_metas_;
    SDKTables tables_;
    SDKDB db_;
};

}  // namespace catalog
}  // namespace rtidb
#endif  // SRC_CATALOG_SDK_CATALOG_H_
