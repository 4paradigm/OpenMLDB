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
#include "proto/name_server.pb.h"
#include "vm/catalog.h"

namespace rtidb {
namespace catalog {

class SDKTableHandler : public ::fesql::vm::TableHandler {
 public:
    explicit SDKTableHandler(const ::rtidb::nameserver::TableInfo& meta);

    ~SDKTableHandler();

    bool Init();

    inline const ::fesql::vm::Schema* GetSchema() { return &schema_; }

    inline const std::string& GetName() { return name_; }

    inline const std::string& GetDatabase() { return db_; }

    inline const ::fesql::vm::Types& GetTypes() { return types_; }

    inline const ::fesql::vm::IndexHint& GetIndex() { return index_hint_; }

    inline const ::fesql::codec::Row Get(int32_t pos) {
        return ::fesql::codec::Row();
    }

    inline std::unique_ptr<::fesql::codec::RowIterator> GetIterator() const {
        return std::move(std::unique_ptr<::fesql::codec::RowIterator>());
    }

    inline ::fesql::codec::RowIterator* GetIterator(
        int8_t* addr) const override {
        return nullptr;
    }

    inline std::unique_ptr<::fesql::codec::WindowIterator> GetWindowIterator(
        const std::string& idx_name) {
        return std::move(std::unique_ptr<::fesql::codec::WindowIterator>());
    }

    virtual const uint64_t GetCount() { return cnt_; }

    inline ::fesql::codec::Row At(uint64_t pos) override {
        return ::fesql::codec::Row();
    }

    virtual std::shared_ptr<::fesql::vm::PartitionHandler> GetPartition(
        std::shared_ptr<::fesql::vm::TableHandler> table_hander,
        const std::string& index_name) const {
        return std::shared_ptr<::fesql::vm::PartitionHandler>();
    }

    inline const std::string GetHandlerTypeName() override {
        return "TabletTableHandler";
    }

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
};

typedef std::map<std::string,
                 std::map<std::string, std::shared_ptr<SDKTableHandler>>>
    SDKTables;
typedef std::map<std::string, std::shared_ptr<::fesql::type::Database>> SDKDB;

class SDKCatalog : public ::fesql::vm::Catalog {
 public:
    SDKCatalog() : mu_(), table_metas_(), tables_(), db_() {}

    ~SDKCatalog() {}

    bool Init(const std::vector<::rtidb::nameserver::TableInfo>& tables);

    bool Refresh(const std::vector<::rtidb::nameserver::TableInfo>& tables) {
        return false;
    }

    std::shared_ptr<::fesql::type::Database> GetDatabase(
        const std::string& db) {
        return std::shared_ptr<::fesql::type::Database>();
    }

    std::shared_ptr<::fesql::vm::TableHandler> GetTable(
        const std::string& db, const std::string& table_name);

    inline bool IndexSupport() override { return true; }

 private:
    ::rtidb::base::SpinMutex mu_;
    std::vector<::rtidb::nameserver::TableInfo> table_metas_;
    SDKTables tables_;
    SDKDB db_;
};

}  // namespace catalog
}  // namespace rtidb
#endif  // SRC_CATALOG_SDK_CATALOG_H_
