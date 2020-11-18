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
#include "sdk/base.h"
#include "sdk/base_impl.h"

namespace rtidb {
namespace catalog {

class SDKTableHandler : public ::fesql::vm::TableHandler {
 public:
    SDKTableHandler(const ::rtidb::nameserver::TableInfo& meta,
            const ClientManager& client_manager);

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

    std::shared_ptr<::fesql::vm::Tablet> GetTablet(const std::string& index_name, const std::string& pk) override;

    std::shared_ptr<TabletAccessor> GetTablet(uint32_t pid);

    bool GetTablet(std::vector<std::shared_ptr<TabletAccessor>>* tablets);

    inline uint32_t GetTid() const { return meta_.tid(); }

    inline uint32_t GetPartitionNum() const { return meta_.table_partition_size(); }

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
    std::shared_ptr<TableClientManager> table_client_manager_;
};

typedef std::map<std::string,
                 std::map<std::string, std::shared_ptr<SDKTableHandler>>>
    SDKTables;
typedef std::map<std::string, std::shared_ptr<::fesql::type::Database>> SDKDB;
typedef std::map<std::string,
        std::map<std::string, std::shared_ptr<::fesql::sdk::ProcedureInfo>>> Procedures;

class SDKCatalog : public ::fesql::vm::Catalog {
 public:
    explicit SDKCatalog(std::shared_ptr<ClientManager> client_manager) :
        tables_(), db_(), client_manager_(client_manager) {}

    ~SDKCatalog() {}

    bool Init(const std::vector<::rtidb::nameserver::TableInfo>& tables,
            const Procedures& sp_map);

    std::shared_ptr<::fesql::type::Database> GetDatabase(const std::string& db) override {
        return std::shared_ptr<::fesql::type::Database>();
    }

    std::shared_ptr<::fesql::vm::TableHandler> GetTable(
        const std::string& db, const std::string& table_name) override;

    bool IndexSupport() override { return true; }

    std::shared_ptr<TabletAccessor> GetTablet() const;

    const std::shared_ptr<::fesql::sdk::ProcedureInfo> GetProcedureInfo(const std::string& db,
            const std::string& sp_name) override;

    const Procedures& GetProcedures() { return sp_map_; }

 private:
    SDKTables tables_;
    SDKDB db_;
    std::shared_ptr<ClientManager> client_manager_;
    Procedures sp_map_;
};

class ProcedureInfoImpl : public fesql::sdk::ProcedureInfo {
 public:
     ProcedureInfoImpl() {}
     ProcedureInfoImpl(const std::string& db_name, const std::string& sp_name,
             const std::string& sql,
             const ::fesql::sdk::SchemaImpl& input_schema,
             const ::fesql::sdk::SchemaImpl& output_schema,
             const std::vector<std::string>& tables,
             const std::string& main_table)
        : db_name_(db_name),
          sp_name_(sp_name),
          sql_(sql),
          input_schema_(input_schema),
          output_schema_(output_schema),
          tables_(tables),
          main_table_(main_table) {}

    ~ProcedureInfoImpl() {}

    const ::fesql::sdk::Schema& GetInputSchema() const { return input_schema_; }

    const ::fesql::sdk::Schema& GetOutputSchema() const { return output_schema_; }

    const std::string& GetDbName() const { return db_name_; }

    const std::string& GetSpName() const { return sp_name_; }

    const std::string& GetSql() const { return sql_; }

    const std::vector<std::string>& GetTables() const { return tables_; }

    const std::string& GetMainTable() const { return main_table_; }

 private:
    std::string db_name_;
    std::string sp_name_;
    std::string sql_;
    ::fesql::sdk::SchemaImpl input_schema_;
    ::fesql::sdk::SchemaImpl output_schema_;
    std::vector<std::string> tables_;
    std::string main_table_;
};

}  // namespace catalog
}  // namespace rtidb
#endif  // SRC_CATALOG_SDK_CATALOG_H_
