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

#ifndef SRC_CATALOG_TABLET_CATALOG_H_
#define SRC_CATALOG_TABLET_CATALOG_H_

#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include <unordered_map>

#include "base/spinlock.h"
#include "catalog/client_manager.h"
#include "catalog/distribute_iterator.h"
#include "client/tablet_client.h"
#include "codec/row.h"
#include "storage/schema.h"
#include "storage/table.h"
#include "sdk/sql_cluster_router.h"

namespace openmldb {
namespace catalog {

class TabletPartitionHandler;
class TabletTableHandler;
class TabletSegmentHandler;

class TabletSegmentHandler : public ::hybridse::vm::TableHandler {
 public:
    TabletSegmentHandler(std::shared_ptr<::hybridse::vm::PartitionHandler> partition_handler, const std::string &key)
        : TableHandler(), partition_handler_(partition_handler), key_(key) {}

    ~TabletSegmentHandler() {}

    const ::hybridse::vm::Schema *GetSchema() override { return partition_handler_->GetSchema(); }

    const std::string &GetName() override { return partition_handler_->GetName(); }

    const std::string &GetDatabase() override { return partition_handler_->GetDatabase(); }

    const ::hybridse::vm::Types &GetTypes() override { return partition_handler_->GetTypes(); }

    const ::hybridse::vm::IndexHint &GetIndex() override { return partition_handler_->GetIndex(); }

    const ::hybridse::vm::OrderType GetOrderType() const override { return partition_handler_->GetOrderType(); }

    std::unique_ptr<::hybridse::vm::RowIterator> GetIterator() override;

    ::hybridse::vm::RowIterator *GetRawIterator() override;

    std::unique_ptr<::hybridse::codec::WindowIterator> GetWindowIterator(const std::string &idx_name) override {
        return std::unique_ptr<::hybridse::codec::WindowIterator>();
    }

    const uint64_t GetCount() override;

    ::hybridse::vm::Row At(uint64_t pos) override {
        auto iter = GetIterator();
        if (!iter) return ::hybridse::vm::Row();
        while (pos-- > 0 && iter->Valid()) {
            iter->Next();
        }
        return iter->Valid() ? iter->GetValue() : ::hybridse::vm::Row();
    }
    const std::string GetHandlerTypeName() override { return "TabletSegmentHandler"; }

 private:
    std::shared_ptr<::hybridse::vm::PartitionHandler> partition_handler_;
    std::string key_;
};

class TabletPartitionHandler : public ::hybridse::vm::PartitionHandler,
                               public std::enable_shared_from_this<hybridse::vm::PartitionHandler> {
 public:
    TabletPartitionHandler(std::shared_ptr<::hybridse::vm::TableHandler> table_hander, const std::string &index_name)
        : PartitionHandler(), table_handler_(table_hander), index_name_(index_name) {}

    ~TabletPartitionHandler() {}

    const ::hybridse::vm::OrderType GetOrderType() const override { return ::hybridse::vm::OrderType::kDescOrder; }

    const ::hybridse::vm::Schema *GetSchema() override { return table_handler_->GetSchema(); }

    const std::string &GetName() override { return table_handler_->GetName(); }

    const std::string &GetDatabase() override { return table_handler_->GetDatabase(); }

    const ::hybridse::vm::Types &GetTypes() override { return table_handler_->GetTypes(); }

    const ::hybridse::vm::IndexHint &GetIndex() override { return table_handler_->GetIndex(); }

    std::unique_ptr<::hybridse::codec::WindowIterator> GetWindowIterator() override {
        DLOG(INFO) << "get window it with name " << index_name_;
        return table_handler_->GetWindowIterator(index_name_);
    }

    const uint64_t GetCount() override {
        auto iter = GetWindowIterator();
        if (!iter) return 0;
        uint64_t cnt = 0;
        iter->SeekToFirst();
        while (iter->Valid()) {
            cnt++;
            iter->Next();
        }
        return cnt;
    }

    std::shared_ptr<::hybridse::vm::TableHandler> GetSegment(const std::string &key) override {
        return std::make_shared<TabletSegmentHandler>(shared_from_this(), key);
    }
    const std::string GetHandlerTypeName() override { return "TabletPartitionHandler"; }

 private:
    std::shared_ptr<::hybridse::vm::TableHandler> table_handler_;
    std::string index_name_;
};

class TabletTableHandler : public ::hybridse::vm::TableHandler,
                           public std::enable_shared_from_this<hybridse::vm::TableHandler> {
 public:
    explicit TabletTableHandler(const ::openmldb::api::TableMeta &meta,
                                std::shared_ptr<hybridse::vm::Tablet> local_tablet);

    explicit TabletTableHandler(const ::openmldb::nameserver::TableInfo &meta,
                                std::shared_ptr<hybridse::vm::Tablet> local_tablet);

    bool Init(const ClientManager &client_manager);

    // TODO(denglong): guarantee threadsafe
    bool UpdateIndex(const ::google::protobuf::RepeatedPtrField<::openmldb::common::ColumnKey>& indexs);

    const ::hybridse::vm::Schema *GetSchema() override { return &schema_; }

    const std::string &GetName() override { return table_st_.GetName(); }

    const std::string &GetDatabase() override { return table_st_.GetDB(); }

    const ::hybridse::vm::Types &GetTypes() override { return types_; }

    const ::hybridse::vm::IndexHint &GetIndex() override;

    const ::hybridse::codec::Row Get(int32_t pos);

    std::unique_ptr<::hybridse::codec::RowIterator> GetIterator() override;

    ::hybridse::codec::RowIterator *GetRawIterator() override;

    std::unique_ptr<::hybridse::codec::WindowIterator> GetWindowIterator(const std::string &idx_name) override;

    const uint64_t GetCount() override;

    ::hybridse::codec::Row At(uint64_t pos) override;

    std::shared_ptr<::hybridse::vm::PartitionHandler> GetPartition(const std::string &index_name) override;
    const std::string GetHandlerTypeName() override { return "TabletTableHandler"; }

    std::shared_ptr<::hybridse::vm::Tablet> GetTablet(const std::string &index_name, const std::string &pk) override;
    std::shared_ptr<::hybridse::vm::Tablet> GetTablet(const std::string &index_name,
                                                      const std::vector<std::string> &pks) override;

    inline int32_t GetTid() { return table_st_.GetTid(); }

    void AddTable(std::shared_ptr<::openmldb::storage::Table> table);

    bool HasLocalTable();

    int DeleteTable(uint32_t pid);

    bool Update(const ::openmldb::nameserver::TableInfo &meta, const ClientManager &client_manager,
            bool* index_updated);

 private:
    inline int32_t GetColumnIndex(const std::string &column) {
        auto it = types_.find(column);
        if (it != types_.end()) {
            return it->second.idx;
        }
        return -1;
    }

 private:
    uint32_t partition_num_;
    ::hybridse::vm::Schema schema_;
    ::openmldb::storage::TableSt table_st_;
    std::shared_ptr<Tables> tables_;
    ::hybridse::vm::Types types_;
    std::atomic<int32_t> index_pos_;
    std::vector<::hybridse::vm::IndexHint> index_hint_vec_;
    std::shared_ptr<TableClientManager> table_client_manager_;
    std::shared_ptr<hybridse::vm::Tablet> local_tablet_;
};

typedef std::map<std::string, std::map<std::string, std::shared_ptr<TabletTableHandler>>> TabletTables;
typedef std::map<std::string, std::shared_ptr<::hybridse::type::Database>> TabletDB;
typedef std::map<std::string, std::map<std::string, std::shared_ptr<::hybridse::sdk::ProcedureInfo>>> Procedures;

class TabletCatalog : public ::hybridse::vm::Catalog {
 public:
    TabletCatalog();

    ~TabletCatalog();

    bool Init();

    bool AddDB(const ::hybridse::type::Database &db);

    bool AddTable(const ::openmldb::api::TableMeta &meta, std::shared_ptr<::openmldb::storage::Table> table);

    bool UpdateTableMeta(const ::openmldb::api::TableMeta &meta);

    bool UpdateTableInfo(const ::openmldb::nameserver::TableInfo& table_info, bool* index_updated);

    std::shared_ptr<::hybridse::type::Database> GetDatabase(const std::string &db) override;

    std::shared_ptr<::hybridse::vm::TableHandler> GetTable(const std::string &db,
                                                           const std::string &table_name) override;

    bool IndexSupport() override;

    bool DeleteTable(const std::string &db, const std::string &table_name, uint32_t pid);

    bool DeleteDB(const std::string &db);

    void Refresh(const std::vector<::openmldb::nameserver::TableInfo> &table_info_vec, uint64_t version,
                 const Procedures &db_sp_map, bool* updated);

    bool AddProcedure(const std::string &db, const std::string &sp_name,
                      const std::shared_ptr<hybridse::sdk::ProcedureInfo> &sp_info);

    bool DropProcedure(const std::string &db, const std::string &sp_name);

    bool UpdateClient(const std::map<std::string, std::string> &real_ep_map);

    uint64_t GetVersion() const;

    void SetLocalTablet(std::shared_ptr<::hybridse::vm::Tablet> local_tablet) { local_tablet_ = local_tablet; }

    std::shared_ptr<::hybridse::sdk::ProcedureInfo> GetProcedureInfo(const std::string &db,
                                                                     const std::string &sp_name) override;

    const Procedures &GetProcedures();

    std::vector<::hybridse::vm::AggrTableInfo> GetAggrTables(const std::string &base_db, const std::string &base_table,
                                                             const std::string &aggr_func, const std::string &aggr_col,
                                                             const std::string &partition_cols,
                                                             const std::string &order_col,
                                                             const std::string &filter_col) override;

    void RefreshAggrTables(const std::vector<::hybridse::vm::AggrTableInfo>& entries);

 private:
    struct AggrTableKey {
        std::string base_db;
        std::string base_table;
        std::string aggr_func;
        std::string aggr_col;
        std::string partition_cols;
        std::string order_by_col;
        std::string filter_col;
    };

    struct AggrTableKeyHash {
        std::size_t operator()(const AggrTableKey& key) const {
            return std::hash<std::string>()(key.base_db + key.base_table + key.aggr_func + key.aggr_col +
                                            key.partition_cols + key.order_by_col + key.filter_col);
        }
    };

    struct AggrTableKeyEqual {
        std::size_t operator()(const AggrTableKey& lhs, const AggrTableKey& rhs) const {
            return lhs.base_db == rhs.base_db &&
                lhs.base_table == rhs.base_table &&
                lhs.aggr_func == rhs.aggr_func &&
                lhs.aggr_col == rhs.aggr_col &&
                lhs.partition_cols == rhs.partition_cols &&
                lhs.order_by_col == rhs.order_by_col &&
                lhs.filter_col == rhs.filter_col;
        }
    };

    using AggrTableMap = std::unordered_map<AggrTableKey,
                                            std::vector<::hybridse::vm::AggrTableInfo>,
                                            AggrTableKeyHash,
                                            AggrTableKeyEqual>;

    ::openmldb::base::SpinMutex mu_;
    TabletTables tables_;
    TabletDB db_;
    Procedures db_sp_map_;
    ClientManager client_manager_;
    std::atomic<uint64_t> version_;
    std::shared_ptr<::hybridse::vm::Tablet> local_tablet_;
    std::shared_ptr<AggrTableMap> aggr_tables_;
};

}  // namespace catalog
}  // namespace openmldb
#endif  // SRC_CATALOG_TABLET_CATALOG_H_
