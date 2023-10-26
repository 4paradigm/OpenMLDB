/*
 * Copyright 2021 4Paradigm
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

#ifndef HYBRIDSE_EXAMPLES_TOYDB_SRC_TABLET_TABLET_CATALOG_H_
#define HYBRIDSE_EXAMPLES_TOYDB_SRC_TABLET_TABLET_CATALOG_H_

#include <map>
#include <memory>
#include <string>
#include <vector>
#include "storage/table_impl.h"
#include "vm/catalog.h"

namespace hybridse {
namespace tablet {

using codec::Row;
using vm::OrderType;
using vm::PartitionHandler;
using vm::RowIterator;
using vm::TableHandler;

class TabletPartitionHandler;
class TabletTableHandler;
class TabletSegmentHandler;

class TabletSegmentHandler : public TableHandler {
 public:
    TabletSegmentHandler(std::shared_ptr<PartitionHandler> partition_hander,
                         const std::string& key);

    ~TabletSegmentHandler();

    const vm::Schema* GetSchema() override {
        return partition_hander_->GetSchema();
    }

    const std::string& GetName() override { return partition_hander_->GetName(); }

    const std::string& GetDatabase() override {
        return partition_hander_->GetDatabase();
    }

    const vm::Types& GetTypes() override { return partition_hander_->GetTypes(); }

    const vm::IndexHint& GetIndex() override {
        return partition_hander_->GetIndex();
    }

    const OrderType GetOrderType() const override {
        return partition_hander_->GetOrderType();
    }

    std::unique_ptr<vm::RowIterator> GetIterator() override;
    RowIterator* GetRawIterator() override;
    std::unique_ptr<codec::WindowIterator> GetWindowIterator(const std::string& idx_name) override;
    const std::string GetHandlerTypeName() override {
        return "TabletSegmentHandler";
    }

 private:
    std::shared_ptr<vm::PartitionHandler> partition_hander_;
    std::string key_;
};

class TabletPartitionHandler final
    : public PartitionHandler,
      public std::enable_shared_from_this<PartitionHandler> {
 public:
    TabletPartitionHandler(std::shared_ptr<TableHandler> table_hander,
                           const std::string& index_name)
        : PartitionHandler(),
          table_handler_(table_hander),
          index_name_(index_name) {}

    ~TabletPartitionHandler() {}

    RowIterator* GetRawIterator() override { return nullptr; }

    const OrderType GetOrderType() const override { return OrderType::kDescOrder; }

    const vm::Schema* GetSchema() override { return table_handler_->GetSchema(); }

    const std::string& GetName() override { return table_handler_->GetName(); }

    const std::string& GetDatabase() override { return table_handler_->GetDatabase(); }

    const vm::Types& GetTypes() override { return table_handler_->GetTypes(); }
    const vm::IndexHint& GetIndex() override { return index_hint_; }
    std::unique_ptr<codec::WindowIterator> GetWindowIterator() override {
        return table_handler_->GetWindowIterator(index_name_);
    }

    const uint64_t GetCount() override;

    std::shared_ptr<TableHandler> GetSegment(const std::string& key) override {
        return std::make_shared<TabletSegmentHandler>(shared_from_this(), key);
    }
    const std::string GetHandlerTypeName() override {
        return "TabletPartitionHandler";
    }

 private:
    std::shared_ptr<TableHandler> table_handler_;
    std::string index_name_;
    vm::IndexHint index_hint_;
};

class TabletTableHandler final
    : public vm::TableHandler,
      public std::enable_shared_from_this<vm::TableHandler> {
 public:
    TabletTableHandler(const vm::Schema schema, const std::string& name,
                       const std::string& db, const vm::IndexList& index_list,
                       std::shared_ptr<storage::Table> table);
    TabletTableHandler(const vm::Schema schema, const std::string& name,
                       const std::string& db, const vm::IndexList& index_list,
                       std::shared_ptr<storage::Table> table,
                       std::shared_ptr<hybridse::vm::Tablet> tablet);

    ~TabletTableHandler();

    bool Init();

    const vm::Schema* GetSchema() override { return &schema_; }

    const std::string& GetName() override { return name_; }

    const std::string& GetDatabase() override { return db_; }

    const vm::Types& GetTypes() override { return types_; }

    const vm::IndexHint& GetIndex() override { return index_hint_; }

    const Row Get(int32_t pos);

    std::shared_ptr<storage::Table> GetTable() { return table_; }
    RowIterator* GetRawIterator() override;
    std::unique_ptr<codec::WindowIterator> GetWindowIterator(const std::string& idx_name) override;

    std::shared_ptr<PartitionHandler> GetPartition(const std::string& index_name) override {
        if (index_hint_.find(index_name) == index_hint_.cend()) {
            LOG(WARNING)
                << "fail to get partition for tablet table handler, index name "
                << index_name;
            return std::shared_ptr<PartitionHandler>();
        }
        return std::shared_ptr<TabletPartitionHandler>(
            new TabletPartitionHandler(shared_from_this(), index_name));
    }
    const std::string GetHandlerTypeName() override {
        return "TabletTableHandler";
    }
    std::shared_ptr<hybridse::vm::Tablet> GetTablet(const std::string& index_name,
                                                            const std::string& pk) override {
        return tablet_;
    }
    std::shared_ptr<hybridse::vm::Tablet> GetTablet(const std::string& index_name,
                                                    const std::vector<std::string>& pks) override {
        return tablet_;
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
    vm::Schema schema_;
    std::string name_;
    std::string db_;
    std::shared_ptr<storage::Table> table_;

    vm::Types types_;
    vm::IndexList index_list_;
    vm::IndexHint index_hint_;
    std::shared_ptr<hybridse::vm::Tablet> tablet_;
};

typedef std::map<std::string,
                 std::map<std::string, std::shared_ptr<TabletTableHandler>>>
    TabletTables;
typedef std::map<std::string, std::shared_ptr<type::Database>> TabletDB;

class TabletCatalog : public vm::Catalog {
 public:
    TabletCatalog();

    ~TabletCatalog();

    bool Init();

    bool AddDB(const type::Database& db);

    bool AddTable(std::shared_ptr<TabletTableHandler> table);

    std::shared_ptr<type::Database> GetDatabase(const std::string& db) override;

    std::shared_ptr<vm::TableHandler> GetTable(const std::string& db, const std::string& table_name) override;

    bool IndexSupport() override;

    std::vector<vm::AggrTableInfo> GetAggrTables(const std::string& base_db, const std::string& base_table,
                                                 const std::string& aggr_func, const std::string& aggr_col,
                                                 const std::string& partition_cols, const std::string& order_col,
                                                 const std::string& filter_col) override {
        vm::AggrTableInfo info = {"aggr_" + base_table, "aggr_db", base_db, base_table, aggr_func, aggr_col,
                                  partition_cols,       order_col, "1000",  filter_col};
        return {info};
    }

 private:
    TabletTables tables_;
    TabletDB db_;
};

}  // namespace tablet
}  // namespace hybridse
#endif  // HYBRIDSE_EXAMPLES_TOYDB_SRC_TABLET_TABLET_CATALOG_H_
