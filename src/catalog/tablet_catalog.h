/*
 * tablet_catalog.h
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

#ifndef SRC_CATALOG_TABLET_CATALOG_H_
#define SRC_CATALOG_TABLET_CATALOG_H_

#include <map>
#include <memory>
#include <string>
#include <utility>

#include "base/spinlock.h"
#include "codec/row.h"
#include "storage/table.h"
#include "vm/catalog.h"

namespace rtidb {
namespace catalog {

class TabletPartitionHandler;
class TabletTableHandler;
class TabletSegmentHandler;

class TabletSegmentHandler : public ::fesql::vm::TableHandler {
 public:
    TabletSegmentHandler(
        std::shared_ptr<::fesql::vm::PartitionHandler> partition_handler,
        const std::string &key)
        : TableHandler(), partition_handler_(partition_handler), key_(key) {}

    ~TabletSegmentHandler() {}

    inline const ::fesql::vm::Schema *GetSchema() {
        return partition_handler_->GetSchema();
    }

    inline const std::string &GetName() {
        return partition_handler_->GetName();
    }

    inline const std::string &GetDatabase() {
        return partition_handler_->GetDatabase();
    }

    inline const ::fesql::vm::Types &GetTypes() {
        return partition_handler_->GetTypes();
    }

    inline const ::fesql::vm::IndexHint &GetIndex() {
        return partition_handler_->GetIndex();
    }

    const ::fesql::vm::OrderType GetOrderType() const override {
        return partition_handler_->GetOrderType();
    }

    inline std::unique_ptr<::fesql::vm::RowIterator> GetIterator() const {
        auto iter = partition_handler_->GetWindowIterator();
        if (iter) {
            DLOG(INFO) << "seek to pk " << key_;
            iter->Seek(key_);
            if (iter->Valid() &&
                0 == iter->GetKey().compare(fesql::codec::Row(key_))) {
                return std::move(iter->GetValue());
            } else {
                return std::unique_ptr<::fesql::vm::RowIterator>();
            }
        }
        return std::unique_ptr<::fesql::vm::RowIterator>();
    }
    ::fesql::vm::RowIterator *GetIterator(int8_t *addr) const override {
        LOG(WARNING) << "can't get iterator with given address";
        return nullptr;
    }
    inline std::unique_ptr<::fesql::vm::WindowIterator> GetWindowIterator(
        const std::string &idx_name) {
        return std::unique_ptr<::fesql::vm::WindowIterator>();
    }
    virtual const uint64_t GetCount() {
        auto iter = GetIterator();
        if (!iter) return 0;
        uint64_t cnt = 0;
        while (iter->Valid()) {
            cnt++;
            iter->Next();
        }
        return cnt;
    }
    ::fesql::vm::Row At(uint64_t pos) override {
        auto iter = GetIterator();
        if (!iter) return ::fesql::vm::Row();
        while (pos-- > 0 && iter->Valid()) {
            iter->Next();
        }
        return iter->Valid() ? iter->GetValue() : ::fesql::vm::Row();
    }
    const std::string GetHandlerTypeName() override {
        return "TabletSegmentHandler";
    }

 private:
    std::shared_ptr<::fesql::vm::PartitionHandler> partition_handler_;
    std::string key_;
};

class TabletPartitionHandler : public ::fesql::vm::PartitionHandler {
 public:
    TabletPartitionHandler(
        std::shared_ptr<::fesql::vm::TableHandler> table_hander,
        const std::string &index_name)
        : PartitionHandler(),
          table_handler_(table_hander),
          index_name_(index_name) {}

    ~TabletPartitionHandler() {}

    const ::fesql::vm::OrderType GetOrderType() const {
        return ::fesql::vm::OrderType::kDescOrder;
    }

    inline const ::fesql::vm::Schema *GetSchema() {
        return table_handler_->GetSchema();
    }

    inline const std::string &GetName() { return table_handler_->GetName(); }

    inline const std::string &GetDatabase() {
        return table_handler_->GetDatabase();
    }

    inline const ::fesql::vm::Types &GetTypes() {
        return table_handler_->GetTypes();
    }

    inline const ::fesql::vm::IndexHint &GetIndex() { return table_handler_->GetIndex(); }

    std::unique_ptr<::fesql::vm::WindowIterator> GetWindowIterator() override {
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
    virtual std::shared_ptr<::fesql::vm::TableHandler> GetSegment(
        std::shared_ptr<::fesql::vm::PartitionHandler> partition_hander,
        const std::string &key) {
        return std::shared_ptr<TabletSegmentHandler>(
            new TabletSegmentHandler(partition_hander, key));
    }
    const std::string GetHandlerTypeName() override {
        return "TabletPartitionHandler";
    }

 private:
    std::shared_ptr<::fesql::vm::TableHandler> table_handler_;
    std::string index_name_;
};

class TabletTableHandler : public ::fesql::vm::TableHandler {
 public:
    TabletTableHandler(const ::rtidb::api::TableMeta &meta,
                       const std::string &db,
                       std::shared_ptr<::rtidb::storage::Table> table);

    ~TabletTableHandler();

    bool Init();

    inline const ::fesql::vm::Schema *GetSchema() { return &schema_; }

    inline const std::string &GetName() { return name_; }

    inline const std::string &GetDatabase() { return db_; }

    inline const ::fesql::vm::Types &GetTypes() { return types_; }

    inline const ::fesql::vm::IndexHint &GetIndex() { return index_hint_; }

    const ::fesql::codec::Row Get(int32_t pos);

    inline std::shared_ptr<storage::Table> GetTable() { return table_; }

    std::unique_ptr<::fesql::codec::RowIterator> GetIterator() const;

    ::fesql::codec::RowIterator *GetIterator(int8_t *addr) const override;

    std::unique_ptr<::fesql::codec::WindowIterator> GetWindowIterator(
        const std::string &idx_name);

    virtual const uint64_t GetCount();

    ::fesql::codec::Row At(uint64_t pos) override;

    virtual std::shared_ptr<::fesql::vm::PartitionHandler> GetPartition(
        std::shared_ptr<::fesql::vm::TableHandler> table_hander,
        const std::string &index_name) const {
        if (!table_hander) {
            LOG(WARNING) << "fail to get partition for tablet table handler: "
                            "table handler is null";
            return std::shared_ptr<::fesql::vm::PartitionHandler>();
        }
        if (table_hander->GetIndex().find(index_name) ==
            table_hander->GetIndex().cend()) {
            LOG(WARNING)
                << "fail to get partition for tablet table handler, index name "
                << index_name;
            return std::shared_ptr<::fesql::vm::PartitionHandler>();
        }
        return std::shared_ptr<TabletPartitionHandler>(
            new TabletPartitionHandler(table_hander, index_name));
    }

    const std::string GetHandlerTypeName() override {
        return "TabletTableHandler";
    }

    inline int32_t GetTid() {
        return meta_.tid();
    }

 private:
    inline int32_t GetColumnIndex(const std::string &column) {
        auto it = types_.find(column);
        if (it != types_.end()) {
            return it->second.idx;
        }
        return -1;
    }

 private:
    ::rtidb::api::TableMeta meta_;
    ::fesql::vm::Schema schema_;
    std::string name_;
    std::string db_;
    std::shared_ptr<::rtidb::storage::Table> table_;
    ::fesql::vm::Types types_;
    ::fesql::vm::IndexList index_list_;
    ::fesql::vm::IndexHint index_hint_;
};

typedef std::map<std::string,
                 std::map<std::string, std::shared_ptr<TabletTableHandler>>>
    TabletTables;
typedef std::map<std::string, std::shared_ptr<::fesql::type::Database>>
    TabletDB;

class TabletCatalog : public ::fesql::vm::Catalog {
 public:
    TabletCatalog();

    ~TabletCatalog();

    bool Init();

    bool AddDB(const ::fesql::type::Database &db);

    bool AddTable(std::shared_ptr<TabletTableHandler> table);

    std::shared_ptr<::fesql::type::Database> GetDatabase(const std::string &db);

    std::shared_ptr<::fesql::vm::TableHandler> GetTable(
        const std::string &db, const std::string &table_name);

    bool IndexSupport() override;

    bool DeleteTable(const std::string &db, const std::string &table_name);

    bool DeleteDB(const std::string &db);

 private:
    ::rtidb::base::SpinMutex mu_;
    TabletTables tables_;
    TabletDB db_;
};

}  // namespace catalog
}  // namespace rtidb
#endif  // SRC_CATALOG_TABLET_CATALOG_H_
