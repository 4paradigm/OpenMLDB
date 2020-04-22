/*
 * catalog.h
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

#ifndef SRC_VM_CATALOG_H_
#define SRC_VM_CATALOG_H_
#include <map>
#include <memory>
#include <string>
#include <vector>
#include <utility>
#include "base/slice.h"
#include "codec/list_iterator_codec.h"
#include "proto/type.pb.h"

namespace fesql {
namespace vm {

using fesql::codec::IteratorV;
using fesql::codec::ListV;
using fesql::codec::Row;
using fesql::codec::RowIterator;
using fesql::codec::WindowIterator;

struct ColInfo {
    ::fesql::type::Type type;
    uint32_t pos;
    std::string name;
};

struct IndexSt {
    std::string name;
    uint32_t index;
    uint32_t ts_pos;
    std::vector<ColInfo> keys;
};

typedef ::google::protobuf::RepeatedPtrField<::fesql::type::ColumnDef> Schema;
typedef ::google::protobuf::RepeatedPtrField<::fesql::type::IndexDef> IndexList;
typedef std::map<std::string, ColInfo> Types;
typedef std::map<std::string, IndexSt> IndexHint;
typedef std::vector<std::pair<const std::string, const Schema*>> NameSchemaList;
class PartitionHandler;

enum HandlerType { kRowHandler, kTableHandler, kPartitionHandler };
class DataHandler : public ListV<Row> {
 public:
    DataHandler() {}
    virtual ~DataHandler() {}
    // get the schema of table
    virtual const Schema* GetSchema() = 0;

    // get the table name
    virtual const std::string& GetName() = 0;

    // get the db name
    virtual const std::string& GetDatabase() = 0;
    virtual const HandlerType GetHanlderType() = 0;
    virtual const std::string GetHandlerTypeName() = 0;
};

class RowHandler : public DataHandler {
 public:
    RowHandler() {}

    virtual ~RowHandler() {}
    std::unique_ptr<IteratorV<uint64_t, Row>> GetIterator() const override {
        return std::unique_ptr<IteratorV<uint64_t, Row>>();
    }
    IteratorV<uint64_t, Row>* GetIterator(int8_t* addr) const override {
        return nullptr;
    }
    const uint64_t GetCount() override { return 0; }
    Row At(uint64_t pos) override { return Row(); }
    const HandlerType GetHanlderType() override { return kRowHandler; }
    virtual const Row& GetValue() const = 0;
    const std::string GetHandlerTypeName() override { return "RowHandler"; }
};

class TableHandler : public DataHandler {
 public:
    TableHandler() : DataHandler() {}

    virtual ~TableHandler() {}

    // get the types
    virtual const Types& GetTypes() = 0;

    // get the index information
    virtual const IndexHint& GetIndex() = 0;
    // get the table iterator

    virtual std::unique_ptr<WindowIterator> GetWindowIterator(
        const std::string& idx_name) = 0;
    virtual const uint64_t GetCount() { return 0; }
    virtual Row At(uint64_t pos) { return Row(); }
    const HandlerType GetHanlderType() override { return kTableHandler; }
    virtual std::shared_ptr<PartitionHandler> GetPartition(
        std::shared_ptr<TableHandler> table_hander,
        const std::string& index_name) const {
        return std::shared_ptr<PartitionHandler>();
    }
    const std::string GetHandlerTypeName() override { return "TableHandler"; }
};

class PartitionHandler : public TableHandler {
 public:
    PartitionHandler() : TableHandler() {}
    ~PartitionHandler() {}
    virtual std::unique_ptr<RowIterator> GetIterator() const {
        return std::unique_ptr<RowIterator>();
    }
    IteratorV<uint64_t, Row>* GetIterator(int8_t* addr) const override {
        return nullptr;
    }
    virtual std::unique_ptr<WindowIterator> GetWindowIterator(
        const std::string& idx_name) {
        return std::unique_ptr<WindowIterator>();
    }
    virtual std::unique_ptr<WindowIterator> GetWindowIterator() = 0;
    virtual const bool IsAsc() = 0;
    const HandlerType GetHanlderType() override { return kPartitionHandler; }
    virtual Row At(uint64_t pos) { return Row(); }
    virtual std::shared_ptr<TableHandler> GetSegment(
        std::shared_ptr<PartitionHandler> partition_hander,
        const std::string& key) {
        return std::shared_ptr<TableHandler>();
    }
    const std::string GetHandlerTypeName() override {
        return "PartitionHandler";
    }
};

class SegmentHandler : public TableHandler {
 public:
    SegmentHandler(std::shared_ptr<PartitionHandler> partition_hander,
                   const std::string& key)
        : partition_hander_(partition_hander), key_(key) {}

    virtual ~SegmentHandler() {}

    inline const vm::Schema* GetSchema() {
        return partition_hander_->GetSchema();
    }

    inline const std::string& GetName() { return partition_hander_->GetName(); }

    inline const std::string& GetDatabase() {
        return partition_hander_->GetDatabase();
    }

    inline const vm::Types& GetTypes() { return partition_hander_->GetTypes(); }

    inline const vm::IndexHint& GetIndex() {
        return partition_hander_->GetIndex();
    }

    std::unique_ptr<vm::RowIterator> GetIterator() const {
        auto iter = partition_hander_->GetWindowIterator();
        if (iter) {
            iter->Seek(key_);
            return iter->Valid() ? std::move(iter->GetValue())
                                 : std::unique_ptr<RowIterator>();
        }
        return std::unique_ptr<RowIterator>();
    }
    vm::IteratorV<uint64_t, Row>* GetIterator(int8_t* addr) const override {
        LOG(WARNING) << "can't get iterator with given address";
        return nullptr;
    }
    std::unique_ptr<vm::WindowIterator> GetWindowIterator(
        const std::string& idx_name) {
        LOG(WARNING) << "SegmentHandler can't support window iterator";
        return std::unique_ptr<WindowIterator>();
    }
    virtual const uint64_t GetCount() {
        auto iter = GetIterator();
        if (!iter) {
            return 0;
        }
        iter->SeekToFirst();
        uint64_t cnt = 0;
        while (iter->Valid()) {
            cnt++;
            iter->Next();
        }
        return cnt;
    }
    Row At(uint64_t pos) override {
        if (pos < 0) {
            return Row();
        }
        auto iter = GetIterator();
        if (!iter) {
            return Row();
        }
        iter->SeekToFirst();
        while (pos-- > 0 && iter->Valid()) {
            iter->Next();
        }
        return iter->Valid() ? iter->GetValue() : Row();
    }

    const std::string GetHandlerTypeName() override { return "SegmentHandler"; }

 private:
    std::shared_ptr<vm::PartitionHandler> partition_hander_;
    std::string key_;
};
// database/table/schema/type management
class Catalog {
 public:
    Catalog() {}

    virtual ~Catalog() {}

    // get database information
    virtual std::shared_ptr<type::Database> GetDatabase(
        const std::string& db) = 0;

    // get table handler
    virtual std::shared_ptr<TableHandler> GetTable(
        const std::string& db, const std::string& table_name) = 0;
};

}  // namespace vm
}  // namespace fesql

#endif  // SRC_VM_CATALOG_H_
