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
#include <utility>
#include <vector>
#include "base/fe_slice.h"
#include "codec/list_iterator_codec.h"
#include "codec/row.h"
#include "proto/fe_type.pb.h"

namespace fesql {
namespace vm {

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
enum OrderType { kDescOrder, kAscOrder, kNoneOrder };
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
    std::unique_ptr<RowIterator> GetIterator() const override {
        return std::unique_ptr<RowIterator>();
    }
    RowIterator* GetIterator(int8_t* addr) const override { return nullptr; }
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
    virtual const OrderType GetOrderType() const { return kNoneOrder; }
};

class PartitionHandler : public TableHandler {
 public:
    PartitionHandler() : TableHandler() {}
    ~PartitionHandler() {}
    virtual std::unique_ptr<RowIterator> GetIterator() const {
        return std::unique_ptr<RowIterator>();
    }
    RowIterator* GetIterator(int8_t* addr) const override { return nullptr; }
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
    const OrderType GetOrderType() const { return kNoneOrder; }
};

// database/table/schema/type management
class Catalog {
 public:
    Catalog() {}

    virtual ~Catalog() {}

    virtual bool IndexSupport() = 0;
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
