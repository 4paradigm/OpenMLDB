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
#include <node/sql_node.h>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "base/fe_slice.h"
#include "codec/fe_row_codec.h"
#include "codec/list_iterator_codec.h"
#include "codec/row.h"
#include "proto/fe_type.pb.h"
#include "sdk/base.h"

namespace fesql {
namespace vm {

using fesql::codec::ColInfo;
using fesql::codec::ListV;
using fesql::codec::Row;
using fesql::codec::RowIterator;
using fesql::codec::Schema;
using fesql::codec::WindowIterator;

/*enum SourceType { kSourceColumn, kSourceConst, kSourceNone };
class ColumnSource;
class Tablet;
typedef std::vector<ColumnSource> ColumnSourceList;
class ColumnSource {
 public:
    ColumnSource()
        : type_(kSourceNone),
          schema_idx_(0),
          column_idx_(0),
          column_name_(""),
          cast_type_chains_(),
          const_value_() {}
    explicit ColumnSource(const node::ConstNode* node)
        : type_(kSourceConst),
          schema_idx_(0),
          column_idx_(0),
          column_name_(""),
          cast_type_chains_(),
          const_value_(node) {}
    ColumnSource(uint32_t schema_idx, uint32_t column_idx,
                 const std::string& column_name)
        : type_(kSourceColumn),
          schema_idx_(schema_idx),
          column_idx_(column_idx),
          column_name_(column_name),
          cast_type_chains_(),
          const_value_() {}

    void AddCastType(const node::DataType& type) {
        cast_type_chains_.push_back(type);
    }

    void AddCastTypes(const std::vector<node::DataType>& types) {
        for (auto type : types) {
            AddCastType(type);
        }
    }
    const std::string ToString() const {
        std::string cast_types = "";
        if (!cast_type_chains_.empty()) {
            for (auto iter = cast_type_chains_.cbegin();
                 iter != cast_type_chains_.cend(); iter++) {
                cast_types.append(":");
                cast_types.append(node::DataTypeName(*iter));
            }
        }
        switch (type_) {
            case kSourceColumn:
                return "<-[" + std::to_string(schema_idx_) + ":" +
                       std::to_string(column_idx_) + cast_types + "]";
            case kSourceConst:
                return "<-" + node::ExprString(const_value_) + cast_types;
            case kSourceNone:
                return "->None";
        }
        return "";
    }
    const SourceType type() const { return type_; }
    const uint32_t schema_idx() const { return schema_idx_; }
    const uint32_t column_idx() const { return column_idx_; }
    const std::string& column_name() const { return column_name_; }
    const node::ConstNode* const_value() const { return const_value_; }
    const std::vector<node::DataType>& cast_types() const {
        return cast_type_chains_;
    }

 private:
    SourceType type_;
    uint32_t schema_idx_;
    uint32_t column_idx_;
    std::string column_name_;
    std::vector<node::DataType> cast_type_chains_;
    const node::ConstNode* const_value_;
};*/

constexpr uint32_t INVALID_POS = UINT32_MAX;

struct IndexSt {
    std::string name;
    uint32_t index;
    uint32_t ts_pos;
    std::vector<ColInfo> keys;
};

typedef ::google::protobuf::RepeatedPtrField<::fesql::type::IndexDef> IndexList;
typedef std::map<std::string, ColInfo> Types;
typedef std::map<std::string, IndexSt> IndexHint;

/*struct SchemaSource {
 public:
    explicit SchemaSource(const vm::Schema* schema)
        : table_name_(""), schema_(schema), sources_(nullptr) {}
    SchemaSource(const std::string& table_name, const vm::Schema* schema)
        : table_name_(table_name), schema_(schema), sources_(nullptr) {}

    std::string table_name_;
    const vm::Schema* schema_;
};

struct NamedSchemaSourceList {
    void AddSchemaSource(const vm::Schema* schema) {
        schema_source_list_.push_back(SchemaSource("", schema));
    }
    void AddSchemaSource(const std::string& table_name,
                         const vm::Schema* schema) {
        schema_source_list_.push_back(SchemaSource(table_name, schema));
    }

    void AddSchemaSource(const std::string& table_name,
                         const vm::Schema* schema,
                         const vm::ColumnSourceList* sources) {
        schema_source_list_.push_back(
            SchemaSource(table_name, schema, sources));
    }
    void AddSchemaSources(const NamedSchemaSourceList& sources) {
        for (auto source : sources.schema_source_list_) {
            schema_source_list_.push_back(source);
        }
    }

    const bool Empty() const { return schema_source_list_.empty(); }
    const std::vector<SchemaSource>& schema_source_list() const {
        return schema_source_list_;
    }
    const vm::SchemaSource& GetSchemaSourceSlice(size_t idx) const {
        return schema_source_list_[idx];
    }
    const vm::Schema* GetSchemaSlice(size_t idx) const {
        return schema_source_list_[idx].schema_;
    }
    const size_t size() const {
        return schema_source_list_.size();
    }

    std::vector<SchemaSource> schema_source_list_;
};*/

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
    virtual base::Status GetStatus() { return base::Status::OK(); }
};
class DataHandlerList {
 public:
    DataHandlerList() {}
    ~DataHandlerList() {}
    virtual size_t GetSize() = 0;
    virtual std::shared_ptr<DataHandler> Get(size_t idx) = 0;
};
class DataHandlerVector : public DataHandlerList {
 public:
    DataHandlerVector() : data_handlers_() {}
    ~DataHandlerVector() {}
    void Add(std::shared_ptr<DataHandler> data_handler) {
        data_handlers_.push_back(data_handler);
    }
    size_t GetSize() { return data_handlers_.size(); }
    std::shared_ptr<DataHandler> Get(size_t idx) { return data_handlers_[idx]; }

 private:
    std::vector<std::shared_ptr<DataHandler>> data_handlers_;
};
class DataHandlerRepeater : public DataHandlerList {
 public:
    DataHandlerRepeater(std::shared_ptr<DataHandler> data_handler, size_t size)
        : size_(size), data_handler_(data_handler) {}
    ~DataHandlerRepeater() {}
    size_t GetSize() { return size_; }
    std::shared_ptr<DataHandler> Get(size_t idx) {
        return idx >= 0 && idx < size_ ? data_handler_
                                       : std::shared_ptr<DataHandler>();
    }

 private:
    size_t size_;
    std::shared_ptr<DataHandler> data_handler_;
};
class RowHandler : public DataHandler {
 public:
    RowHandler() {}

    virtual ~RowHandler() {}
    std::unique_ptr<RowIterator> GetIterator() const override {
        return std::unique_ptr<RowIterator>();
    }
    RowIterator* GetRawIterator() const override { return nullptr; }
    const uint64_t GetCount() override { return 0; }
    Row At(uint64_t pos) override { return Row(); }
    const HandlerType GetHanlderType() override { return kRowHandler; }
    virtual const Row& GetValue() = 0;
    const std::string GetHandlerTypeName() override { return "RowHandler"; }
};

class ErrorRowHandler : public RowHandler {
 public:
    ErrorRowHandler(common::StatusCode status_code, const std::string& msg_str)
        : status_(status_code, msg_str),
          table_name_(""),
          db_(""),
          schema_(nullptr),
          row_() {}
    ~ErrorRowHandler() {}
    virtual const Row& GetValue() { return row_; }
    const std::string GetHandlerTypeName() override {
        return "ErrorRowHandler";
    }
    const Schema* GetSchema() override { return nullptr; }
    const std::string& GetName() override { return table_name_; }
    const std::string& GetDatabase() override { return db_; }

 private:
    base::Status status_;
    std::string table_name_;
    std::string db_;
    const Schema* schema_;
    Row row_;
};

class Tablet {
 public:
    Tablet() {}
    virtual ~Tablet() {}
    virtual std::shared_ptr<RowHandler> SubQuery(uint32_t task_id,
                                                 const std::string& db,
                                                 const std::string& sql,
                                                 const fesql::codec::Row& row,
                                                 const bool is_procedure,
                                                 const bool is_debug) = 0;
    virtual std::shared_ptr<RowHandler> SubQuery(
        uint32_t task_id, const std::string& db, const std::string& sql,
        const std::vector<fesql::codec::Row>& rows, const bool is_procedure,
        const bool is_debug) = 0;
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
    const HandlerType GetHanlderType() override { return kTableHandler; }
    virtual std::shared_ptr<PartitionHandler> GetPartition(
        const std::string& index_name) {
        return std::shared_ptr<PartitionHandler>();
    }
    const std::string GetHandlerTypeName() override { return "TableHandler"; }
    virtual const OrderType GetOrderType() const { return kNoneOrder; }
    virtual std::shared_ptr<Tablet> GetTablet(const std::string& index_name,
                                              const std::string& pk) {
        return std::shared_ptr<Tablet>();
    }
};

class PartitionHandler : public TableHandler {
 public:
    PartitionHandler() : TableHandler() {}
    ~PartitionHandler() {}
    virtual std::unique_ptr<RowIterator> GetIterator() const {
        return std::unique_ptr<RowIterator>();
    }
    RowIterator* GetRawIterator() const { return nullptr; }
    virtual std::unique_ptr<WindowIterator> GetWindowIterator(
        const std::string& idx_name) {
        return std::unique_ptr<WindowIterator>();
    }
    virtual std::unique_ptr<WindowIterator> GetWindowIterator() = 0;
    const HandlerType GetHanlderType() override { return kPartitionHandler; }
    virtual Row At(uint64_t pos) { return Row(); }
    virtual std::shared_ptr<TableHandler> GetSegment(const std::string& key) {
        return std::shared_ptr<TableHandler>();
    }

    // Return batch segments with given keys vector
    // this is default implementation of GetSegments
    virtual std::vector<std::shared_ptr<TableHandler>> GetSegments(
        const std::vector<std::string>& keys) {
        std::vector<std::shared_ptr<TableHandler>> segments;
        for (auto key : keys) {
            segments.push_back(GetSegment(key));
        }
        return segments;
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

    virtual std::shared_ptr<fesql::sdk::ProcedureInfo> GetProcedureInfo(
        const std::string& db, const std::string& sp_name) {
        return nullptr;
    }
};

}  // namespace vm
}  // namespace fesql

#endif  // SRC_VM_CATALOG_H_
