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
#include "base/slice.h"
#include "proto/type.pb.h"

namespace fesql {
namespace vm {

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

typedef ::google::protobuf::RepeatedPtrField< ::fesql::type::ColumnDef> Schema;
typedef ::google::protobuf::RepeatedPtrField< ::fesql::type::IndexDef>
    IndexList;
typedef std::map<std::string, ColInfo> Types;
typedef std::map<std::string, IndexSt> IndexHint;

class Iterator {
 public:
    Iterator() {}

    virtual ~Iterator() {}

    virtual void Seek(uint64_t ts) = 0;

    virtual void SeekToFirst() = 0;

    virtual bool Valid() = 0;

    virtual void Next() = 0;

    virtual const base::Slice GetValue() = 0;

    virtual const uint64_t GetKey() = 0;
};

class WindowIterator {
 public:
    WindowIterator() {}
    virtual ~WindowIterator() {}
    virtual void Seek(const std::string& key) = 0;
    virtual void SeekToFirst() = 0;
    virtual void Next() = 0;
    virtual bool Valid() = 0;
    virtual std::unique_ptr<Iterator> GetValue() = 0;
    virtual const base::Slice GetKey() = 0;
};


class TableHandler {
 public:
    TableHandler() {}

    virtual ~TableHandler() {}

    // get the schema of table
    virtual const Schema& GetSchema() = 0;

    // get the table name
    virtual const std::string& GetName() = 0;

    // get the db name
    virtual const std::string& GetDatabase() = 0;

    // get the types
    virtual const Types& GetTypes() = 0;

    // get the index information
    virtual const IndexHint& GetIndex() = 0;

    // get the table iterator
    virtual std::unique_ptr<Iterator> GetIterator() = 0;

    virtual std::unique_ptr<WindowIterator> GetWindowIterator(
        const std::string& idx_name) = 0;

    virtual const bool IsPartitionTable() {
        return false;
    };
};

class PartitionHandler : public TableHandler {
 public:
    PartitionHandler(const std::shared_ptr<TableHandler>& table,
                     const std::string& index_name)
        : TableHandler(), table_handler_(table), index_name_(index_name) {}
    ~PartitionHandler() {}

    const Schema& GetSchema() override { return table_handler_->GetSchema(); }

    const std::string& GetName() override { return table_handler_->GetName(); }

    const std::string& GetDatabase() override {
        return table_handler_->GetDatabase();
    }
    const Types& GetTypes() override { return table_handler_->GetTypes(); }

    const IndexHint& GetIndex() override { return index_hint_; }
    std::unique_ptr<Iterator> GetIterator() override {
        return table_handler_->GetIterator();
    }
    std::unique_ptr<WindowIterator> GetWindowIterator(
        const std::string& idx_name) override {
        return std::unique_ptr<WindowIterator>();
    }
    const std::string& GetIndexName() { return index_name_; }
    std::unique_ptr<WindowIterator> GetWindowIterator() {
        return table_handler_->GetWindowIterator(index_name_);
    }
    const bool IsPartitionTable() override { return true; }

 private:
    std::shared_ptr<TableHandler> table_handler_;
    const std::string index_name_;
    const IndexHint index_hint_;
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
