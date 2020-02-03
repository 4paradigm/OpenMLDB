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

#include <memory>
#include "proto/type.pb.h"
#include "base/iterator.h"
#include "base/slice.h"

namespace fesql {
namespace vm {

typedef ::google::protobuf::RepeatedPtrField< ::fesql::type::ColumnDef> Schema;
typedef ::google::protobuf::RepeatedPtrField< ::fesql::type::IndexDef> IndexList;
typedef std::map<std::string, std::pair<::fesql::type::Type, int32_t>> Types;

class Iterator {
 public:

    Iterator() {}

    virtual ~Iterator(){}

    void Seek(uint64_t ts) = 0;

    void SeekToFirst() = 0;

    bool Valid() = 0;

    void Next() = 0;

    const base::Slice GetValue() = 0;

    const uint64_t GetKey() = 0;
};

class WindowIterator {

 public:
    WindowIterator() {}
    virtual ~WindowIterator() {}
    void Seek(const std::string& key) = 0;
    void SeekToFirst() = 0;
    void Next() = 0;
    bool Valid() = 0;
    std::unique_ptr<Iterator> GetValue() = 0;
    const base::Slice GetKey() = 0;
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
    virtual const IndexList& GetIndex() = 0;

    // get the table iterator
    virtual std::unique_ptr<Iterator> GetIterator() = 0;

    virtual std::unique_ptr<WindowIterator> GetWindowIterator(const std::string& idx_name) = 0;
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
