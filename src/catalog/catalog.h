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

#ifndef SRC_CATALOG_CATALOG_H_
#define SRC_CATALOG_CATALOG_H_

#include <memory>
#include "proto/type.pb.h"

namespace fesql {
namespace catalog {

typedef ::google::protobuf::RepeatedPtrField< ::fesql::type::ColumnDef > Schema;
class TableHandler { 

 public:

    TableHandler();

    virtual ~TableHandler();

    // get the schema of table
    virtual Schema& GetSchema() = 0;

    // get the table name
    virtual const std::string&  GetName() = 0;

    // get the db name
    virtual const std::string& GetDataBase() = 0;
};

// database/table/schema/type management
class Catalog {

 public:
    Catalog();

    virtual ~Catalog();

    // get database information
    virtual std::share_ptr<type::Database> GetDatabase(const std::string& db) = 0;

    // get table handler
    virtual std::share_ptr<TableHandler> GetTable(const std::string& db, 
            const std::string& table_name);

};

}  // namespace catalog
}  // namespace fesql

#endif  // SRC_CATALOG_CATALOG_H_
