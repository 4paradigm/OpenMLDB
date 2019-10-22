/*
 * type_def.h
 * Copyright (C) 4paradigm.com 2019 wangtaize <wangtaize@4paradigm.com>
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

#ifndef TYPE_TYPE_DEF_H_
#define TYPE_TYPE_DEF_H_
#include <stdint.h>
#include <string>
#include "absl/time/time.h"

namespace fesql {
namespace type {

enum TypeDef {
    kBool = 0,
    kInt16,
    kUInt16,
    kInt32,
    kUInt32,
    kInt64,
    kUInt64,
    kFloat,
    kDouble,
    kString,
    kDate,
    kTimestamp
};

struct ColumnDef {
    TypeDef type;
    std::string name;
    uint32_t offset;
};

struct TableDef {
    std::string name;
    std::vector<ColumnDef> columns;
};

struct Table {
    uint64_t catalog_id;
    uint64_t table_id;
    TableDef table;
    ::absl::Time ctime;
};

struct CataLog {
    uint64_t id;
    std::string name;
    ::absl::Time ctime;
};

class TableBuilder {

public:
    TableBuilder(uint64_t tid, uint64_t cid,
            const std::string& name);
    ~TableBuilder();

    void AddColumn(const TypeDef& type, 
            const std::string name);
    Table& Build();
private:
    Table table_;
    uint32_t offset_;
};

} // namespace of type
} // namespace of fesql
#endif /* !TYPE_TYPE_DEF_H_ */
