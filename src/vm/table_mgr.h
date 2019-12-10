/*
 * table_mgr.h
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

#ifndef SRC_VM_TABLE_MGR_H_
#define SRC_VM_TABLE_MGR_H_

#include <memory>
#include <string>
#include <map>
#include <utility>
#include "proto/type.pb.h"
#include "storage/table.h"

namespace fesql {
namespace vm {

struct TableStatus {
    uint32_t tid;
    uint32_t pid;
    std::string db;
    ::fesql::type::TableDef table_def;
    std::unique_ptr<::fesql::storage::Table> table;
    // ColInfos: map<col_name, pair<col_type, col_idx>>
    typedef std::map<std::string, std::pair<::fesql::type::Type, int32_t>>
        ColInfos;
    ColInfos col_infos;

    TableStatus() {}
    TableStatus(uint32_t tid, uint32_t pid, const std::string& db,
                const ::fesql::type::TableDef& table_def)
        : tid(tid), pid(pid), db(db), table_def(table_def) {}
    ~TableStatus() {}
    void InitColumnInfos() {
        col_infos.clear();
        for (int32_t i = 0; i < table_def.columns_size(); i++) {
            const ::fesql::type::ColumnDef& column = table_def.columns(i);
            col_infos.insert(std::make_pair(column.name(),
                                            std::make_pair(column.type(), i)));
        }
    }
};

class TableMgr {
 public:
    virtual ~TableMgr() {}

    virtual std::shared_ptr<TableStatus> GetTableDef(
        const std::string& db, const std::string& name) = 0;

    virtual std::shared_ptr<TableStatus> GetTableDef(const std::string& db,
                                                     const uint32_t tid) = 0;
};

}  // namespace vm
}  // namespace fesql
#endif  // SRC_VM_TABLE_MGR_H_
