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

#ifndef HYBRIDSE_INCLUDE_VM_ROUTER_H_
#define HYBRIDSE_INCLUDE_VM_ROUTER_H_

#include <string>
#include "vm/physical_op.h"

namespace hybridse {
namespace vm {

class Router {
 public:
    Router() : main_db_(), main_table_(), router_col_() {}
    virtual ~Router() {}
    void SetMainDb(const std::string& main_db) {
        main_db_ = main_db;
    }
    const std::string& GetMainDb() const { return main_db_; }
    void SetMainTable(const std::string& main_table) {
        main_table_ = main_table;
    }

    const std::string& GetMainTable() const { return main_table_; }

    int Parse(const PhysicalOpNode* physical_plan);

    const std::string& GetRouterCol() const { return router_col_; }

 private:
    bool IsWindowNode(const PhysicalOpNode* physical_node);

 private:
    std::string main_db_;
    std::string main_table_;
    std::string router_col_;
};

}  // namespace vm
}  // namespace hybridse
#endif  // HYBRIDSE_INCLUDE_VM_ROUTER_H_
