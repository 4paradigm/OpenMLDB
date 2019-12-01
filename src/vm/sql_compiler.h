/*
 * sql_compiler.h
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

#ifndef SRC_VM_SQL_COMPILER_H_
#define SRC_VM_SQL_COMPILER_H_

#include <string>
#include <memory>
#include <vector>
#include "base/status.h"
#include "llvm/IR/Module.h"
#include "parser/parser.h"
#include "vm/jit.h"
#include "vm/op_generator.h"
#include "vm/table_mgr.h"
namespace fesql {
namespace vm {

using fesql::base::Status;
struct SQLContext {
    // the sql content
    std::string sql;
    // the database
    std::string db;
    // the operators
    OpVector ops;
    // TODO(wangtaize) add a light jit engine
    // eg using bthead to compile ir
    std::unique_ptr<FeSQLJIT> jit;
    std::vector<::fesql::type::ColumnDef> schema;
    uint32_t row_size;
};

class SQLCompiler {
 public:
    explicit SQLCompiler(TableMgr* table_mgr);

    ~SQLCompiler();

    bool Compile(SQLContext& ctx, Status& status);  // NOLINT

 private:
    bool Parse(SQLContext& ctx, ::fesql::node::NodeManager& node_mgr, // NOLINT
               ::fesql::node::PlanNodeList& trees, Status& status);  // NOLINT

 private:
    TableMgr* table_mgr_;
};

}  // namespace vm
}  // namespace fesql
#endif  // SRC_VM_SQL_COMPILER_H_
