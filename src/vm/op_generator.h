/*
 * op_generator.h
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

#ifndef SRC_VM_OP_GENERATOR_H_
#define SRC_VM_OP_GENERATOR_H_

#include "llvm/IR/Module.h"
#include "node/plan_node.h"
#include "node/sql_node.h"
#include "vm/op.h"
#include "vm/table_mgr.h"
#include "base/status.h"

namespace fesql {
namespace vm {

struct OpVector {
    std::string sql;
    // TOD(wangtaize) add free logic
    std::vector<OpNode*> ops;
};

class OpGenerator {
 public:
    OpGenerator(TableMgr* table_mgr);
    ~OpGenerator();

    bool Gen(const ::fesql::node::PlanNodeList& trees, const std::string& db,
             ::llvm::Module* module, OpVector* ops, base::Status& status);

 private:
    bool GenFnDef(::llvm::Module* module,
                  const ::fesql::node::FuncDefPlanNode* plan);

    bool GenSQL(const ::fesql::node::SelectPlanNode* node,
                const std::string& db, ::llvm::Module* module, OpVector* ops,
                base::Status& status);

    bool GenProject(const ::fesql::node::ProjectListPlanNode* node,
                    const std::string& db, ::llvm::Module* module,
                    OpVector* ops);

    bool GenScan(const ::fesql::node::ScanPlanNode* node, const std::string& db,
                 ::llvm::Module* module, OpVector* ops);

    bool GenLimit(const ::fesql::node::LimitPlanNode* node,
                  const std::string& db, ::llvm::Module* module, OpVector* ops);

    bool RoutingNode(const ::fesql::node::PlanNode* node, const std::string& db,
                     ::llvm::Module* module, OpVector* ops);

 private:
    TableMgr* table_mgr_;
};

}  // namespace vm
}  // namespace fesql

#endif  // SRC_VM_OP_GENERATOR_H_
