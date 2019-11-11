/*
 * op_generator.cc
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

#include "vm/op_generator.h"

#include "codegen/fn_ir_builder.h"
#include "codegen/fn_let_ir_builder.h"
#include "plan/planner.h"
#include "node/node_manager.h"

namespace fesql {
namespace vm {

OpGenerator::OpGenerator(TableMgr* table_mgr):table_mgr_(table_mgr) {}

OpGenerator::~OpGenerator() {}

bool OpGenerator::Gen(const ::fesql::node::NodePointVector& trees,
        ::llvm::Module* module,
        OpVector* ops) {
    if (module == NULL || ops == NULL) {
        LOG(WARNING) << "module or ops is null";
        return false;
    }

    std::vector<::fesql::node::SQLNode* >::const_iterator it = trees.begin();
    for (; it != trees.end(); ++it) {
        const ::fesql::node::SQLNode* node = *it;
        switch (node->GetType()) {
            case ::fesql::node::kFnList:
                {
                    const ::fesql::node::FnNode* fn_node = (const ::fesql::node::FnNode*) node;
                    bool ok = GenFnDef(module, fn_node);
                    if (!ok) {
                        return false;
                    }
                    break;
                }
            case ::fesql::node::kSelectStmt: 
                {
                    bool ok = GenSQL(node, module, ops);
                    if (!ok) {
                        return false;
                    }
                    break;
                }
            default:
                {
                    LOG(WARNING) << "not supported";
                    return false;
                }
        }
    }
}

bool OpGenerator::GenSQL(const ::fesql::node::SQLNode* node,
        ::llvm::Module* module,
        OpVector* ops) {

    if (node == NULL || module == NULL || ops == NULL) {
        LOG(WARNING) << "input args has null";
        return false;
    }

    ::fesql::node::NodeManager nm;
    ::fesql::plan::SimplePlanner planer(&nm);
    PlanNode* plan =  planer.CreatePlan(node);
    if (plan == NULL) {
        LOG(WARNING) << "fail to create sql plan";
        return false;
    }
    bool ok = RoutingNode(plan->GetChildren()[0], module, ops);
    if (!ok) {
        LOG(WARNING) << "fail to gen op";
    }
    return ok;
}

bool OpGenerator::RoutingNode(const ::fesql::node::PlanNode* node,
        ::llvm::Module* module,
        OpVector* ops) {

    if (node == NULL || module == NULL
            || ops == NULL) {
        LOG(WARNING) << "input args has null";
        return false;
    }

    switch(node->GetType()) {
        case ::fesql::node::kPlanTypeLimit:
            {
                const ::fesql::node::LimitPlanNode* limit_node = 
                    (const ::fesql::node::LimitPlanNode*)node;
                return GenLimit(limit_node, module, ops);
            }
        case ::fesql::node::kPlanTypeScan:
            {
                const ::fesql::node::ScanPlanNode* scan_node = 
                    (const ::fesql::node::ScanPlanNode*)node;
                return GenScan(scan_node, module, ops);
            }
        case ::fesql::node::kProjectList:
            {
                const ::fesql::node::ProjectListPlanNode* ppn = 
                    (const ::fesql::node::ProjectListPlanNode*)node;
                return GenProject(ppn, module, ops);
            }
        default:
            {
                LOG(WARNING) << "not supported "
            }
    }
}

bool OpGenerator::GenScan(const ::fesql::node::ScanPlanNode* node,
        ::llvm::Module* module,
        OpVector* ops) {

    if (node == NULL || module == NULL
            || ops == NULL) {
        LOG(WARNING) << "input args has null";
        return false;
    }

    // TODO(wangtaize) share_ptr
    TableStatus* table_status = NULL;
    // TODO(wangtaize) add db
    std::string db = "";
    bool ok = table_mgr_->GetTableDef(db, node->GetTable(), &table_status);
    if (!ok || table_status == NULL) {
        LOG(WARNING) << "fail to find table " << node->GetTable();
        return false;
    }
    ScanOp* sop = new ScanOp();
    sop->type = kOpScan;
    sop->tid = table_status->tid;
    sop->pid = table_status->pid;
    for (int32_t i = 0; i < table_status->table.columns_size(); i++) {
        sop->input_schema.push_back(table_status->table.columns(i));
        sop->output_schema.push_back(table_status->table.columns(i));
    }
    ops->ops.push_back(static_cast<OpNode>(sop));
    return true;
}

bool OpGenerator::GenProject(const ::fesql::node::ProjectListPlanNode* node,
        ::llvm::Module* module,
        OpVector* ops) {

    if (node == null || module == null
            || ops == null) {
        log(warning) << "input args has null";
        return false;
    }
    // deeping first
    std::vector<::fesql::node::PlanNode *>::const_iterator it = node->GetChildren().begin();
    for (; it != node->GetChildren().end(); ++it) {
        const ::fesql::node::PlanNode* pn = *it;
        bool ok = RoutingNode(pn, module, ops);
        if (!ok) {
            LOG(WARNING) << "fail to rouing node ";
            return false;
        }
    }
    std::string db = "";
    TableStatus* table_status = NULL;
    bool ok = table_mgr_->GetTableDef(db, node->GetTable(), &table_status);
    if (!ok) {
        LOG(WARNING) << "fail to find table with name " << node->GetTable();
        return false;
    }

    // TODO(wangtaize) use ops end op output schema
    ::fesql::codegen::RowFnLetIRBuilder builder(&table_status->table, module);
    std::string fn_name = "__internal_sql_codegen";
    std::vector<::fesql::type::ColumnDef> output_schema;
    bool ok = builder.Build(fn_name, node, output_schema);

    if (!ok) {
        LOG(WARNING) << "fail to run row fn builder";
        return false;
    }

    ProjectOp* pop = new ProjectOp();
    pop->type = kOpProject;
    pop->output_schema = output_schema;
    pop->fn_name = fn_name;
    pop->fn = NULL;
}

bool OpGenerator::GenLimit(const ::fesql::node::LimitPlanNode* node,
        ::llvm::Module* module,
        OpVector* ops) {

    if (node == null || module == null
            || ops == null) {
        log(warning) << "input args has null";
        return false;
    }
    if (node->GetChildrenSize() != 1)  {
        LOG(WARNING) << "invalid limit node for has no child";
        return false;
    }

    std::vector<PlanNode *>::const_iterator it = node->GetChildren().begin();
    for (; it != node->GetChildren().end(); ++it) {
        const ::fesql::node::PlanNode* pn = *it;
        bool ok = RoutingNode(pn, module, ops);
        if (!ok) {
            LOG(WARNING) << "fail to routing node";
            return false;
        }
    }
    if (ops->ops.empty()) {
        LOG(WARNING) << "invalid state";
        return false;
    }
    LimitOp* limit_op = new LimitOp();
    limit_op->type = kOpLimit;
    limit_op->limit = node->GetLimitCnt();
    ops->ops.push_back(static_cast<OpNode*>(limit_op));
    return true;
}

bool OpGenerator::GenFnDef(::llvm::Module* module,
        const ::fesql::node::FnNode* node) {

    if (module == NULL || node == NULL) {
        LOG(WARNING) << "module or node is null";
        return false;
    }

    ::fesql::codegen::FnIRBuilder builder(module);
    bool ok = builder.build(node);
    if (!ok) {
        LOG(WARNING) << "fail to build fn node with line " << node->GetLineNum();
    }
    return ok;
}


}  // namespace vm
}  // namespace fesql

