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
        const std::string& db,
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
                    bool ok = GenSQL(trees, db, module, ops);
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
    return true;
}

bool OpGenerator::GenSQL(const ::fesql::node::NodePointVector &trees,
        const std::string& db,
        ::llvm::Module* module,
        OpVector* ops) {

    if (module == NULL || ops == NULL) {
        LOG(WARNING) << "input args has null";
        return false;
    }

    ::fesql::node::NodeManager nm;
    ::fesql::plan::SimplePlanner planer(&nm);
    ::fesql::base::Status status;
    ::fesql::node::PlanNodeList pnl;
    int ret =  planer.CreatePlanTree(trees, pnl, status);
    if (ret != 0) {
        LOG(WARNING) << "Fail create sql plan: " << status.msg;
        return false;
    }
    bool ok = RoutingNode(pnl[0]->GetChildren()[0], db, module, ops);
    if (!ok) {
        LOG(WARNING) << "Fail to gen op";
    }
    return ok;
}

bool OpGenerator::RoutingNode(const ::fesql::node::PlanNode* node,
        const std::string& db,
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
                return GenLimit(limit_node, db, module, ops);
            }
        case ::fesql::node::kPlanTypeScan:
            {
                const ::fesql::node::ScanPlanNode* scan_node = 
                    (const ::fesql::node::ScanPlanNode*)node;
                return GenScan(scan_node, db, module, ops);
            }
        case ::fesql::node::kProjectList:
            {
                const ::fesql::node::ProjectListPlanNode* ppn = 
                    (const ::fesql::node::ProjectListPlanNode*)node;
                return GenProject(ppn, db, module, ops);
            }
        default:
            {
                LOG(WARNING) << "not supported ";
                return false;
            }
    }

}

bool OpGenerator::GenScan(const ::fesql::node::ScanPlanNode* node,
        const std::string& db,
        ::llvm::Module* module,
        OpVector* ops) {

    if (node == NULL || module == NULL
            || ops == NULL) {
        LOG(WARNING) << "input args has null";
        return false;
    }

    std::shared_ptr<TableStatus> table_status = table_mgr_->GetTableDef(db, node->GetTable());
    if (!table_status) {
        LOG(WARNING) << "fail to find table " << node->GetTable();
        return false;
    }

    ScanOp* sop = new ScanOp();
    sop->db = db;
    sop->type = kOpScan;
    sop->tid = table_status->tid;
    sop->pid = table_status->pid;
    for (int32_t i = 0; i < table_status->table_def.columns_size(); i++) {
        sop->input_schema.push_back(table_status->table_def.columns(i));
        sop->output_schema.push_back(table_status->table_def.columns(i));
    }
    ops->ops.push_back((OpNode*)sop);
    return true;
}

bool OpGenerator::GenProject(const ::fesql::node::ProjectListPlanNode* node,
        const std::string& db,
        ::llvm::Module* module,
        OpVector* ops) {

    if (node == NULL || module == NULL
            || ops == NULL) {
        LOG(WARNING) << "input args has null";
        return false;
    }

    // deeping first
    std::vector<::fesql::node::PlanNode *>::const_iterator it = node->GetChildren().begin();
    for (; it != node->GetChildren().end(); ++it) {
        const ::fesql::node::PlanNode* pn = *it;
        bool ok = RoutingNode(pn, db, module, ops);
        if (!ok) {
            LOG(WARNING) << "fail to rouing node ";
            return false;
        }
    }
    std::shared_ptr<TableStatus> table_status = table_mgr_->GetTableDef(db,
            node->GetTable());
    if (!table_status) {
        LOG(WARNING) << "fail to find table with name " << node->GetTable();
        return false;
    }

    // TODO(wangtaize) use ops end op output schema
    ::fesql::codegen::RowFnLetIRBuilder builder(&table_status->table_def, module);
    std::string fn_name = "__internal_sql_codegen";
    std::vector<::fesql::type::ColumnDef> output_schema;
    bool ok = builder.Build(fn_name, node, output_schema);

    if (!ok) {
        LOG(WARNING) << "fail to run row fn builder";
        return false;
    }

    uint32_t output_size = 0;
    for (uint32_t i = 0; i < output_schema.size(); i++) {
        ::fesql::type::ColumnDef& column = output_schema[i];
        switch (column.type()) {
            case ::fesql::type::kInt16:
                {
                    output_size += 2;
                    break;
                }
            case ::fesql::type::kInt32:
            case ::fesql::type::kFloat:
                {
                    output_size += 4;
                    break;
                }
            case ::fesql::type::kInt64:
            case ::fesql::type::kDouble:
            case ::fesql::type::kString:
                {
                    output_size += 8;
                    break;
                }
            default:
                {
                    LOG(WARNING) << "not supported type";
                    return false;
                }
        }
    }

    ProjectOp* pop = new ProjectOp();
    pop->type = kOpProject;
    pop->output_schema = output_schema;
    pop->fn_name = fn_name;
    pop->fn = NULL;
    pop->output_size = output_size;
    ops->ops.push_back((OpNode*)pop);
    return true;
}

bool OpGenerator::GenLimit(const ::fesql::node::LimitPlanNode* node,
        const std::string& db,
        ::llvm::Module* module,
        OpVector* ops) {

    if (node == NULL || module == NULL 
            || ops == NULL) {
        LOG(WARNING) << "input args has null";
        return false;
    }
    if (node->GetChildrenSize() != 1)  {
        LOG(WARNING) << "invalid limit node for has no child";
        return false;
    }

    std::vector<::fesql::node::PlanNode *>::const_iterator it = node->GetChildren().begin();
    for (; it != node->GetChildren().end(); ++it) {
        const ::fesql::node::PlanNode* pn = *it;
        bool ok = RoutingNode(pn, db, module, ops);
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
    ops->ops.push_back((OpNode*)limit_op);
    return true;
}

bool OpGenerator::GenFnDef(::llvm::Module* module,
        const ::fesql::node::FnNode* node) {

    if (module == NULL || node == NULL ) {
        LOG(WARNING) << "module or node is null";
        return false;
    }

    ::fesql::codegen::FnIRBuilder builder(module);
    bool ok = builder.Build(node);
    if (!ok) {
        LOG(WARNING) << "fail to build fn node with line " << node->GetLineNum();
    }
    return ok;
}

}  // namespace vm
}  // namespace fesql

