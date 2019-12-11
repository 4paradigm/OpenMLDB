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
#include <codegen/buf_ir_builder.h>
#include <proto/common.pb.h>
#include <memory>
#include "codegen/fn_ir_builder.h"
#include "codegen/fn_let_ir_builder.h"
#include "node/node_manager.h"
#include "plan/planner.h"

namespace fesql {
namespace vm {

OpGenerator::OpGenerator(TableMgr* table_mgr) : table_mgr_(table_mgr) {}

OpGenerator::~OpGenerator() {}

bool OpGenerator::Gen(const ::fesql::node::PlanNodeList& trees,
                      const std::string& db, ::llvm::Module* module,
                      OpVector* ops, base::Status& status) {
    if (module == NULL || ops == NULL) {
        LOG(WARNING) << "module or ops is null";
        return false;
    }

    std::vector<::fesql::node::PlanNode*>::const_iterator it = trees.begin();
    for (; it != trees.end(); ++it) {
        const ::fesql::node::PlanNode* node = *it;
        switch (node->GetType()) {
            case ::fesql::node::kPlanTypeFuncDef: {
                const ::fesql::node::FuncDefPlanNode* func_def_plan =
                    dynamic_cast<const ::fesql::node::FuncDefPlanNode*>(node);
                bool ok = GenFnDef(module, func_def_plan);
                if (!ok) {
                    status.code = (common::kCodegenError);
                    status.msg = ("Fail to codegen function");
                    return false;
                }
                break;
            }
            case ::fesql::node::kPlanTypeSelect: {
                const ::fesql::node::SelectPlanNode* select_plan =
                    dynamic_cast<const ::fesql::node::SelectPlanNode*>(node);
                bool ok = GenSQL(select_plan, db, module, ops, status);
                if (!ok) {
                    status.code = (common::kCodegenError);
                    status.msg = ("Fail to codegen select sql");
                    return false;
                }
                break;
            }
            default: {
                LOG(WARNING) << "not supported";
                return false;
            }
        }
    }
    return true;
}

bool OpGenerator::GenSQL(const ::fesql::node::SelectPlanNode* tree,
                         const std::string& db, ::llvm::Module* module,
                         OpVector* ops, base::Status& status) {
    if (module == NULL || ops == NULL || tree == NULL) {
        LOG(WARNING) << "input args has null";
        return false;
    }

    std::map<const std::string, OpNode*> ops_map;
    if (1 != tree->GetChildren().size()) {
        status.msg =
            "fail to handle select plan node: children size should be "
            "1, but " +
            std::to_string(tree->GetChildrenSize());
        status.code = common::kCodegenError;
        LOG(WARNING) << status.msg;
        return false;
    }
    RoutingNode(tree->GetChildren()[0], db, module, ops_map, ops, status);
    if (common::kOk != status.code) {
        LOG(WARNING) << "Fail to gen op";
        return false;
    } else {
        return true;
    }
}

OpNode* OpGenerator::RoutingNode(
    const ::fesql::node::PlanNode* node, const std::string& db,
    ::llvm::Module* module,
    std::map<const std::string, OpNode*>& ops_map,  // NOLINT
    OpVector* ops, Status& status) {                // NOLINT
    if (node == NULL || module == NULL || ops == NULL) {
        status.msg = "input args has null";
        status.code = common::kNullPointer;
        LOG(WARNING) << status.msg;
        return nullptr;
    }
    // handle plan node that has been generated before
    std::stringstream ss;
    ss << node;
    std::string key = ss.str();
    auto iter = ops_map.find(key);
    DLOG(INFO) << "ops_map size: " << ops_map.size();
    if (iter != ops_map.end()) {
        DLOG(INFO) << "plan node already generate op node";
        return iter->second;
    }
    // firstly, generate operator for node's children
    std::vector<::fesql::node::PlanNode*>::const_iterator it =
        node->GetChildren().cbegin();
    std::vector<OpNode*> children;
    for (; it != node->GetChildren().cend(); ++it) {
        const ::fesql::node::PlanNode* pn = *it;
        OpNode* child = RoutingNode(pn, db, module, ops_map, ops, status);
        if (common::kOk != status.code) {
            LOG(WARNING) << status.msg;
            return nullptr;
        }
        children.push_back(child);
    }

    // secondly, generate operator for current node
    OpNode* cur_op = nullptr;
    switch (node->GetType()) {
        case ::fesql::node::kPlanTypeLimit: {
            const ::fesql::node::LimitPlanNode* limit_node =
                (const ::fesql::node::LimitPlanNode*)node;
            cur_op = GenLimit(limit_node, db, module, status);
            break;
        }
        case ::fesql::node::kPlanTypeScan: {
            const ::fesql::node::ScanPlanNode* scan_node =
                (const ::fesql::node::ScanPlanNode*)node;
            cur_op = GenScan(scan_node, db, module, status);
            break;
        }
        case ::fesql::node::kProjectList: {
            const ::fesql::node::ProjectListPlanNode* ppn =
                (const ::fesql::node::ProjectListPlanNode*)node;
            cur_op = GenProject(ppn, db, module, status);
            break;
        }
        case ::fesql::node::kPlanTypeMerge: {
            const ::fesql::node::MergePlanNode* merge_node =
                (const ::fesql::node::MergePlanNode*)node;
            if (children.size() != 2) {
                status.msg =
                    "fail to generate merge operator: children operators size "
                    "should be 2, but " +
                    std::to_string(children.size());
                status.code = common::kUnSupport;
                LOG(WARNING) << status.msg;
                return nullptr;
            }
            cur_op = GenMerge(merge_node, module, status);
            cur_op->type = kOpMerge;
            break;
        }
        default: {
            status.msg = "not supported plan node" +
                         node::NameOfPlanNodeType(node->GetType());
            status.code = common::kNullPointer;
            LOG(WARNING) << status.msg;
            return nullptr;
        }
    }
    if (common::kOk != status.code) {
        LOG(WARNING) << status.msg;
        if (nullptr != cur_op) {
            delete cur_op;
        }
        return nullptr;
    }

    if (nullptr != cur_op) {
        cur_op->children = children;
        cur_op->idx = ops->ops.size();
        ops->ops.push_back(cur_op);
        ops_map[key] = cur_op;
        DLOG(INFO) << "generate operator for plan "
                   << node::NameOfPlanNodeType(node->GetType());
        DLOG(INFO) << "ops_map size: " << ops_map.size();
        DLOG(INFO) << "ops_map insert key : " << key;
    } else {
        LOG(WARNING) << "fail to gen op " << node;
    }
    return cur_op;
}
OpNode* OpGenerator::GenScan(const ::fesql::node::ScanPlanNode* node,
                             const std::string& db, ::llvm::Module* module,
                             Status& status) {  // NOLINT
    if (node == NULL || module == NULL) {
        status.code = common::kNullPointer;
        status.msg = "input args has null";
        LOG(WARNING) << status.msg;
        return nullptr;
    }

    std::shared_ptr<TableStatus> table_status =
        table_mgr_->GetTableDef(db, node->GetTable());
    if (!table_status) {
        status.code = common::kTableNotFound;
        status.msg = "fail to find table " + node->GetTable();
        LOG(WARNING) << status.msg;
        return nullptr;
    }

    ScanOp* sop = new ScanOp();
    sop->db = db;
    sop->type = kOpScan;
    sop->tid = table_status->tid;
    sop->pid = table_status->pid;
    sop->limit = node->GetLimit();
    for (int32_t i = 0; i < table_status->table_def.columns_size(); i++) {
        sop->input_schema.push_back(table_status->table_def.columns(i));
        sop->output_schema.push_back(table_status->table_def.columns(i));
    }
    return sop;
}

OpNode* OpGenerator::GenProject(const ::fesql::node::ProjectListPlanNode* node,
                                const std::string& db, ::llvm::Module* module,
                                Status& status) {  // NOLINT
    if (node == NULL || module == NULL) {
        status.code = common::kNullPointer;
        status.msg = "input args has null";
        LOG(WARNING) << status.msg;
        return nullptr;
    }

    std::shared_ptr<TableStatus> table_status =
        table_mgr_->GetTableDef(db, node->GetTable());

    if (!table_status) {
        status.code = common::kTableNotFound;
        status.msg = "fail to find table " + node->GetTable();
        LOG(WARNING) << status.msg;
        return nullptr;
    }

    // TODO(wangtaize) use ops end op output schema
    ::fesql::codegen::RowFnLetIRBuilder builder(&table_status->table_def,
                                                module, node->IsWindowAgg());
    std::string fn_name = nullptr == node->GetW() ? "__internal_sql_codegen"
                                                  : "__internal_sql_codegen_" +
                                                        node->GetW()->GetName();
    std::vector<::fesql::type::ColumnDef> output_schema;

    bool ok = builder.Build(fn_name, node, output_schema);

    if (!ok) {
        status.code = common::kCodegenError;
        status.msg = "fail to run row fn builder";
        LOG(WARNING) << status.msg;
        return nullptr;
    }

    uint32_t output_size = 0;
    for (uint32_t i = 0; i < output_schema.size(); i++) {
        ::fesql::type::ColumnDef& column = output_schema[i];

        DLOG(INFO) << "output : " << column.name()
                   << " offset: " << output_size;
        switch (column.type()) {
            case ::fesql::type::kInt16: {
                output_size += 2;
                break;
            }
            case ::fesql::type::kInt32:
            case ::fesql::type::kFloat: {
                output_size += 4;
                break;
            }
            case ::fesql::type::kInt64:
            case ::fesql::type::kDouble:
            case ::fesql::type::kVarchar: {
                output_size += 8;
                break;
            }
            default: {
                status.code = common::kNullPointer;
                status.msg =
                    "not supported column type" + Type_Name(column.type());
                LOG(WARNING) << status.msg;
                return nullptr;
            }
        }
    }
    ProjectOp* pop = new ProjectOp();
    pop->type = kOpProject;
    pop->output_schema = output_schema;
    pop->fn_name = fn_name;
    pop->fn = NULL;
    pop->tid = table_status->tid;
    pop->db = table_status->db;
    pop->pid = table_status->pid;
    // handle window info
    if (nullptr != node->GetW()) {
        pop->window_agg = true;
        table_status->InitColumnInfos();
        for (auto key : node->GetW()->GetKeys()) {
            auto it = table_status->col_infos.find(key);
            if (it == table_status->col_infos.end()) {
                status.msg = "column " + key + " is not exist in table " +
                             table_status->table_def.name();
                status.code = common::kColumnNotFound;
                LOG(WARNING) << status.msg;
                return nullptr;
            }
            pop->w.keys.push_back(it->second);
        }
        for (auto order : node->GetW()->GetOrders()) {
            auto it = table_status->col_infos.find(order);
            if (it == table_status->col_infos.end()) {
                status.msg = "column " + order + " is not exist in table " +
                             table_status->table_def.name();
                status.code = common::kColumnNotFound;
                LOG(WARNING) << status.msg;
                return nullptr;
            }
            pop->w.orders.push_back(it->second);
        }
        pop->w.start_offset = node->GetW()->GetStartOffset();
        pop->w.end_offset = node->GetW()->GetEndOffset();
        pop->w.is_range_between = node->GetW()->IsRangeBetween();

        ::fesql::codegen::BufIRBuilder buf_if_builder(&table_status->table_def,
                                                      nullptr, nullptr);
    } else {
        pop->window_agg = false;
    }
    DLOG(INFO) << "project output size " << output_size;
    return pop;
}

OpNode* OpGenerator::GenLimit(const ::fesql::node::LimitPlanNode* node,
                              const std::string& db, ::llvm::Module* module,
                              Status& status) {  // NOLINT
    if (node == NULL || module == NULL) {
        status.code = common::kNullPointer;
        status.msg = "input args has null";
        LOG(WARNING) << status.msg;
        return nullptr;
    }

    LimitOp* limit_op = new LimitOp();
    limit_op->type = kOpLimit;
    limit_op->limit = node->GetLimitCnt();
    return limit_op;
}

bool OpGenerator::GenFnDef(::llvm::Module* module,
                           const ::fesql::node::FuncDefPlanNode* plan) {
    if (module == NULL || plan == NULL || plan->GetFnNodeList() == NULL) {
        LOG(WARNING) << "module or node is null";
        return false;
    }

    ::fesql::codegen::FnIRBuilder builder(module);
    bool ok = builder.Build(plan->GetFnNodeList());
    if (!ok) {
        LOG(WARNING) << "fail to build fn node with line ";
    }
    return ok;
}
OpNode* OpGenerator::GenMerge(const ::fesql::node::MergePlanNode* node,
                              ::llvm::Module* module,
                              Status& status) {  // NOLINT
    if (node == NULL || module == NULL) {
        status.code = common::kNullPointer;
        status.msg = "input args has null";
        LOG(WARNING) << status.msg;
        return nullptr;
    }

    MergeOp* merge_op = new MergeOp();
    merge_op->type = kOpLimit;
    merge_op->fn = nullptr;
    return merge_op;
}

}  // namespace vm
}  // namespace fesql
