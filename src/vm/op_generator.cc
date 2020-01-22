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

OpGenerator::OpGenerator(const std::shared_ptr<catalog::Catalog>& catalog) : catalog_(catalog) {}

OpGenerator::~OpGenerator() {}

bool OpGenerator::Gen(const ::fesql::node::PlanNodeList& trees,
                      const std::string& db, ::llvm::Module* module,
                      OpVector* ops, base::Status& status) {
    if (module == NULL || ops == NULL) {
        status.msg = "module or ops is null";
        status.code = common::kNullPointer;
        LOG(WARNING) << status.msg;
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
        status.msg = "input args has null";
        status.code = common::kNullPointer;
        LOG(WARNING) << status.msg;
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
    std::vector<OpNode*> tmp;
    if (false == RoutingNode(tree->GetChildren()[0], db, module, ops_map, ops,
                             tmp, status)) {
        LOG(WARNING) << "Fail to gen op";
        return false;
    } else {
        return true;
    }
}

bool OpGenerator::RoutingNode(
    const ::fesql::node::PlanNode* node, const std::string& db,
    ::llvm::Module* module,
    std::map<const std::string, OpNode*>& ops_map,  // NOLINT
    OpVector* ops, std::vector<OpNode*>& prev_children,
    Status& status) {  // NOLINT
    if (node == NULL || module == NULL || ops == NULL) {
        status.msg = "input args has null";
        status.code = common::kNullPointer;
        LOG(WARNING) << status.msg;
        return false;
    }
    // handle plan node that has been generated before
    std::stringstream ss;
    ss << node;
    std::string key = ss.str();
    auto iter = ops_map.find(key);
    DLOG(INFO) << "ops_map size: " << ops_map.size();
    if (iter != ops_map.end()) {
        DLOG(INFO) << "plan node already generate op node";
        prev_children.push_back(iter->second);
        return true;
    }
    // firstly, generate operator for node's children
    std::vector<::fesql::node::PlanNode*>::const_iterator it =
        node->GetChildren().cbegin();
    std::vector<OpNode*> children;
    for (; it != node->GetChildren().cend(); ++it) {
        const ::fesql::node::PlanNode* pn = *it;
        if (false ==
            RoutingNode(pn, db, module, ops_map, ops, children, status)) {
            return false;
        }
    }

    // secondly, generate operator for current node
    OpNode* cur_op = nullptr;
    switch (node->GetType()) {
        case ::fesql::node::kPlanTypeLimit: {
            const ::fesql::node::LimitPlanNode* limit_node =
                (const ::fesql::node::LimitPlanNode*)node;
            GenLimit(limit_node, db, module, &cur_op, status);
            break;
        }
        case ::fesql::node::kPlanTypeScan: {
            const ::fesql::node::ScanPlanNode* scan_node =
                (const ::fesql::node::ScanPlanNode*)node;
            GenScan(scan_node, db, module, &cur_op, status);
            break;
        }
        case ::fesql::node::kProjectList: {
            const ::fesql::node::ProjectListPlanNode* ppn =
                (const ::fesql::node::ProjectListPlanNode*)node;
            GenProject(ppn, db, module, &cur_op, status);
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
                return false;
            }
            GenMerge(merge_node, children, module, &cur_op, status);
            break;
        }
        default: {
            status.msg = "not supported plan node " +
                         node::NameOfPlanNodeType(node->GetType());
            status.code = common::kNullPointer;
            LOG(WARNING) << status.msg;
            return false;
        }
    }

    if (common::kOk != status.code) {
        return false;
    }

    if (nullptr != cur_op) {
        cur_op->children = children;
        cur_op->idx = ops->ops.size();
        ops->ops.push_back(cur_op);
        prev_children.push_back(cur_op);
        ops_map[key] = cur_op;
        DLOG(INFO) << "generate operator for plan "
                   << node::NameOfPlanNodeType(node->GetType());
    } else {
        LOG(WARNING) << "fail to gen op " << node;
    }
    return cur_op;
}
bool OpGenerator::GenScan(const ::fesql::node::ScanPlanNode* node,
                          const std::string& db, ::llvm::Module* module,
                          OpNode** op,
                          Status& status) {  // NOLINT

    if (node == NULL || module == NULL || nullptr == op) {
        status.code = common::kNullPointer;
        status.msg = "input args has null";
        LOG(WARNING) << status.msg;
        return false;
    }

    std::shared_ptr<catalog::TableHandler> table_handler = catalog_->GetTable(db, node->GetTable());
    if (!table_handler) {
        status.code = common::kTableNotFound;
        status.msg = "fail to find table " + node->GetTable();
        LOG(WARNING) << status.msg;
        return false;
    }

    ScanOp* sop = new ScanOp();
    sop->db = db;
    sop->type = kOpScan;
    sop->table_handler = table_handler;
    sop->limit = node->GetLimit();
    sop->output_schema = table_handler->GetSchema();
    *op = sop;
    return true;
}

bool OpGenerator::GenProject(const ::fesql::node::ProjectListPlanNode* node,
                             const std::string& db, ::llvm::Module* module,
                             OpNode** op,
                             Status& status) {  // NOLINT
    if (node == NULL || module == NULL || nullptr == op) {
        status.code = common::kNullPointer;
        status.msg = "input args has null";
        LOG(WARNING) << status.msg;
        return false;
    }
    std::shared_ptr<catalog::TableHandler> table_handler = catalog_->GetTable(db, node->GetTable());
    if (!table_handler) {
        status.code = common::kTableNotFound;
        status.msg = "fail to find table " + node->GetTable();
        LOG(WARNING) << status.msg;
        return false;
    }

    // TODO(wangtaize) use ops end op output schema
    ::fesql::codegen::RowFnLetIRBuilder builder(table_handler->GetSchema(),
                                                module, node->IsWindowAgg());
    std::string fn_name = nullptr == node->GetW() ? "__internal_sql_codegen"
                                                  : "__internal_sql_codegen_" +
                                                        node->GetW()->GetName();
    catalog::Schema output_schema;

    bool ok = builder.Build(fn_name, node, output_schema);

    if (!ok) {
        status.code = common::kCodegenError;
        status.msg = "fail to run row fn builder";
        LOG(WARNING) << status.msg;
        return false;
    }

    uint32_t output_size = 0;
    for (int32_t i = 0; i < output_schema.size(); i++) {
        const ::fesql::type::ColumnDef& column = output_schema.Get(i);

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
                return false;
            }
        }
    }
    ProjectOp* pop = new ProjectOp();
    pop->type = kOpProject;
    pop->output_schema = output_schema;
    pop->fn_name = fn_name;
    pop->fn = NULL;
    pop->table_handler = table_handler;
    pop->scan_limit = node->GetScanLimit();
    // handle window info
    if (nullptr != node->GetW()) {
        pop->window_agg = true;
        // validate and get col info of window keys
        for (auto key : node->GetW()->GetKeys()) {
            auto it = table_handler->GetTypes().find(key);
            if (it == table_handler->GetTypes().end()) {
                status.msg = "key column " + key + " is not exist in table " +
                             table_handler->GetName();
                status.code = common::kColumnNotFound;
                LOG(WARNING) << status.msg;
                delete pop;
                return false;
            }
            pop->w.keys.insert(it->second);
        }

        // validate and get col info of window order
        if (!node->GetW()->GetOrders().empty()) {
            if (node->GetW()->GetOrders().size() > 1) {
                status.msg =
                    "currently not support multiple ts columns in window";
                status.code = common::kColumnNotFound;
                LOG(WARNING) << status.msg;
                delete pop;
                return false;
            }
            auto order = node->GetW()->GetOrders()[0];
            auto it = table_handler->GetTypes().find(order);
            if (it == table_handler->GetTypes().end()) {
                status.msg = "ts column " + order + " is not exist in table " +
                             table_handler->GetName();
                status.code = common::kColumnNotFound;
                LOG(WARNING) << status.msg;
                delete pop;
                return false;
            }
            pop->w.order.first = it->second.first;
            pop->w.order.second = it->second.second;
            pop->w.has_order = true;
            pop->w.is_range_between = node->GetW()->IsRangeBetween();
        } else {
            pop->w.has_order = false;
        }

        // validate index
        /*auto index_map = table_status->table->GetIndexMap();
        bool index_check = false;
        for (auto iter = index_map.cbegin(); iter != index_map.cend(); iter++) {
            auto col_infos = iter->second.keys;
            // keys size match
            if (col_infos.size() != node->GetW()->GetKeys().size()) {
                continue;
            }
            // keys match
            bool match_keys = true;
            for (auto col_info : col_infos) {
                if (pop->w.keys.find(std::make_pair(
                        col_info.type, col_info.pos)) == pop->w.keys.end()) {
                    match_keys = false;
                    break;
                }
            }
            if (!match_keys) {
                continue;
            }
            // skip validate when order cols empty
            if (!pop->w.has_order) {
                index_check = true;
                pop->w.index_name = iter->second.name;
                break;
            }
            // ts col match
            auto ts_iter = pop->w.order;
            // ts col match
            if (ts_iter.second == iter->second.ts_pos) {
                index_check = true;
                pop->w.index_name = iter->second.name;
                break;
            }
            // currently not support multi orders
        }

        if (!index_check) {
            status.msg =
                "fail to generate project operator: index is not match window";
            status.code = common::kIndexNotFound;
            LOG(WARNING) << status.msg;
            delete pop;
            return false;
        }*/

        pop->w.start_offset = node->GetW()->GetStartOffset();
        pop->w.end_offset = node->GetW()->GetEndOffset();
    } else {
        pop->window_agg = false;
    }
    *op = pop;
    DLOG(INFO) << "project output size " << output_size;
    return true;
}

bool OpGenerator::GenLimit(const ::fesql::node::LimitPlanNode* node,
                           const std::string& db, ::llvm::Module* module,
                           OpNode** op,
                           Status& status) {  // NOLINT
    if (node == NULL || module == NULL || nullptr == op) {
        status.code = common::kNullPointer;
        status.msg = "input args has null";
        LOG(WARNING) << status.msg;
        return false;
    }

    LimitOp* limit_op = new LimitOp();
    limit_op->type = kOpLimit;
    limit_op->limit = node->GetLimitCnt();
    *op = limit_op;
    return true;
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

bool OpGenerator::GenMerge(const ::fesql::node::MergePlanNode* node,
                           const std::vector<OpNode*> children,
                           ::llvm::Module* module, OpNode** op,
                           Status& status) {  // NOLINT
    if (node == NULL || module == NULL || NULL == op) {
        status.code = common::kNullPointer;
        status.msg = "input args has null";
        LOG(WARNING) << status.msg;
        return false;
    }

    if (children.empty()) {
        status.code = common::kOpGenError;
        status.msg = "merge children is empty";
        LOG(WARNING) << status.msg;
        return false;
    }

    MergeOp* merge_op = new MergeOp();
    merge_op->pos_mapping = node->GetPosMapping();
    merge_op->output_schema.Clear();
    for (auto pair : merge_op->pos_mapping) {
        type::ColumnDef* cd = merge_op->output_schema.Add();
        cd->CopyFrom(children[pair.first]->output_schema.Get(pair.second));
    }
    merge_op->type = kOpMerge;
    merge_op->fn = nullptr;
    *op = merge_op;
    return true;
}

}  // namespace vm
}  // namespace fesql
