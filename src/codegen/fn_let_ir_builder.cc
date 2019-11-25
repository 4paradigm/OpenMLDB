/*
 * fn_let_ir_builder.cc
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

#include "codegen/fn_let_ir_builder.h"

#include "codegen/buf_ir_builder.h"
#include "codegen/expr_ir_builder.h"
#include "codegen/ir_base_builder.h"
#include "glog/logging.h"

namespace fesql {
namespace codegen {

RowFnLetIRBuilder::RowFnLetIRBuilder(::fesql::type::TableDef* table,
                                     ::llvm::Module* module)
    : table_(table), module_(module) {}

RowFnLetIRBuilder::~RowFnLetIRBuilder() {}

/**
 * IR build for col_of_row(row, out_data)
 * @param name
 * @param col
 * @param type
 * @return
 */
bool RowFnLetIRBuilder::Build(const std::string& name, const std::string& col,
                              ::fesql::type::Type &type) {
    if (col.empty()) {
        LOG(WARNING) << "column name is empty";
        return false;
    }

    ::llvm::Function* fn = NULL;
    std::string row_ptr_name = "row_ptr_name";
    std::string output_ptr_name = "output_ptr_name";
    ::llvm::StringRef name_ref(name);
    if (module_->getFunction(name_ref) != NULL) {
        LOG(WARNING) << "function with name " << name << " exist";
        return false;
    }

    bool ok = BuildFnHeader(name, &fn);
    if (!ok) {
        LOG(WARNING) << "fail to build fn header for name " << name;
        return false;
    }

    ScopeVar sv;
    sv.Enter(name);

    ok = FillArgs(row_ptr_name, output_ptr_name, fn, sv);

    if (!ok) {
        LOG(WARNING) << "fail to fill args ";
        return false;
    }

    ::llvm::BasicBlock* block =
        ::llvm::BasicBlock::Create(module_->getContext(), "entry", fn);

    // TODO(wangtaize)
    BufIRBuilder buf_ir_builder(table_, block, &sv);
    ExprIRBuilder sql_expr_ir_builder(block, &sv, &buf_ir_builder, row_ptr_name,
                                      output_ptr_name, module_);

    int64_t offset = 0;
    const ::fesql::node::ColumnRefNode column_ref(col, "");
    ::llvm::Value* expr_out_val = NULL;
    ok = sql_expr_ir_builder.Build(&column_ref, &expr_out_val);

    if (!ok) {
        return false;
    }

    ::fesql::type::Type ctype;
    ok = GetTableType(expr_out_val->getType(), &ctype);
    if (!ok) {
        return false;
    }

    type = ctype;
    ok = StoreColumn(offset, expr_out_val, sv, output_ptr_name, block);
    if (!ok) {
        return false;
    }
    ::llvm::IRBuilder<> ir_builder(block);
    ::llvm::Value* ret = ir_builder.getInt32(0);
    ir_builder.CreateRet(ret);
    return true;
}
bool RowFnLetIRBuilder::Build(const std::string& name,
                              const ::fesql::node::ProjectListPlanNode* node,
                              std::vector<::fesql::type::ColumnDef>& schema) {
    if (node == NULL) {
        LOG(WARNING) << "node is null";
        return false;
    }

    ::llvm::Function* fn = NULL;
    std::string row_ptr_name = "row_ptr_name";
    std::string output_ptr_name = "output_ptr_name";
    ::llvm::StringRef name_ref(name);
    if (module_->getFunction(name_ref) != NULL) {
        LOG(WARNING) << "function with name " << name << " exist";
        return false;
    }

    bool ok = BuildFnHeader(name, &fn);
    if (!ok) {
        LOG(WARNING) << "fail to build fn header for name " << name;
        return false;
    }

    ScopeVar sv;
    sv.Enter(name);

    ok = FillArgs(row_ptr_name, output_ptr_name, fn, sv);

    if (!ok) {
        LOG(WARNING) << "fail to fill args ";
        return false;
    }

    ::llvm::BasicBlock* block =
        ::llvm::BasicBlock::Create(module_->getContext(), "entry", fn);

    // TODO(wangtaize)
    BufIRBuilder buf_ir_builder(table_, block, &sv);
    ExprIRBuilder sql_expr_ir_builder(block, &sv, &buf_ir_builder, row_ptr_name,
                                      output_ptr_name, module_);

    const ::fesql::node::PlanNodeList& children = node->GetProjects();
    ::fesql::node::PlanNodeList::const_iterator it = children.begin();
    int64_t offset = 0;
    for (; it != children.end(); it++) {
        const ::fesql::node::PlanNode* pn = *it;
        if (pn == NULL) {
            LOG(WARNING) << "plan node is null";
            continue;
        }

        if (pn->GetType() != ::fesql::node::kProject) {
            LOG(WARNING) << "project node is required but "
                         << ::fesql::node::NameOfPlanNodeType(pn->GetType());
            continue;
        }

        const ::fesql::node::ProjectPlanNode* pp_node =
            (const ::fesql::node::ProjectPlanNode*)pn;
        const ::fesql::node::ExprNode* sql_node = pp_node->GetExpression();

        ::llvm::Value* expr_out_val = NULL;
        std::string col_name = pp_node->GetName();
        ok = sql_expr_ir_builder.Build(sql_node, &expr_out_val);

        if (!ok) {
            return false;
        }

        ::fesql::type::Type ctype;
        ok = GetTableType(expr_out_val->getType(), &ctype);
        if (!ok) {
            return false;
        }

        ::fesql::type::ColumnDef cdef;
        cdef.set_name(col_name);
        cdef.set_type(ctype);
        schema.push_back(cdef);
        ok = StoreColumn(offset, expr_out_val, sv, output_ptr_name, block);

        if (!ok) {
            return false;
        }
        switch (cdef.type()) {
            case ::fesql::type::kInt16: {
                offset += 2;
                break;
            }
            case ::fesql::type::kInt32:
            case ::fesql::type::kFloat: {
                offset += 4;
                break;
            }
            case ::fesql::type::kInt64:
            case ::fesql::type::kDouble: {
                offset += 8;
                break;
            }
            default: {
                LOG(WARNING) << "not supported type ";
                return false;
            }
        }
    }
    ::llvm::IRBuilder<> ir_builder(block);
    ::llvm::Value* ret = ir_builder.getInt32(0);
    ir_builder.CreateRet(ret);
    return true;
}

bool RowFnLetIRBuilder::StoreColumn(int64_t offset, ::llvm::Value* value,
                                    ScopeVar& sv,
                                    const std::string& output_ptr_name,
                                    ::llvm::BasicBlock* block) {
    if (value == NULL || block == NULL) {
        LOG(WARNING) << "value is null";
        return true;
    }

    ::llvm::Value* out_ptr = NULL;
    bool ok = sv.FindVar(output_ptr_name, &out_ptr);

    if (!ok || out_ptr == NULL) {
        LOG(WARNING) << "fail to find output ptr with " << output_ptr_name;
        return false;
    }

    ::llvm::IRBuilder<> builder(block);
    ::llvm::Value* offset_val = builder.getInt64(offset);
    return BuildStoreOffset(builder, out_ptr, offset_val, value);
}


bool RowFnLetIRBuilder::BuildFnHeader(const std::string& name,
                                      std::vector<::llvm::Type*> &args_type,
                                      ::llvm::Type* ret_type,
                                      ::llvm::Function** fn) {
    if (fn == NULL) {
        LOG(WARNING) << "fn is null";
        return false;
    }
    ::llvm::ArrayRef<::llvm::Type*> array_ref(args_type);
    ::llvm::FunctionType* fnt = ::llvm::FunctionType::get(
        ret_type, array_ref, false);
    *fn = ::llvm::Function::Create(fnt, ::llvm::Function::ExternalLinkage, name,
                                   module_);
    LOG(INFO) << "create fn header " << name << " done";
    return true;

}

/**
 * Build function header with two int8 pointer and return int32
 * @param name
 * @param fn
 * @return
 */
bool RowFnLetIRBuilder::BuildFnHeader(const std::string& name,
                                      ::llvm::Function** fn) {
    std::vector<::llvm::Type*> args_type;
    args_type.push_back(::llvm::Type::getInt8PtrTy(module_->getContext()));
    args_type.push_back(::llvm::Type::getInt8PtrTy(module_->getContext()));
    return BuildFnHeader(name, args_type, ::llvm::Type::getInt32Ty(module_->getContext()), fn);
}

bool RowFnLetIRBuilder::FillArgs(const std::string& row_ptr_name,
                                 const std::string& output_ptr_name,
                                 ::llvm::Function* fn, ScopeVar& sv) {
    if (fn == NULL || fn->arg_size() != 2) {
        LOG(WARNING) << "fn is null";
        return false;
    }

    ::llvm::Function::arg_iterator it = fn->arg_begin();
    sv.AddVar(row_ptr_name, &*it);
    ++it;
    sv.AddVar(output_ptr_name, &*it);
    return true;
}


}  // namespace codegen
}  // namespace fesql
