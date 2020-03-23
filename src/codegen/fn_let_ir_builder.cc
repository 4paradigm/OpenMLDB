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
#include "codegen/expr_ir_builder.h"
#include "codegen/ir_base_builder.h"
#include "codegen/variable_ir_builder.h"
#include "glog/logging.h"

namespace fesql {
namespace codegen {

RowFnLetIRBuilder::RowFnLetIRBuilder(const vm::Schema& schema,
                                     ::llvm::Module* module)
    : schema_(schema), module_(module) {}

RowFnLetIRBuilder::RowFnLetIRBuilder(const vm::Schema& schema,
                                     ::llvm::Module* module, bool is_window_agg)
    : schema_(schema), module_(module) {}

RowFnLetIRBuilder::~RowFnLetIRBuilder() {}

bool RowFnLetIRBuilder::Build(
    const std::string& name, const node::PlanNodeList& projects,
    const bool row_mode,
    vm::Schema& output_schema) {  // NOLINT (runtime/references)
    ::llvm::Function* fn = NULL;
    std::string row_ptr_name = "row_ptr_name";
    std::string window_ptr_name = "window_ptr_name";
    std::string row_size_name = "row_size_name";
    std::string output_ptr_name = "output_ptr_name";
    ::llvm::StringRef name_ref(name);
    if (module_->getFunction(name_ref) != NULL) {
        LOG(WARNING) << "function with name " << name << " exist";
        return false;
    }

    base::Status status;
    bool ok = BuildFnHeader(name, &fn);

    if (!ok || fn == NULL) {
        LOG(WARNING) << "fail to build fn header for name " << name;
        return false;
    }

    ScopeVar sv;
    sv.Enter(name);

    std::vector<std::string> args = {row_ptr_name, window_ptr_name,
                                     row_size_name, output_ptr_name};
    ok = FillArgs(args, fn, sv);

    if (!ok) {
        return false;
    }

    ::llvm::BasicBlock* block =
        ::llvm::BasicBlock::Create(module_->getContext(), "entry", fn);
    VariableIRBuilder variable_ir_builder(block, &sv);
    ExprIRBuilder expr_ir_builder(
        block, &sv, schema_, true, row_ptr_name, window_ptr_name, row_size_name, module_);

    ::fesql::node::PlanNodeList::const_iterator it = projects.cbegin();
    std::map<uint32_t, ::llvm::Value*> outputs;
    uint32_t index = 0;
    for (; it != projects.cend(); it++) {
        const ::fesql::node::PlanNode* pn = *it;
        if (pn == NULL) {
            LOG(WARNING) << "plan node is null";
            return false;
        }
        if (pn->GetType() != ::fesql::node::kProjectNode) {
            LOG(WARNING) << "project node is required but "
                         << ::fesql::node::NameOfPlanNodeType(pn->GetType());
            return false;
        }

        const ::fesql::node::ProjectNode* pp_node =
            (const ::fesql::node::ProjectNode*)pn;
        const ::fesql::node::ExprNode* sql_node = pp_node->GetExpression();

        switch (sql_node->expr_type_) {
            case node::kExprAll: {
                const node::AllNode* expr_all =
                    dynamic_cast<const node::AllNode*>(sql_node);
                for (auto expr : expr_all->children_) {
                    auto column_ref = dynamic_cast<node::ColumnRefNode*>(expr);
                    if (!BuildProject(index, column_ref,
                                      column_ref->GetColumnName(), &outputs,
                                      expr_ir_builder, output_schema,
                                      status)) {
                        return false;
                    }
                    index++;
                }
                break;
            }
            default: {
                std::string col_name = pp_node->GetName();
                if (!BuildProject(index, sql_node, col_name, &outputs,
                                  expr_ir_builder, output_schema,
                                  status)) {
                    return false;
                }
                index++;
                break;
            }
        }
    }

    ok = EncodeBuf(&outputs, output_schema, variable_ir_builder, block,
                   output_ptr_name);
    if (!ok) {
        return false;
    }

    ::llvm::IRBuilder<> ir_builder(block);
    ::llvm::Value* ret = ir_builder.getInt32(0);
    ir_builder.CreateRet(ret);
    return true;
}

bool RowFnLetIRBuilder::Build(const std::string& name,
                              const node::ProjectListNode* projects,
                              vm::Schema& output_schema) {
    return Build(name, projects->GetProjects(), !projects->is_window_agg_,
                 output_schema);
}

bool RowFnLetIRBuilder::EncodeBuf(
    const std::map<uint32_t, ::llvm::Value*>* values, const vm::Schema& schema,
    VariableIRBuilder& variable_ir_builder,  // NOLINT (runtime/references)
    ::llvm::BasicBlock* block, const std::string& output_ptr_name) {
    base::Status status;
    BufNativeEncoderIRBuilder encoder(values, schema, block);
    ::llvm::Value* row_ptr = NULL;
    bool ok = variable_ir_builder.LoadValue(output_ptr_name, &row_ptr, status);
    if (!ok) {
        LOG(WARNING) << "fail to get row ptr";
        return false;
    }
    return encoder.BuildEncode(row_ptr);
}

bool RowFnLetIRBuilder::BuildFnHeader(
    const std::string& name, const std::vector<::llvm::Type*>& args_type,
    ::llvm::Type* ret_type, ::llvm::Function** fn) {
    if (fn == NULL) {
        LOG(WARNING) << "fn is null";
        return false;
    }
    DLOG(INFO) << "create fn header " << name << " start";
    ::llvm::ArrayRef<::llvm::Type*> array_ref(args_type);
    ::llvm::FunctionType* fnt =
        ::llvm::FunctionType::get(ret_type, array_ref, false);

    ::llvm::Function* f = ::llvm::Function::Create(
        fnt, ::llvm::Function::ExternalLinkage, name, module_);
    if (f == NULL) {
        LOG(WARNING) << "fail to create fn with name " << name;
        return false;
    }
    *fn = f;
    DLOG(INFO) << "create fn header " << name << " done";
    return true;
}

// Build function header with two int8 pointer and return int32
// param name
// param fn
// return
bool RowFnLetIRBuilder::BuildFnHeader(const std::string& name,
                                      ::llvm::Function** fn) {
    std::vector<::llvm::Type*> args_type;
    args_type.push_back(::llvm::Type::getInt8PtrTy(module_->getContext()));
    args_type.push_back(::llvm::Type::getInt8PtrTy(module_->getContext()));
    args_type.push_back(::llvm::Type::getInt32Ty(module_->getContext()));
    args_type.push_back(
        ::llvm::Type::getInt8PtrTy(module_->getContext())->getPointerTo());

    return BuildFnHeader(name, args_type,
                         ::llvm::Type::getInt32Ty(module_->getContext()), fn);
}

bool RowFnLetIRBuilder::FillArgs(const std::vector<std::string>& args,
                                 ::llvm::Function* fn,
                                 ScopeVar& sv) {  // NOLINT
    if (fn == NULL || fn->arg_size() != args.size()) {
        LOG(WARNING) << "fn is null or fn arg size mismatch";
        return false;
    }
    ::llvm::Function::arg_iterator it = fn->arg_begin();
    for (auto arg : args) {
        sv.AddVar(arg, &*it);
        ++it;
    }
    return true;
}
bool RowFnLetIRBuilder::BuildProject(
    const uint32_t index, const node::ExprNode* expr,
    const std::string& col_name, std::map<uint32_t, ::llvm::Value*>* outputs,
    ExprIRBuilder& expr_ir_builder, vm::Schema& output_schema,  // NOLINT
    base::Status& status) {                                     // NOLINT
    ::llvm::Value* expr_out_val = NULL;
    bool ok = expr_ir_builder.Build(expr, &expr_out_val, status);
    if (!ok) {
        LOG(WARNING) << "fail to codegen project expression: " << status.msg;
        return false;
    }
    ::fesql::node::DataType data_type;
    ok = GetBaseType(expr_out_val->getType(), &data_type);
    if (!ok) {
        return false;
    }
    ::fesql::type::Type ctype;
    if (!DataType2SchemaType(data_type, &ctype)) {
        return false;
    }
    outputs->insert(std::make_pair(index, expr_out_val));
    ::fesql::type::ColumnDef* cdef = output_schema.Add();
    cdef->set_name(col_name);
    cdef->set_type(ctype);
    return true;
}

}  // namespace codegen
}  // namespace fesql
