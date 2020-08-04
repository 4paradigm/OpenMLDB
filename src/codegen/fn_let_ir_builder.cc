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
#include "codegen/aggregate_ir_builder.h"
#include "codegen/expr_ir_builder.h"
#include "codegen/ir_base_builder.h"
#include "codegen/variable_ir_builder.h"
#include "glog/logging.h"
#include "vm/transform.h"

namespace fesql {
namespace codegen {

RowFnLetIRBuilder::RowFnLetIRBuilder(const vm::SchemaSourceList& schema_sources,
                                     const node::FrameNode* frame,
                                     ::llvm::Module* module)
    : schema_context_(schema_sources), frame_(frame), module_(module) {}
RowFnLetIRBuilder::~RowFnLetIRBuilder() {}

bool RowFnLetIRBuilder::Build(const std::string& name,
                              node::LambdaNode* project_func,
                              vm::Schema* output_schema,
                              vm::ColumnSourceList* output_column_sources) {}

/**
 * Codegen For int32 RowFnLetUDF(int_8* row_ptrs, int8_t* window_ptr, int32 *
 * row_sizes, int8_t * output_ptr)
 * @param name
 * @param projects
 * @param output_schema
 * @return
 */
bool RowFnLetIRBuilder::Build(
    const std::string& name, const node::PlanNodeList& projects,
    vm::Schema* output_schema,
    vm::ColumnSourceList*
        output_column_sources) {  // NOLINT (runtime/references)
    ::llvm::Function* fn = NULL;
    std::string output_ptr_name = "output_ptr_name";
    ::llvm::StringRef name_ref(name);
    if (module_->getFunction(name_ref) != NULL) {
        LOG(WARNING) << "function with name " << name << " exist";
        return false;
    }

    std::vector<std::string> args;
    std::vector<::llvm::Type*> args_llvm_type;
    args_llvm_type.push_back(
        ::llvm::Type::getInt8PtrTy(module_->getContext())->getPointerTo());
    args_llvm_type.push_back(::llvm::Type::getInt8PtrTy(module_->getContext()));
    args_llvm_type.push_back(
        ::llvm::Type::getInt32Ty(module_->getContext())->getPointerTo());
    args_llvm_type.push_back(
        ::llvm::Type::getInt8PtrTy(module_->getContext())->getPointerTo());

    args.push_back("@row_ptrs");
    args.push_back("@window");
    args.push_back("@row_sizes");
    args.push_back(output_ptr_name);

    base::Status status;
    bool ok =
        BuildFnHeader(name, args_llvm_type,
                      ::llvm::Type::getInt32Ty(module_->getContext()), &fn);

    if (!ok || fn == NULL) {
        LOG(WARNING) << "fail to build fn header for name " << name;
        return false;
    }

    ScopeVar sv;
    sv.Enter(name);
    ok = FillArgs(args, fn, sv);
    if (!ok) {
        return false;
    }

    ::llvm::BasicBlock* block =
        ::llvm::BasicBlock::Create(module_->getContext(), "entry", fn);
    VariableIRBuilder variable_ir_builder(block, &sv);

    NativeValue window;
    variable_ir_builder.LoadWindow("", &window, status);
    variable_ir_builder.StoreWindow(
        nullptr == frame_ ? "" : frame_->GetExprString(), window.GetRaw(),
        status);
    if (schema_context_.row_schema_info_list_.empty()) {
        LOG(WARNING) << "fail to build fn: row info list is empty";
        return false;
    }
    ExprIRBuilder expr_ir_builder(block, &sv, &schema_context_, true, module_);
    ::fesql::node::PlanNodeList::const_iterator it = projects.cbegin();
    std::map<uint32_t, NativeValue> outputs;

    // maybe collect agg expressions
    std::map<std::string, AggregateIRBuilder> window_agg_builder;
    uint32_t agg_builder_id = 0;

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
        ::fesql::type::Type col_agg_type;
        const std::string frame_str = nullptr == pp_node->frame()
                                          ? ""
                                          : pp_node->frame()->GetExprString();
        auto agg_iter = window_agg_builder.find(frame_str);

        if (agg_iter == window_agg_builder.end()) {
            window_agg_builder.insert(std::make_pair(
                frame_str,
                AggregateIRBuilder(&schema_context_, module_, pp_node->frame(),
                                   agg_builder_id++)));
            agg_iter = window_agg_builder.find(frame_str);
        }
        if (agg_iter->second.CollectAggColumn(sql_node, index, &col_agg_type)) {
            AddOutputColumnInfo(pp_node->GetName(), col_agg_type, sql_node,
                                output_schema, output_column_sources);
            index++;
            continue;
        }

        switch (sql_node->expr_type_) {
            case node::kExprAll: {
                const node::AllNode* expr_all =
                    dynamic_cast<const node::AllNode*>(sql_node);
                for (auto expr : expr_all->children_) {
                    auto column_ref = dynamic_cast<node::ColumnRefNode*>(expr);
                    expr_ir_builder.set_frame(pp_node->frame());
                    if (!BuildProject(index, column_ref,
                                      column_ref->GetColumnName(), &outputs,
                                      expr_ir_builder, output_schema,
                                      output_column_sources, status)) {
                        return false;
                    }
                    index++;
                }
                break;
            }
            default: {
                std::string col_name = pp_node->GetName();
                expr_ir_builder.set_frame(pp_node->frame());
                if (!BuildProject(index, sql_node, col_name, &outputs,
                                  expr_ir_builder, output_schema,
                                  output_column_sources, status)) {
                    return false;
                }
                index++;
                break;
            }
        }
    }

    ok = EncodeBuf(&outputs, *output_schema, variable_ir_builder, block,
                   output_ptr_name);
    if (!ok) {
        return false;
    }

    if (!window_agg_builder.empty()) {
        for (auto iter = window_agg_builder.begin();
             iter != window_agg_builder.end(); iter++) {
            if (!iter->second.empty()) {
                if (!iter->second.BuildMulti(name, &expr_ir_builder,
                                             &variable_ir_builder, block,
                                             output_ptr_name, output_schema)) {
                    LOG(WARNING) << "Multi column sum codegen failed";
                    return false;
                }
            }
        }
    }

    ::llvm::IRBuilder<> ir_builder(block);
    ::llvm::Value* ret = ir_builder.getInt32(0);
    ir_builder.CreateRet(ret);
    return true;
}

bool RowFnLetIRBuilder::EncodeBuf(
    const std::map<uint32_t, NativeValue>* values, const vm::Schema& schema,
    VariableIRBuilder& variable_ir_builder,  // NOLINT (runtime/references)
    ::llvm::BasicBlock* block, const std::string& output_ptr_name) {
    base::Status status;
    BufNativeEncoderIRBuilder encoder(values, schema, block);
    NativeValue row_ptr;
    bool ok = variable_ir_builder.LoadValue(output_ptr_name, &row_ptr, status);
    if (!ok) {
        LOG(WARNING) << "fail to get row ptr";
        return false;
    }
    ::llvm::IRBuilder<> ir_builder(block);
    return encoder.BuildEncode(row_ptr.GetValue(&ir_builder));
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

bool RowFnLetIRBuilder::FillArgs(const std::vector<std::string>& args,
                                 ::llvm::Function* fn,
                                 ScopeVar& sv) {  // NOLINT
    if (fn == NULL || fn->arg_size() != args.size()) {
        LOG(WARNING) << "fn is null or fn arg size mismatch";
        return false;
    }
    ::llvm::Function::arg_iterator it = fn->arg_begin();
    for (auto arg : args) {
        sv.AddVar(arg, NativeValue::Create(&*it));
        ++it;
    }
    return true;
}
bool RowFnLetIRBuilder::BuildProject(
    const uint32_t index, const node::ExprNode* expr,
    const std::string& col_name, std::map<uint32_t, NativeValue>* outputs,
    ExprIRBuilder& expr_ir_builder, vm::Schema* output_schema,  // NOLINT
    vm::ColumnSourceList* output_column_sources,
    base::Status& status) {  // NOLINT

    NativeValue expr_out_val;
    bool ok = expr_ir_builder.Build(expr, &expr_out_val, status);
    if (!ok) {
        LOG(WARNING) << "fail to codegen project expression: " << status.msg;
        return false;
    }
    ::fesql::node::TypeNode data_type;
    ok = GetFullType(expr_out_val.GetType(), &data_type);
    if (!ok) {
        return false;
    }
    ::fesql::type::Type ctype;
    if (!DataType2SchemaType(data_type, &ctype)) {
        return false;
    }
    llvm::IRBuilder<> builder(expr_ir_builder.block());
    outputs->insert(std::make_pair(index, expr_out_val));

    return AddOutputColumnInfo(col_name, ctype, expr, output_schema,
                               output_column_sources);
}

bool RowFnLetIRBuilder::AddOutputColumnInfo(
    const std::string& col_name, ::fesql::type::Type ctype,
    const node::ExprNode* expr, vm::Schema* output_schema,
    vm::ColumnSourceList* output_column_sources) {
    ::fesql::type::ColumnDef* cdef = output_schema->Add();
    cdef->set_name(col_name);
    cdef->set_type(ctype);
    switch (expr->GetExprType()) {
        case fesql::node::kExprColumnRef: {
            const ::fesql::node::ColumnRefNode* column_expr =
                (const ::fesql::node::ColumnRefNode*)expr;
            output_column_sources->push_back(
                schema_context_.ColumnSourceResolved(
                    column_expr->GetRelationName(),
                    column_expr->GetColumnName()));
            break;
        }
        case fesql::node::kExprPrimary: {
            auto const_expr = dynamic_cast<const node::ConstNode*>(expr);
            output_column_sources->push_back(vm::ColumnSource(const_expr));
            break;
        }
        default: {
            output_column_sources->push_back(vm::ColumnSource());
        }
    }
    return true;
}

}  // namespace codegen
}  // namespace fesql
