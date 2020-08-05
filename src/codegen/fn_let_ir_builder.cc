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

using ::fesql::base::Status;

namespace fesql {
namespace codegen {

RowFnLetIRBuilder::RowFnLetIRBuilder(const vm::SchemaSourceList& schema_sources,
                                     const node::FrameNode* frame,
                                     ::llvm::Module* module)
    : schema_context_(schema_sources), frame_(frame), module_(module) {}
RowFnLetIRBuilder::~RowFnLetIRBuilder() {}

Status RowFnLetIRBuilder::Build(
    const std::string& name, node::LambdaNode* compile_func,
    const std::vector<std::string>& project_names,
    const std::vector<node::FrameNode*>& project_frames,
    vm::Schema* output_schema, vm::ColumnSourceList* output_column_sources) {
    ::llvm::Function* fn = NULL;
    CHECK_TRUE(module_->getFunction(name) == NULL, "function ", name,
               " already exists");

    std::vector<std::string> args;
    std::vector<::llvm::Type*> args_llvm_type;
    args_llvm_type.push_back(::llvm::Type::getInt8PtrTy(module_->getContext()));
    args_llvm_type.push_back(::llvm::Type::getInt8PtrTy(module_->getContext()));
    args_llvm_type.push_back(
        ::llvm::Type::getInt8PtrTy(module_->getContext())->getPointerTo());

    std::string output_ptr_name = "output_ptr_name";
    args.push_back("@row_ptr");
    args.push_back("@window");
    args.push_back(output_ptr_name);

    Status status;
    bool ok =
        BuildFnHeader(name, args_llvm_type,
                      ::llvm::Type::getInt32Ty(module_->getContext()), &fn);
    CHECK_TRUE(ok && fn != nullptr, "Fail to build fn header for name ", name);

    // bind input function argument
    ScopeVar sv;
    sv.Enter(name);
    CHECK_TRUE(FillArgs(args, fn, sv));

    // bind row arg
    NativeValue row_arg_value;
    CHECK_TRUE(sv.FindVar("@row_ptr", &row_arg_value));
    sv.AddVar(compile_func->GetArg(0)->GetExprString(), row_arg_value);

    ::llvm::BasicBlock* block =
        ::llvm::BasicBlock::Create(module_->getContext(), "entry", fn);
    VariableIRBuilder variable_ir_builder(block, &sv);

    NativeValue window;
    variable_ir_builder.LoadWindow("", &window, status);
    variable_ir_builder.StoreWindow(
        nullptr == frame_ ? "" : frame_->GetExprString(), window.GetRaw(),
        status);
    CHECK_TRUE(!schema_context_.row_schema_info_list_.empty(),
               "Fail to build fn: row info list is empty");

    ExprIRBuilder expr_ir_builder(block, &sv, &schema_context_, true, module_);

    std::map<uint32_t, NativeValue> outputs;

    // maybe collect agg expressions
    std::map<std::string, AggregateIRBuilder> window_agg_builder;
    uint32_t agg_builder_id = 0;

    auto expr_list = compile_func->body();
    CHECK_TRUE(project_frames.size() == expr_list->GetChildNum(),
               "Frame num should match expr num");
    CHECK_TRUE(project_names.size() == expr_list->GetChildNum(),
               "Output name num should match expr num");

    for (size_t i = 0; i < expr_list->GetChildNum(); ++i) {
        const ::fesql::node::ExprNode* expr = expr_list->GetChild(i);

        std::string project_name = project_names[i];
        auto frame = project_frames[i];
        const std::string frame_str =
            frame == nullptr ? "" : frame->GetExprString();

        ::fesql::type::Type col_agg_type;

        auto agg_iter = window_agg_builder.find(frame_str);

        if (agg_iter == window_agg_builder.end()) {
            window_agg_builder.insert(std::make_pair(
                frame_str, AggregateIRBuilder(&schema_context_, module_, frame,
                                              agg_builder_id++)));
            agg_iter = window_agg_builder.find(frame_str);
        }
        if (agg_iter->second.CollectAggColumn(expr, i, &col_agg_type)) {
            AddOutputColumnInfo(project_name, col_agg_type, expr, output_schema,
                                output_column_sources);
            continue;
        }

        CHECK_TRUE(expr->GetExprType() != node::kExprAll,
                   "* should be resolved before codegen stage");

        // bind window frame
        CHECK_STATUS(BindProjectFrame(&expr_ir_builder, frame, compile_func,
                                      block, &sv));

        CHECK_TRUE(
            BuildProject(i, expr, project_name, &outputs, expr_ir_builder,
                         output_schema, output_column_sources, status),
            "Build expr failed: ", status.msg, "\n", expr->GetTreeString());
    }

    CHECK_TRUE(EncodeBuf(&outputs, *output_schema, variable_ir_builder, block,
                         output_ptr_name),
               "Gen encode into output buffer failed");

    if (!window_agg_builder.empty()) {
        for (auto iter = window_agg_builder.begin();
             iter != window_agg_builder.end(); iter++) {
            if (!iter->second.empty()) {
                CHECK_TRUE(iter->second.BuildMulti(
                               name, &expr_ir_builder, &variable_ir_builder,
                               block, output_ptr_name, output_schema),
                           "Multi column sum codegen failed");
            }
        }
    }

    ::llvm::IRBuilder<> ir_builder(block);
    ::llvm::Value* ret = ir_builder.getInt32(0);
    ir_builder.CreateRet(ret);
    return Status::OK();
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
        case fesql::node::kExprGetField: {
            const ::fesql::node::GetFieldExpr* column_expr =
                (const ::fesql::node::GetFieldExpr*)expr;
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

Status RowFnLetIRBuilder::BindProjectFrame(ExprIRBuilder* expr_ir_builder,
                                           node::FrameNode* frame,
                                           node::LambdaNode* compile_func,
                                           ::llvm::BasicBlock* block,
                                           ScopeVar* sv) {
    Status status;
    expr_ir_builder->set_frame(frame);
    auto window_arg = compile_func->GetArg(1);
    auto window_key = window_arg->GetExprString();
    NativeValue window_arg_value;
    CHECK_TRUE(expr_ir_builder->BuildWindow(&window_arg_value, status),
               "Bind window failed: ", status.msg);

    ::llvm::IRBuilder<> builder(block);
    ::llvm::Value* frame_ptr = window_arg_value.GetValue(&builder);
    CHECK_TRUE(frame_ptr != nullptr && frame_ptr->getType()->isPointerTy());
    ::llvm::Type* row_list_ptr_ty = nullptr;
    CHECK_TRUE(
        GetLLVMType(module_, window_arg->GetOutputType(), &row_list_ptr_ty));
    frame_ptr = builder.CreatePointerCast(frame_ptr, row_list_ptr_ty);
    window_arg_value = window_arg_value.Replace(frame_ptr);
    if (sv->ExistVar(window_key)) {
        sv->ReplaceVar(window_key, window_arg_value);
    } else {
        sv->AddVar(window_key, window_arg_value);
    }
    return Status::OK();
}

}  // namespace codegen
}  // namespace fesql
