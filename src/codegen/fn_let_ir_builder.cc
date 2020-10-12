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
#include "codegen/context.h"
#include "codegen/expr_ir_builder.h"
#include "codegen/ir_base_builder.h"
#include "codegen/variable_ir_builder.h"
#include "glog/logging.h"
#include "vm/transform.h"

using ::fesql::base::Status;

namespace fesql {
namespace codegen {

RowFnLetIRBuilder::RowFnLetIRBuilder(CodeGenContext* ctx,
                                     const node::FrameNode* frame)
    : ctx_(ctx), frame_(frame) {}
RowFnLetIRBuilder::~RowFnLetIRBuilder() {}

Status RowFnLetIRBuilder::Build(
    const std::string& name, node::LambdaNode* compile_func,
    const std::vector<std::string>& project_names,
    const std::vector<node::FrameNode*>& project_frames,
    vm::Schema* output_schema, vm::ColumnSourceList* output_column_sources) {
    ::llvm::Function* fn = NULL;

    ::llvm::Module* module = ctx_->GetModule();
    CHECK_TRUE(module->getFunction(name) == NULL, kCodegenError, "function ",
               name, " already exists");

    std::vector<std::string> args;
    std::vector<::llvm::Type*> args_llvm_type;
    args_llvm_type.push_back(::llvm::Type::getInt8PtrTy(module->getContext()));
    args_llvm_type.push_back(::llvm::Type::getInt8PtrTy(module->getContext()));
    args_llvm_type.push_back(
        ::llvm::Type::getInt8PtrTy(module->getContext())->getPointerTo());

    std::string output_ptr_name = "output_ptr_name";
    args.push_back("@row_ptr");
    args.push_back("@window");
    args.push_back(output_ptr_name);

    Status status;
    bool ok =
        BuildFnHeader(name, args_llvm_type,
                      ::llvm::Type::getInt32Ty(module->getContext()), &fn);
    CHECK_TRUE(ok && fn != nullptr, kCodegenError,
               "Fail to build fn header for name ", name);

    // Enter in function
    FunctionScopeGuard fn_guard(fn, ctx_);

    // Bind input function argument
    auto sv = ctx_->GetCurrentScope()->sv();
    CHECK_TRUE(FillArgs(args, fn, sv), kCodegenError);

    // bind row arg
    NativeValue row_arg_value;
    CHECK_TRUE(sv->FindVar("@row_ptr", &row_arg_value), kCodegenError);
    sv->AddVar(compile_func->GetArg(0)->GetExprString(), row_arg_value);

    ::llvm::BasicBlock* block = ctx_->GetCurrentBlock();
    VariableIRBuilder variable_ir_builder(block, sv);

    NativeValue window;
    variable_ir_builder.LoadWindow("", &window, status);
    variable_ir_builder.StoreWindow(
        nullptr == frame_ ? "" : frame_->GetExprString(), window.GetRaw(),
        status);

    ExprIRBuilder expr_ir_builder(ctx_);

    std::map<uint32_t, NativeValue> outputs;

    // maybe collect agg expressions
    std::map<std::string, AggregateIRBuilder> window_agg_builder;
    uint32_t agg_builder_id = 0;

    auto expr_list = compile_func->body();
    CHECK_TRUE(project_frames.size() == expr_list->GetChildNum(), kCodegenError,
               "Frame num should match expr num");
    CHECK_TRUE(project_names.size() == expr_list->GetChildNum(), kCodegenError,
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
                frame_str, AggregateIRBuilder(ctx_->schemas_context(), module,
                                              frame, agg_builder_id++)));
            agg_iter = window_agg_builder.find(frame_str);
        }
        if (agg_iter->second.CollectAggColumn(expr, i, &col_agg_type)) {
            AddOutputColumnInfo(project_name, col_agg_type, expr, output_schema,
                                output_column_sources);
            continue;
        }

        CHECK_TRUE(expr->GetExprType() != node::kExprAll, kCodegenError,
                   "* should be resolved before codegen stage");

        // bind window frame
        CHECK_STATUS(
            BindProjectFrame(&expr_ir_builder, frame, compile_func, block, sv));

        CHECK_STATUS(
            BuildProject(&expr_ir_builder, i, expr, project_name, &outputs,
                         output_schema, output_column_sources),
            "Build expr failed at ", i, ":\n", expr->GetTreeString());
    }

    CHECK_TRUE(EncodeBuf(&outputs, output_schema, variable_ir_builder, block,
                         output_ptr_name),
               kCodegenError, "Gen encode into output buffer failed");

    if (!window_agg_builder.empty()) {
        for (auto iter = window_agg_builder.begin();
             iter != window_agg_builder.end(); iter++) {
            if (!iter->second.empty()) {
                CHECK_TRUE(iter->second.BuildMulti(
                               name, &expr_ir_builder, &variable_ir_builder,
                               block, output_ptr_name, output_schema),
                           kCodegenError, "Multi column sum codegen failed");
            }
        }
    }

    ::llvm::IRBuilder<> ir_builder(block);
    ::llvm::Value* ret = ir_builder.getInt32(0);
    ir_builder.CreateRet(ret);

    // reformat scope
    auto root_scope = ctx_->GetCurrentScope();
    root_scope->blocks()->DropEmptyBlocks();
    root_scope->blocks()->ReInsertTo(fn);
    return Status::OK();
}

bool RowFnLetIRBuilder::EncodeBuf(
    const std::map<uint32_t, NativeValue>* values, const vm::Schema* schema,
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
        fnt, ::llvm::Function::ExternalLinkage, name, ctx_->GetModule());
    if (f == NULL) {
        LOG(WARNING) << "fail to create fn with name " << name;
        return false;
    }
    *fn = f;
    DLOG(INFO) << "create fn header " << name << " done";
    return true;
}

bool RowFnLetIRBuilder::FillArgs(const std::vector<std::string>& args,
                                 ::llvm::Function* fn, ScopeVar* sv) {
    if (fn == NULL || fn->arg_size() != args.size()) {
        LOG(WARNING) << "fn is null or fn arg size mismatch";
        return false;
    }
    ::llvm::Function::arg_iterator it = fn->arg_begin();
    for (auto arg : args) {
        sv->AddVar(arg, NativeValue::Create(&*it));
        ++it;
    }
    return true;
}

Status RowFnLetIRBuilder::BuildProject(
    ExprIRBuilder* expr_ir_builder, const uint32_t index,
    const node::ExprNode* expr, const std::string& col_name,
    std::map<uint32_t, NativeValue>* outputs,
    vm::Schema* output_schema,  // NOLINT
    vm::ColumnSourceList* output_column_sources) {
    NativeValue expr_out_val;

    CHECK_STATUS(expr_ir_builder->Build(expr, &expr_out_val),
                 "Fail to codegen project expression: ", expr->GetExprString());

    const ::fesql::node::TypeNode* data_type = nullptr;
    CHECK_TRUE(!expr_out_val.IsTuple(), kCodegenError,
               "Output do not support tuple");
    ::llvm::Type* llvm_ty = expr_out_val.GetType();
    CHECK_TRUE(llvm_ty != nullptr, kCodegenError);
    CHECK_TRUE(GetFullType(ctx_->node_manager(), llvm_ty, &data_type),
               kCodegenError, "Fail to get output type at ", index, ", expect ",
               expr->GetOutputType()->GetName());

    ::fesql::type::Type ctype;
    CHECK_TRUE(DataType2SchemaType(*data_type, &ctype), kCodegenError);

    outputs->insert(std::make_pair(index, expr_out_val));
    AddOutputColumnInfo(col_name, ctype, expr, output_schema,
                        output_column_sources);
    return Status::OK();
}

bool RowFnLetIRBuilder::AddOutputColumnInfo(
    const std::string& col_name, ::fesql::type::Type ctype,
    const node::ExprNode* expr, vm::Schema* output_schema,
    vm::ColumnSourceList* output_column_sources) {
    ::fesql::type::ColumnDef* cdef = output_schema->Add();
    cdef->set_name(col_name);
    cdef->set_type(ctype);
    LOG(INFO) << "add output column info: expression type: "
              << fesql::node::ExprTypeName(expr->expr_type_);
    switch (expr->GetExprType()) {
        case fesql::node::kExprGetField: {
            const ::fesql::node::GetFieldExpr* column_expr =
                (const ::fesql::node::GetFieldExpr*)expr;
            output_column_sources->push_back(
                ctx_->schemas_context()->ColumnSourceResolved(
                    column_expr->GetRelationName(),
                    column_expr->GetColumnName()));
            break;
        }
        case fesql::node::kExprCast: {
            const ::fesql::node::CastExprNode* cast_expr =
                dynamic_cast<const ::fesql::node::CastExprNode*>(expr);

            if (nullptr == cast_expr->expr()) {
                return false;
            }
            LOG(INFO) << "cast expression type: "
                      << fesql::node::ExprTypeName(
                             cast_expr->expr()->expr_type_);
            switch (cast_expr->expr()->expr_type_) {
                case fesql::node::kExprGetField: {
                    const ::fesql::node::GetFieldExpr* column_expr =
                        (const ::fesql::node::GetFieldExpr*)cast_expr->expr();
                    output_column_sources->push_back(
                        ctx_->schemas_context()->ColumnSourceResolved(
                            column_expr->GetRelationName(),
                            column_expr->GetColumnName(),
                            cast_expr->cast_type_));
                    break;
                }
                case fesql::node::kExprPrimary: {
                    auto const_expr =
                        dynamic_cast<const node::ConstNode*>(cast_expr->expr());
                    output_column_sources->push_back(
                        vm::ColumnSource(const_expr, cast_expr->cast_type_));
                    break;
                }
                default: {
                    output_column_sources->push_back(vm::ColumnSource());
                }
            }
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
    auto frame_arg = compile_func->GetArg(1);
    expr_ir_builder->set_frame(frame_arg, frame);

    auto window_key = frame_arg->GetExprString();
    NativeValue window_arg_value;
    CHECK_STATUS(expr_ir_builder->BuildWindow(&window_arg_value),
                 "Bind window failed");

    ::llvm::IRBuilder<> builder(block);
    ::llvm::Value* frame_ptr = window_arg_value.GetValue(&builder);
    CHECK_TRUE(frame_ptr != nullptr && frame_ptr->getType()->isPointerTy(),
               kCodegenError);
    ::llvm::Type* row_list_ptr_ty = nullptr;
    CHECK_TRUE(GetLLVMType(ctx_->GetModule(), frame_arg->GetOutputType(),
                           &row_list_ptr_ty),
               kCodegenError);
    frame_ptr = builder.CreatePointerCast(frame_ptr, row_list_ptr_ty);
    window_arg_value = window_arg_value.Replace(frame_ptr);
    if (sv->HasVar(window_key)) {
        sv->ReplaceVar(window_key, window_arg_value);
    } else {
        sv->AddVar(window_key, window_arg_value);
    }
    return Status::OK();
}

}  // namespace codegen
}  // namespace fesql
