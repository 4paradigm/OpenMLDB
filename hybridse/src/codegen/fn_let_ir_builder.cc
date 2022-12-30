/*
 * Copyright 2021 4Paradigm
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

using ::hybridse::common::kCodegenError;

namespace hybridse {
namespace codegen {

RowFnLetIRBuilder::RowFnLetIRBuilder(CodeGenContext* ctx) : ctx_(ctx) {}
RowFnLetIRBuilder::~RowFnLetIRBuilder() {}

Status RowFnLetIRBuilder::Build(
    const std::string& name, const node::LambdaNode* compile_func,
    const node::FrameNode* primary_frame,
    const std::vector<const node::FrameNode*>& project_frames,
    const vm::Schema& output_schema) {
    ::llvm::Function* fn = NULL;

    ::llvm::Module* module = ctx_->GetModule();
    CHECK_TRUE(module->getFunction(name) == NULL, kCodegenError, "function ",
               name, " already exists");

    // Compute function for SQL query, with five parameters (first four input and last output).
    // Called for each row(`key` & `row`), with window and parameter info,
    // output the result row(`output_buf`).
    // Function returns int32
    //
    // Function is built with the information of query SQL, including
    // select list (all column names, expressions and function calls),
    // window definitions, group by infos, parameters etc
    //
    // key::int64
    // row::int8*
    // window::int8*
    // parameter::int8*
    // output_buf::int8**
    std::vector<std::string> args;
    std::vector<::llvm::Type*> args_llvm_type;
    args_llvm_type.push_back(::llvm::Type::getInt64Ty(module->getContext()));
    args_llvm_type.push_back(::llvm::Type::getInt8PtrTy(module->getContext()));
    args_llvm_type.push_back(::llvm::Type::getInt8PtrTy(module->getContext()));
    args_llvm_type.push_back(::llvm::Type::getInt8PtrTy(module->getContext()));
    args_llvm_type.push_back(::llvm::Type::getInt8PtrTy(module->getContext())->getPointerTo());

    std::string output_ptr_name = "output_ptr_name";
    args.push_back("@row_key");
    args.push_back("@row_ptr");
    args.push_back("@window");
    args.push_back("@parameter");
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
    CHECK_TRUE(FillArgs(args, fn, sv), kCodegenError, "Fail to fill arguments of function let");

    // bind row arg
    NativeValue row_arg_value;
    CHECK_TRUE(sv->FindVar("@row_ptr", &row_arg_value), kCodegenError, "Fail to find @row_ptr");
    sv->AddVar(compile_func->GetArg(0)->GetExprString(), row_arg_value);

    ::llvm::BasicBlock* block = ctx_->GetCurrentBlock();
    VariableIRBuilder variable_ir_builder(block, sv);

    if (primary_frame != nullptr && !primary_frame->IsPureHistoryFrame()) {
        NativeValue window;
        CHECK_TRUE(variable_ir_builder.LoadWindow("", &window, status), kCodegenError);
        CHECK_TRUE(variable_ir_builder.StoreWindow(primary_frame->GetExprString(), window.GetRaw(), status),
                   kCodegenError);
    }

    ExprIRBuilder expr_ir_builder(ctx_);

    std::map<uint32_t, NativeValue> outputs;

    // maybe collect agg expressions
    std::map<std::string, AggregateIRBuilder> window_agg_builder;
    uint32_t agg_builder_id = 0;

    auto expr_list = compile_func->body();
    CHECK_TRUE(project_frames.size() == expr_list->GetChildNum(), kCodegenError,
               "Frame num should match expr num");

    for (size_t i = 0; i < expr_list->GetChildNum(); ++i) {
        const ::hybridse::node::ExprNode* expr = expr_list->GetChild(i);
        auto frame = project_frames[i];
        const std::string frame_str =
            frame == nullptr ? "" : frame->GetExprString();

        ::hybridse::type::Type col_agg_type;

        auto res = window_agg_builder.try_emplace(frame_str, ctx_->schemas_context(), module, frame, agg_builder_id);
        auto agg_iter = res.first;
        if (res.second) {
            agg_builder_id++;
        }

        if (agg_iter->second.CollectAggColumn(expr, i, &col_agg_type)) {
            continue;
        }

        CHECK_TRUE(expr->GetExprType() != node::kExprAll, kCodegenError,
                   "* should be resolved before codegen stage");

        // bind window frame
        CHECK_STATUS(BindProjectFrame(&expr_ir_builder, frame, compile_func,
                                      ctx_->GetCurrentBlock(), sv));

        CHECK_STATUS(BuildProject(&expr_ir_builder, i, expr, &outputs),
                     "Build expr failed at ", i, ":\n", expr->GetTreeString());
    }

    CHECK_STATUS(EncodeBuf(&outputs, output_schema, variable_ir_builder,
                         ctx_->GetCurrentBlock(), output_ptr_name),
               "Gen encode into output buffer failed");

    if (!window_agg_builder.empty()) {
        for (auto iter = window_agg_builder.begin(); iter != window_agg_builder.end(); iter++) {
            if (!iter->second.empty()) {
                CHECK_STATUS(iter->second.BuildMulti(name, &expr_ir_builder, &variable_ir_builder,
                                                     ctx_->GetCurrentBlock(), output_ptr_name, output_schema),
                             "Multi column sum codegen failed");
            }
        }
    }

    ::llvm::IRBuilder<> ir_builder(ctx_->GetCurrentBlock());
    ::llvm::Value* ret = ir_builder.getInt32(0);
    ir_builder.CreateRet(ret);

    // reformat scope
    auto root_scope = ctx_->GetCurrentScope();
    root_scope->blocks()->DropEmptyBlocks();
    root_scope->blocks()->ReInsertTo(fn);
    return Status::OK();
}

base::Status RowFnLetIRBuilder::EncodeBuf(
    const std::map<uint32_t, NativeValue>* values, const vm::Schema& schema,
    VariableIRBuilder& variable_ir_builder,  // NOLINT (runtime/references)
    ::llvm::BasicBlock* block, const std::string& output_ptr_name) {
    base::Status status;
    BufNativeEncoderIRBuilder encoder(values, &schema, block);
    NativeValue row_ptr;
    variable_ir_builder.LoadValue(output_ptr_name, &row_ptr, status);
    CHECK_STATUS(status)
    ::llvm::IRBuilder<> ir_builder(block);
    CHECK_STATUS(encoder.BuildEncode(row_ptr.GetValue(&ir_builder)))
    return base::Status::OK();
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
    const node::ExprNode* expr, std::map<uint32_t, NativeValue>* outputs) {
    NativeValue expr_out_val;

    CHECK_STATUS(expr_ir_builder->Build(expr, &expr_out_val),
                 "Fail to codegen project expression: ", expr->GetExprString());

    const ::hybridse::node::TypeNode* data_type = nullptr;
    CHECK_TRUE(!expr_out_val.IsTuple(), kCodegenError,
               "Output do not support tuple");
    ::llvm::Type* llvm_ty = expr_out_val.GetType();
    CHECK_TRUE(llvm_ty != nullptr, kCodegenError);
    CHECK_TRUE(GetFullType(ctx_->node_manager(), llvm_ty, &data_type),
               kCodegenError, "Fail to get output type at ", index, ", expect ",
               expr->GetOutputType()->GetName());

    ::hybridse::type::Type ctype;
    CHECK_TRUE(DataType2SchemaType(*data_type, &ctype), kCodegenError);

    outputs->insert(std::make_pair(index, expr_out_val));
    return Status::OK();
}

Status RowFnLetIRBuilder::BindProjectFrame(ExprIRBuilder* expr_ir_builder,
                                           const node::FrameNode* frame,
                                           const node::LambdaNode* compile_func,
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
    CHECK_TRUE(GetLlvmType(ctx_->GetModule(), frame_arg->GetOutputType(),
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
}  // namespace hybridse
