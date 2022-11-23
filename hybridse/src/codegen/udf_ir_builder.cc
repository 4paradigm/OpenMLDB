/*
 * Copyright 2021 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "codegen/udf_ir_builder.h"
#include <iostream>
#include <utility>
#include <vector>
#include "codegen/context.h"
#include "codegen/date_ir_builder.h"
#include "codegen/fn_ir_builder.h"
#include "codegen/list_ir_builder.h"
#include "codegen/null_ir_builder.h"
#include "codegen/timestamp_ir_builder.h"
#include "llvm/IR/Attributes.h"
#include "node/node_manager.h"
#include "node/sql_node.h"
#include "udf/udf.h"
#include "udf/udf_registry.h"

using ::hybridse::common::kCodegenError;

namespace hybridse {
namespace codegen {

using TypeNodeVec = std::vector<const node::TypeNode*>;

UdfIRBuilder::UdfIRBuilder(CodeGenContext* ctx, node::ExprNode* frame_arg,
                           const node::FrameNode* frame)
    : ctx_(ctx), frame_arg_(frame_arg), frame_(frame) {}

Status UdfIRBuilder::BuildCall(
    const node::FnDefNode* fn,
    const std::vector<const node::TypeNode*>& arg_types,
    const std::vector<NativeValue>& args, NativeValue* output) {
    // sanity checks
    auto status = fn->Validate(arg_types);
    if (!status.isOK()) {
        LOG(WARNING) << "Validation error: " << status;
    }

    switch (fn->GetType()) {
        case node::kExternalFnDef: {
            // Handler built-in function
            auto node = dynamic_cast<const node::ExternalFnDefNode*>(fn);
            return BuildExternCall(node, args, output);
        }
        case node::kDynamicUdfFnDef: {
            auto node = dynamic_cast<const node::DynamicUdfFnDefNode*>(fn);
            return BuildDynamicUdfCall(node, args, output);
        }
        case node::kUdfByCodeGenDef: {
            auto node = dynamic_cast<const node::UdfByCodeGenDefNode*>(fn);
            return BuildCodeGenUdfCall(node, args, output);
        }
        case node::kUdfDef: {
            auto node = dynamic_cast<const node::UdfDefNode*>(fn);
            return BuildUdfCall(node, args, output);
        }
        case node::kUdafDef: {
            auto node = dynamic_cast<const node::UdafDefNode*>(fn);
            return BuildUdafCall(node, args, output);
        }
        case node::kLambdaDef: {
            auto node = dynamic_cast<const node::LambdaNode*>(fn);
            return BuildLambdaCall(node, args, output);
        }
        default:
            return Status(common::kCodegenError, "Unknown function def type");
    }
}

Status UdfIRBuilder::BuildUdfCall(
    const node::UdfDefNode* fn,
    const std::vector<NativeValue>& args, NativeValue* output) {
    // resolve to llvm function
    bool return_by_arg;
    ::llvm::FunctionCallee callee;
    CHECK_STATUS(GetUdfCallee(fn, &callee, &return_by_arg));
    std::vector<const node::TypeNode*> arg_types;
    std::vector<int> arg_nullable;
    for (size_t i = 0; i < fn->GetArgSize(); ++i) {
        arg_types.push_back(fn->GetArgType(i));
        arg_nullable.push_back(fn->IsArgNullable(i));
    }
    return BuildLlvmCall(fn, callee, arg_types, arg_nullable, args, return_by_arg, output);
}

Status UdfIRBuilder::GetUdfCallee(
    const node::UdfDefNode* fn,
    ::llvm::FunctionCallee* callee, bool* return_by_arg) {
    std::string fn_name = fn->def()->header_->GeIRFunctionName();
    ::llvm::Type* llvm_ret_ty = nullptr;
    CHECK_TRUE(
        GetLlvmType(ctx_->GetModule(), fn->GetReturnType(), &llvm_ret_ty),
        kCodegenError);
    if (TypeIRBuilder::IsStructPtr(llvm_ret_ty)) {
        *return_by_arg = true;
    } else {
        *return_by_arg = false;
    }

    if (fn->def()->header_->ret_type_ != nullptr) {
        switch (fn->def()->header_->ret_type_->base_) {
            case node::kDate:
            case node::kTimestamp:
            case node::kVarchar:
                fn_name.append(".").append(
                    fn->def()->header_->ret_type_->GetName());
            default:
                break;
        }
    }
    ::llvm::Function* llvm_func = ctx_->GetModule()->getFunction(fn_name);
    if (llvm_func == nullptr) {
        FnIRBuilder fn_builder(ctx_->GetModule());
        Status status;
        CHECK_TRUE(fn_builder.Build(fn->def(), &llvm_func, status),
                   kCodegenError, "Build udf failed: ", status.str());
    }
    *callee = ctx_->GetModule()->getOrInsertFunction(
        fn_name, llvm_func->getFunctionType());
    return Status::OK();
}

Status UdfIRBuilder::BuildLambdaCall(
    const node::LambdaNode* fn,
    const std::vector<NativeValue>& args, NativeValue* output) {
    // bind lambda arguments
    auto sv = ctx_->GetCurrentScope()->sv();
    ScopeVar inline_scope(sv->parent());
    sv->SetParent(&inline_scope);

    for (size_t i = 0; i < fn->GetArgSize(); ++i) {
        auto expr_id = fn->GetArg(i);
        inline_scope.AddVar(expr_id->GetExprString(), args[i]);
    }

    base::Status status;
    ExprIRBuilder expr_builder(ctx_);
    expr_builder.set_frame(frame_arg_, frame_);
    CHECK_STATUS(expr_builder.Build(fn->body(), output),
                 "Error during build lambda body: ", status.str(), "\n",
                 fn->body()->GetExprString());

    sv->SetParent(inline_scope.parent());
    return Status::OK();
}

Status UdfIRBuilder::BuildCodeGenUdfCall(
    const node::UdfByCodeGenDefNode* fn,
    const std::vector<NativeValue>& args, NativeValue* output) {
    auto gen_impl = fn->GetGenImpl();

    ::llvm::Value* ret_null = nullptr;
    for (size_t i = 0; i < fn->GetArgSize(); ++i) {
        if (!fn->IsArgNullable(i) && i < args.size() && args[i].IsNullable()) {
            NullIRBuilder null_ir_builder;
            null_ir_builder.CheckAnyNull(ctx_->GetCurrentBlock(), args[i],
                                         &ret_null);
        }
    }

    NativeValue gen_output;
    CHECK_STATUS(gen_impl->gen(ctx_, args, &gen_output));

    if (ret_null != nullptr) {
        if (gen_output.IsNullable()) {
            auto builder = ctx_->GetBuilder();
            ret_null =
                builder->CreateOr(ret_null, gen_output.GetIsNull(builder));
        }
        *output = gen_output.WithFlag(ret_null);
    } else {
        *output = gen_output;
    }
    return Status::OK();
}

/**
  * Expand virtual argument values into llvm arguments.
  * For extern call, if null value can not handled by
  * extern function, just return null.
  *
  * @returns arg_vec: lowerred llvm arguments
  * @returns should_ret_null:
        Value flag to indicate some argument is null but
        extern function call do not handle it.
  */
Status UdfIRBuilder::ExpandLlvmCallArgs(const node::TypeNode* dtype,
                                        bool nullable, const NativeValue& value,
                                        ::llvm::IRBuilder<>* builder,
                                        std::vector<::llvm::Value*>* arg_vec,
                                        ::llvm::Value** should_ret_null) {
    if (dtype->base() == node::kTuple) {
        CHECK_TRUE(value.IsTuple(), kCodegenError,
                   "Must bind tuple value to tuple argument type");
        CHECK_TRUE(value.GetFieldNum() == dtype->GetGenericSize(),
                   kCodegenError, "Tuple ", dtype->GetName(), " expect ",
                   dtype->GetGenericSize(), " fields, but get ",
                   value.GetFieldNum());
        CHECK_TRUE(!nullable, kCodegenError, "kTuple should never be null");

        for (size_t i = 0; i < value.GetFieldNum(); ++i) {
            CHECK_STATUS(ExpandLlvmCallArgs(
                dtype->GetGenericType(i), dtype->IsGenericNullable(i),
                value.GetField(i), builder, arg_vec, should_ret_null));
        }
    } else if (nullable) {
        arg_vec->push_back(value.GetValue(builder));
        if (value.IsNullable()) {
            arg_vec->push_back(value.GetIsNull(builder));
        } else {
            arg_vec->push_back(
                ::llvm::ConstantInt::getFalse(builder->getContext()));
        }
    } else {
        if (value.IsNullable()) {
            // Nullable value under non-null type specification
            if (*should_ret_null == nullptr) {
                *should_ret_null = value.GetIsNull(builder);
            } else {
                *should_ret_null = builder->CreateOr(*should_ret_null,
                                                     value.GetIsNull(builder));
            }
        }
        arg_vec->push_back(value.GetValue(builder));
    }
    return Status::OK();
}

/**
  * Expand virtual argument values in variadic part into llvm arguments.
  * For extern call, if null value can not handled by extern function,
  * just return null.
  *
  * @returns arg_vec: lowerred llvm arguments
  * @returns should_ret_null:
        Value flag to indicate some argument is null but
        extern function call do not handle it.
  */
Status UdfIRBuilder::ExpandLlvmCallVariadicArgs(
    const NativeValue& value, ::llvm::IRBuilder<>* builder,
    std::vector<::llvm::Value*>* arg_vec, ::llvm::Value** should_ret_null) {
    CHECK_TRUE(!value.IsTuple(), kCodegenError,
               "kTuple is not allowed in variadic part");
    if (value.IsNullable()) {
        // extern variadic part can not process null
        if (*should_ret_null == nullptr) {
            *should_ret_null = value.GetIsNull(builder);
        } else {
            *should_ret_null =
                builder->CreateAnd(*should_ret_null, value.GetIsNull(builder));
        }
    }
    arg_vec->push_back(value.GetValue(builder));
    return Status::OK();
}

/**
 * Expand return addr into llvm arguments.
 */
Status UdfIRBuilder::ExpandLlvmCallReturnArgs(
    const node::TypeNode* dtype, bool nullable, ::llvm::IRBuilder<>* builder,
    std::vector<::llvm::Value*>* arg_vec) {
    if (dtype->base() == node::kTuple) {
        for (size_t i = 0; i < dtype->GetGenericSize(); ++i) {
            CHECK_STATUS(ExpandLlvmCallReturnArgs(dtype->GetGenericType(i),
                                                  dtype->IsGenericNullable(i),
                                                  builder, arg_vec));
        }
    } else {
        ::llvm::Type* llvm_ty = nullptr;
        CHECK_TRUE(GetLlvmType(ctx_->GetModule(), dtype, &llvm_ty),
                   kCodegenError);

        ::llvm::Value* ret_alloca;
        auto opaque_ret_type = dynamic_cast<const node::OpaqueTypeNode*>(dtype);
        if (opaque_ret_type != nullptr) {
            ret_alloca = CreateAllocaAtHead(
                builder, ::llvm::Type::getInt8Ty(builder->getContext()),
                "udf_opaque_type_return_addr",
                builder->getInt64(opaque_ret_type->bytes()));
        } else if (TypeIRBuilder::IsStructPtr(llvm_ty)) {
            ret_alloca = CreateAllocaAtHead(
                builder,
                reinterpret_cast<llvm::PointerType*>(llvm_ty)->getElementType(),
                "udf_struct_type_return_addr");
            // fill empty content for string
            if (dtype->base() == node::kVarchar) {
                // empty string
                builder->CreateStore(builder->getInt32(0),
                                     builder->CreateStructGEP(ret_alloca, 0));
                builder->CreateStore(builder->CreateGlobalStringPtr(""),
                                     builder->CreateStructGEP(ret_alloca, 1));
            }
        } else {
            ret_alloca =
                CreateAllocaAtHead(builder, llvm_ty, "udf_return_addr");
        }
        arg_vec->push_back(ret_alloca);
        if (nullable) {
            auto bool_ty = ::llvm::Type::getInt1Ty(builder->getContext());
            arg_vec->push_back(CreateAllocaAtHead(builder, bool_ty,
                                                  "udf_is_null_return_addr"));
        }
    }
    return Status::OK();
}

/**
 * Extract llvm return results into native value wrapper.
 */
Status UdfIRBuilder::ExtractLlvmReturnValue(
    const node::TypeNode* dtype, bool nullable,
    const std::vector<::llvm::Value*>& llvm_args, ::llvm::IRBuilder<>* builder,
    size_t* pos_idx, NativeValue* output) {
    if (dtype->base() == node::kTuple) {
        CHECK_TRUE(!nullable, kCodegenError, "kTuple should never be null");
        std::vector<NativeValue> fields;
        for (size_t i = 0; i < dtype->GetGenericSize(); ++i) {
            NativeValue sub_field;
            CHECK_STATUS(ExtractLlvmReturnValue(
                dtype->GetGenericType(i), dtype->IsGenericNullable(i),
                llvm_args, builder, pos_idx, &sub_field));
            fields.push_back(sub_field);
        }
        *output = NativeValue::CreateTuple(fields);
    } else {
        ::llvm::Type* llvm_ty = nullptr;
        CHECK_TRUE(GetLlvmType(ctx_->GetModule(), dtype, &llvm_ty),
                   kCodegenError);

        ::llvm::Value* raw;
        if (TypeIRBuilder::IsStructPtr(llvm_ty)) {
            raw = llvm_args[*pos_idx];
        } else if (dtype->base() == node::kOpaque) {
            raw = llvm_args[*pos_idx];
        } else {
            raw = builder->CreateLoad(llvm_args[*pos_idx]);
        }
        *pos_idx += 1;

        if (nullable) {
            ::llvm::Value* is_null = builder->CreateLoad(llvm_args[*pos_idx]);
            *pos_idx += 1;
            *output = NativeValue::CreateWithFlag(raw, is_null);
        } else {
            *output = NativeValue::Create(raw);
        }
    }
    return Status::OK();
}

Status UdfIRBuilder::BuildLlvmCall(const node::FnDefNode* fn,
                                   ::llvm::FunctionCallee callee,
                                   const std::vector<const node::TypeNode*>& arg_types,
                                   const std::vector<int>& arg_nullable,
                                   const std::vector<NativeValue>& args,
                                   bool return_by_arg, NativeValue* output) {
    ::llvm::IRBuilder<> builder(ctx_->GetCurrentBlock());
    auto function_ty = callee.getFunctionType();

    // variadic pos, default non-variadic
    auto extern_fn = dynamic_cast<const node::ExternalFnDefNode*>(fn);
    size_t variadic_pos = args.size();
    if (extern_fn != nullptr && extern_fn->variadic_pos() >= 0) {
        variadic_pos = static_cast<size_t>(extern_fn->variadic_pos());
    }

    // expand actual llvm function args
    // [non-variadic args], [return addrs], [variadic args]
    ::llvm::Value* should_ret_null = nullptr;
    std::vector<llvm::Value*> llvm_args;
    for (size_t i = 0; i < variadic_pos; ++i) {
        CHECK_STATUS(ExpandLlvmCallArgs(arg_types[i], arg_nullable[i],
                                        args[i], &builder, &llvm_args,
                                        &should_ret_null));
    }
    size_t ret_pos_begin = llvm_args.size();
    if (return_by_arg) {
        CHECK_STATUS(ExpandLlvmCallReturnArgs(
            fn->GetReturnType(), fn->IsReturnNullable(), &builder, &llvm_args));
    }
    for (size_t i = variadic_pos; i < args.size(); ++i) {
        CHECK_STATUS(ExpandLlvmCallVariadicArgs(args[i], &builder, &llvm_args,
                                                &should_ret_null));
    }

    // arg type validation on llvm level
    for (size_t i = 0; i < llvm_args.size(); ++i) {
        if (i >= function_ty->getNumParams()) {
            CHECK_TRUE(function_ty->isVarArg(), kCodegenError,
                       "Argument num out of bound for non-variadic function: ",
                       "expect ", function_ty->getNumParams(), " but get ",
                       llvm_args.size(), ": ",
                       GetLlvmObjectString(function_ty));
            break;
        }
        ::llvm::Type* actual_llvm_ty = llvm_args[i]->getType();
        ::llvm::Type* expect_llvm_ty = function_ty->params()[i];
        CHECK_TRUE(expect_llvm_ty == actual_llvm_ty, kCodegenError,
                   "LLVM argument type mismatch at ", i, ", expect ",
                   GetLlvmObjectString(expect_llvm_ty), " but get ",
                   GetLlvmObjectString(actual_llvm_ty), ": ",
                   GetLlvmObjectString(function_ty));
    }
    ::llvm::Value* raw = builder.CreateCall(callee, llvm_args);

    // extract return value
    NativeValue return_value;
    if (return_by_arg) {
        size_t pos_idx = ret_pos_begin;
        CHECK_STATUS(ExtractLlvmReturnValue(fn->GetReturnType(),
                                            fn->IsReturnNullable(), llvm_args,
                                            &builder, &pos_idx, &return_value));
    } else {
        return_value = NativeValue::Create(raw);
    }

    // Detect whether some arg is non-nullable
    // but actually feed with null value.
    if (should_ret_null != nullptr) {
        should_ret_null =
            builder.CreateOr(should_ret_null, return_value.GetIsNull(&builder));
        *output = return_value.WithFlag(should_ret_null);
    } else {
        *output = return_value;
    }
    return Status::OK();
}

Status UdfIRBuilder::BuildExternCall(
    const node::ExternalFnDefNode* fn,
    const std::vector<NativeValue>& args, NativeValue* output) {
    // resolve to llvm function
    ::llvm::FunctionType* func_ty = nullptr;
    std::vector<const node::TypeNode*> arg_types;
    std::vector<int> arg_nullable;
    for (size_t i = 0; i < fn->GetArgSize(); ++i) {
        arg_types.push_back(fn->GetArgType(i));
        arg_nullable.push_back(fn->IsArgNullable(i));
    }
    bool return_by_arg = fn->return_by_arg();
    bool variadic = fn->variadic_pos() >= 0;
    CHECK_STATUS(codegen::GetLlvmFunctionType(
        ctx_->GetModule(), arg_types, arg_nullable, fn->GetReturnType(),
        fn->IsReturnNullable(), variadic, &return_by_arg, &func_ty));

    auto callee =
        ctx_->GetModule()->getOrInsertFunction(fn->function_name(), func_ty);
    // set i16 signext attr for extern call
    // https://releases.llvm.org/9.0.0/docs/LangRef.html#parameter-attributes
    auto function = ctx_->GetModule()->getFunction(fn->function_name());
    CHECK_TRUE(function != nullptr, kCodegenError);
    auto int16_ty = ::llvm::Type::getInt16Ty(function->getContext());
    auto sext_attr = ::llvm::Attribute::AttrKind::SExt;
    for (size_t i = 0; i < func_ty->getNumParams(); ++i) {
        if (func_ty->getParamType(i) != int16_ty) {
            continue;
        } else if (!function->hasParamAttribute(i, sext_attr)) {
            function->addParamAttr(i, sext_attr);
        }
    }
    if (func_ty->getReturnType() == int16_ty) {
        if (!function->hasAttribute(::llvm::AttributeList::ReturnIndex,
                                    sext_attr)) {
            function->addAttribute(::llvm::AttributeList::ReturnIndex,
                                   sext_attr);
        }
    }

    return BuildLlvmCall(fn, callee, arg_types, arg_nullable, args, fn->return_by_arg(), output);
}

Status UdfIRBuilder::BuildDynamicUdfCall(
    const node::DynamicUdfFnDefNode* fn,
    const std::vector<NativeValue>& args, NativeValue* output) {
    auto init_context_fn = fn->GetInitContextNode();
    auto opaque_type_node = dynamic_cast<const node::OpaqueTypeNode*>(init_context_fn->ret_type());
    NativeValue udfcontext_output;
    UdfIRBuilder sub_udf_builder(ctx_, frame_arg_, frame_);
    CHECK_STATUS(
        sub_udf_builder.BuildExternCall(init_context_fn, {}, &udfcontext_output),
        "Build output function call failed");
    std::vector<NativeValue> new_args = {udfcontext_output};
    new_args.insert(new_args.end(), args.begin(), args.end());
    std::vector<const node::TypeNode*> arg_types = {opaque_type_node};
    std::vector<int> arg_nullable = {0};
    for (size_t i = 0; i < fn->GetArgSize(); ++i) {
        arg_types.push_back(fn->GetArgType(i));
        arg_nullable.push_back(fn->IsArgNullable(i));
    }
    bool return_by_arg = fn->return_by_arg();
    bool variadic = false;
    ::llvm::FunctionType* func_ty = nullptr;
    CHECK_STATUS(codegen::GetLlvmFunctionType(
            ctx_->GetModule(), arg_types, arg_nullable, fn->GetReturnType(),
            fn->IsReturnNullable(), variadic, &return_by_arg, &func_ty));

    auto callee =
        ctx_->GetModule()->getOrInsertFunction(fn->GetName(), func_ty);
    // set i16 signext attr for extern call
    // https://releases.llvm.org/9.0.0/docs/LangRef.html#parameter-attributes
    auto function = ctx_->GetModule()->getFunction(fn->GetName());
    CHECK_TRUE(function != nullptr, kCodegenError);
    auto int16_ty = ::llvm::Type::getInt16Ty(function->getContext());
    auto sext_attr = ::llvm::Attribute::AttrKind::SExt;
    for (size_t i = 0; i < func_ty->getNumParams(); ++i) {
        if (func_ty->getParamType(i) != int16_ty) {
            continue;
        } else if (!function->hasParamAttribute(i, sext_attr)) {
            function->addParamAttr(i, sext_attr);
        }
    }
    if (func_ty->getReturnType() == int16_ty) {
        if (!function->hasAttribute(::llvm::AttributeList::ReturnIndex,
                                    sext_attr)) {
            function->addAttribute(::llvm::AttributeList::ReturnIndex,
                                   sext_attr);
        }
    }

    return BuildLlvmCall(fn, callee, arg_types, arg_nullable, new_args, fn->return_by_arg(), output);
}

Status UdfIRBuilder::BuildUdafCall(
    const node::UdafDefNode* fn,
    const std::vector<NativeValue>& args, NativeValue* output) {
    // udaf state type
    const node::TypeNode* state_type = fn->GetStateType();
    CHECK_TRUE(state_type != nullptr, kCodegenError, "Missing state type");
    size_t state_num = 1;
    if (state_type->base() == node::kTuple) {
        state_num = state_type->GetGenericSize();
    }

    // udaf input elements type
    size_t input_num = fn->GetArgSize();
    std::vector<const node::TypeNode*> elem_types(input_num);
    std::vector<int> elem_nullable(input_num);
    for (size_t i = 0; i < input_num; ++i) {
        elem_types[i] = fn->GetElementType(i);
        elem_nullable[i] = fn->IsElementNullable(i);
    }
    Status status;

    // infer state llvm types
    std::vector<llvm::Type*> state_llvm_tys;
    if (state_type->base() == node::kTuple) {
        // tuple state should be expanded
        for (auto field_type : state_type->generics()) {
            llvm::Type* llvm_ty = nullptr;
            CHECK_TRUE(GetLlvmType(ctx_->GetModule(), field_type, &llvm_ty),
                       kCodegenError,
                       "Fail to get llvm type for " + field_type->GetName());
            state_llvm_tys.push_back(llvm_ty);
        }
    } else {
        llvm::Type* llvm_ty = nullptr;
        CHECK_TRUE(GetLlvmType(ctx_->GetModule(), state_type, &llvm_ty),
                   kCodegenError,
                   "Fail to get llvm type for " + state_type->GetName());
        state_llvm_tys.push_back(llvm_ty);
    }

    std::vector<::llvm::Value*> list_ptrs;
    for (size_t i = 0; i < input_num; ++i) {
        list_ptrs.push_back(args[i].GetValue(ctx_));
    }

    // iter head
    ::llvm::BasicBlock* head_block = ctx_->GetCurrentBlock();
    ListIRBuilder iter_head_builder(head_block, nullptr);
    std::vector<::llvm::Value*> iterators;
    for (size_t i = 0; i < input_num; ++i) {
        ::llvm::Value* iter = nullptr;
        CHECK_STATUS(iter_head_builder.BuildIterator(list_ptrs[i],
                                                     elem_types[i], &iter));
        iterators.push_back(iter);
    }

    // build init state
    NativeValue init_value;
    CHECK_TRUE(fn->init_expr() != nullptr, kCodegenError);
    ExprIRBuilder init_expr_builder(ctx_);
    init_expr_builder.set_frame(frame_arg_, frame_);
    CHECK_STATUS(init_expr_builder.Build(fn->init_expr(), &init_value),
                 "Build init expr ", fn->init_expr()->GetExprString(),
                 " failed: ", status.str());

    // local states storage
    ::llvm::IRBuilder<> builder(ctx_->GetCurrentBlock());
    std::vector<::llvm::Value*> states_storage(state_num);
    if (state_num > 1) {
        CHECK_TRUE(
            init_value.IsTuple() && init_value.GetFieldNum() == state_num,
            kCodegenError);
        for (size_t i = 0; i < state_num; ++i) {
            NativeValue sub = init_value.GetField(i);
            if (TypeIRBuilder::IsStructPtr(state_llvm_tys[i])) {
                states_storage[i] = sub.GetValue(&builder);
            } else {
                states_storage[i] = CreateAllocaAtHead(
                    &builder, state_llvm_tys[i], "state_alloca");
                builder.CreateStore(sub.GetValue(&builder), states_storage[i]);
            }
        }
    } else {
        if (TypeIRBuilder::IsStructPtr(state_llvm_tys[0])) {
            states_storage[0] = init_value.GetValue(&builder);
        } else {
            states_storage[0] =
                CreateAllocaAtHead(&builder, state_llvm_tys[0], "state_alloca");
            builder.CreateStore(init_value.GetValue(&builder),
                                states_storage[0]);
        }
    }

    CHECK_STATUS(ctx_->CreateWhile(
        [&](::llvm::Value** has_next) {
            // enter
            auto enter_block = ctx_->GetCurrentBlock();
            builder.SetInsertPoint(enter_block);
            ListIRBuilder iter_enter_builder(enter_block, nullptr);
            for (size_t i = 0; i < input_num; ++i) {
                ::llvm::Value* cur_has_next = nullptr;
                CHECK_STATUS(iter_enter_builder.BuildIteratorHasNext(
                                 iterators[i], elem_types[i], &cur_has_next),
                             status.str());
                if (*has_next == nullptr) {
                    *has_next = cur_has_next;
                } else {
                    *has_next = builder.CreateAnd(cur_has_next, *has_next);
                }
            }
            return Status::OK();
        },
        [&]() {
            // iter body
            auto body_begin_block = ctx_->GetCurrentBlock();
            ListIRBuilder iter_next_builder(body_begin_block, nullptr);
            UdfIRBuilder sub_udf_builder(ctx_, frame_arg_, frame_);

            std::vector<NativeValue> cur_state_values;
            std::vector<NativeValue> update_args;
            for (size_t i = 0; i < state_num; ++i) {
                if (TypeIRBuilder::IsStructPtr(states_storage[i]->getType())) {
                    cur_state_values.push_back(
                        NativeValue::Create(states_storage[i]));
                } else {
                    auto load_raw =
                        ctx_->GetBuilder()->CreateLoad(states_storage[i]);
                    cur_state_values.push_back(NativeValue::Create(load_raw));
                }
            }
            if (state_num > 1) {
                update_args.push_back(
                    NativeValue::CreateTuple(cur_state_values));
            } else {
                update_args.push_back(cur_state_values[0]);
            }
            for (size_t i = 0; i < input_num; ++i) {
                NativeValue next_val;
                CHECK_STATUS(iter_next_builder.BuildIteratorNext(
                    iterators[i], elem_types[i], elem_nullable[i], &next_val));
                update_args.push_back(next_val);
            }

            NativeValue update_value;
            std::vector<const node::TypeNode*> update_arg_types;
            update_arg_types.push_back(state_type);
            for (size_t i = 0; i < input_num; ++i) {
                update_arg_types.push_back(elem_types[i]);
            }
            CHECK_TRUE(fn->update_func() != nullptr, kCodegenError);
            CHECK_STATUS(sub_udf_builder.BuildCall(fn->update_func(),
                                                   update_arg_types,
                                                   update_args, &update_value));

            builder.SetInsertPoint(ctx_->GetCurrentBlock());
            if (update_value.IsTuple()) {
                CHECK_TRUE(update_value.GetFieldNum() == state_num,
                           kCodegenError);
                for (size_t i = 0; i < state_num; ++i) {
                    NativeValue sub = update_value.GetField(i);
                    ::llvm::Value* raw_update = sub.GetValue(ctx_);
                    if (TypeIRBuilder::IsStructPtr(raw_update->getType())) {
                        raw_update = builder.CreateLoad(raw_update);
                    }
                    builder.CreateStore(raw_update, states_storage[i]);
                }
            } else {
                ::llvm::Value* raw_update = update_value.GetValue(ctx_);
                if (TypeIRBuilder::IsStructPtr(raw_update->getType())) {
                    raw_update = builder.CreateLoad(raw_update);
                }
                builder.CreateStore(raw_update, states_storage[0]);
            }
            return Status::OK();
        }));

    builder.SetInsertPoint(ctx_->GetCurrentBlock());
    std::vector<NativeValue> final_state_values;
    for (size_t i = 0; i < state_num; ++i) {
        if (TypeIRBuilder::IsStructPtr(states_storage[i]->getType())) {
            final_state_values.push_back(
                NativeValue::Create(states_storage[i]));
        } else {
            final_state_values.push_back(
                NativeValue::Create(builder.CreateLoad(states_storage[i])));
        }
    }
    NativeValue single_final_state;
    if (state_num > 1) {
        single_final_state = NativeValue::CreateTuple(final_state_values);
    } else {
        single_final_state = final_state_values[0];
    }

    UdfIRBuilder sub_udf_builder_exit(ctx_, frame_arg_, frame_);
    NativeValue local_output;
    CHECK_STATUS(
        sub_udf_builder_exit.BuildCall(fn->output_func(), {state_type},
                                       {single_final_state}, &local_output),
        "Build output function call failed");

    ListIRBuilder iter_delete_builder(ctx_->GetCurrentBlock(), nullptr);
    for (size_t i = 0; i < input_num; ++i) {
        ::llvm::Value* delete_iter_res;
        CHECK_STATUS(iter_delete_builder.BuildIteratorDelete(
            iterators[i], elem_types[i], &delete_iter_res));
    }
    *output = local_output;
    return Status::OK();
}

}  // namespace codegen
}  // namespace hybridse
