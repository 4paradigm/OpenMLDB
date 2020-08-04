/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * udf_ir_builder.cc
 *
 * Author: chenjing
 * Date: 2020/6/17
 *--------------------------------------------------------------------------
 **/
#include "codegen/udf_ir_builder.h"
#include <iostream>
#include <utility>
#include <vector>
#include "codegen/context.h"
#include "codegen/date_ir_builder.h"
#include "codegen/fn_ir_builder.h"
#include "codegen/list_ir_builder.h"
#include "codegen/timestamp_ir_builder.h"
#include "node/node_manager.h"
#include "node/sql_node.h"
#include "udf/udf.h"
#include "udf/udf_registry.h"

namespace fesql {
namespace codegen {

using TypeNodeVec = std::vector<const node::TypeNode*>;

UDFIRBuilder::UDFIRBuilder(::llvm::BasicBlock* block, ScopeVar* scope_var,
                           const vm::SchemasContext* schemas_context,
                           ::llvm::Module* module)
    : block_(block),
      sv_(scope_var),
      module_(module),
      schemas_context_(schemas_context) {}

Status UDFIRBuilder::BuildCall(
    const node::FnDefNode* fn,
    const std::vector<const node::TypeNode*>& arg_types,
    const std::vector<NativeValue>& args, NativeValue* output) {
    // sanity checks
    auto status = fn->Validate(arg_types);
    if (!status.isOK()) {
        LOG(WARNING) << "Validation error: " << status.msg;
    }

    switch (fn->GetType()) {
        case node::kExternalFnDef: {
            auto node = dynamic_cast<const node::ExternalFnDefNode*>(fn);
            return BuildExternCall(node, arg_types, args, output);
        }
        case node::kUDFByCodeGenDef: {
            auto node = dynamic_cast<const node::UDFByCodeGenDefNode*>(fn);
            return BuildCodeGenUDFCall(node, arg_types, args, output);
        }
        case node::kUDFDef: {
            auto node = dynamic_cast<const node::UDFDefNode*>(fn);
            return BuildUDFCall(node, arg_types, args, output);
        }
        case node::kUDAFDef: {
            auto node = dynamic_cast<const node::UDAFDefNode*>(fn);
            return BuildUDAFCall(node, arg_types, args, output);
        }
        case node::kLambdaDef: {
            auto node = dynamic_cast<const node::LambdaNode*>(fn);
            return BuildLambdaCall(node, arg_types, args, output);
        }
        default:
            return Status(common::kCodegenError, "Unknown function def type");
    }
}

Status UDFIRBuilder::BuildUDFCall(
    const node::UDFDefNode* fn,
    const std::vector<const node::TypeNode*>& arg_types,
    const std::vector<NativeValue>& args, NativeValue* output) {
    // resolve to llvm function
    bool return_by_arg;
    ::llvm::FunctionCallee callee;
    CHECK_STATUS(GetUDFCallee(fn, arg_types, &callee, &return_by_arg));
    return BuildLLVMCall(fn, callee, args, return_by_arg, output);
}

Status UDFIRBuilder::GetUDFCallee(
    const node::UDFDefNode* fn,
    const std::vector<const node::TypeNode*>& arg_types,
    ::llvm::FunctionCallee* callee, bool* return_by_arg) {
    std::string fn_name = fn->def()->header_->GeIRFunctionName();
    ::llvm::Type* llvm_ret_ty = nullptr;
    CHECK_TRUE(GetLLVMType(module_, fn->GetReturnType(), &llvm_ret_ty));
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
    ::llvm::Function* llvm_func = module_->getFunction(fn_name);
    if (llvm_func == nullptr) {
        FnIRBuilder fn_builder(module_);
        Status status;
        CHECK_TRUE(fn_builder.Build(fn->def(), &llvm_func, status),
                   "Build udf failed: ", status.msg);
    }
    *callee =
        module_->getOrInsertFunction(fn_name, llvm_func->getFunctionType());
    return Status::OK();
}

Status UDFIRBuilder::BuildLambdaCall(
    const node::LambdaNode* fn,
    const std::vector<const node::TypeNode*>& arg_types,
    const std::vector<NativeValue>& args, NativeValue* output) {
    // bind lambda arguments
    // sv_->Enter("Lambda"); name redef
    for (size_t i = 0; i < fn->GetArgSize(); ++i) {
        auto expr_id = fn->GetArg(i);
        sv_->AddVar(expr_id->GetExprString(), args[i]);
    }

    base::Status status;
    ExprIRBuilder expr_builder(block_, sv_, schemas_context_, true, module_);
    bool ok = expr_builder.Build(fn->body(), output, status);
    // sv_->Exit();
    CHECK_TRUE(ok && status.isOK(),
               "Error during build lambda body: ", status.msg, "\n",
               fn->body()->GetExprString());
    return Status::OK();
}

Status UDFIRBuilder::BuildCodeGenUDFCall(
    const node::UDFByCodeGenDefNode* fn,
    const std::vector<const node::TypeNode*>& arg_types,
    const std::vector<NativeValue>& args, NativeValue* output) {
    auto gen_impl = fn->GetGenImpl();

    CodeGenContext codegen_ctx(module_);
    BlockGuard guard(block_, &codegen_ctx);

    CHECK_STATUS(gen_impl->gen(&codegen_ctx, args, output));
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
Status UDFIRBuilder::ExpandLLVMCallArgs(const node::TypeNode* dtype,
                                        bool nullable, const NativeValue& value,
                                        ::llvm::IRBuilder<>* builder,
                                        std::vector<::llvm::Value*>* arg_vec,
                                        ::llvm::Value** should_ret_null) {
    if (dtype->base() == node::kTuple) {
        CHECK_TRUE(value.IsTuple(),
                   "Must bind tuple value to tuple argument type");
        CHECK_TRUE(value.GetFieldNum() == dtype->GetGenericSize(), "Tuple ",
                   dtype->GetName(), " expect ", dtype->GetGenericSize(),
                   " fields, but get ", value.GetFieldNum());
        CHECK_TRUE(!nullable, "kTuple should never be null");

        for (size_t i = 0; i < value.GetFieldNum(); ++i) {
            CHECK_STATUS(ExpandLLVMCallArgs(
                dtype->GetGenericType(i), dtype->IsGenericNullable(i),
                value.GetField(i), builder, arg_vec, should_ret_null));
        }
    } else if (nullable) {
        arg_vec->push_back(value.GetValue(builder));
        if (value.HasFlag()) {
            arg_vec->push_back(value.GetIsNull(builder));
        } else {
            arg_vec->push_back(
                ::llvm::ConstantInt::getFalse(builder->getContext()));
        }
    } else {
        if (value.HasFlag()) {
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
Status UDFIRBuilder::ExpandLLVMCallVariadicArgs(
    const NativeValue& value, ::llvm::IRBuilder<>* builder,
    std::vector<::llvm::Value*>* arg_vec, ::llvm::Value** should_ret_null) {
    CHECK_TRUE(!value.IsTuple(), "kTuple is not allowed in variadic part");
    if (value.HasFlag()) {
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
Status UDFIRBuilder::ExpandLLVMCallReturnArgs(
    const node::TypeNode* dtype, bool nullable, ::llvm::IRBuilder<>* builder,
    std::vector<::llvm::Value*>* arg_vec) {
    if (dtype->base() == node::kTuple) {
        for (size_t i = 0; i < dtype->GetGenericSize(); ++i) {
            CHECK_STATUS(ExpandLLVMCallReturnArgs(dtype->GetGenericType(i),
                                                  dtype->IsGenericNullable(i),
                                                  builder, arg_vec));
        }
    } else {
        ::llvm::Type* llvm_ty = nullptr;
        CHECK_TRUE(GetLLVMType(module_, dtype, &llvm_ty));

        ::llvm::Value* ret_alloca;
        auto opaque_ret_type = dynamic_cast<const node::OpaqueTypeNode*>(dtype);
        if (opaque_ret_type != nullptr) {
            ret_alloca = builder->CreateAlloca(
                ::llvm::Type::getInt8Ty(builder->getContext()),
                builder->getInt64(opaque_ret_type->bytes()));
        } else if (TypeIRBuilder::IsStructPtr(llvm_ty)) {
            ret_alloca = builder->CreateAlloca(
                reinterpret_cast<llvm::PointerType*>(llvm_ty)
                    ->getElementType());
        } else {
            ret_alloca = builder->CreateAlloca(llvm_ty);
        }
        arg_vec->push_back(ret_alloca);
        if (nullable) {
            auto bool_ty = ::llvm::Type::getInt1Ty(builder->getContext());
            arg_vec->push_back(builder->CreateAlloca(bool_ty));
        }
    }
    return Status::OK();
}

/**
 * Extract llvm return results into native value wrapper.
 */
Status UDFIRBuilder::ExtractLLVMReturnValue(
    const node::TypeNode* dtype, bool nullable,
    const std::vector<::llvm::Value*>& llvm_args, ::llvm::IRBuilder<>* builder,
    size_t* pos_idx, NativeValue* output) {
    if (dtype->base() == node::kTuple) {
        CHECK_TRUE(!nullable, "kTuple should never be null");
        std::vector<NativeValue> fields;
        for (size_t i = 0; i < dtype->GetGenericSize(); ++i) {
            NativeValue sub_field;
            CHECK_STATUS(ExtractLLVMReturnValue(
                dtype->GetGenericType(i), dtype->IsGenericNullable(i),
                llvm_args, builder, pos_idx, &sub_field));
            fields.push_back(sub_field);
        }
        *output = NativeValue::CreateTuple(fields);
    } else {
        ::llvm::Type* llvm_ty = nullptr;
        CHECK_TRUE(GetLLVMType(module_, dtype, &llvm_ty));

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

Status UDFIRBuilder::BuildLLVMCall(const node::FnDefNode* fn,
                                   ::llvm::FunctionCallee callee,
                                   const std::vector<NativeValue>& args,
                                   bool return_by_arg, NativeValue* output) {
    ::llvm::IRBuilder<> builder(block_);
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
        CHECK_STATUS(ExpandLLVMCallArgs(fn->GetArgType(i), fn->IsArgNullable(i),
                                        args[i], &builder, &llvm_args,
                                        &should_ret_null));
    }
    size_t ret_pos_begin = llvm_args.size();
    if (return_by_arg) {
        CHECK_STATUS(ExpandLLVMCallReturnArgs(
            fn->GetReturnType(), fn->IsReturnNullable(), &builder, &llvm_args));
    }
    for (size_t i = variadic_pos; i < args.size(); ++i) {
        CHECK_STATUS(ExpandLLVMCallVariadicArgs(args[i], &builder, &llvm_args,
                                                &should_ret_null));
    }

    // arg type validation on llvm level
    for (size_t i = 0; i < llvm_args.size(); ++i) {
        if (i >= function_ty->getNumParams()) {
            CHECK_TRUE(function_ty->isVarArg(),
                       "Argument num out of bound for non-variadic function: ",
                       "expect ", function_ty->getNumParams(), " but get ",
                       llvm_args.size(), ": ",
                       GetLLVMObjectString(function_ty));
            break;
        }
        ::llvm::Type* actual_llvm_ty = llvm_args[i]->getType();
        ::llvm::Type* expect_llvm_ty = function_ty->params()[i];
        CHECK_TRUE(expect_llvm_ty == actual_llvm_ty,
                   "LLVM argument type mismatch at ", i, ", expect ",
                   GetLLVMObjectString(expect_llvm_ty), " but get ",
                   GetLLVMObjectString(actual_llvm_ty), ": ",
                   GetLLVMObjectString(function_ty));
    }
    ::llvm::Value* raw = builder.CreateCall(callee, llvm_args);

    // extract return value
    NativeValue return_value;
    if (return_by_arg) {
        size_t pos_idx = ret_pos_begin;
        CHECK_STATUS(ExtractLLVMReturnValue(fn->GetReturnType(),
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

Status UDFIRBuilder::GetLLVMFunctionType(const node::FnDefNode* fn,
                                         ::llvm::FunctionType** func_ty) {
    std::vector<const node::TypeNode*> arg_types;
    std::vector<int> arg_nullable;
    for (size_t i = 0; i < fn->GetArgSize(); ++i) {
        arg_types.push_back(fn->GetArgType(i));
        arg_nullable.push_back(fn->IsArgNullable(i));
    }
    bool return_by_arg = false;
    bool variadic = false;
    auto extern_fn = dynamic_cast<const node::ExternalFnDefNode*>(fn);
    if (extern_fn != nullptr) {
        return_by_arg = extern_fn->return_by_arg();
        variadic = extern_fn->variadic_pos() >= 0;
    }
    return codegen::GetLLVMFunctionType(
        module_, arg_types, arg_nullable, fn->GetReturnType(),
        fn->IsReturnNullable(), variadic, &return_by_arg, func_ty);
}

Status UDFIRBuilder::BuildExternCall(
    const node::ExternalFnDefNode* fn,
    const std::vector<const node::TypeNode*>& arg_types,
    const std::vector<NativeValue>& args, NativeValue* output) {
    // resolve to llvm function
    ::llvm::FunctionType* func_ty = nullptr;
    CHECK_STATUS(GetLLVMFunctionType(fn, &func_ty));

    auto callee = module_->getOrInsertFunction(fn->function_name(), func_ty);
    return BuildLLVMCall(fn, callee, args, fn->return_by_arg(), output);
}

Status UDFIRBuilder::BuildUDAFCall(
    const node::UDAFDefNode* udaf,
    const std::vector<const node::TypeNode*>& arg_types,
    const std::vector<NativeValue>& args, NativeValue* output) {
    // Compute udaf in sub-function
    // "void f_udaf_call_impl_(Inputs, Output*)"
    std::stringstream ss;
    ss << block_->getParent()->getName().str() << "_udaf_call_impl_"
       << udaf->GetName() << "." << arg_types[0]->GetName();
    std::string sub_function_name = ss.str();

    // udaf state type
    const node::TypeNode* state_type = udaf->GetStateType();
    CHECK_TRUE(state_type != nullptr, "Missing state type");
    size_t state_num = 1;
    if (state_type->base() == node::kTuple) {
        state_num = state_type->GetGenericSize();
    }

    // udaf input elements type
    size_t input_num = udaf->GetArgSize();
    std::vector<const node::TypeNode*> elem_types(input_num);
    for (size_t i = 0; i < input_num; ++i) {
        elem_types[i] = udaf->GetElementType(i);
    }

    // udaf output type
    const node::TypeNode* output_type = udaf->GetReturnType();
    size_t output_num = 1;
    if (output_type->base() == node::kTuple) {
        output_num = output_type->GetGenericSize();
    }

    node::NodeManager nm;
    auto subfunc_ret_type = nm.MakeTypeNode(node::kTuple);
    subfunc_ret_type->AddGeneric(output_type, false);
    subfunc_ret_type->AddGeneric(nm.MakeTypeNode(node::kBool), false);

    std::vector<int> arg_nullable(arg_types.size(), false);
    NativeValue tuple;
    auto sub_def =
        nm.MakeExternalFnDefNode(sub_function_name, 0, subfunc_ret_type, false,
                                 arg_types, arg_nullable, -1, true);
    CHECK_STATUS(this->BuildExternCall(sub_def, arg_types, args, &tuple));
    CHECK_TRUE(tuple.IsTuple());

    // extract null flag and values
    ::llvm::IRBuilder<> builder(block_);
    ::llvm::Value* is_null = tuple.GetField(1).GetValue(&builder);
    *output = NativeValue::CreateWithFlag(tuple.GetField(0).GetValue(&builder),
                                          is_null);

    // build sub-function
    ::llvm::Function* sub_fn = module_->getFunction(sub_function_name);
    CHECK_TRUE(sub_fn != nullptr, sub_function_name, " not generated");

    std::vector<::llvm::Value*> output_addrs;
    for (size_t i = 0; i < output_num; ++i) {
        output_addrs.push_back(sub_fn->arg_begin() + input_num + i);
    }
    ::llvm::Value* output_is_null_ptr =
        sub_fn->arg_begin() + input_num + output_num;

    Status status;
    ::llvm::LLVMContext& llvm_ctx = module_->getContext();

    // infer state llvm types
    std::vector<llvm::Type*> state_llvm_tys;
    if (state_type->base() == node::kTuple) {
        // tuple state should be expanded
        for (auto field_type : state_type->generics()) {
            llvm::Type* llvm_ty = nullptr;
            CHECK_TRUE(GetLLVMType(module_, field_type, &llvm_ty),
                       "Fail to get llvm type for " + field_type->GetName());
            state_llvm_tys.push_back(llvm_ty);
        }
    } else {
        llvm::Type* llvm_ty = nullptr;
        CHECK_TRUE(GetLLVMType(module_, state_type, &llvm_ty),
                   "Fail to get llvm type for " + state_type->GetName());
        state_llvm_tys.push_back(llvm_ty);
    }

    std::vector<::llvm::Value*> list_ptrs;
    for (size_t i = 0; i < input_num; ++i) {
        list_ptrs.push_back(sub_fn->arg_begin() + i);
    }
    ::llvm::BasicBlock* head_block =
        ::llvm::BasicBlock::Create(llvm_ctx, "head", sub_fn);
    ::llvm::BasicBlock* enter_block =
        ::llvm::BasicBlock::Create(llvm_ctx, "enter_iter", sub_fn);
    ::llvm::BasicBlock* body_block =
        ::llvm::BasicBlock::Create(llvm_ctx, "iter_body", sub_fn);
    ::llvm::BasicBlock* exit_block =
        ::llvm::BasicBlock::Create(llvm_ctx, "exit_iter", sub_fn);

    // iter head
    builder.SetInsertPoint(head_block);
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
    if (udaf->init_expr() != nullptr) {
        ExprIRBuilder expr_builder(head_block, sv_, schemas_context_, true,
                                   module_);
        CHECK_TRUE(expr_builder.Build(udaf->init_expr(), &init_value, status),
                   "Build init expr ", udaf->init_expr()->GetExprString(),
                   " failed: ", status.msg);
    } else {
        ::llvm::Value* has_first = nullptr;
        for (size_t i = 0; i < input_num; ++i) {
            ::llvm::Value* cur_has_first = nullptr;
            CHECK_STATUS(iter_head_builder.BuildIteratorHasNext(
                iterators[i], elem_types[i], &cur_has_first));
            if (has_first == nullptr) {
                has_first = cur_has_first;
            } else {
                has_first = builder.CreateAnd(has_first, cur_has_first);
            }
        }

        ::llvm::BasicBlock* iter_first =
            ::llvm::BasicBlock::Create(llvm_ctx, "iter_first", sub_fn);
        ::llvm::BasicBlock* iter_empty =
            ::llvm::BasicBlock::Create(llvm_ctx, "exit_empty", sub_fn);
        builder.CreateCondBr(has_first, iter_first, iter_empty);

        // iter empty
        builder.SetInsertPoint(iter_empty);
        builder.CreateStore(::llvm::ConstantInt::getTrue(llvm_ctx),
                            output_is_null_ptr);
        builder.CreateRetVoid();

        // iter first elements
        builder.SetInsertPoint(iter_first);
        ListIRBuilder iter_first_builder(iter_first, nullptr);
        CHECK_STATUS(iter_first_builder.BuildIteratorNext(
            iterators[0], elem_types[0], &init_value));
    }

    // local states storage
    std::vector<::llvm::Value*> states_storage(state_num);
    if (state_num > 1) {
        CHECK_TRUE(init_value.IsTuple() &&
                   init_value.GetFieldNum() == state_num);
        for (size_t i = 0; i < state_num; ++i) {
            NativeValue sub = init_value.GetField(i);
            if (TypeIRBuilder::IsStructPtr(state_llvm_tys[i])) {
                states_storage[i] = sub.GetValue(&builder);
            } else {
                states_storage[i] = builder.CreateAlloca(state_llvm_tys[i]);
                builder.CreateStore(sub.GetValue(&builder), states_storage[i]);
            }
        }
    } else {
        if (TypeIRBuilder::IsStructPtr(state_llvm_tys[0])) {
            states_storage[0] = init_value.GetValue(&builder);
        } else {
            states_storage[0] = builder.CreateAlloca(state_llvm_tys[0]);
            builder.CreateStore(init_value.GetValue(&builder),
                                states_storage[0]);
        }
    }
    builder.CreateBr(enter_block);

    // iter enter
    builder.SetInsertPoint(enter_block);
    ListIRBuilder iter_enter_builder(enter_block, nullptr);
    ::llvm::Value* has_next = nullptr;
    for (size_t i = 0; i < input_num; ++i) {
        ::llvm::Value* cur_has_next = nullptr;
        CHECK_STATUS(iter_enter_builder.BuildIteratorHasNext(
                         iterators[i], elem_types[i], &cur_has_next),
                     status.msg);
        if (has_next == nullptr) {
            has_next = cur_has_next;
        } else {
            has_next = builder.CreateAnd(cur_has_next, has_next);
        }
    }
    builder.CreateCondBr(has_next, body_block, exit_block);

    // iter body
    builder.SetInsertPoint(body_block);
    ListIRBuilder iter_next_builder(body_block, nullptr);
    UDFIRBuilder sub_udf_builder(body_block, sv_, schemas_context_, module_);

    std::vector<NativeValue> cur_state_values;
    std::vector<NativeValue> update_args;
    for (size_t i = 0; i < state_num; ++i) {
        if (TypeIRBuilder::IsStructPtr(states_storage[i]->getType())) {
            cur_state_values.push_back(NativeValue::Create(states_storage[i]));
        } else {
            auto load_raw = builder.CreateLoad(states_storage[i]);
            cur_state_values.push_back(NativeValue::Create(load_raw));
        }
    }
    if (state_num > 1) {
        update_args.push_back(NativeValue::CreateTuple(cur_state_values));
    } else {
        update_args.push_back(cur_state_values[0]);
    }
    for (size_t i = 0; i < input_num; ++i) {
        NativeValue next_val;
        CHECK_STATUS(iter_next_builder.BuildIteratorNext(
            iterators[i], elem_types[i], &next_val));
        update_args.push_back(next_val);
    }

    NativeValue update_value;
    std::vector<const node::TypeNode*> update_arg_types;
    update_arg_types.push_back(state_type);
    for (size_t i = 0; i < input_num; ++i) {
        update_arg_types.push_back(elem_types[i]);
    }
    CHECK_STATUS(sub_udf_builder.BuildCall(
        udaf->update_func(), update_arg_types, update_args, &update_value));

    if (update_value.IsTuple()) {
        CHECK_TRUE(update_value.GetFieldNum() == state_num);
        for (size_t i = 0; i < state_num; ++i) {
            NativeValue sub = update_value.GetField(i);
            ::llvm::Value* raw_update = sub.GetValue(&builder);
            if (TypeIRBuilder::IsStructPtr(raw_update->getType())) {
                raw_update = builder.CreateLoad(raw_update);
            }
            builder.CreateStore(raw_update, states_storage[i]);
        }
    } else {
        ::llvm::Value* raw_update = update_value.GetValue(&builder);
        if (TypeIRBuilder::IsStructPtr(raw_update->getType())) {
            raw_update = builder.CreateLoad(raw_update);
        }
        builder.CreateStore(raw_update, states_storage[0]);
    }
    builder.CreateBr(enter_block);

    builder.SetInsertPoint(exit_block);
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

    UDFIRBuilder sub_udf_builder_exit(exit_block, sv_, schemas_context_,
                                      module_);
    NativeValue local_output;
    CHECK_STATUS(
        sub_udf_builder_exit.BuildCall(udaf->output_func(), {state_type},
                                       {single_final_state}, &local_output),
        "Build output function call failed");

    if (output_num > 1) {
        CHECK_TRUE(local_output.IsTuple() &&
                   local_output.GetFieldNum() == output_num);
        for (size_t i = 0; i < output_num; ++i) {
            ::llvm::Value* raw_output =
                local_output.GetField(i).GetValue(&builder);
            if (!TypeIRBuilder::IsStructPtr(raw_output->getType())) {
                raw_output = builder.CreateLoad(raw_output);
            }
            builder.CreateStore(raw_output, output_addrs[i]);
        }
    } else {
        ::llvm::Value* raw_output = local_output.GetValue(&builder);
        if (TypeIRBuilder::IsStructPtr(raw_output->getType())) {
            raw_output = builder.CreateLoad(raw_output);
        }
        builder.CreateStore(raw_output, output_addrs[0]);
    }

    ListIRBuilder iter_delete_builder(exit_block, nullptr);
    for (size_t i = 0; i < input_num; ++i) {
        ::llvm::Value* delete_iter_res;
        CHECK_STATUS(iter_delete_builder.BuildIteratorDelete(
            iterators[i], elem_types[i], &delete_iter_res));
    }
    builder.CreateStore(::llvm::ConstantInt::getFalse(llvm_ctx),
                        output_is_null_ptr);
    builder.CreateRetVoid();
    return Status::OK();
}

}  // namespace codegen
}  // namespace fesql
