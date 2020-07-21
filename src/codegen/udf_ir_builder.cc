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

    ::llvm::IRBuilder<> builder(block_);
    ::llvm::Value* raw_output = nullptr;
    std::vector<::llvm::Value*> raw_args;
    for (auto arg : args) {
        raw_args.push_back(arg.GetValue(&builder));
    }
    CHECK_STATUS(BuildCallWithLLVMCallee(fn, callee, raw_args, return_by_arg,
                                         &raw_output));
    *output = NativeValue::Create(raw_output);
    return Status::OK();
}

Status UDFIRBuilder::BuildLambdaCall(
    const node::LambdaNode* fn,
    const std::vector<const node::TypeNode*>& arg_types,
    const std::vector<NativeValue>& args, NativeValue* output) {
    // sanity checks
    CHECK_TRUE(fn->Validate(arg_types));

    // bind args
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

Status UDFIRBuilder::BuildExternCall(
    const node::ExternalFnDefNode* fn,
    const std::vector<const node::TypeNode*>& arg_types,
    const std::vector<NativeValue>& args, NativeValue* output) {
    // resolve to llvm function
    bool return_by_arg;
    ::llvm::FunctionCallee callee;
    CHECK_STATUS(GetExternCallee(fn, arg_types, &callee, &return_by_arg));

    ::llvm::IRBuilder<> builder(block_);
    ::llvm::Value* raw_output = nullptr;
    std::vector<::llvm::Value*> raw_args;
    for (auto arg : args) {
        raw_args.push_back(arg.GetValue(&builder));
    }
    CHECK_STATUS(BuildCallWithLLVMCallee(fn, callee, raw_args, return_by_arg,
                                         &raw_output));
    *output = NativeValue::Create(raw_output);
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

Status UDFIRBuilder::BuildUDAFCall(
    const node::UDAFDefNode* udaf,
    const std::vector<const node::TypeNode*>& arg_types,
    const std::vector<NativeValue>& args, NativeValue* output) {
    CHECK_TRUE(args.size() == 1, "UDAF should take single input");
    CHECK_TRUE(arg_types[0] != nullptr && arg_types[0]->base_ == node::kList,
               "UDAF input is not list");

    const node::TypeNode* state_type = udaf->GetStateType();
    CHECK_TRUE(state_type != nullptr, "Missing state type");
    const node::TypeNode* elem_type = udaf->GetInputElementType();
    CHECK_TRUE(elem_type != nullptr, "Missing elem type");
    const node::TypeNode* output_type = udaf->GetReturnType();
    CHECK_TRUE(output_type != nullptr, "Missing return type");

    Status status;
    ::llvm::IRBuilder<> builder(block_);
    ::llvm::LLVMContext& llvm_ctx = module_->getContext();
    ::llvm::Value* input_value = args[0].GetValue(&builder);

    // infer state llvm type
    llvm::Type* state_llvm_ty = nullptr;
    CHECK_TRUE(GetLLVMType(module_, state_type, &state_llvm_ty),
               "Fail to get llvm type for " + state_type->GetName());
    bool is_struct_ptr_state = TypeIRBuilder::IsStructPtr(state_llvm_ty);

    // infer output llvm type
    llvm::Type* output_llvm_ty = nullptr;
    CHECK_TRUE(GetLLVMType(module_, output_type, &output_llvm_ty),
               "Fail to get llvm type for " + output_type->GetName());
    bool is_struct_ptr_out = TypeIRBuilder::IsStructPtr(output_llvm_ty);

    // for type stored as struct ptr, do not use T** state
    ::llvm::PointerType* output_ptr_ty;
    if (is_struct_ptr_out) {
        output_ptr_ty = reinterpret_cast<::llvm::PointerType*>(output_llvm_ty);
    } else {
        output_ptr_ty = output_llvm_ty->getPointerTo();
    }

    // iter update in sub-function
    // void f_udaf_call_impl_(Iter, State*)
    std::stringstream ss;
    ss << block_->getParent()->getName().str() << "_udaf_call_impl_"
       << udaf->GetSimpleName() << "." << arg_types[0]->GetName();
    std::string sub_function_name = ss.str();
    auto bool_ty = ::llvm::Type::getInt1Ty(llvm_ctx);
    auto fn_ty = ::llvm::FunctionType::get(
        bool_ty, {input_value->getType(), output_ptr_ty}, false);
    ::llvm::Function* fn = ::llvm::Function::Create(
        fn_ty, llvm::Function::ExternalLinkage, sub_function_name, module_);
    auto list_ptr = fn->arg_begin();
    auto output_ptr = fn->arg_begin() + 1;

    ::llvm::BasicBlock* head_block =
        ::llvm::BasicBlock::Create(llvm_ctx, "head", fn);
    ::llvm::BasicBlock* enter_block =
        ::llvm::BasicBlock::Create(llvm_ctx, "enter_iter", fn);
    ::llvm::BasicBlock* body_block =
        ::llvm::BasicBlock::Create(llvm_ctx, "iter_body", fn);
    ::llvm::BasicBlock* exit_block =
        ::llvm::BasicBlock::Create(llvm_ctx, "exit_iter", fn);

    // iter head
    builder.SetInsertPoint(head_block);
    ListIRBuilder iter_head_builder(head_block, nullptr);
    ::llvm::Value* iter = nullptr;
    CHECK_TRUE(iter_head_builder.BuildIterator(list_ptr, &iter, status),
               status.msg);

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
        CHECK_TRUE(
            iter_head_builder.BuildIteratorHasNext(iter, &has_first, status),
            status.msg);

        ::llvm::BasicBlock* iter_first =
            ::llvm::BasicBlock::Create(llvm_ctx, "iter_first", fn);
        ::llvm::BasicBlock* iter_empty =
            ::llvm::BasicBlock::Create(llvm_ctx, "exit_empty", fn);
        builder.CreateCondBr(has_first, iter_first, iter_empty);

        // iter empty
        builder.SetInsertPoint(iter_empty);
        builder.CreateRet(::llvm::ConstantInt::getTrue(llvm_ctx));

        // iter first
        builder.SetInsertPoint(iter_first);
        ListIRBuilder iter_first_builder(iter_first, nullptr);
        ::llvm::Value* first_value = nullptr;
        CHECK_TRUE(
            iter_first_builder.BuildIteratorNext(iter, &first_value, status),
            status.msg);
        init_value = NativeValue::Create(first_value);
    }
    ::llvm::Value* local_state;
    if (is_struct_ptr_state) {
        local_state = init_value.GetValue(&builder);
    } else {
        local_state = builder.CreateAlloca(state_llvm_ty);
        builder.CreateStore(init_value.GetValue(&builder), local_state);
    }
    builder.CreateBr(enter_block);

    // iter enter
    builder.SetInsertPoint(enter_block);
    ListIRBuilder iter_enter_builder(enter_block, nullptr);
    ::llvm::Value* has_next = nullptr;
    CHECK_TRUE(iter_enter_builder.BuildIteratorHasNext(iter, &has_next, status),
               status.msg);
    builder.CreateCondBr(has_next, body_block, exit_block);

    // iter body
    builder.SetInsertPoint(body_block);
    ListIRBuilder iter_next_builder(body_block, nullptr);
    ::llvm::Value* next_value = nullptr;
    CHECK_TRUE(iter_next_builder.BuildIteratorNext(iter, &next_value, status),
               status.msg);

    // call update in iter body
    UDFIRBuilder sub_udf_builder(body_block, sv_, schemas_context_, module_);
    ::llvm::Value* cur_state_value;
    if (is_struct_ptr_state) {
        cur_state_value = local_state;
    } else {
        cur_state_value = builder.CreateLoad(local_state);
    }
    NativeValue update_value;
    CHECK_STATUS(sub_udf_builder.BuildCall(
        udaf->update_func(), {state_type, elem_type},
        {NativeValue::Create(cur_state_value), NativeValue::Create(next_value)},
        &update_value));
    ::llvm::Value* raw_update = update_value.GetValue(&builder);
    if (is_struct_ptr_state) {
        raw_update = builder.CreateLoad(raw_update);
    }
    builder.CreateStore(raw_update, local_state);
    builder.CreateBr(enter_block);

    builder.SetInsertPoint(exit_block);
    ::llvm::Value* final_state_value;
    if (is_struct_ptr_state) {
        final_state_value = local_state;
    } else {
        final_state_value = builder.CreateLoad(local_state);
    }
    UDFIRBuilder sub_udf_builder_exit(exit_block, sv_, schemas_context_,
                                      module_);
    NativeValue local_output;
    CHECK_STATUS(sub_udf_builder_exit.BuildCall(
                     udaf->output_func(), {state_type},
                     {NativeValue::Create(final_state_value)}, &local_output),
                 "Build output function call failed");
    ::llvm::Value* raw_output = local_output.GetValue(&builder);
    if (is_struct_ptr_out) {
        builder.CreateStore(builder.CreateLoad(raw_output), output_ptr);
    } else {
        builder.CreateStore(raw_output, output_ptr);
    }
    builder.CreateRet(::llvm::ConstantInt::getFalse(llvm_ctx));

    // call udaf under root function
    builder.SetInsertPoint(block_);
    ::llvm::Value* output_value =
        builder.CreateAlloca(output_ptr_ty->getElementType());
    auto callee = module_->getOrInsertFunction(sub_function_name, fn_ty);
    auto is_null = builder.CreateCall(callee, {input_value, output_value});
    if (!is_struct_ptr_out) {
        output_value = builder.CreateLoad(output_value);
    }
    *output = NativeValue::CreateWithFlag(output_value, is_null);
    return Status::OK();
}

Status UDFIRBuilder::GetUDFCallee(
    const node::UDFDefNode* fn,
    const std::vector<const node::TypeNode*>& arg_types,
    ::llvm::FunctionCallee* callee, bool* return_by_arg) {
    // signature validation
    std::string fn_name = fn->def()->header_->GeIRFunctionName();
    CHECK_TRUE(fn->Validate(arg_types),
               "UDF function call validation error of ", fn_name);

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

Status UDFIRBuilder::GetExternCallee(
    const node::ExternalFnDefNode* fn,
    const std::vector<const node::TypeNode*>& arg_types,
    ::llvm::FunctionCallee* callee, bool* return_by_arg) {
    // signature validation
    std::string fn_name = fn->function_name();
    CHECK_TRUE(fn->Validate(arg_types),
               "External function call validation error of ", fn_name);

    std::vector<::llvm::Type*> llvm_arg_types;
    for (int i = 0; i < fn->GetArgSize(); ++i) {
        ::llvm::Type* expect_llvm_ty = nullptr;
        CHECK_TRUE(GetLLVMType(module_, fn->GetArgType(i), &expect_llvm_ty));
        llvm_arg_types.push_back(expect_llvm_ty);
    }

    ::llvm::Type* ret_llvm_ty = nullptr;
    CHECK_TRUE(GetLLVMType(module_, fn->GetReturnType(), &ret_llvm_ty));

    // external call convention for return_by_arg=true:
    // R f(ARGS...) -> void f(ARGS, R*, ...)
    // note that R should be trivally constructible
    if (fn->return_by_arg()) {
        llvm_arg_types.push_back(ret_llvm_ty);
        ret_llvm_ty = ::llvm::Type::getVoidTy(ret_llvm_ty->getContext());
    }
    auto llvm_fn_ty = llvm::FunctionType::get(ret_llvm_ty, llvm_arg_types,
                                              fn->variadic_pos() >= 0);
    *callee = module_->getOrInsertFunction(fn_name, llvm_fn_ty);
    *return_by_arg = fn->return_by_arg();
    return Status::OK();
}

Status UDFIRBuilder::BuildCallWithLLVMCallee(
    const node::FnDefNode* fn, ::llvm::FunctionCallee callee,
    const std::vector<llvm::Value*>& input_args, bool return_by_arg,
    ::llvm::Value** output) {
    ::llvm::IRBuilder<> builder(block_);
    std::vector<llvm::Value*> all_args = input_args;
    auto function_ty = callee.getFunctionType();
    if (return_by_arg) {
        int ret_pos = function_ty->getNumParams() - 1;
        CHECK_TRUE(ret_pos >= 0, "Return by arg but argument size = 0");

        auto ret_ptr_ty = function_ty->getParamType(ret_pos);
        CHECK_TRUE(ret_ptr_ty->isPointerTy(), "Return by arg but arg at ",
                   ret_pos, " is not pointer type");

        ::llvm::Value* ret_alloca;
        auto opaque_ret_type =
            dynamic_cast<const node::OpaqueTypeNode*>(fn->GetReturnType());
        if (opaque_ret_type != nullptr) {
            ret_alloca = builder.CreateAlloca(
                ::llvm::Type::getInt8Ty(builder.getContext()),
                builder.getInt64(opaque_ret_type->bytes()));
        } else {
            ret_alloca = builder.CreateAlloca(
                reinterpret_cast<llvm::PointerType*>(ret_ptr_ty)
                    ->getElementType());
        }
        all_args.insert(all_args.begin() + ret_pos, ret_alloca);
    }

    // arg type validation on llvm level
    for (size_t i = 0; i < all_args.size(); ++i) {
        if (i >= function_ty->getNumParams()) {
            CHECK_TRUE(function_ty->isVarArg(),
                       "Argument num out of bound for non-variadic function: ",
                       "expect ", function_ty->getNumParams(), " but get ",
                       all_args.size());
            break;
        }
        ::llvm::Type* actual_llvm_ty = all_args[i]->getType();
        ::llvm::Type* expect_llvm_ty = function_ty->params()[i];
        CHECK_TRUE(expect_llvm_ty == actual_llvm_ty,
                   "LLVM argument type mismatch at ", i, ", expect ",
                   GetLLVMObjectString(expect_llvm_ty), " but get ",
                   GetLLVMObjectString(actual_llvm_ty));
    }
    *output = builder.CreateCall(callee, all_args);
    if (return_by_arg) {
        int ret_pos = function_ty->getNumParams() - 1;
        *output = all_args[ret_pos];
    }
    return Status::OK();
}

}  // namespace codegen
}  // namespace fesql
