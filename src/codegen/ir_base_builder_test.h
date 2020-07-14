/*
 * ir_base_builder.h
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

#ifndef SRC_CODEGEN_IR_BASE_BUILDER_TEST_H_
#define SRC_CODEGEN_IR_BASE_BUILDER_TEST_H_

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "llvm/IR/Verifier.h"
#include "llvm/Support/InitLLVM.h"
#include "llvm/Support/TargetSelect.h"

#include "base/fe_status.h"
#include "codegen/context.h"
#include "codegen/type_ir_builder.h"
#include "udf/default_udf_library.h"
#include "udf/literal_traits.h"
#include "udf/udf.h"
#include "vm/sql_compiler.h"

namespace fesql {
namespace codegen {

using udf::DataTypeTrait;

template <typename Ret, typename... Args>
class ModuleFunctionBuilderWithFullInfo;

template <typename Ret, typename... Args>
class ModuleTestFunction {
 public:
    Ret operator()(Args... args) {
        if (return_by_arg) {
            auto fn = reinterpret_cast<void (*)(Args..., Ret*)>(fn_ptr);
            Ret res;
            fn(args..., &res);
            return res;
        } else {
            auto fn = reinterpret_cast<Ret (*)(Args...)>(fn_ptr);
            return fn(args...);
        }
    }

    bool valid() const { return jit != nullptr && fn_ptr != nullptr; }

    ModuleTestFunction(ModuleTestFunction&& inst)
        : jit(std::move(inst.jit)), fn_ptr(inst.fn_ptr) {}

 private:
    friend class ModuleFunctionBuilderWithFullInfo<Ret, Args...>;

    ModuleTestFunction() {}

    ModuleTestFunction(const std::string& fn_name, bool return_by_arg,
                       udf::UDFLibrary* library,
                       std::unique_ptr<::llvm::Module> module,
                       std::unique_ptr<::llvm::LLVMContext> llvm_ctx) {
        llvm::InitializeNativeTarget();
        llvm::InitializeNativeTargetAsmPrinter();
        ::llvm::ExitOnError ExitOnErr;
        jit = std::move(ExitOnErr(vm::FeSQLJITBuilder().create()));
        auto& jd = jit->getMainJITDylib();
        ::llvm::orc::MangleAndInterner mi(jit->getExecutionSession(),
                                          jit->getDataLayout());
        library->InitJITSymbols(jit.get());
        ::fesql::vm::InitCodecSymbol(jd, mi);
        ::fesql::udf::InitUDFSymbol(jd, mi);

        llvm::errs() << *(module.get()) << "\n";
        if (llvm::verifyModule(*(module.get()), &llvm::errs(), nullptr)) {
            LOG(WARNING) << "fail to verify codegen module";
            return;
        }

        ExitOnErr(jit->addIRModule(::llvm::orc::ThreadSafeModule(
            std::move(module), std::move(llvm_ctx))));
        auto load_fn = ExitOnErr(jit->lookup(fn_name));
        this->fn_ptr = reinterpret_cast<void*>(load_fn.getAddress());
        this->return_by_arg = return_by_arg;
    }

    std::unique_ptr<vm::FeSQLJIT> jit = nullptr;
    void* fn_ptr = nullptr;
    bool return_by_arg;
};

struct ModuleFunctionBuilderState {
    std::vector<node::TypeNode*> arg_types;
    node::TypeNode* ret_type;
    node::NodeManager nm;
    udf::DefaultUDFLibrary library;
};

typedef std::unique_ptr<ModuleFunctionBuilderState> BuilderStatePtr;

template <typename Ret, typename... Args>
class ModuleFunctionBuilderWithFullInfo {
 public:
    explicit ModuleFunctionBuilderWithFullInfo(BuilderStatePtr&& state)
        : state(std::move(state)) {}

    ModuleTestFunction<Ret, Args...> build(
        const std::function<base::Status(CodeGenContext*)>&);

 private:
    BuilderStatePtr state;
};

template <typename... Args>
class ModuleFunctionBuilderWithArgs {
 public:
    explicit ModuleFunctionBuilderWithArgs(BuilderStatePtr&& state)
        : state(std::move(state)) {}

    template <typename Ret>
    auto returns() {
        state->ret_type = DataTypeTrait<Ret>::to_type_node(&(state->nm));
        return ModuleFunctionBuilderWithFullInfo<Ret, Args...>(
            std::move(state));
    }

 private:
    BuilderStatePtr state;
};

template <typename Ret>
class ModuleFunctionBuilderWithRet {
 public:
    explicit ModuleFunctionBuilderWithRet(BuilderStatePtr&& state)
        : state(std::move(state)) {}

    template <typename... Args>
    auto args() {
        state->arg_types = {DataTypeTrait<Args>::to_type_node(&(state->nm))...};
        return ModuleFunctionBuilderWithFullInfo<Ret, Args...>(
            std::move(state));
    }

 private:
    BuilderStatePtr state;
};

class ModuleFunctionBuilder {
 public:
    ModuleFunctionBuilder() : state(new ModuleFunctionBuilderState()) {}

    template <typename... Args>
    auto args() {
        state->arg_types = {DataTypeTrait<Args>::to_type_node(&(state->nm))...};
        return ModuleFunctionBuilderWithArgs<Args...>(std::move(state));
    }

    template <typename Ret>
    auto returns() {
        state->ret_type = DataTypeTrait<Ret>::to_type_node(&(state->nm));
        return ModuleFunctionBuilderWithRet<Ret>(std::move(state));
    }

 private:
    BuilderStatePtr state;
};

template <typename Ret, typename... Args>
ModuleTestFunction<Ret, Args...>
ModuleFunctionBuilderWithFullInfo<Ret, Args...>::build(
    const std::function<base::Status(CodeGenContext*)>& build_module) {
    ModuleTestFunction<Ret, Args...> nil;
    auto llvm_ctx =
        std::unique_ptr<::llvm::LLVMContext>(new llvm::LLVMContext());
    auto module =
        std::unique_ptr<::llvm::Module>(new ::llvm::Module("Test", *llvm_ctx));
    ::fesql::udf::RegisterUDFToModule(module.get());

    CodeGenContext context(module.get());

    node::NodeManager nm;
    std::vector<node::TypeNode*> arg_types = {
        DataTypeTrait<Args>::to_type_node(&nm)...};
    std::vector<::llvm::Type*> llvm_arg_types;
    for (auto type_node : arg_types) {
        ::llvm::Type* llvm_ty = nullptr;
        if (!codegen::GetLLVMType(module.get(), type_node, &llvm_ty)) {
            LOG(WARNING) << "Fail for arg type " << type_node->GetName();
            return nil;
        }
        if (codegen::TypeIRBuilder::IsStructPtr(llvm_ty)) {
            llvm_ty = reinterpret_cast<::llvm::PointerType*>(llvm_ty)
                          ->getElementType();
        }
        llvm_arg_types.push_back(llvm_ty);
    }

    ::llvm::Type* llvm_ret_ty = nullptr;
    auto ret_type = DataTypeTrait<Ret>::to_type_node(&nm);
    if (!codegen::GetLLVMType(module.get(), ret_type, &llvm_ret_ty)) {
        return nil;
    }
    if (codegen::TypeIRBuilder::IsStructPtr(llvm_ret_ty)) {
        llvm_ret_ty = reinterpret_cast<::llvm::PointerType*>(llvm_ret_ty)
                          ->getElementType();
    }
    bool ret_by_arg = false;
    if (ret_type->base_ == node::kVarchar) {
        // strange problem to return StringRef
        llvm_arg_types.push_back(llvm_ret_ty->getPointerTo());
        llvm_ret_ty = ::llvm::Type::getVoidTy(*(llvm_ctx.get()));
        ret_by_arg = true;
    }

    std::string fn_name = "apply";
    for (auto type_node : arg_types) {
        fn_name.append(".").append(type_node->GetName());
    }
    auto function_ty =
        ::llvm::FunctionType::get(llvm_ret_ty, llvm_arg_types, false);
    auto function = ::llvm::Function::Create(
        function_ty, llvm::Function::ExternalLinkage, fn_name, module.get());
    FunctionScopeGuard func_guard(function, &context);

    auto block = ::llvm::BasicBlock::Create(*llvm_ctx, "entry", function);
    BlockGuard block_guard(block, &context);

    auto status = build_module(&context);
    if (!status.isOK()) {
        LOG(WARNING) << status.msg;
        return nil;
    }

    udf::DefaultUDFLibrary lib;
    return ModuleTestFunction<Ret, Args...>(
        fn_name, ret_by_arg, &lib, std::move(module), std::move(llvm_ctx));
}

}  // namespace codegen
}  // namespace fesql
#endif  // SRC_CODEGEN_IR_BASE_BUILDER_TEST_H_
