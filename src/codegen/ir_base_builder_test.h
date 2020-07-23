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

template <typename T, typename Dummy = void>
struct ToLLVMArgTrait;

// For struct type arg, pass pointer to llvm
template <>
struct ToLLVMArgTrait<codec::Date> {
    using ArgT = codec::Date *;
    static ArgT to_llvm_arg(codec::Date &val) { return &val; }  // NOLINT
};
template <>
struct ToLLVMArgTrait<codec::Timestamp> {
    using ArgT = codec::Timestamp *;
    static ArgT to_llvm_arg(codec::Timestamp &val) { return &val; }  // NOLINT
};
template <>
struct ToLLVMArgTrait<codec::StringRef> {
    using ArgT = codec::StringRef *;
    static ArgT to_llvm_arg(codec::StringRef &val) { return &val; }  // NOLINT
};
template <typename T>
struct ToLLVMArgTrait<codec::ListRef<T>> {
    using ArgT = codec::ListRef<T> *;
    static ArgT to_llvm_arg(codec::ListRef<T> &val) { return &val; }  // NOLINT
};

// For row type, pass row ptr
template <typename... ArgTypes>
struct ToLLVMArgTrait<udf::LiteralTypedRow<ArgTypes...>> {
    using ArgT = int8_t *;
    static ArgT to_llvm_arg(udf::LiteralTypedRow<ArgTypes...> &val) {  // NOLINT
        return val.row_ptr;
    }
};

// For primitive type arg, pass directly
template <typename T>
struct ToLLVMArgTrait<
    T, typename std::enable_if_t<std::is_fundamental<T>::value>> {
    using ArgT = T;
    static ArgT to_llvm_arg(const T &val) { return val; }
};

template <typename Ret, typename... Args>
class ModuleTestFunction {
 public:
    Ret operator()(Args... args) {
        if (return_by_arg) {
            auto fn = reinterpret_cast<void (*)(
                typename ToLLVMArgTrait<Args>::ArgT..., Ret *)>(fn_ptr);
            Ret res;
            fn(ToLLVMArgTrait<Args>::to_llvm_arg(args)..., &res);
            return res;
        } else {
            auto fn = reinterpret_cast<Ret (*)(
                typename ToLLVMArgTrait<Args>::ArgT...)>(fn_ptr);
            return fn(ToLLVMArgTrait<Args>::to_llvm_arg(args)...);
        }
    }

    bool valid() const { return jit != nullptr && fn_ptr != nullptr; }

    ModuleTestFunction(ModuleTestFunction &&inst)
        : jit(std::move(inst.jit)), fn_ptr(inst.fn_ptr) {}

 private:
    friend class ModuleFunctionBuilderWithFullInfo<Ret, Args...>;

    ModuleTestFunction() {}

    ModuleTestFunction(const std::string &fn_name, bool return_by_arg,
                       udf::UDFLibrary *library,
                       std::unique_ptr<::llvm::Module> module,
                       std::unique_ptr<::llvm::LLVMContext> llvm_ctx) {
        llvm::InitializeNativeTarget();
        llvm::InitializeNativeTargetAsmPrinter();
        ::llvm::ExitOnError ExitOnErr;
        jit = std::move(ExitOnErr(vm::FeSQLJITBuilder().create()));
        auto &jd = jit->getMainJITDylib();
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
        this->fn_ptr = reinterpret_cast<void *>(load_fn.getAddress());
        this->return_by_arg = return_by_arg;
    }

    std::unique_ptr<vm::FeSQLJIT> jit = nullptr;
    void *fn_ptr = nullptr;
    bool return_by_arg;
};

struct ModuleFunctionBuilderState {
    std::vector<node::TypeNode *> arg_types;
    node::TypeNode *ret_type;
    node::NodeManager nm;
    udf::DefaultUDFLibrary library;
};

typedef std::unique_ptr<ModuleFunctionBuilderState> BuilderStatePtr;

template <typename Ret, typename... Args>
class ModuleFunctionBuilderWithFullInfo {
 public:
    explicit ModuleFunctionBuilderWithFullInfo(BuilderStatePtr &&state)
        : state(std::move(state)) {}

    ModuleTestFunction<Ret, Args...> build(
        const std::function<base::Status(CodeGenContext *)> &);

 private:
    BuilderStatePtr state;
};

template <typename... Args>
class ModuleFunctionBuilderWithArgs {
 public:
    explicit ModuleFunctionBuilderWithArgs(BuilderStatePtr &&state)
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
    explicit ModuleFunctionBuilderWithRet(BuilderStatePtr &&state)
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
    const std::function<base::Status(CodeGenContext *)> &build_module) {
    ModuleTestFunction<Ret, Args...> nil;
    auto llvm_ctx =
        std::unique_ptr<::llvm::LLVMContext>(new llvm::LLVMContext());
    auto module =
        std::unique_ptr<::llvm::Module>(new ::llvm::Module("Test", *llvm_ctx));
    ::fesql::udf::RegisterUDFToModule(module.get());

    CodeGenContext context(module.get());

    node::NodeManager nm;
    std::vector<node::TypeNode *> arg_types = {
        DataTypeTrait<Args>::to_type_node(&nm)...};
    std::vector<::llvm::Type *> llvm_arg_types;
    for (auto type_node : arg_types) {
        ::llvm::Type *llvm_ty = nullptr;
        if (!codegen::GetLLVMType(module.get(), type_node, &llvm_ty)) {
            LOG(WARNING) << "Fail for arg type " << type_node->GetName();
            return nil;
        }
        llvm_arg_types.push_back(llvm_ty);
    }

    ::llvm::Type *llvm_ret_ty = nullptr;
    auto ret_type = DataTypeTrait<Ret>::to_type_node(&nm);
    if (!codegen::GetLLVMType(module.get(), ret_type, &llvm_ret_ty)) {
        return nil;
    }

    // for struct type, return/pass by value is in danger
    bool ret_by_arg = false;
    if (codegen::TypeIRBuilder::IsStructPtr(llvm_ret_ty)) {
        llvm_arg_types.push_back(llvm_ret_ty);
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

/**
 * Helper function to apply vector of args to variadic function
 */
template <typename F, std::size_t... I>
static node::ExprNode *ApplyExprFuncHelper(
    node::NodeManager *nm, const std::vector<node::ExprIdNode *> &args,
    const std::index_sequence<I...> &, const F &expr_func) {
    return expr_func(nm, args[I]...);
}

/**
 * Build a callable function object from expr build function.
 */
template <typename Ret, typename... Args>
ModuleTestFunction<Ret, Args...> BuildExprFunction(
    const std::function<node::ExprNode *(
        node::NodeManager *,
        typename std::pair<Args, node::ExprNode *>::second_type...)>
        &expr_func) {
    return ModuleFunctionBuilder()
        .returns<Ret>()
        .template args<Args...>()
        .build([&](CodeGenContext *ctx) {
            node::NodeManager nm;
            std::vector<::llvm::Type *> llvm_arg_types;

            std::vector<node::TypeNode *> arg_node_types = {
                udf::DataTypeTrait<Args>::to_type_node(&nm)...};

            std::vector<node::ExprIdNode *> arg_exprs;
            for (size_t i = 0; i < sizeof...(Args); ++i) {
                auto arg = nm.MakeExprIdNode("arg_" + std::to_string(i),
                                             node::ExprIdNode::GetNewId());
                auto dtype = arg_node_types[i];
                arg->SetOutputType(dtype);
                arg_exprs.push_back(arg);

                ::llvm::Type *llvm_ty = nullptr;
                codegen::GetLLVMType(ctx->GetModule(), dtype, &llvm_ty);
                llvm_arg_types.push_back(llvm_ty);
            };
            node::ExprNode *body = ApplyExprFuncHelper(
                &nm, arg_exprs, std::index_sequence_for<Args...>(), expr_func);
            CHECK_TRUE(body != nullptr, "Build output expr failed");

            vm::SchemaSourceList empty;
            vm::SchemasContext empty_context(empty);
            ScopeVar sv;
            sv.Enter("entry");

            ::llvm::IRBuilder<> builder(ctx->GetCurrentBlock());

            auto llvm_func = ctx->GetCurrentFunction();
            for (size_t i = 0; i < llvm_arg_types.size(); ++i) {
                ::llvm::Value *llvm_arg = llvm_func->arg_begin() + i;
                node::ExprIdNode *arg_expr_id = arg_exprs[i];
                sv.AddVar(arg_expr_id->GetExprString(),
                          NativeValue::Create(llvm_arg));
            }

            ExprIRBuilder expr_builder(ctx->GetCurrentBlock(), &sv,
                                       &empty_context, false, ctx->GetModule());
            NativeValue out;
            base::Status status;
            CHECK_TRUE(expr_builder.Build(body, &out, status), status.msg);

            auto ret_value = out.GetValue(&builder);
            if (codegen::TypeIRBuilder::IsStructPtr(ret_value->getType())) {
                ret_value = builder.CreateLoad(ret_value);
                builder.CreateStore(
                    ret_value, llvm_func->arg_begin() + llvm_arg_types.size());
                builder.CreateRetVoid();
            } else {
                builder.CreateRet(ret_value);
            }
            return Status::OK();
        });
}

}  // namespace codegen
}  // namespace fesql
#endif  // SRC_CODEGEN_IR_BASE_BUILDER_TEST_H_
