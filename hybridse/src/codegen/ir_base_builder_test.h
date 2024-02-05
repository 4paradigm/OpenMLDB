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

#ifndef HYBRIDSE_SRC_CODEGEN_IR_BASE_BUILDER_TEST_H_
#define HYBRIDSE_SRC_CODEGEN_IR_BASE_BUILDER_TEST_H_

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "codegen/ir_base_builder.h"
#include "llvm/IR/Verifier.h"
#include "llvm/Support/TargetSelect.h"

#include "base/fe_status.h"
#include "codegen/context.h"
#include "codegen/expr_ir_builder.h"
#include "codegen/struct_ir_builder.h"
#include "codegen/type_ir_builder.h"
#include "passes/resolve_fn_and_attrs.h"
#include "udf/default_udf_library.h"
#include "udf/literal_traits.h"
#include "vm/jit_wrapper.h"

namespace hybridse {
namespace codegen {

using udf::DataTypeTrait;

template <typename Ret, typename... Args>
class ModuleFunctionBuilderWithFullInfo;

template <typename T>
struct ProxyArgTrait {
    static void expand(T* ptr, std::vector<int8_t*>* proxy_args) {
        proxy_args->push_back(reinterpret_cast<int8_t*>(ptr));
    }
};

template <typename T>
struct ProxyArgTrait<udf::Nullable<T>> {
    static void expand(udf::Nullable<T>* ptr,
                       std::vector<int8_t*>* proxy_args) {
        if (ptr->is_null()) {
            proxy_args->push_back(nullptr);
        } else {
            proxy_args->push_back(reinterpret_cast<int8_t*>(ptr->ptr()));
        }
    }
};

template <typename... T>
struct ProxyArgTrait<udf::Tuple<T...>> {
    static void expand(udf::Tuple<T...>* ptr,
                       std::vector<int8_t*>* proxy_args) {
        Impl<0, T...>()(ptr, proxy_args);
    }

    template <size_t I, typename...>
    struct Impl;

    template <size_t I, typename Head, typename... Tail>
    struct Impl<I, Head, Tail...> {
        void operator()(udf::Tuple<T...>* ptr,
                        std::vector<int8_t*>* proxy_args) {
            ProxyArgTrait<Head>::expand(&(std::get<I>(ptr->tuple)), proxy_args);
            Impl<I + 1, Tail...>()(ptr, proxy_args);
        }
    };

    template <size_t I, typename Head>
    struct Impl<I, Head> {
        void operator()(udf::Tuple<T...>* ptr,
                        std::vector<int8_t*>* proxy_args) {
            ProxyArgTrait<Head>::expand(&(std::get<I>(ptr->tuple)), proxy_args);
        }
    };
};

template <typename T>
struct ProxyReturnTrait {
    static void expand(T* addr, std::vector<int8_t*>* proxy_args) {
        proxy_args->push_back(reinterpret_cast<int8_t*>(addr));
    }
};

template <typename T>
struct ProxyReturnTrait<udf::Nullable<T>> {
    static void expand(udf::Nullable<T>* addr,
                       std::vector<int8_t*>* proxy_args) {
        proxy_args->push_back(reinterpret_cast<int8_t*>(addr->ptr()));
        proxy_args->push_back(reinterpret_cast<int8_t*>(&addr->is_null_));
    }
};

template <typename... T>
struct ProxyReturnTrait<udf::Tuple<T...>> {
    static void expand(udf::Tuple<T...>* ptr,
                       std::vector<int8_t*>* proxy_args) {
        Impl<0, T...>()(ptr, proxy_args);
    }

    template <size_t I, typename...>
    struct Impl;

    template <size_t I, typename Head, typename... Tail>
    struct Impl<I, Head, Tail...> {
        void operator()(udf::Tuple<T...>* ptr,
                        std::vector<int8_t*>* proxy_args) {
            ProxyReturnTrait<Head>::expand(&(std::get<I>(ptr->tuple)),
                                           proxy_args);
            Impl<I + 1, Tail...>()(ptr, proxy_args);
        }
    };

    template <size_t I, typename Head>
    struct Impl<I, Head> {
        void operator()(udf::Tuple<T...>* ptr,
                        std::vector<int8_t*>* proxy_args) {
            ProxyReturnTrait<Head>::expand(&(std::get<I>(ptr->tuple)),
                                           proxy_args);
        }
    };
};

template <typename... T>
struct FillProxyArgs;

template <typename Head, typename... Tail>
struct FillProxyArgs<Head, Tail...> {
    void operator()(Head* head, Tail*... tail,
                    std::vector<int8_t*>* proxy_args) {
        ProxyArgTrait<Head>::expand(head, proxy_args);
        FillProxyArgs<Tail...>()(tail..., proxy_args);
    }
};

template <typename Head>
struct FillProxyArgs<Head> {
    void operator()(Head* head, std::vector<int8_t*>* proxy_args) {
        ProxyArgTrait<Head>::expand(head, proxy_args);
    }
};

template <typename Ret>
struct FillProxyReturns {
    void operator()(Ret* addr, std::vector<int8_t*>* proxy_args) {
        ProxyReturnTrait<Ret>::expand(addr, proxy_args);
    }
};

template <typename Ret, typename... Args>
class ModuleTestFunction {
 public:
    Ret operator()(const Args&... args) {
        std::vector<int8_t*> proxy_arg_ptrs;
        FillProxyArgs<Args...>()(const_cast<Args*>(&args)..., &proxy_arg_ptrs);

        Ret ret;
        FillProxyReturns<Ret>()(&ret, &proxy_arg_ptrs);

        auto fn = reinterpret_cast<void (*)(int8_t**)>(proxy_fn_ptr);
        fn(proxy_arg_ptrs.data());
        return ret;
    }

    bool valid() const { return jit != nullptr && fn_ptr != nullptr; }

    void* GetApplyfn_ptr() const { return fn_ptr; }

    ModuleTestFunction(ModuleTestFunction&& inst)
        : jit(std::move(inst.jit)), fn_ptr(inst.fn_ptr) {}

 private:
    friend class ModuleFunctionBuilderWithFullInfo<Ret, Args...>;

    ModuleTestFunction() {}

    ModuleTestFunction(const std::string& fn_name,
                       const std::string& proxy_fn_name,
                       udf::UdfLibrary* library,
                       std::unique_ptr<::llvm::Module> module,
                       std::unique_ptr<::llvm::LLVMContext> llvm_ctx) {
        llvm::InitializeNativeTarget();
        llvm::InitializeNativeTargetAsmPrinter();
        jit = std::unique_ptr<vm::HybridSeJitWrapper>(
            vm::HybridSeJitWrapper::Create());
        jit->Init();
        InitBuiltinJitSymbols(jit.get());
        if (library != nullptr) {
            library->InitJITSymbols(jit.get());
        } else {
            udf::DefaultUdfLibrary::get()->InitJITSymbols(jit.get());
        }

        llvm::errs() << *(module.get()) << "\n";
        if (llvm::verifyModule(*(module.get()), &llvm::errs(), nullptr)) {
            LOG(WARNING) << "fail to verify codegen module";
            return;
        }
        jit->AddModule(std::move(module), std::move(llvm_ctx));
        this->fn_ptr = const_cast<int8_t*>(jit->FindFunction(fn_name));
        this->proxy_fn_ptr =
            const_cast<int8_t*>(jit->FindFunction(proxy_fn_name));
    }

    std::unique_ptr<vm::HybridSeJitWrapper> jit = nullptr;
    void* fn_ptr = nullptr;
    void* proxy_fn_ptr = nullptr;
};

struct ModuleFunctionBuilderState {
    std::vector<const node::TypeNode*> arg_types;
    std::vector<int> arg_nullable;
    node::TypeNode* ret_type;
    bool ret_nullable;
    node::NodeManager nm;
    udf::UdfLibrary* library = nullptr;
};

typedef std::unique_ptr<ModuleFunctionBuilderState> BuilderStatePtr;

template <typename Ret, typename... Args>
class ModuleFunctionBuilderWithFullInfo {
 public:
    explicit ModuleFunctionBuilderWithFullInfo(BuilderStatePtr&& state)
        : state(std::move(state)) {}

    auto& library(udf::UdfLibrary* library) {
        state->library = library;
        return *this;
    }

    ModuleTestFunction<Ret, Args...> build(
        const std::function<base::Status(CodeGenContext*)>&);

 private:
    static size_t GetProxyArgCount(const node::TypeNode* arg_type);

    static void ExpandApplyArg(const node::TypeNode* arg_type, bool nullable,
                               ::llvm::Function* function, size_t* arg_idx,
                               std::vector<::llvm::Value*>* arg_vec);

    static void ExpandApplyReturn(const node::TypeNode* ret_type, bool nullable,
                                  ::llvm::Function* function, size_t* arg_idx,
                                  std::vector<::llvm::Value*>* arg_vec,
                                  std::vector<int>* arg_nullable);
    BuilderStatePtr state;
};

template <typename... Args>
class ModuleFunctionBuilderWithArgs {
 public:
    explicit ModuleFunctionBuilderWithArgs(BuilderStatePtr&& state)
        : state(std::move(state)) {}

    auto& library(udf::UdfLibrary* library) {
        state->library = library;
        return *this;
    }

    template <typename Ret>
    auto returns() {
        state->ret_type = DataTypeTrait<Ret>::to_type_node(&(state->nm));
        state->ret_nullable = udf::IsNullableTrait<Ret>::value;
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

    auto& library(udf::UdfLibrary* library) {
        state->library = library;
        return *this;
    }

    template <typename... Args>
    auto args() {
        state->arg_types = {DataTypeTrait<Args>::to_type_node(&(state->nm))...};
        state->arg_nullable = {udf::IsNullableTrait<Args>::value...};
        return ModuleFunctionBuilderWithFullInfo<Ret, Args...>(
            std::move(state));
    }

 private:
    BuilderStatePtr state;
};

class ModuleFunctionBuilder {
 public:
    ModuleFunctionBuilder() : state(new ModuleFunctionBuilderState()) {}

    auto& library(udf::UdfLibrary* library) {
        state->library = library;
        return *this;
    }

    template <typename... Args>
    auto args() {
        state->arg_types = {DataTypeTrait<Args>::to_type_node(&(state->nm))...};
        state->arg_nullable = {udf::IsNullableTrait<Args>::value...};
        return ModuleFunctionBuilderWithArgs<Args...>(std::move(state));
    }

    template <typename Ret>
    auto returns() {
        state->ret_type = DataTypeTrait<Ret>::to_type_node(&(state->nm));
        state->ret_nullable = udf::IsNullableTrait<Ret>::value;
        return ModuleFunctionBuilderWithRet<Ret>(std::move(state));
    }

 private:
    BuilderStatePtr state;
};

template <typename Ret, typename... Args>
void ModuleFunctionBuilderWithFullInfo<Ret, Args...>::ExpandApplyArg(
    const node::TypeNode* arg_type, bool nullable, ::llvm::Function* function,
    size_t* arg_idx, std::vector<::llvm::Value*>* arg_vec) {
    if (arg_type->base() == node::kTuple) {
        for (size_t i = 0; i < arg_type->GetGenericSize(); ++i) {
            ExpandApplyArg(arg_type->GetGenericType(i),
                           arg_type->IsGenericNullable(i), function, arg_idx,
                           arg_vec);
        }
    } else {
        // extract actual args to apply function from input ptr
        ::llvm::IRBuilder<> builder(&function->getEntryBlock());
        ::llvm::Type* expect_ty = nullptr;
        GetLlvmType(function->getParent(), arg_type, &expect_ty);
        ::llvm::Value* arg_arr = function->arg_begin();
        ::llvm::Value* arg = builder.CreateLoad(
            builder.CreateGEP(arg_arr, builder.getInt64(*arg_idx)));
        *arg_idx += 1;
        if (nullable) {
            ::llvm::Value* is_null = builder.CreateIsNull(arg);
            ::llvm::Value* alloca;
            if (TypeIRBuilder::IsStructPtr(expect_ty)) {
                auto struct_builder =
                    StructTypeIRBuilder::CreateStructTypeIRBuilder(function->getEntryBlock().getModule(), expect_ty);
                struct_builder->CreateDefault(&function->getEntryBlock(),
                                              &alloca);
                arg = builder.CreateSelect(
                    is_null, alloca, builder.CreatePointerCast(arg, expect_ty));
            } else {
                alloca = CreateAllocaAtHead(&builder, expect_ty, "arg_ptr");
                arg = builder.CreatePointerCast(arg, expect_ty->getPointerTo());
                arg = builder.CreateSelect(is_null, alloca, arg);
                arg = builder.CreateLoad(arg);
            }
            arg_vec->push_back(arg);
            arg_vec->push_back(is_null);
        } else {
            if (TypeIRBuilder::IsStructPtr(expect_ty)) {
                arg = builder.CreatePointerCast(arg, expect_ty);
            } else {
                arg = builder.CreatePointerCast(arg, expect_ty->getPointerTo());
                arg = builder.CreateLoad(arg);
            }
            arg_vec->push_back(arg);
        }
    }
}

template <typename Ret, typename... Args>
void ModuleFunctionBuilderWithFullInfo<Ret, Args...>::ExpandApplyReturn(
    const node::TypeNode* ret_type, bool nullable, ::llvm::Function* function,
    size_t* arg_idx, std::vector<::llvm::Value*>* arg_vec,
    std::vector<int>* arg_nullable) {
    if (ret_type->base() == node::kTuple) {
        for (size_t i = 0; i < ret_type->GetGenericSize(); ++i) {
            ExpandApplyReturn(ret_type->GetGenericType(i),
                              ret_type->IsGenericNullable(i), function, arg_idx,
                              arg_vec, arg_nullable);
        }
    } else {
        // extract actual return ptrs to apply function from input ptr
        ::llvm::IRBuilder<> builder(&function->getEntryBlock());
        ::llvm::Type* expect_ty = nullptr;
        GetLlvmType(function->getParent(), ret_type, &expect_ty);
        if (!TypeIRBuilder::IsStructPtr(expect_ty)) {
            expect_ty = expect_ty->getPointerTo();
        }
        ::llvm::Value* arg_arr = function->arg_begin();
        ::llvm::Value* arg = builder.CreateLoad(
            builder.CreateGEP(arg_arr, builder.getInt64(*arg_idx)));
        *arg_idx += 1;

        arg = builder.CreatePointerCast(
            arg, reinterpret_cast<::llvm::PointerType*>(expect_ty));  // T*
        arg_vec->push_back(arg);

        if (nullable) {
            ::llvm::Value* is_null_addr = builder.CreateLoad(
                builder.CreateGEP(arg_arr, builder.getInt64(*arg_idx)));
            *arg_idx += 1;
            is_null_addr = builder.CreatePointerCast(
                is_null_addr,
                ::llvm::Type::getInt1Ty(builder.getContext())->getPointerTo());
            arg_vec->push_back(is_null_addr);
            arg_nullable->push_back(1);
        } else {
            arg_nullable->push_back(0);
        }
    }
}

template <typename Ret, typename... Args>
ModuleTestFunction<Ret, Args...>
ModuleFunctionBuilderWithFullInfo<Ret, Args...>::build(
    const std::function<base::Status(CodeGenContext*)>& build_module) {
    ModuleTestFunction<Ret, Args...> nil;
    auto llvm_ctx =
        std::unique_ptr<::llvm::LLVMContext>(new llvm::LLVMContext());
    auto module =
        std::unique_ptr<::llvm::Module>(new ::llvm::Module("Test", *llvm_ctx));

    vm::SchemasContext schemas_context;
    CodeGenContext context(module.get(), &schemas_context, nullptr, &state->nm);

    auto arg_types = state->arg_types;
    auto arg_nullable = state->arg_nullable;
    auto ret_type = state->ret_type;
    bool ret_nullable = state->ret_nullable;

    bool return_by_arg = true;  // always use return_by_arg convention in test
    ::llvm::FunctionType* function_ty = nullptr;
    auto status =
        GetLlvmFunctionType(module.get(), arg_types, arg_nullable, ret_type,
                            ret_nullable, false, &return_by_arg, &function_ty);
    if (!status.isOK()) {
        LOG(WARNING) << status;
        return nil;
    }

    std::string fn_name = "apply";
    for (auto type_node : arg_types) {
        fn_name.append(".").append(type_node->GetName());
    }

    auto function = ::llvm::Function::Create(
        function_ty, llvm::Function::ExternalLinkage, fn_name, module.get());
    FunctionScopeGuard func_guard(function, &context);

    status = build_module(&context);
    if (!status.isOK()) {
        LOG(WARNING) << status;
        return nil;
    }

    // build proxy func
    // "invoke_apply(int8_t*, int8_t*, ...)"
    std::string proxy_fn_name = "invoke_" + fn_name;
    ::llvm::PointerType* ptr_ty = ::llvm::Type::getInt8PtrTy(*llvm_ctx.get());
    auto proxy_function_ty =
        ::llvm::FunctionType::get(::llvm::Type::getVoidTy(*llvm_ctx.get()),
                                  {ptr_ty->getPointerTo()}, false);
    auto proxy_function = ::llvm::Function::Create(
        proxy_function_ty, llvm::Function::ExternalLinkage, proxy_fn_name,
        module.get());
    auto proxy_block =
        ::llvm::BasicBlock::Create(*llvm_ctx, "entry", proxy_function);
    ::llvm::IRBuilder<> builder(proxy_block);
    std::vector<::llvm::Value*> llvm_apply_args;
    size_t llvm_arg_idx = 0;
    for (size_t i = 0; i < arg_types.size(); ++i) {
        ExpandApplyArg(arg_types[i], arg_nullable[i], proxy_function,
                       &llvm_arg_idx, &llvm_apply_args);
    }
    std::vector<int> ret_has_null_flag;
    ExpandApplyReturn(ret_type, ret_nullable, proxy_function, &llvm_arg_idx,
                      &llvm_apply_args, &ret_has_null_flag);
    auto callee = module->getOrInsertFunction(fn_name, function_ty);
    builder.CreateCall(callee, llvm_apply_args);
    builder.CreateRetVoid();

    return ModuleTestFunction<Ret, Args...>(fn_name, proxy_fn_name,
                                            state->library, std::move(module),
                                            std::move(llvm_ctx));
}

static inline NativeValue BindValueFromLlvmFunction(
    const node::TypeNode* arg_type, bool nullable, ::llvm::Function* llvm_func,
    size_t* arg_idx) {
    if (arg_type->base() == node::kTuple) {
        std::vector<NativeValue> sub_vec;
        for (size_t i = 0; i < arg_type->GetGenericSize(); ++i) {
            NativeValue sub_field = BindValueFromLlvmFunction(
                arg_type->GetGenericType(i), arg_type->IsGenericNullable(i),
                llvm_func, arg_idx);
            sub_vec.push_back(sub_field);
        }
        return NativeValue::CreateTuple(sub_vec);
    } else if (nullable) {
        ::llvm::Value* llvm_arg = llvm_func->arg_begin() + *arg_idx;
        *arg_idx += 1;
        ::llvm::Value* is_null = llvm_func->arg_begin() + *arg_idx;
        *arg_idx += 1;
        return NativeValue::CreateWithFlag(llvm_arg, is_null);
    } else {
        ::llvm::Value* llvm_arg = llvm_func->arg_begin() + *arg_idx;
        *arg_idx += 1;
        return NativeValue::Create(llvm_arg);
    }
}

static void WriteReturnValueArgs(const NativeValue& value,
                                 const node::TypeNode* ret_type, bool nullable,
                                 CodeGenContext* ctx,
                                 ::llvm::Function* llvm_func, size_t* arg_idx) {
    auto builder = ctx->GetBuilder();
    if (ret_type->base() == node::kTuple) {
        for (size_t i = 0; i < ret_type->GetGenericSize(); ++i) {
            WriteReturnValueArgs(value.GetField(i), ret_type->GetGenericType(i),
                                 ret_type->IsGenericNullable(i), ctx, llvm_func,
                                 arg_idx);
        }
    } else if (nullable) {
        ::llvm::Value* ret_addr = llvm_func->arg_begin() + *arg_idx;
        *arg_idx += 1;
        ::llvm::Value* is_null_addr = llvm_func->arg_begin() + *arg_idx;
        *arg_idx += 1;
        ::llvm::Value* is_null = value.GetIsNull(ctx);
        builder->CreateStore(is_null, is_null_addr);
        ctx->CreateBranchNot(is_null, [&]() {
            ::llvm::Value* raw = value.GetValue(ctx);
            if (TypeIRBuilder::IsStructPtr(raw->getType())) {
                raw = builder->CreateLoad(raw);
            }
            builder->CreateStore(raw, ret_addr);
            return Status::OK();
        });
    } else {
        ::llvm::Value* raw = value.GetValue(ctx);
        ::llvm::Value* ret_addr = llvm_func->arg_begin() + *arg_idx;
        *arg_idx += 1;
        if (TypeIRBuilder::IsStructPtr(raw->getType())) {
            raw = builder->CreateLoad(raw);
        }
        builder->CreateStore(raw, ret_addr);
    }
}

/**
 * Helper function to apply vector of args to variadic function
 */
template <typename F, std::size_t... I>
static node::ExprNode* ApplyExprFuncHelper(
    node::NodeManager* nm, const std::vector<node::ExprIdNode*>& args,
    const std::index_sequence<I...>&, const F& expr_func) {
    return expr_func(nm, args[I]...);
}

/**
 * Build a callable function object from expr build function.
 */
template <typename Ret, typename... Args>
ModuleTestFunction<Ret, Args...> BuildExprFunction(
    udf::UdfLibrary* library,
    const std::function<node::ExprNode*(
        node::NodeManager*,
        typename std::pair<Args, node::ExprNode*>::second_type...)>&
        expr_func) {
    return ModuleFunctionBuilder()
        .library(library)
        .returns<Ret>()
        .template args<Args...>()
        .build([&](CodeGenContext* ctx) {
            node::NodeManager& nm = *ctx->node_manager();

            std::vector<node::TypeNode*> arg_types = {
                udf::DataTypeTrait<Args>::to_type_node(&nm)...};
            std::vector<int> arg_nullable = {
                udf::IsNullableTrait<Args>::value...};

            std::vector<node::ExprIdNode*> arg_exprs;
            for (size_t i = 0; i < sizeof...(Args); ++i) {
                auto arg = nm.MakeExprIdNode("arg_" + std::to_string(i));
                auto arg_type = arg_types[i];
                arg->SetOutputType(arg_type);
                arg->SetNullable(arg_nullable[i]);
                arg_exprs.push_back(arg);
            }
            node::ExprNode* body = ApplyExprFuncHelper(
                &nm, arg_exprs, std::index_sequence_for<Args...>(), expr_func);
            CHECK_TRUE(body != nullptr, ::hybridse::common::kCodegenError,
                       "Build output expr failed");

            // type infer
            vm::SchemasContext empty_context;
            node::ExprAnalysisContext expr_pass_ctx(&nm, library,
                                                    &empty_context, nullptr);
            passes::ResolveFnAndAttrs resolver(&expr_pass_ctx);
            node::ExprNode* resolved_body = nullptr;
            auto status = resolver.VisitExpr(body, &resolved_body);
            if (!status.isOK()) {
                LOG(WARNING) << "Expr resolve err: " << status;
            }

            ScopeVar* sv = ctx->GetCurrentScope()->sv();
            auto llvm_func = ctx->GetCurrentFunction();
            size_t llvm_arg_idx = 0;
            for (size_t i = 0; i < sizeof...(Args); ++i) {
                auto value = BindValueFromLlvmFunction(
                    arg_types[i], arg_nullable[i], llvm_func, &llvm_arg_idx);
                sv->AddVar(arg_exprs[i]->GetExprString(), value);
            }

            ExprIRBuilder expr_builder(ctx);
            NativeValue out;
            CHECK_STATUS(expr_builder.Build(resolved_body, &out));

            auto ret_type = udf::DataTypeTrait<Ret>::to_type_node(&nm);
            bool ret_nullable = udf::IsNullableTrait<Ret>::value;
            WriteReturnValueArgs(out, ret_type, ret_nullable, ctx, llvm_func,
                                 &llvm_arg_idx);
            ctx->GetBuilder()->CreateRetVoid();
            return Status::OK();
        });
}

template <typename Ret, typename... Args>
ModuleTestFunction<Ret, Args...> BuildExprFunction(
    const std::function<node::ExprNode*(
        node::NodeManager*,
        typename std::pair<Args, node::ExprNode*>::second_type...)>&
        expr_func) {
    return BuildExprFunction<Ret, Args...>(udf::DefaultUdfLibrary::get(),
                                           expr_func);
}

}  // namespace codegen
}  // namespace hybridse
#endif  // HYBRIDSE_SRC_CODEGEN_IR_BASE_BUILDER_TEST_H_
