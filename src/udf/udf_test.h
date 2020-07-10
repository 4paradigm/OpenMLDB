/*-------------------------------------------------------------------------
 * Copyright (C) 2019, 4paradigm
 * udf_test.h
 *
 * Author: chenjing
 * Date: 2019/11/26
 *--------------------------------------------------------------------------
 **/
#ifndef SRC_UDF_UDF_TEST_H_
#define SRC_UDF_UDF_TEST_H_

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "llvm/Support/InitLLVM.h"
#include "llvm/Support/TargetSelect.h"

#include "codegen/expr_ir_builder.h"
#include "codegen/ir_base_builder.h"
#include "codegen/type_ir_builder.h"
#include "udf/default_udf_library.h"
#include "udf/literal_traits.h"
#include "udf/udf.h"
#include "vm/jit.h"
#include "vm/sql_compiler.h"

namespace fesql {
namespace udf {

template <typename Ret, typename... Args>
class UDFFunctionBuilderWithFullInfo;

template <typename Ret, typename... Args>
class UDFFunctionInst {
 public:
    Ret operator()(Args... args) {
        auto fn = reinterpret_cast<Ret (*)(Args...)>(fn_ptr);
        return fn(args...);
    }

    bool valid() const { return jit != nullptr && fn_ptr != nullptr; }

    UDFFunctionInst(UDFFunctionInst&& inst)
        : jit(std::move(inst.jit)), fn_ptr(inst.fn_ptr) {}

 private:
    friend class UDFFunctionBuilderWithFullInfo<Ret, Args...>;
    UDFFunctionInst(const std::string& fn_name, UDFLibrary* library,
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

        ExitOnErr(jit->addIRModule(::llvm::orc::ThreadSafeModule(
            std::move(module), std::move(llvm_ctx))));
        auto load_fn = ExitOnErr(jit->lookup(fn_name));
        fn_ptr = reinterpret_cast<void*>(load_fn.getAddress());
    }

    UDFFunctionInst() {}

    std::unique_ptr<vm::FeSQLJIT> jit = nullptr;
    void* fn_ptr = nullptr;
};

struct UDFFunctionBuilderState {
    std::string name;
    std::vector<node::TypeNode*> arg_types;
    node::TypeNode* ret_type;
    node::NodeManager nm;
    DefaultUDFLibrary library;
};

typedef std::unique_ptr<UDFFunctionBuilderState> BuilderStatePtr;

template <typename Ret, typename... Args>
class UDFFunctionBuilderWithFullInfo {
 public:
    explicit UDFFunctionBuilderWithFullInfo(BuilderStatePtr&& state)
        : state(std::move(state)) {}

    UDFFunctionInst<Ret, Args...> build();

 private:
    BuilderStatePtr state;
};

template <typename... Args>
class UDFFunctionBuilderWithArgs {
 public:
    explicit UDFFunctionBuilderWithArgs(BuilderStatePtr&& state)
        : state(std::move(state)) {}

    template <typename Ret>
    auto returns() {
        state->ret_type = DataTypeTrait<Ret>::to_type_node(&(state->nm));
        return UDFFunctionBuilderWithFullInfo<Ret, Args...>(std::move(state));
    }

 private:
    BuilderStatePtr state;
};

template <typename Ret>
class UDFFunctionBuilderWithRet {
 public:
    template <typename... Args>
    auto args() {
        state->arg_types = {DataTypeTrait<Args>::to_type_node(&(state->nm))...};
        return UDFFunctionBuilderWithFullInfo<Ret, Args...>(std::move(state));
    }

 private:
    BuilderStatePtr state;
};

class UDFFunctionBuilder {
 public:
    explicit UDFFunctionBuilder(const std::string& name)
        : state(new UDFFunctionBuilderState()) {
        state->name = name;
    }

    template <typename... Args>
    auto args() {
        state->arg_types = {DataTypeTrait<Args>::to_type_node(&(state->nm))...};
        return UDFFunctionBuilderWithArgs<Args...>(std::move(state));
    }

    template <typename Ret>
    auto returns() {
        state->ret_type = DataTypeTrait<Ret>::to_type_node(&(state->nm));
        return UDFFunctionBuilderWithRet<Ret>(std::move(state));
    }

 private:
    BuilderStatePtr state;
};

template <typename Ret, typename... Args>
UDFFunctionInst<Ret, Args...>
UDFFunctionBuilderWithFullInfo<Ret, Args...>::build() {
    auto nm = &(state->nm);
    auto& arg_types = state->arg_types;
    auto arg_list = reinterpret_cast<node::ExprListNode*>(nm->MakeExprList());
    for (size_t i = 0; i < arg_types.size(); ++i) {
        std::string arg_name = "arg_" + std::to_string(i);
        auto expr = nm->MakeExprIdNode(arg_name);
        expr->SetOutputType(arg_types[i]);
        arg_list->AddChild(expr);
    }
    node::ExprNode* expr = nullptr;
    node::ExprAnalysisContext analysis_ctx(nm, nullptr, true);
    auto status = state->library.Transform(state->name, arg_list, nullptr,
                                           &analysis_ctx, &expr);
    if (!status.isOK() || expr == nullptr) {
        LOG(WARNING) << status.msg;
        return UDFFunctionInst<Ret, Args...>();
    }

    status = expr->InferAttr(&analysis_ctx);
    if (!status.isOK()) {
        LOG(WARNING) << "Fail to infer type for udf expr: "
                     << expr->GetExprString();
        return UDFFunctionInst<Ret, Args...>();
    }

    auto llvm_ctx =
        std::unique_ptr<::llvm::LLVMContext>(new llvm::LLVMContext());
    auto module =
        std::unique_ptr<::llvm::Module>(new ::llvm::Module("Test", *llvm_ctx));
    ::fesql::udf::RegisterUDFToModule(module.get());

    std::vector<::llvm::Type*> llvm_arg_types;
    std::vector<int> is_struct_arg;
    for (auto type_node : arg_types) {
        ::llvm::Type* llvm_ty = nullptr;
        if (!codegen::GetLLVMType(module.get(), type_node, &llvm_ty)) {
            LOG(WARNING) << "Fail for arg type " << type_node->GetName();
            return UDFFunctionInst<Ret, Args...>();
        }
        if (codegen::TypeIRBuilder::IsStructPtr(llvm_ty)) {
            is_struct_arg.push_back(1);
            llvm_ty = reinterpret_cast<::llvm::PointerType*>(llvm_ty)
                          ->getElementType();
        } else {
            is_struct_arg.push_back(0);
        }
        llvm_arg_types.push_back(llvm_ty);
    }

    bool is_struct_ret = false;
    ::llvm::Type* llvm_ret_ty = nullptr;
    if (!codegen::GetLLVMType(module.get(), expr->GetOutputType(),
                              &llvm_ret_ty)) {
        return UDFFunctionInst<Ret, Args...>();
    }
    if (codegen::TypeIRBuilder::IsStructPtr(llvm_ret_ty)) {
        is_struct_ret = true;
        llvm_ret_ty = reinterpret_cast<::llvm::PointerType*>(llvm_ret_ty)
                          ->getElementType();
    }

    std::string fn_name = "__udf_wrapper_" + state->name;
    for (auto type_node : arg_types) {
        fn_name.append(".").append(type_node->GetName());
    }
    auto function_ty =
        ::llvm::FunctionType::get(llvm_ret_ty, llvm_arg_types, false);
    auto function = ::llvm::Function::Create(
        function_ty, llvm::Function::ExternalLinkage, fn_name, module.get());
    auto block = ::llvm::BasicBlock::Create(*llvm_ctx, "entry", function);

    ::llvm::IRBuilder<> builder(block);

    // bind args
    codegen::ScopeVar sv;
    sv.Enter(fn_name);
    for (size_t i = 0; i < arg_types.size(); ++i) {
        std::string arg_name = "arg_" + std::to_string(i);
        ::llvm::Value* raw_arg = function->arg_begin() + i;
        if (is_struct_arg[i] == 1) {
            auto alloca = builder.CreateAlloca(raw_arg->getType());
            builder.CreateStore(raw_arg, alloca);
            raw_arg = alloca;
        }
        sv.AddVar(arg_name, codegen::NativeValue::Create(raw_arg));
    }

    vm::SchemaSourceList empty_schema;
    vm::SchemasContext schemas_context(empty_schema);

    codegen::NativeValue result;
    codegen::ExprIRBuilder expr_builder(block, &sv, &schemas_context, false,
                                        module.get());
    if (!expr_builder.Build(expr, &result, status)) {
        LOG(WARNING) << status.msg;
        return UDFFunctionInst<Ret, Args...>();
    }
    if (is_struct_ret) {
        builder.CreateRet(builder.CreateLoad(result.GetValue(&builder)));
    } else {
        builder.CreateRet(result.GetValue(&builder));
    }

    module->print(::llvm::errs(), NULL);

    return UDFFunctionInst<Ret, Args...>(
        fn_name, &state->library, std::move(module), std::move(llvm_ctx));
}

}  // namespace udf
}  // namespace fesql

#endif  // SRC_UDF_UDF_TEST_H_
