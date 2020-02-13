/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * mutable_value_ir_builder_test.cc
 *
 * Author: chenjing
 * Date: 2020/2/11
 *--------------------------------------------------------------------------
 **/
#include "codegen/variable_ir_builder.h"
#include <memory>
#include <utility>
#include "codegen/ir_base_builder.h"
#include "gtest/gtest.h"
#include "llvm/ExecutionEngine/Orc/LLJIT.h"
#include "llvm/Support/TargetSelect.h"
#include "llvm/Support/raw_ostream.h"
#include "node/node_manager.h"

using namespace llvm;       // NOLINT
using namespace llvm::orc;  // NOLINT
ExitOnError ExitOnErr;
namespace fesql {
namespace codegen {
class VariableIRBuilderTest : public ::testing::Test {
 public:
    VariableIRBuilderTest() { manager_ = new node::NodeManager(); }
    ~VariableIRBuilderTest() { delete manager_; }

 protected:
    node::NodeManager *manager_;
};

template <class V1>
void MutableVariableCheck(::fesql::type::Type type, V1 value1, V1 result) {
    auto ctx = llvm::make_unique<LLVMContext>();
    auto m = make_unique<Module>("predicate_func", *ctx);
    llvm::Type *llvm_type = NULL;
    ASSERT_TRUE(::fesql::codegen::GetLLVMType(m.get(), type, &llvm_type));

    // Create the add1 function entry and insert this entry into module M.  The
    // function will have a return type of "D" and take an argument of "S".
    Function *load_fn =
        Function::Create(FunctionType::get(llvm_type, {llvm_type}, false),
                         Function::ExternalLinkage, "load_fn", m.get());

    BasicBlock *entry_block = BasicBlock::Create(*ctx, "EntryBlock", load_fn);
    IRBuilder<> builder(entry_block);
    auto iter = load_fn->arg_begin();
    Argument *arg0 = &(*iter);
    ScopeVar scope_var;
    scope_var.Enter("fn_base");
    scope_var.AddVar("a", arg0);
    VariableIRBuilder ir_builder(entry_block, &scope_var);
    llvm::Value *output;
    base::Status status;

    ASSERT_TRUE(ir_builder.StoreValue("x", arg0, false, status));
    ASSERT_TRUE(ir_builder.LoadValue("x", &output, status));
    builder.CreateRet(output);
    m->print(::llvm::errs(), NULL, true, true);
    auto J = ExitOnErr(LLJITBuilder().create());
    ExitOnErr(J->addIRModule(
        std::move(ThreadSafeModule(std::move(m), std::move(ctx)))));
    auto load_fn_jit = ExitOnErr(J->lookup("load_fn"));
    V1 (*decode)(V1) = (V1(*)(V1))load_fn_jit.getAddress();
    V1 ret = decode(value1);
    ASSERT_EQ(ret, result);
}

TEST_F(VariableIRBuilderTest, test_mutable_variable_assign) {
    MutableVariableCheck<int32_t>(::fesql::type::Type::kInt32, 999, 999);
    MutableVariableCheck<int64_t>(::fesql::type::Type::kInt64, 99999999L,
                                  99999999L);
    MutableVariableCheck<float>(::fesql::type::Type::kFloat, 0.999f, 0.999f);
    MutableVariableCheck<double>(::fesql::type::Type::kDouble, 0.999, 0.999);
    MutableVariableCheck<int16_t>(::fesql::type::Type::kInt16, 99, 99);
}

}  // namespace codegen
}  // namespace fesql
int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    InitializeNativeTarget();
    InitializeNativeTargetAsmPrinter();
    return RUN_ALL_TESTS();
}
