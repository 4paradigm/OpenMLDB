/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * predicate_expr_ir_builder_cc.h.cpp
 *
 * Author: chenjing
 * Date: 2020/1/9
 *--------------------------------------------------------------------------
 **/
#include "codegen/predicate_expr_ir_builder.h"
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
class PredicateIRBuilderTest : public ::testing::Test {
 public:
    PredicateIRBuilderTest() { manager_ = new node::NodeManager(); }
    ~PredicateIRBuilderTest() { delete manager_; }

 protected:
    node::NodeManager *manager_;
};

template <class V1, class V2, class R>
void BinaryPredicateExprCheck(::fesql::type::Type left_type,
                               ::fesql::type::Type right_type,
                               ::fesql::type::Type dist_type, V1 value1,
                               V2 value2, R result,
                               fesql::node::FnOperator op) {
    auto ctx = llvm::make_unique<LLVMContext>();
    auto m = make_unique<Module>("arithmetic_func", *ctx);

    llvm::Type *left_llvm_type = NULL;
    llvm::Type *right_llvm_type = NULL;
    llvm::Type *dist_llvm_type = NULL;
    ASSERT_TRUE(
        ::fesql::codegen::GetLLVMType(m.get(), left_type, &left_llvm_type));
    ASSERT_TRUE(
        ::fesql::codegen::GetLLVMType(m.get(), right_type, &right_llvm_type));
    ASSERT_TRUE(GetLLVMType(m.get(), dist_type, &dist_llvm_type));

    // Create the add1 function entry and insert this entry into module M.  The
    // function will have a return type of "D" and take an argument of "S".
    Function *load_fn = Function::Create(
        FunctionType::get(dist_llvm_type, {left_llvm_type, right_llvm_type},
                          false),
        Function::ExternalLinkage, "load_fn", m.get());
    BasicBlock *entry_block = BasicBlock::Create(*ctx, "EntryBlock", load_fn);
    IRBuilder<> builder(entry_block);
    auto iter = load_fn->arg_begin();
    Argument *arg0 = &(*iter);
    iter++;
    Argument *arg1 = &(*iter);

    ScopeVar scope_var;
    scope_var.Enter("fn_base");
    scope_var.AddVar("a", arg0);
    scope_var.AddVar("b", arg1);
    PredicateIRBuilder ir_builder(entry_block, &scope_var);
    llvm::Value *output;
    base::Status status;

    bool ok;
    switch (op) {
        case fesql::node::kFnOpAnd:
            ok =
                ir_builder.BuildAndExpr(arg0, arg1, &output, status);
            break;
        case fesql::node::kFnOpOr:
            ok =
                ir_builder.BuildOrExpr(arg0, arg1, &output, status);
            break;
        case fesql::node::kFnOpEq:
            ok = ir_builder.BuildEqExpr(arg0, arg1, &output,status);
            break;
        case fesql::node::kFnOpLt:
            ok = ir_builder.BuildLtExpr(arg0, arg1, &output, status);
            break;
        case fesql::node::kFnOpLe:
            ok = ir_builder.BuildLeExpr(arg0, arg1, &output, status);
        case fesql::node::kFnOpGt:
            ok = ir_builder.BuildGtExpr(arg0, arg1, &output, status);
            break;
        case fesql::node::kFnOpGe:
            ok = ir_builder.BuildGeExpr(arg0, arg1, &output, status);
            break;
        default: {
            FAIL();
        }
    }
//    ASSERT_TRUE(ok);
//    builder.CreateRet(output);
//    m->print(::llvm::errs(), NULL);
//    auto J = ExitOnErr(LLJITBuilder().create());
//    ExitOnErr(J->addIRModule(
//        std::move(ThreadSafeModule(std::move(m), std::move(ctx)))));
//    auto load_fn_jit = ExitOnErr(J->lookup("load_fn"));
//    R (*decode)(V1, V2) = (R(*)(V1, V2))load_fn_jit.getAddress();
//    R ret = decode(value1, value2);
//    ASSERT_EQ(ret, result);
}
TEST_F(PredicateIRBuilderTest, test_eq_expr) {
    BinaryPredicateExprCheck<int16_t, int16_t, bool>(
        ::fesql::type::kInt16, ::fesql::type::kInt16, ::fesql::type::kBool, 1,
        1, true, ::fesql::node::kFnOpEq);

}

}  // namespace codegen
}  // namespace fesql
int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    InitializeNativeTarget();
    InitializeNativeTargetAsmPrinter();
    return RUN_ALL_TESTS();
}
