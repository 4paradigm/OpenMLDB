/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * predicate_expr_ir_builder_cc.h.cpp
 *
 * Author: chenjing
 * Date: 2020/1/9
 *--------------------------------------------------------------------------
 **/
#include "codegen/predicate_expr_ir_builder.h"
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
class PredicateIRBuilderTest : public ::testing::Test {
 public:
    PredicateIRBuilderTest() { manager_ = new node::NodeManager(); }
    ~PredicateIRBuilderTest() { delete manager_; }

 protected:
    node::NodeManager *manager_;
};

template <class V1, class R>
void UnaryPredicateExprCheck(::fesql::type::Type left_type,
                             ::fesql::type::Type dist_type, V1 value1, R result,
                             fesql::node::FnOperator op) {
    auto ctx = llvm::make_unique<LLVMContext>();
    auto m = make_unique<Module>("predicate_func", *ctx);

    llvm::Type *left_llvm_type = NULL;
    llvm::Type *dist_llvm_type = NULL;
    ASSERT_TRUE(
        ::fesql::codegen::GetLLVMType(m.get(), left_type, &left_llvm_type));
    ASSERT_TRUE(GetLLVMType(m.get(), dist_type, &dist_llvm_type));

    // Create the add1 function entry and insert this entry into module M.  The
    // function will have a return type of "D" and take an argument of "S".
    Function *load_fn = Function::Create(
        FunctionType::get(dist_llvm_type, {left_llvm_type}, false),
        Function::ExternalLinkage, "load_fn", m.get());
    BasicBlock *entry_block = BasicBlock::Create(*ctx, "EntryBlock", load_fn);
    IRBuilder<> builder(entry_block);
    auto iter = load_fn->arg_begin();
    Argument *arg0 = &(*iter);
    ScopeVar scope_var;
    scope_var.Enter("fn_base");
    scope_var.AddVar("a", arg0);
    PredicateIRBuilder ir_builder(entry_block, &scope_var);
    llvm::Value *output;
    base::Status status;

    bool ok;
    switch (op) {
        case fesql::node::kFnOpNot:
            ok = ir_builder.BuildNotExpr(arg0, &output, status);
            break;
        default: {
            FAIL();
        }
    }
    builder.CreateRet(output);
    m->print(::llvm::errs(), NULL);
    ASSERT_TRUE(ok);
    auto J = ExitOnErr(LLJITBuilder().create());
    ExitOnErr(J->addIRModule(
        std::move(ThreadSafeModule(std::move(m), std::move(ctx)))));
    auto load_fn_jit = ExitOnErr(J->lookup("load_fn"));
    R (*decode)(V1) = (R(*)(V1))load_fn_jit.getAddress();
    R ret = decode(value1);
    ASSERT_EQ(ret, result);
}

template <class V1, class V2, class R>
void BinaryPredicateExprCheck(::fesql::type::Type left_type,
                              ::fesql::type::Type right_type,
                              ::fesql::type::Type dist_type, V1 value1,
                              V2 value2, R result, fesql::node::FnOperator op) {
    auto ctx = llvm::make_unique<LLVMContext>();
    auto m = make_unique<Module>("predicate_func", *ctx);

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
            ok = ir_builder.BuildAndExpr(arg0, arg1, &output, status);
            break;
        case fesql::node::kFnOpOr:
            ok = ir_builder.BuildOrExpr(arg0, arg1, &output, status);
            break;
        case fesql::node::kFnOpEq:
            ok = ir_builder.BuildEqExpr(arg0, arg1, &output, status);
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
        case fesql::node::kFnOpNeq:
            ok = ir_builder.BuildNeqExpr(arg0, arg1, &output, status);
            break;
        default: {
            FAIL();
        }
    }
    builder.CreateRet(output);
    m->print(::llvm::errs(), NULL);
    ASSERT_TRUE(ok);
    auto J = ExitOnErr(LLJITBuilder().create());
    ExitOnErr(J->addIRModule(
        std::move(ThreadSafeModule(std::move(m), std::move(ctx)))));
    auto load_fn_jit = ExitOnErr(J->lookup("load_fn"));
    R (*decode)(V1, V2) = (R(*)(V1, V2))load_fn_jit.getAddress();
    R ret = decode(value1, value2);
    ASSERT_EQ(ret, result);
}

TEST_F(PredicateIRBuilderTest, test_eq_expr_true) {
    BinaryPredicateExprCheck<int16_t, int16_t, bool>(
        ::fesql::type::kInt16, ::fesql::type::kInt16, ::fesql::type::kBool, 1,
        1, true, ::fesql::node::kFnOpEq);

    BinaryPredicateExprCheck<int32_t, int32_t, bool>(
        ::fesql::type::kInt32, ::fesql::type::kInt32, ::fesql::type::kBool, 1,
        1, true, ::fesql::node::kFnOpEq);

    BinaryPredicateExprCheck<int64_t, int64_t, bool>(
        ::fesql::type::kInt64, ::fesql::type::kInt64, ::fesql::type::kBool, 1,
        1, true, ::fesql::node::kFnOpEq);

    BinaryPredicateExprCheck<float, float, bool>(
        ::fesql::type::kFloat, ::fesql::type::kFloat, ::fesql::type::kBool,
        1.0f, 1.0f, true, ::fesql::node::kFnOpEq);

    BinaryPredicateExprCheck<double, double, bool>(
        ::fesql::type::kDouble, ::fesql::type::kDouble, ::fesql::type::kBool,
        1.0, 1.0, true, ::fesql::node::kFnOpEq);

    BinaryPredicateExprCheck<int32_t, float, bool>(
        ::fesql::type::kInt32, ::fesql::type::kFloat, ::fesql::type::kBool, 1,
        1.0f, true, ::fesql::node::kFnOpEq);

    BinaryPredicateExprCheck<int32_t, double, bool>(
        ::fesql::type::kInt32, ::fesql::type::kDouble, ::fesql::type::kBool, 1,
        1.0, true, ::fesql::node::kFnOpEq);
}

TEST_F(PredicateIRBuilderTest, test_eq_expr_false) {
    BinaryPredicateExprCheck<int16_t, int16_t, bool>(
        ::fesql::type::kInt16, ::fesql::type::kInt16, ::fesql::type::kBool, 1,
        2, false, ::fesql::node::kFnOpEq);

    BinaryPredicateExprCheck<int32_t, int32_t, bool>(
        ::fesql::type::kInt32, ::fesql::type::kInt32, ::fesql::type::kBool, 1,
        2, false, ::fesql::node::kFnOpEq);

    BinaryPredicateExprCheck<int64_t, int64_t, bool>(
        ::fesql::type::kInt64, ::fesql::type::kInt64, ::fesql::type::kBool, 1,
        2, false, ::fesql::node::kFnOpEq);

    BinaryPredicateExprCheck<float, float, bool>(
        ::fesql::type::kFloat, ::fesql::type::kFloat, ::fesql::type::kBool,
        1.0f, 1.1f, false, ::fesql::node::kFnOpEq);

    BinaryPredicateExprCheck<double, double, bool>(
        ::fesql::type::kDouble, ::fesql::type::kDouble, ::fesql::type::kBool,
        1.0, 1.1, false, ::fesql::node::kFnOpEq);
}

TEST_F(PredicateIRBuilderTest, test_neq_expr_true) {
    BinaryPredicateExprCheck<int16_t, int16_t, bool>(
        ::fesql::type::kInt16, ::fesql::type::kInt16, ::fesql::type::kBool, 1,
        2, true, ::fesql::node::kFnOpNeq);

    BinaryPredicateExprCheck<int32_t, int32_t, bool>(
        ::fesql::type::kInt32, ::fesql::type::kInt32, ::fesql::type::kBool, 1,
        2, true, ::fesql::node::kFnOpNeq);

    BinaryPredicateExprCheck<int64_t, int64_t, bool>(
        ::fesql::type::kInt64, ::fesql::type::kInt64, ::fesql::type::kBool, 1,
        2, true, ::fesql::node::kFnOpNeq);

    BinaryPredicateExprCheck<float, float, bool>(
        ::fesql::type::kFloat, ::fesql::type::kFloat, ::fesql::type::kBool,
        1.0f, 1.1f, true, ::fesql::node::kFnOpNeq);

    BinaryPredicateExprCheck<double, double, bool>(
        ::fesql::type::kDouble, ::fesql::type::kDouble, ::fesql::type::kBool,
        1.0, 1.1, true, ::fesql::node::kFnOpNeq);
}

TEST_F(PredicateIRBuilderTest, test_neq_expr_false) {
    BinaryPredicateExprCheck<int16_t, int16_t, bool>(
        ::fesql::type::kInt16, ::fesql::type::kInt16, ::fesql::type::kBool, 1,
        1, false, ::fesql::node::kFnOpNeq);

    BinaryPredicateExprCheck<int32_t, int32_t, bool>(
        ::fesql::type::kInt32, ::fesql::type::kInt32, ::fesql::type::kBool, 1,
        1, false, ::fesql::node::kFnOpNeq);

    BinaryPredicateExprCheck<int64_t, int64_t, bool>(
        ::fesql::type::kInt64, ::fesql::type::kInt64, ::fesql::type::kBool, 1,
        1, false, ::fesql::node::kFnOpNeq);

    BinaryPredicateExprCheck<float, float, bool>(
        ::fesql::type::kFloat, ::fesql::type::kFloat, ::fesql::type::kBool,
        1.0f, 1.0f, false, ::fesql::node::kFnOpNeq);

    BinaryPredicateExprCheck<double, double, bool>(
        ::fesql::type::kDouble, ::fesql::type::kDouble, ::fesql::type::kBool,
        1.0, 1.0, false, ::fesql::node::kFnOpNeq);

    BinaryPredicateExprCheck<int32_t, float, bool>(
        ::fesql::type::kInt32, ::fesql::type::kFloat, ::fesql::type::kBool, 1,
        1.0f, false, ::fesql::node::kFnOpNeq);

    BinaryPredicateExprCheck<int32_t, double, bool>(
        ::fesql::type::kInt32, ::fesql::type::kDouble, ::fesql::type::kBool, 1,
        1.0, false, ::fesql::node::kFnOpNeq);
}

TEST_F(PredicateIRBuilderTest, test_gt_expr_true) {
    BinaryPredicateExprCheck<int16_t, int16_t, bool>(
        ::fesql::type::kInt16, ::fesql::type::kInt16, ::fesql::type::kBool, 2,
        1, true, ::fesql::node::kFnOpGt);

    BinaryPredicateExprCheck<int32_t, int32_t, bool>(
        ::fesql::type::kInt32, ::fesql::type::kInt32, ::fesql::type::kBool, 2,
        1, true, ::fesql::node::kFnOpGt);

    BinaryPredicateExprCheck<int64_t, int64_t, bool>(
        ::fesql::type::kInt64, ::fesql::type::kInt64, ::fesql::type::kBool, 2,
        1, true, ::fesql::node::kFnOpGt);

    BinaryPredicateExprCheck<float, float, bool>(
        ::fesql::type::kFloat, ::fesql::type::kFloat, ::fesql::type::kBool,
        1.1f, 1.0f, true, ::fesql::node::kFnOpGt);

    BinaryPredicateExprCheck<double, double, bool>(
        ::fesql::type::kDouble, ::fesql::type::kDouble, ::fesql::type::kBool,
        1.1, 1.0, true, ::fesql::node::kFnOpGt);

    BinaryPredicateExprCheck<int32_t, float, bool>(
        ::fesql::type::kInt32, ::fesql::type::kFloat, ::fesql::type::kBool, 2,
        1.9f, true, ::fesql::node::kFnOpGt);

    BinaryPredicateExprCheck<int32_t, double, bool>(
        ::fesql::type::kInt32, ::fesql::type::kDouble, ::fesql::type::kBool, 2,
        1.9, true, ::fesql::node::kFnOpGt);
}

TEST_F(PredicateIRBuilderTest, test_gt_expr_false) {
    BinaryPredicateExprCheck<int16_t, int16_t, bool>(
        ::fesql::type::kInt16, ::fesql::type::kInt16, ::fesql::type::kBool, 2,
        2, false, ::fesql::node::kFnOpGt);
    BinaryPredicateExprCheck<int16_t, int16_t, bool>(
        ::fesql::type::kInt16, ::fesql::type::kInt16, ::fesql::type::kBool, 2,
        3, false, ::fesql::node::kFnOpGt);

    BinaryPredicateExprCheck<int32_t, int32_t, bool>(
        ::fesql::type::kInt32, ::fesql::type::kInt32, ::fesql::type::kBool, 2,
        2, false, ::fesql::node::kFnOpGt);
    BinaryPredicateExprCheck<int32_t, int32_t, bool>(
        ::fesql::type::kInt32, ::fesql::type::kInt32, ::fesql::type::kBool, 2,
        3, false, ::fesql::node::kFnOpGt);

    BinaryPredicateExprCheck<int64_t, int64_t, bool>(
        ::fesql::type::kInt64, ::fesql::type::kInt64, ::fesql::type::kBool, 2,
        2, false, ::fesql::node::kFnOpGt);
    BinaryPredicateExprCheck<int64_t, int64_t, bool>(
        ::fesql::type::kInt64, ::fesql::type::kInt64, ::fesql::type::kBool, 2,
        3, false, ::fesql::node::kFnOpGt);

    BinaryPredicateExprCheck<float, float, bool>(
        ::fesql::type::kFloat, ::fesql::type::kFloat, ::fesql::type::kBool,
        1.1f, 1.2f, false, ::fesql::node::kFnOpGt);

    BinaryPredicateExprCheck<double, double, bool>(
        ::fesql::type::kDouble, ::fesql::type::kDouble, ::fesql::type::kBool,
        1.1, 1.2, false, ::fesql::node::kFnOpGt);

    BinaryPredicateExprCheck<int32_t, float, bool>(
        ::fesql::type::kInt32, ::fesql::type::kFloat, ::fesql::type::kBool, 2,
        2.1f, false, ::fesql::node::kFnOpGt);

    BinaryPredicateExprCheck<int32_t, double, bool>(
        ::fesql::type::kInt32, ::fesql::type::kDouble, ::fesql::type::kBool, 2,
        2.1, false, ::fesql::node::kFnOpGt);
}
TEST_F(PredicateIRBuilderTest, test_ge_expr_true) {
    BinaryPredicateExprCheck<int16_t, int16_t, bool>(
        ::fesql::type::kInt16, ::fesql::type::kInt16, ::fesql::type::kBool, 2,
        1, true, ::fesql::node::kFnOpGe);
    BinaryPredicateExprCheck<int16_t, int16_t, bool>(
        ::fesql::type::kInt16, ::fesql::type::kInt16, ::fesql::type::kBool, 2,
        2, true, ::fesql::node::kFnOpGe);

    BinaryPredicateExprCheck<int32_t, int32_t, bool>(
        ::fesql::type::kInt32, ::fesql::type::kInt32, ::fesql::type::kBool, 2,
        1, true, ::fesql::node::kFnOpGe);
    BinaryPredicateExprCheck<int32_t, int32_t, bool>(
        ::fesql::type::kInt32, ::fesql::type::kInt32, ::fesql::type::kBool, 2,
        2, true, ::fesql::node::kFnOpGe);

    BinaryPredicateExprCheck<int64_t, int64_t, bool>(
        ::fesql::type::kInt64, ::fesql::type::kInt64, ::fesql::type::kBool, 2,
        1, true, ::fesql::node::kFnOpGe);
    BinaryPredicateExprCheck<int64_t, int64_t, bool>(
        ::fesql::type::kInt64, ::fesql::type::kInt64, ::fesql::type::kBool, 2,
        2, true, ::fesql::node::kFnOpGe);

    BinaryPredicateExprCheck<float, float, bool>(
        ::fesql::type::kFloat, ::fesql::type::kFloat, ::fesql::type::kBool,
        1.1f, 1.0f, true, ::fesql::node::kFnOpGe);
    BinaryPredicateExprCheck<float, float, bool>(
        ::fesql::type::kFloat, ::fesql::type::kFloat, ::fesql::type::kBool,
        1.1f, 1.1f, true, ::fesql::node::kFnOpGe);

    BinaryPredicateExprCheck<double, double, bool>(
        ::fesql::type::kDouble, ::fesql::type::kDouble, ::fesql::type::kBool,
        1.1, 1.0, true, ::fesql::node::kFnOpGe);

    BinaryPredicateExprCheck<double, double, bool>(
        ::fesql::type::kDouble, ::fesql::type::kDouble, ::fesql::type::kBool,
        1.1, 1.1, true, ::fesql::node::kFnOpGe);
    BinaryPredicateExprCheck<int32_t, float, bool>(
        ::fesql::type::kInt32, ::fesql::type::kFloat, ::fesql::type::kBool, 2,
        1.9f, true, ::fesql::node::kFnOpGe);
    BinaryPredicateExprCheck<int32_t, float, bool>(
        ::fesql::type::kInt32, ::fesql::type::kFloat, ::fesql::type::kBool, 2,
        2.0f, true, ::fesql::node::kFnOpGe);

    BinaryPredicateExprCheck<int32_t, double, bool>(
        ::fesql::type::kInt32, ::fesql::type::kDouble, ::fesql::type::kBool, 2,
        1.9, true, ::fesql::node::kFnOpGe);
    BinaryPredicateExprCheck<int32_t, double, bool>(
        ::fesql::type::kInt32, ::fesql::type::kDouble, ::fesql::type::kBool, 2,
        2.0, true, ::fesql::node::kFnOpGe);
}

TEST_F(PredicateIRBuilderTest, test_ge_expr_false) {
    BinaryPredicateExprCheck<int16_t, int16_t, bool>(
        ::fesql::type::kInt16, ::fesql::type::kInt16, ::fesql::type::kBool, 2,
        3, false, ::fesql::node::kFnOpGe);

    BinaryPredicateExprCheck<int32_t, int32_t, bool>(
        ::fesql::type::kInt32, ::fesql::type::kInt32, ::fesql::type::kBool, 2,
        3, false, ::fesql::node::kFnOpGe);

    BinaryPredicateExprCheck<int64_t, int64_t, bool>(
        ::fesql::type::kInt64, ::fesql::type::kInt64, ::fesql::type::kBool, 2,
        3, false, ::fesql::node::kFnOpGe);

    BinaryPredicateExprCheck<float, float, bool>(
        ::fesql::type::kFloat, ::fesql::type::kFloat, ::fesql::type::kBool,
        1.1f, 1.2f, false, ::fesql::node::kFnOpGe);

    BinaryPredicateExprCheck<double, double, bool>(
        ::fesql::type::kDouble, ::fesql::type::kDouble, ::fesql::type::kBool,
        1.1, 1.2, false, ::fesql::node::kFnOpGe);

    BinaryPredicateExprCheck<int32_t, float, bool>(
        ::fesql::type::kInt32, ::fesql::type::kFloat, ::fesql::type::kBool, 2,
        2.1f, false, ::fesql::node::kFnOpGe);
}

TEST_F(PredicateIRBuilderTest, test_lt_expr_true) {
    BinaryPredicateExprCheck<int16_t, int16_t, bool>(
        ::fesql::type::kInt16, ::fesql::type::kInt16, ::fesql::type::kBool, 2,
        3, true, ::fesql::node::kFnOpLt);

    BinaryPredicateExprCheck<int32_t, int32_t, bool>(
        ::fesql::type::kInt32, ::fesql::type::kInt32, ::fesql::type::kBool, 2,
        3, true, ::fesql::node::kFnOpLt);

    BinaryPredicateExprCheck<int64_t, int64_t, bool>(
        ::fesql::type::kInt64, ::fesql::type::kInt64, ::fesql::type::kBool, 2,
        3, true, ::fesql::node::kFnOpLt);

    BinaryPredicateExprCheck<float, float, bool>(
        ::fesql::type::kFloat, ::fesql::type::kFloat, ::fesql::type::kBool,
        1.1f, 1.2f, true, ::fesql::node::kFnOpLt);

    BinaryPredicateExprCheck<double, double, bool>(
        ::fesql::type::kDouble, ::fesql::type::kDouble, ::fesql::type::kBool,
        1.1, 1.2, true, ::fesql::node::kFnOpLt);

    BinaryPredicateExprCheck<int32_t, float, bool>(
        ::fesql::type::kInt32, ::fesql::type::kFloat, ::fesql::type::kBool, 2,
        2.1f, true, ::fesql::node::kFnOpLt);
}

TEST_F(PredicateIRBuilderTest, test_lt_expr_false) {
    BinaryPredicateExprCheck<int16_t, int16_t, bool>(
        ::fesql::type::kInt16, ::fesql::type::kInt16, ::fesql::type::kBool, 2,
        1, false, ::fesql::node::kFnOpLt);
    BinaryPredicateExprCheck<int16_t, int16_t, bool>(
        ::fesql::type::kInt16, ::fesql::type::kInt16, ::fesql::type::kBool, 2,
        2, false, ::fesql::node::kFnOpLt);

    BinaryPredicateExprCheck<int32_t, int32_t, bool>(
        ::fesql::type::kInt32, ::fesql::type::kInt32, ::fesql::type::kBool, 2,
        1, false, ::fesql::node::kFnOpLt);
    BinaryPredicateExprCheck<int32_t, int32_t, bool>(
        ::fesql::type::kInt32, ::fesql::type::kInt32, ::fesql::type::kBool, 2,
        2, false, ::fesql::node::kFnOpLt);

    BinaryPredicateExprCheck<int64_t, int64_t, bool>(
        ::fesql::type::kInt64, ::fesql::type::kInt64, ::fesql::type::kBool, 2,
        1, false, ::fesql::node::kFnOpLt);
    BinaryPredicateExprCheck<int64_t, int64_t, bool>(
        ::fesql::type::kInt64, ::fesql::type::kInt64, ::fesql::type::kBool, 2,
        2, false, ::fesql::node::kFnOpLt);

    BinaryPredicateExprCheck<float, float, bool>(
        ::fesql::type::kFloat, ::fesql::type::kFloat, ::fesql::type::kBool,
        1.1f, 1.0f, false, ::fesql::node::kFnOpLt);
    BinaryPredicateExprCheck<float, float, bool>(
        ::fesql::type::kFloat, ::fesql::type::kFloat, ::fesql::type::kBool,
        1.1f, 1.1f, false, ::fesql::node::kFnOpLt);

    BinaryPredicateExprCheck<double, double, bool>(
        ::fesql::type::kDouble, ::fesql::type::kDouble, ::fesql::type::kBool,
        1.1, 1.0, false, ::fesql::node::kFnOpLt);

    BinaryPredicateExprCheck<double, double, bool>(
        ::fesql::type::kDouble, ::fesql::type::kDouble, ::fesql::type::kBool,
        1.1, 1.1, false, ::fesql::node::kFnOpLt);
    BinaryPredicateExprCheck<int32_t, float, bool>(
        ::fesql::type::kInt32, ::fesql::type::kFloat, ::fesql::type::kBool, 2,
        1.9f, false, ::fesql::node::kFnOpLt);
    BinaryPredicateExprCheck<int32_t, float, bool>(
        ::fesql::type::kInt32, ::fesql::type::kFloat, ::fesql::type::kBool, 2,
        2.0f, false, ::fesql::node::kFnOpLt);

    BinaryPredicateExprCheck<int32_t, double, bool>(
        ::fesql::type::kInt32, ::fesql::type::kDouble, ::fesql::type::kBool, 2,
        1.9, false, ::fesql::node::kFnOpLt);
    BinaryPredicateExprCheck<int32_t, double, bool>(
        ::fesql::type::kInt32, ::fesql::type::kDouble, ::fesql::type::kBool, 2,
        2.0, false, ::fesql::node::kFnOpLt);
}

TEST_F(PredicateIRBuilderTest, test_and_expr_true) {
    BinaryPredicateExprCheck<bool, bool, bool>(
        ::fesql::type::kBool, ::fesql::type::kBool, ::fesql::type::kBool, true,
        true, true, ::fesql::node::kFnOpAnd);
}

TEST_F(PredicateIRBuilderTest, test_and_expr_false) {
    BinaryPredicateExprCheck<bool, bool, bool>(
        ::fesql::type::kBool, ::fesql::type::kBool, ::fesql::type::kBool, false,
        true, false, ::fesql::node::kFnOpAnd);

    BinaryPredicateExprCheck<bool, bool, bool>(
        ::fesql::type::kBool, ::fesql::type::kBool, ::fesql::type::kBool, false,
        false, false, ::fesql::node::kFnOpAnd);
    BinaryPredicateExprCheck<bool, bool, bool>(
        ::fesql::type::kBool, ::fesql::type::kBool, ::fesql::type::kBool, true,
        false, false, ::fesql::node::kFnOpAnd);
}

TEST_F(PredicateIRBuilderTest, test_or_expr_true) {
    BinaryPredicateExprCheck<bool, bool, bool>(
        ::fesql::type::kBool, ::fesql::type::kBool, ::fesql::type::kBool, true,
        true, true, ::fesql::node::kFnOpOr);
    BinaryPredicateExprCheck<bool, bool, bool>(
        ::fesql::type::kBool, ::fesql::type::kBool, ::fesql::type::kBool, true,
        false, true, ::fesql::node::kFnOpOr);
    BinaryPredicateExprCheck<bool, bool, bool>(
        ::fesql::type::kBool, ::fesql::type::kBool, ::fesql::type::kBool, false,
        true, true, ::fesql::node::kFnOpOr);
}

TEST_F(PredicateIRBuilderTest, test_or_expr_false) {
    BinaryPredicateExprCheck<bool, bool, bool>(
        ::fesql::type::kBool, ::fesql::type::kBool, ::fesql::type::kBool, false,
        false, false, ::fesql::node::kFnOpOr);
}

TEST_F(PredicateIRBuilderTest, test_not_expr_false) {
    UnaryPredicateExprCheck<bool, bool>(::fesql::type::kBool,
                                        ::fesql::type::kBool, true, false,
                                        ::fesql::node::kFnOpNot);
}

}  // namespace codegen
}  // namespace fesql
int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    InitializeNativeTarget();
    InitializeNativeTargetAsmPrinter();
    return RUN_ALL_TESTS();
}
