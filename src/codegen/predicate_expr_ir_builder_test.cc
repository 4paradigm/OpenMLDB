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
#include "codegen/date_ir_builder.h"
#include "codegen/ir_base_builder.h"
#include "codegen/string_ir_builder.h"
#include "codegen/timestamp_ir_builder.h"
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
void UnaryPredicateExprCheck(::fesql::node::DataType left_type,
                             ::fesql::node::DataType dist_type, V1 value1,
                             R result, fesql::node::FnOperator op) {
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
    PredicateIRBuilder ir_builder(entry_block);
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
    ExitOnErr(J->addIRModule(ThreadSafeModule(std::move(m), std::move(ctx))));
    auto load_fn_jit = ExitOnErr(J->lookup("load_fn"));
    R (*decode)(V1) = (R(*)(V1))load_fn_jit.getAddress();
    R ret = decode(value1);
    ASSERT_EQ(ret, result);
}

template <class V1, class V2, class R>
void BinaryPredicateExprCheck(::fesql::node::DataType left_type,
                              ::fesql::node::DataType right_type,
                              ::fesql::node::DataType dist_type, V1 value1,
                              V2 value2, R expected,
                              fesql::node::FnOperator op) {
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

    if (left_llvm_type->isStructTy()) {
        left_llvm_type = left_llvm_type->getPointerTo();
    }

    if (right_llvm_type->isStructTy()) {
        right_llvm_type = right_llvm_type->getPointerTo();
    }
    // Create the add1 function entry and insert this entry into module M.  The
    // function will have a return type of "D" and take an argument of "S".
    Function *load_fn = Function::Create(
        FunctionType::get(
            ::llvm::Type::getVoidTy(*ctx),
            {left_llvm_type, right_llvm_type, dist_llvm_type->getPointerTo()},
            false),
        Function::ExternalLinkage, "load_fn", m.get());
    BasicBlock *entry_block = BasicBlock::Create(*ctx, "EntryBlock", load_fn);
    IRBuilder<> builder(entry_block);
    auto iter = load_fn->arg_begin();
    Argument *arg0 = &(*iter);
    iter++;
    Argument *arg1 = &(*iter);
    iter++;
    Argument *arg2 = &(*iter);

    ScopeVar scope_var;
    scope_var.Enter("fn_base");
    scope_var.AddVar("a", arg0);
    scope_var.AddVar("b", arg1);
    PredicateIRBuilder ir_builder(entry_block);
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
            break;
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
    switch (dist_type) {
        case node::kTimestamp: {
            codegen::TimestampIRBuilder timestamp_builder(m.get());
            ASSERT_TRUE(timestamp_builder.CopyFrom(builder.GetInsertBlock(),
                                                   output, arg2));
            break;
        }
        case node::kDate: {
            codegen::DateIRBuilder date_builder(m.get());
            ASSERT_TRUE(
                date_builder.CopyFrom(builder.GetInsertBlock(), output, arg2));
            break;
        }
        case node::kVarchar: {
            codegen::StringIRBuilder string_builder(m.get());
            ASSERT_TRUE(string_builder.CopyFrom(builder.GetInsertBlock(),
                                                output, arg2));
            break;
        }
        default: {
            builder.CreateStore(output, arg2);
        }
    }
    builder.CreateRet(output);
    m->print(::llvm::errs(), NULL);
    ASSERT_TRUE(ok);
    auto J = ExitOnErr(LLJITBuilder().create());
    ExitOnErr(J->addIRModule(ThreadSafeModule(std::move(m), std::move(ctx))));
    auto load_fn_jit = ExitOnErr(J->lookup("load_fn"));
    void (*decode)(V1, V2, R *) =
        (void (*)(V1, V2, R *))load_fn_jit.getAddress();
    R result;
    decode(value1, value2, &result);
    ASSERT_EQ(expected, result);
}

TEST_F(PredicateIRBuilderTest, test_eq_expr_true) {
    BinaryPredicateExprCheck<int16_t, int16_t, bool>(
        ::fesql::node::kInt16, ::fesql::node::kInt16, ::fesql::node::kBool, 1,
        1, true, ::fesql::node::kFnOpEq);

    BinaryPredicateExprCheck<int32_t, int32_t, bool>(
        ::fesql::node::kInt32, ::fesql::node::kInt32, ::fesql::node::kBool, 1,
        1, true, ::fesql::node::kFnOpEq);

    BinaryPredicateExprCheck<int64_t, int64_t, bool>(
        ::fesql::node::kInt64, ::fesql::node::kInt64, ::fesql::node::kBool, 1,
        1, true, ::fesql::node::kFnOpEq);

    BinaryPredicateExprCheck<float, float, bool>(
        ::fesql::node::kFloat, ::fesql::node::kFloat, ::fesql::node::kBool,
        1.0f, 1.0f, true, ::fesql::node::kFnOpEq);

    BinaryPredicateExprCheck<double, double, bool>(
        ::fesql::node::kDouble, ::fesql::node::kDouble, ::fesql::node::kBool,
        1.0, 1.0, true, ::fesql::node::kFnOpEq);

    BinaryPredicateExprCheck<int32_t, float, bool>(
        ::fesql::node::kInt32, ::fesql::node::kFloat, ::fesql::node::kBool, 1,
        1.0f, true, ::fesql::node::kFnOpEq);

    BinaryPredicateExprCheck<int32_t, double, bool>(
        ::fesql::node::kInt32, ::fesql::node::kDouble, ::fesql::node::kBool, 1,
        1.0, true, ::fesql::node::kFnOpEq);
}

TEST_F(PredicateIRBuilderTest, test_timestamp_compare) {
    codec::Timestamp t1(1590115420000L);
    codec::Timestamp t2(1590115420000L);
    codec::Timestamp t3(1590115430000L);
    codec::Timestamp t4(1590115410000L);
    BinaryPredicateExprCheck<codec::Timestamp *, codec::Timestamp *, bool>(
        ::fesql::node::kTimestamp, ::fesql::node::kTimestamp,
        ::fesql::node::kBool, &t1, &t2, true, ::fesql::node::kFnOpEq);

    BinaryPredicateExprCheck<codec::Timestamp *, codec::Timestamp *, bool>(
        ::fesql::node::kTimestamp, ::fesql::node::kTimestamp,
        ::fesql::node::kBool, &t1, &t3, true, ::fesql::node::kFnOpNeq);

    BinaryPredicateExprCheck<codec::Timestamp *, codec::Timestamp *, bool>(
        ::fesql::node::kTimestamp, ::fesql::node::kTimestamp,
        ::fesql::node::kBool, &t1, &t3, true, ::fesql::node::kFnOpLe);

    BinaryPredicateExprCheck<codec::Timestamp *, codec::Timestamp *, bool>(
        ::fesql::node::kTimestamp, ::fesql::node::kTimestamp,
        ::fesql::node::kBool, &t1, &t3, true, ::fesql::node::kFnOpLt);

    BinaryPredicateExprCheck<codec::Timestamp *, codec::Timestamp *, bool>(
        ::fesql::node::kTimestamp, ::fesql::node::kTimestamp,
        ::fesql::node::kBool, &t1, &t4, true, ::fesql::node::kFnOpGe);
    BinaryPredicateExprCheck<codec::Timestamp *, codec::Timestamp *, bool>(
        ::fesql::node::kTimestamp, ::fesql::node::kTimestamp,
        ::fesql::node::kBool, &t1, &t4, true, ::fesql::node::kFnOpGt);
}

TEST_F(PredicateIRBuilderTest, test_date_compare) {
    codec::Date d1(2020, 05, 27);
    codec::Date d2(2020, 05, 27);
    codec::Date d3(2020, 05, 28);
    codec::Date d4(2020, 05, 26);
    BinaryPredicateExprCheck<codec::Date *, codec::Date *, bool>(
        ::fesql::node::kDate, ::fesql::node::kDate, ::fesql::node::kBool, &d1,
        &d2, true, ::fesql::node::kFnOpEq);

    BinaryPredicateExprCheck<codec::Date *, codec::Date *, bool>(
        ::fesql::node::kDate, ::fesql::node::kDate, ::fesql::node::kBool, &d1,
        &d3, true, ::fesql::node::kFnOpNeq);

    BinaryPredicateExprCheck<codec::Date *, codec::Date *, bool>(
        ::fesql::node::kDate, ::fesql::node::kDate, ::fesql::node::kBool, &d1,
        &d3, true, ::fesql::node::kFnOpLt);

    BinaryPredicateExprCheck<codec::Date *, codec::Date *, bool>(
        ::fesql::node::kDate, ::fesql::node::kDate, ::fesql::node::kBool, &d1,
        &d3, true, ::fesql::node::kFnOpLe);

    BinaryPredicateExprCheck<codec::Date *, codec::Date *, bool>(
        ::fesql::node::kDate, ::fesql::node::kDate, ::fesql::node::kBool, &d1,
        &d2, true, ::fesql::node::kFnOpLe);

    BinaryPredicateExprCheck<codec::Date *, codec::Date *, bool>(
        ::fesql::node::kDate, ::fesql::node::kDate, ::fesql::node::kBool, &d1,
        &d2, true, ::fesql::node::kFnOpGe);
    BinaryPredicateExprCheck<codec::Date *, codec::Date *, bool>(
        ::fesql::node::kDate, ::fesql::node::kDate, ::fesql::node::kBool, &d1,
        &d4, true, ::fesql::node::kFnOpGe);
    BinaryPredicateExprCheck<codec::Date *, codec::Date *, bool>(
        ::fesql::node::kDate, ::fesql::node::kDate, ::fesql::node::kBool, &d1,
        &d4, true, ::fesql::node::kFnOpGt);
}

TEST_F(PredicateIRBuilderTest, test_eq_expr_false) {
    BinaryPredicateExprCheck<int16_t, int16_t, bool>(
        ::fesql::node::kInt16, ::fesql::node::kInt16, ::fesql::node::kBool, 1,
        2, false, ::fesql::node::kFnOpEq);

    BinaryPredicateExprCheck<int32_t, int32_t, bool>(
        ::fesql::node::kInt32, ::fesql::node::kInt32, ::fesql::node::kBool, 1,
        2, false, ::fesql::node::kFnOpEq);

    BinaryPredicateExprCheck<int64_t, int64_t, bool>(
        ::fesql::node::kInt64, ::fesql::node::kInt64, ::fesql::node::kBool, 1,
        2, false, ::fesql::node::kFnOpEq);

    BinaryPredicateExprCheck<float, float, bool>(
        ::fesql::node::kFloat, ::fesql::node::kFloat, ::fesql::node::kBool,
        1.0f, 1.1f, false, ::fesql::node::kFnOpEq);

    BinaryPredicateExprCheck<double, double, bool>(
        ::fesql::node::kDouble, ::fesql::node::kDouble, ::fesql::node::kBool,
        1.0, 1.1, false, ::fesql::node::kFnOpEq);
}

TEST_F(PredicateIRBuilderTest, test_neq_expr_true) {
    BinaryPredicateExprCheck<int16_t, int16_t, bool>(
        ::fesql::node::kInt16, ::fesql::node::kInt16, ::fesql::node::kBool, 1,
        2, true, ::fesql::node::kFnOpNeq);

    BinaryPredicateExprCheck<int32_t, int32_t, bool>(
        ::fesql::node::kInt32, ::fesql::node::kInt32, ::fesql::node::kBool, 1,
        2, true, ::fesql::node::kFnOpNeq);

    BinaryPredicateExprCheck<int64_t, int64_t, bool>(
        ::fesql::node::kInt64, ::fesql::node::kInt64, ::fesql::node::kBool, 1,
        2, true, ::fesql::node::kFnOpNeq);

    BinaryPredicateExprCheck<float, float, bool>(
        ::fesql::node::kFloat, ::fesql::node::kFloat, ::fesql::node::kBool,
        1.0f, 1.1f, true, ::fesql::node::kFnOpNeq);

    BinaryPredicateExprCheck<double, double, bool>(
        ::fesql::node::kDouble, ::fesql::node::kDouble, ::fesql::node::kBool,
        1.0, 1.1, true, ::fesql::node::kFnOpNeq);
}

TEST_F(PredicateIRBuilderTest, test_neq_expr_false) {
    BinaryPredicateExprCheck<int16_t, int16_t, bool>(
        ::fesql::node::kInt16, ::fesql::node::kInt16, ::fesql::node::kBool, 1,
        1, false, ::fesql::node::kFnOpNeq);

    BinaryPredicateExprCheck<int32_t, int32_t, bool>(
        ::fesql::node::kInt32, ::fesql::node::kInt32, ::fesql::node::kBool, 1,
        1, false, ::fesql::node::kFnOpNeq);

    BinaryPredicateExprCheck<int64_t, int64_t, bool>(
        ::fesql::node::kInt64, ::fesql::node::kInt64, ::fesql::node::kBool, 1,
        1, false, ::fesql::node::kFnOpNeq);

    BinaryPredicateExprCheck<float, float, bool>(
        ::fesql::node::kFloat, ::fesql::node::kFloat, ::fesql::node::kBool,
        1.0f, 1.0f, false, ::fesql::node::kFnOpNeq);

    BinaryPredicateExprCheck<double, double, bool>(
        ::fesql::node::kDouble, ::fesql::node::kDouble, ::fesql::node::kBool,
        1.0, 1.0, false, ::fesql::node::kFnOpNeq);

    BinaryPredicateExprCheck<int32_t, float, bool>(
        ::fesql::node::kInt32, ::fesql::node::kFloat, ::fesql::node::kBool, 1,
        1.0f, false, ::fesql::node::kFnOpNeq);

    BinaryPredicateExprCheck<int32_t, double, bool>(
        ::fesql::node::kInt32, ::fesql::node::kDouble, ::fesql::node::kBool, 1,
        1.0, false, ::fesql::node::kFnOpNeq);
}

TEST_F(PredicateIRBuilderTest, test_gt_expr_true) {
    BinaryPredicateExprCheck<int16_t, int16_t, bool>(
        ::fesql::node::kInt16, ::fesql::node::kInt16, ::fesql::node::kBool, 2,
        1, true, ::fesql::node::kFnOpGt);

    BinaryPredicateExprCheck<int32_t, int32_t, bool>(
        ::fesql::node::kInt32, ::fesql::node::kInt32, ::fesql::node::kBool, 2,
        1, true, ::fesql::node::kFnOpGt);

    BinaryPredicateExprCheck<int64_t, int64_t, bool>(
        ::fesql::node::kInt64, ::fesql::node::kInt64, ::fesql::node::kBool, 2,
        1, true, ::fesql::node::kFnOpGt);

    BinaryPredicateExprCheck<float, float, bool>(
        ::fesql::node::kFloat, ::fesql::node::kFloat, ::fesql::node::kBool,
        1.1f, 1.0f, true, ::fesql::node::kFnOpGt);

    BinaryPredicateExprCheck<double, double, bool>(
        ::fesql::node::kDouble, ::fesql::node::kDouble, ::fesql::node::kBool,
        1.1, 1.0, true, ::fesql::node::kFnOpGt);

    BinaryPredicateExprCheck<int32_t, float, bool>(
        ::fesql::node::kInt32, ::fesql::node::kFloat, ::fesql::node::kBool, 2,
        1.9f, true, ::fesql::node::kFnOpGt);

    BinaryPredicateExprCheck<int32_t, double, bool>(
        ::fesql::node::kInt32, ::fesql::node::kDouble, ::fesql::node::kBool, 2,
        1.9, true, ::fesql::node::kFnOpGt);
}

TEST_F(PredicateIRBuilderTest, test_gt_expr_false) {
    BinaryPredicateExprCheck<int16_t, int16_t, bool>(
        ::fesql::node::kInt16, ::fesql::node::kInt16, ::fesql::node::kBool, 2,
        2, false, ::fesql::node::kFnOpGt);
    BinaryPredicateExprCheck<int16_t, int16_t, bool>(
        ::fesql::node::kInt16, ::fesql::node::kInt16, ::fesql::node::kBool, 2,
        3, false, ::fesql::node::kFnOpGt);

    BinaryPredicateExprCheck<int32_t, int32_t, bool>(
        ::fesql::node::kInt32, ::fesql::node::kInt32, ::fesql::node::kBool, 2,
        2, false, ::fesql::node::kFnOpGt);
    BinaryPredicateExprCheck<int32_t, int32_t, bool>(
        ::fesql::node::kInt32, ::fesql::node::kInt32, ::fesql::node::kBool, 2,
        3, false, ::fesql::node::kFnOpGt);

    BinaryPredicateExprCheck<int64_t, int64_t, bool>(
        ::fesql::node::kInt64, ::fesql::node::kInt64, ::fesql::node::kBool, 2,
        2, false, ::fesql::node::kFnOpGt);
    BinaryPredicateExprCheck<int64_t, int64_t, bool>(
        ::fesql::node::kInt64, ::fesql::node::kInt64, ::fesql::node::kBool, 2,
        3, false, ::fesql::node::kFnOpGt);

    BinaryPredicateExprCheck<float, float, bool>(
        ::fesql::node::kFloat, ::fesql::node::kFloat, ::fesql::node::kBool,
        1.1f, 1.2f, false, ::fesql::node::kFnOpGt);

    BinaryPredicateExprCheck<double, double, bool>(
        ::fesql::node::kDouble, ::fesql::node::kDouble, ::fesql::node::kBool,
        1.1, 1.2, false, ::fesql::node::kFnOpGt);

    BinaryPredicateExprCheck<int32_t, float, bool>(
        ::fesql::node::kInt32, ::fesql::node::kFloat, ::fesql::node::kBool, 2,
        2.1f, false, ::fesql::node::kFnOpGt);

    BinaryPredicateExprCheck<int32_t, double, bool>(
        ::fesql::node::kInt32, ::fesql::node::kDouble, ::fesql::node::kBool, 2,
        2.1, false, ::fesql::node::kFnOpGt);
}
TEST_F(PredicateIRBuilderTest, test_ge_expr_true) {
    BinaryPredicateExprCheck<int16_t, int16_t, bool>(
        ::fesql::node::kInt16, ::fesql::node::kInt16, ::fesql::node::kBool, 2,
        1, true, ::fesql::node::kFnOpGe);
    BinaryPredicateExprCheck<int16_t, int16_t, bool>(
        ::fesql::node::kInt16, ::fesql::node::kInt16, ::fesql::node::kBool, 2,
        2, true, ::fesql::node::kFnOpGe);

    BinaryPredicateExprCheck<int32_t, int32_t, bool>(
        ::fesql::node::kInt32, ::fesql::node::kInt32, ::fesql::node::kBool, 2,
        1, true, ::fesql::node::kFnOpGe);
    BinaryPredicateExprCheck<int32_t, int32_t, bool>(
        ::fesql::node::kInt32, ::fesql::node::kInt32, ::fesql::node::kBool, 2,
        2, true, ::fesql::node::kFnOpGe);

    BinaryPredicateExprCheck<int64_t, int64_t, bool>(
        ::fesql::node::kInt64, ::fesql::node::kInt64, ::fesql::node::kBool, 2,
        1, true, ::fesql::node::kFnOpGe);
    BinaryPredicateExprCheck<int64_t, int64_t, bool>(
        ::fesql::node::kInt64, ::fesql::node::kInt64, ::fesql::node::kBool, 2,
        2, true, ::fesql::node::kFnOpGe);

    BinaryPredicateExprCheck<float, float, bool>(
        ::fesql::node::kFloat, ::fesql::node::kFloat, ::fesql::node::kBool,
        1.1f, 1.0f, true, ::fesql::node::kFnOpGe);
    BinaryPredicateExprCheck<float, float, bool>(
        ::fesql::node::kFloat, ::fesql::node::kFloat, ::fesql::node::kBool,
        1.1f, 1.1f, true, ::fesql::node::kFnOpGe);

    BinaryPredicateExprCheck<double, double, bool>(
        ::fesql::node::kDouble, ::fesql::node::kDouble, ::fesql::node::kBool,
        1.1, 1.0, true, ::fesql::node::kFnOpGe);

    BinaryPredicateExprCheck<double, double, bool>(
        ::fesql::node::kDouble, ::fesql::node::kDouble, ::fesql::node::kBool,
        1.1, 1.1, true, ::fesql::node::kFnOpGe);
    BinaryPredicateExprCheck<int32_t, float, bool>(
        ::fesql::node::kInt32, ::fesql::node::kFloat, ::fesql::node::kBool, 2,
        1.9f, true, ::fesql::node::kFnOpGe);
    BinaryPredicateExprCheck<int32_t, float, bool>(
        ::fesql::node::kInt32, ::fesql::node::kFloat, ::fesql::node::kBool, 2,
        2.0f, true, ::fesql::node::kFnOpGe);

    BinaryPredicateExprCheck<int32_t, double, bool>(
        ::fesql::node::kInt32, ::fesql::node::kDouble, ::fesql::node::kBool, 2,
        1.9, true, ::fesql::node::kFnOpGe);
    BinaryPredicateExprCheck<int32_t, double, bool>(
        ::fesql::node::kInt32, ::fesql::node::kDouble, ::fesql::node::kBool, 2,
        2.0, true, ::fesql::node::kFnOpGe);
}

TEST_F(PredicateIRBuilderTest, test_ge_expr_false) {
    BinaryPredicateExprCheck<int16_t, int16_t, bool>(
        ::fesql::node::kInt16, ::fesql::node::kInt16, ::fesql::node::kBool, 2,
        3, false, ::fesql::node::kFnOpGe);

    BinaryPredicateExprCheck<int32_t, int32_t, bool>(
        ::fesql::node::kInt32, ::fesql::node::kInt32, ::fesql::node::kBool, 2,
        3, false, ::fesql::node::kFnOpGe);

    BinaryPredicateExprCheck<int64_t, int64_t, bool>(
        ::fesql::node::kInt64, ::fesql::node::kInt64, ::fesql::node::kBool, 2,
        3, false, ::fesql::node::kFnOpGe);

    BinaryPredicateExprCheck<float, float, bool>(
        ::fesql::node::kFloat, ::fesql::node::kFloat, ::fesql::node::kBool,
        1.1f, 1.2f, false, ::fesql::node::kFnOpGe);

    BinaryPredicateExprCheck<double, double, bool>(
        ::fesql::node::kDouble, ::fesql::node::kDouble, ::fesql::node::kBool,
        1.1, 1.2, false, ::fesql::node::kFnOpGe);

    BinaryPredicateExprCheck<int32_t, float, bool>(
        ::fesql::node::kInt32, ::fesql::node::kFloat, ::fesql::node::kBool, 2,
        2.1f, false, ::fesql::node::kFnOpGe);
}

TEST_F(PredicateIRBuilderTest, test_lt_expr_true) {
    BinaryPredicateExprCheck<int16_t, int16_t, bool>(
        ::fesql::node::kInt16, ::fesql::node::kInt16, ::fesql::node::kBool, 2,
        3, true, ::fesql::node::kFnOpLt);

    BinaryPredicateExprCheck<int32_t, int32_t, bool>(
        ::fesql::node::kInt32, ::fesql::node::kInt32, ::fesql::node::kBool, 2,
        3, true, ::fesql::node::kFnOpLt);

    BinaryPredicateExprCheck<int64_t, int64_t, bool>(
        ::fesql::node::kInt64, ::fesql::node::kInt64, ::fesql::node::kBool, 2,
        3, true, ::fesql::node::kFnOpLt);

    BinaryPredicateExprCheck<float, float, bool>(
        ::fesql::node::kFloat, ::fesql::node::kFloat, ::fesql::node::kBool,
        1.1f, 1.2f, true, ::fesql::node::kFnOpLt);

    BinaryPredicateExprCheck<double, double, bool>(
        ::fesql::node::kDouble, ::fesql::node::kDouble, ::fesql::node::kBool,
        1.1, 1.2, true, ::fesql::node::kFnOpLt);

    BinaryPredicateExprCheck<int32_t, float, bool>(
        ::fesql::node::kInt32, ::fesql::node::kFloat, ::fesql::node::kBool, 2,
        2.1f, true, ::fesql::node::kFnOpLt);
}

TEST_F(PredicateIRBuilderTest, test_lt_expr_false) {
    BinaryPredicateExprCheck<int16_t, int16_t, bool>(
        ::fesql::node::kInt16, ::fesql::node::kInt16, ::fesql::node::kBool, 2,
        1, false, ::fesql::node::kFnOpLt);
    BinaryPredicateExprCheck<int16_t, int16_t, bool>(
        ::fesql::node::kInt16, ::fesql::node::kInt16, ::fesql::node::kBool, 2,
        2, false, ::fesql::node::kFnOpLt);

    BinaryPredicateExprCheck<int32_t, int32_t, bool>(
        ::fesql::node::kInt32, ::fesql::node::kInt32, ::fesql::node::kBool, 2,
        1, false, ::fesql::node::kFnOpLt);
    BinaryPredicateExprCheck<int32_t, int32_t, bool>(
        ::fesql::node::kInt32, ::fesql::node::kInt32, ::fesql::node::kBool, 2,
        2, false, ::fesql::node::kFnOpLt);

    BinaryPredicateExprCheck<int64_t, int64_t, bool>(
        ::fesql::node::kInt64, ::fesql::node::kInt64, ::fesql::node::kBool, 2,
        1, false, ::fesql::node::kFnOpLt);
    BinaryPredicateExprCheck<int64_t, int64_t, bool>(
        ::fesql::node::kInt64, ::fesql::node::kInt64, ::fesql::node::kBool, 2,
        2, false, ::fesql::node::kFnOpLt);

    BinaryPredicateExprCheck<float, float, bool>(
        ::fesql::node::kFloat, ::fesql::node::kFloat, ::fesql::node::kBool,
        1.1f, 1.0f, false, ::fesql::node::kFnOpLt);
    BinaryPredicateExprCheck<float, float, bool>(
        ::fesql::node::kFloat, ::fesql::node::kFloat, ::fesql::node::kBool,
        1.1f, 1.1f, false, ::fesql::node::kFnOpLt);

    BinaryPredicateExprCheck<double, double, bool>(
        ::fesql::node::kDouble, ::fesql::node::kDouble, ::fesql::node::kBool,
        1.1, 1.0, false, ::fesql::node::kFnOpLt);

    BinaryPredicateExprCheck<double, double, bool>(
        ::fesql::node::kDouble, ::fesql::node::kDouble, ::fesql::node::kBool,
        1.1, 1.1, false, ::fesql::node::kFnOpLt);
    BinaryPredicateExprCheck<int32_t, float, bool>(
        ::fesql::node::kInt32, ::fesql::node::kFloat, ::fesql::node::kBool, 2,
        1.9f, false, ::fesql::node::kFnOpLt);
    BinaryPredicateExprCheck<int32_t, float, bool>(
        ::fesql::node::kInt32, ::fesql::node::kFloat, ::fesql::node::kBool, 2,
        2.0f, false, ::fesql::node::kFnOpLt);

    BinaryPredicateExprCheck<int32_t, double, bool>(
        ::fesql::node::kInt32, ::fesql::node::kDouble, ::fesql::node::kBool, 2,
        1.9, false, ::fesql::node::kFnOpLt);
    BinaryPredicateExprCheck<int32_t, double, bool>(
        ::fesql::node::kInt32, ::fesql::node::kDouble, ::fesql::node::kBool, 2,
        2.0, false, ::fesql::node::kFnOpLt);
}

TEST_F(PredicateIRBuilderTest, test_and_expr_true) {
    BinaryPredicateExprCheck<bool, bool, bool>(
        ::fesql::node::kBool, ::fesql::node::kBool, ::fesql::node::kBool, true,
        true, true, ::fesql::node::kFnOpAnd);
}

TEST_F(PredicateIRBuilderTest, test_and_expr_false) {
    BinaryPredicateExprCheck<bool, bool, bool>(
        ::fesql::node::kBool, ::fesql::node::kBool, ::fesql::node::kBool, false,
        true, false, ::fesql::node::kFnOpAnd);

    BinaryPredicateExprCheck<bool, bool, bool>(
        ::fesql::node::kBool, ::fesql::node::kBool, ::fesql::node::kBool, false,
        false, false, ::fesql::node::kFnOpAnd);
    BinaryPredicateExprCheck<bool, bool, bool>(
        ::fesql::node::kBool, ::fesql::node::kBool, ::fesql::node::kBool, true,
        false, false, ::fesql::node::kFnOpAnd);
}

TEST_F(PredicateIRBuilderTest, test_or_expr_true) {
    BinaryPredicateExprCheck<bool, bool, bool>(
        ::fesql::node::kBool, ::fesql::node::kBool, ::fesql::node::kBool, true,
        true, true, ::fesql::node::kFnOpOr);
    BinaryPredicateExprCheck<bool, bool, bool>(
        ::fesql::node::kBool, ::fesql::node::kBool, ::fesql::node::kBool, true,
        false, true, ::fesql::node::kFnOpOr);
    BinaryPredicateExprCheck<bool, bool, bool>(
        ::fesql::node::kBool, ::fesql::node::kBool, ::fesql::node::kBool, false,
        true, true, ::fesql::node::kFnOpOr);
}

TEST_F(PredicateIRBuilderTest, test_or_expr_false) {
    BinaryPredicateExprCheck<bool, bool, bool>(
        ::fesql::node::kBool, ::fesql::node::kBool, ::fesql::node::kBool, false,
        false, false, ::fesql::node::kFnOpOr);
}

TEST_F(PredicateIRBuilderTest, test_not_expr_false) {
    UnaryPredicateExprCheck<bool, bool>(::fesql::node::kBool,
                                        ::fesql::node::kBool, true, false,
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
