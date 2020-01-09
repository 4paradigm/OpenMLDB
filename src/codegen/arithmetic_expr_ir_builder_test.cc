/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * arithmetic_expr_ir_builder_test.cc
 *
 * Author: chenjing
 * Date: 2020/1/8
 *--------------------------------------------------------------------------
 **/
#include "codegen/arithmetic_expr_ir_builder.h"
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
class ArithmeticIRBuilderTest : public ::testing::Test {
 public:
    ArithmeticIRBuilderTest() { manager_ = new node::NodeManager(); }
    ~ArithmeticIRBuilderTest() { delete manager_; }

 protected:
    node::NodeManager *manager_;
};

template <class V1, class V2, class R>
void BinaryArithmeticExprCheck(::fesql::type::Type left_type,
                               ::fesql::type::Type right_type,
                               ::fesql::type::Type dist_type, V1 value1,
                               V2 value2, R result,
                               fesql::node::FnOperator op) {
    // Create an LLJIT instance.
    // Create an LLJIT instance.
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
    ArithmeticIRBuilder arithmetic_ir_builder(entry_block, &scope_var);
    llvm::Value *output;
    base::Status status;

    bool ok;
    switch (op) {
        case fesql::node::kFnOpAdd:
            ok =
                arithmetic_ir_builder.BuildAddExpr(arg0, arg1, &output, status);
            break;
        case fesql::node::kFnOpMinus:
            ok =
                arithmetic_ir_builder.BuildSubExpr(arg0, arg1, &output, status);
            break;
        case fesql::node::kFnOpMulti:
            ok = arithmetic_ir_builder.BuildMultiExpr(arg0, arg1, &output,status);
            break;
        case fesql::node::kFnOpDiv:
            ok = arithmetic_ir_builder.BuildFDivExpr(arg0, arg1, &output, status);
            break;
        case fesql::node::kFnOpMod:
            ok = arithmetic_ir_builder.BuildModExpr(arg0, arg1, &output, status);
            break;
            break;
        case fesql::node::kFnOpAnd:
        case fesql::node::kFnOpOr:
        default: {
            FAIL();
        }
    }
    ASSERT_TRUE(ok);
    builder.CreateRet(output);
    m->print(::llvm::errs(), NULL);
    auto J = ExitOnErr(LLJITBuilder().create());
    ExitOnErr(J->addIRModule(
        std::move(ThreadSafeModule(std::move(m), std::move(ctx)))));
    auto load_fn_jit = ExitOnErr(J->lookup("load_fn"));
    R (*decode)(V1, V2) = (R(*)(V1, V2))load_fn_jit.getAddress();
    R ret = decode(value1, value2);
    ASSERT_EQ(ret, result);
}
TEST_F(ArithmeticIRBuilderTest, test_add_int16_x_expr) {
    BinaryArithmeticExprCheck<int16_t, int16_t, int16_t>(
        ::fesql::type::kInt16, ::fesql::type::kInt16, ::fesql::type::kInt16, 1,
        1, 2, ::fesql::node::kFnOpAdd);

    BinaryArithmeticExprCheck<int16_t, int32_t, int32_t>(
        ::fesql::type::kInt16, ::fesql::type::kInt32, ::fesql::type::kInt32, 1,
        1, 2, ::fesql::node::kFnOpAdd);

    BinaryArithmeticExprCheck<int16_t, int64_t, int64_t>(
        ::fesql::type::kInt16, ::fesql::type::kInt64, ::fesql::type::kInt64, 1,
        8000000000L, 8000000001L, ::fesql::node::kFnOpAdd);

    BinaryArithmeticExprCheck<int16_t, float, float>(
        ::fesql::type::kInt16, ::fesql::type::kFloat, ::fesql::type::kFloat, 1,
        12345678.5f, 12345678.5f + 1.0f, ::fesql::node::kFnOpAdd);
    BinaryArithmeticExprCheck<int16_t, double, double>(
        ::fesql::type::kInt16, ::fesql::type::kDouble, ::fesql::type::kDouble,
        1, 12345678.5, 12345678.5 + 1.0, ::fesql::node::kFnOpAdd);
}

TEST_F(ArithmeticIRBuilderTest, test_add_int32_x_expr) {
    BinaryArithmeticExprCheck<int32_t, int16_t, int32_t>(
        ::fesql::type::kInt32, ::fesql::type::kInt16, ::fesql::type::kInt32, 1,
        1, 2, ::fesql::node::kFnOpAdd);

    BinaryArithmeticExprCheck<int32_t, int32_t, int32_t>(
        ::fesql::type::kInt32, ::fesql::type::kInt32, ::fesql::type::kInt32, 1,
        1, 2, ::fesql::node::kFnOpAdd);

    BinaryArithmeticExprCheck<int32_t, int64_t, int64_t>(
        ::fesql::type::kInt32, ::fesql::type::kInt64, ::fesql::type::kInt64, 1,
        8000000000L, 8000000001L, ::fesql::node::kFnOpAdd);

    BinaryArithmeticExprCheck<int32_t, float, float>(
        ::fesql::type::kInt32, ::fesql::type::kFloat, ::fesql::type::kFloat, 1,
        12345678.5f, 12345678.5f + 1.0f, ::fesql::node::kFnOpAdd);
    BinaryArithmeticExprCheck<int32_t, double, double>(
        ::fesql::type::kInt32, ::fesql::type::kDouble, ::fesql::type::kDouble,
        1, 12345678.5, 12345678.5 + 1.0, ::fesql::node::kFnOpAdd);
}

TEST_F(ArithmeticIRBuilderTest, test_add_int64_x_expr) {
    BinaryArithmeticExprCheck<int64_t, int16_t, int64_t>(
        ::fesql::type::kInt64, ::fesql::type::kInt16, ::fesql::type::kInt64,
        8000000000L, 1, 8000000001L, ::fesql::node::kFnOpAdd);

    BinaryArithmeticExprCheck<int64_t, int32_t, int64_t>(
        ::fesql::type::kInt64, ::fesql::type::kInt32, ::fesql::type::kInt64,
        8000000000L, 1, 8000000001L, ::fesql::node::kFnOpAdd);

    BinaryArithmeticExprCheck<int64_t, int64_t, int64_t>(
        ::fesql::type::kInt64, ::fesql::type::kInt64, ::fesql::type::kInt64, 1L,
        8000000000L, 8000000001L, ::fesql::node::kFnOpAdd);

    BinaryArithmeticExprCheck<int64_t, float, float>(
        ::fesql::type::kInt64, ::fesql::type::kFloat, ::fesql::type::kFloat, 1L,
        12345678.5f, 12345678.5f + 1.0f, ::fesql::node::kFnOpAdd);
    BinaryArithmeticExprCheck<int64_t, double, double>(
        ::fesql::type::kInt64, ::fesql::type::kDouble, ::fesql::type::kDouble,
        1, 12345678.5, 12345678.5 + 1.0, ::fesql::node::kFnOpAdd);
}

TEST_F(ArithmeticIRBuilderTest, test_add_float_x_expr) {
    BinaryArithmeticExprCheck<float, int16_t, float>(
        ::fesql::type::kFloat, ::fesql::type::kInt16, ::fesql::type::kFloat,
        1.0f, 1, 2.0f, ::fesql::node::kFnOpAdd);

    BinaryArithmeticExprCheck<float, int32_t, float>(
        ::fesql::type::kFloat, ::fesql::type::kInt32, ::fesql::type::kFloat,
        8000000000L, 1, 8000000001L, ::fesql::node::kFnOpAdd);

    BinaryArithmeticExprCheck<float, int64_t, float>(
        ::fesql::type::kFloat, ::fesql::type::kInt64, ::fesql::type::kFloat,
        1.0f, 200000L, 1.0f + 200000.0f, ::fesql::node::kFnOpAdd);

    BinaryArithmeticExprCheck<float, float, float>(
        ::fesql::type::kFloat, ::fesql::type::kFloat, ::fesql::type::kFloat,
        1.0f, 12345678.5f, 12345678.5f + 1.0f, ::fesql::node::kFnOpAdd);
    BinaryArithmeticExprCheck<float, double, double>(
        ::fesql::type::kFloat, ::fesql::type::kDouble, ::fesql::type::kDouble,
        1.0f, 12345678.5, 12345678.5 + 1.0, ::fesql::node::kFnOpAdd);
}

TEST_F(ArithmeticIRBuilderTest, test_add_double_x_expr) {
    BinaryArithmeticExprCheck<double, int16_t, double>(
        ::fesql::type::kDouble, ::fesql::type::kInt16, ::fesql::type::kDouble,
        1.0, 1, 2.0, ::fesql::node::kFnOpAdd);

    BinaryArithmeticExprCheck<double, int32_t, double>(
        ::fesql::type::kDouble, ::fesql::type::kInt32, ::fesql::type::kDouble,
        8000000000L, 1, 8000000001.0, ::fesql::node::kFnOpAdd);

    BinaryArithmeticExprCheck<double, int64_t, double>(
        ::fesql::type::kDouble, ::fesql::type::kInt64, ::fesql::type::kDouble,
        1.0f, 200000L, 200001.0, ::fesql::node::kFnOpAdd);

    BinaryArithmeticExprCheck<double, float, double>(
        ::fesql::type::kDouble, ::fesql::type::kFloat, ::fesql::type::kDouble,
        1.0, 12345678.5f, static_cast<double>(12345678.5f) + 1.0,
        ::fesql::node::kFnOpAdd);
    BinaryArithmeticExprCheck<double, double, double>(
        ::fesql::type::kDouble, ::fesql::type::kDouble, ::fesql::type::kDouble,
        1.0, 12345678.5, 12345678.5 + 1.0, ::fesql::node::kFnOpAdd);
}

TEST_F(ArithmeticIRBuilderTest, test_sub_int16_x_expr) {
    BinaryArithmeticExprCheck<int16_t, int16_t, int16_t>(
        ::fesql::type::kInt16, ::fesql::type::kInt16, ::fesql::type::kInt16, 2,
        1, 1, ::fesql::node::kFnOpMinus);

    BinaryArithmeticExprCheck<int16_t, int32_t, int32_t>(
        ::fesql::type::kInt16, ::fesql::type::kInt32, ::fesql::type::kInt32, 2,
        1, 1, ::fesql::node::kFnOpMinus);

    BinaryArithmeticExprCheck<int16_t, int64_t, int64_t>(
        ::fesql::type::kInt16, ::fesql::type::kInt64, ::fesql::type::kInt64, 1,
        8000000000L, 1L - 8000000000L, ::fesql::node::kFnOpMinus);

    BinaryArithmeticExprCheck<int16_t, float, float>(
        ::fesql::type::kInt16, ::fesql::type::kFloat, ::fesql::type::kFloat, 1,
        12345678.5f, 1.0f - 12345678.5f, ::fesql::node::kFnOpMinus);
    BinaryArithmeticExprCheck<int16_t, double, double>(
        ::fesql::type::kInt16, ::fesql::type::kDouble, ::fesql::type::kDouble,
        1, 12345678.5, 1.0 - 12345678.5, ::fesql::node::kFnOpMinus);
}

TEST_F(ArithmeticIRBuilderTest, test_sub_int32_x_expr) {
    BinaryArithmeticExprCheck<int32_t, int16_t, int32_t>(
        ::fesql::type::kInt32, ::fesql::type::kInt16, ::fesql::type::kInt32, 2,
        1, 1, ::fesql::node::kFnOpMinus);

    BinaryArithmeticExprCheck<int32_t, int32_t, int32_t>(
        ::fesql::type::kInt32, ::fesql::type::kInt32, ::fesql::type::kInt32, 2,
        1, 1, ::fesql::node::kFnOpMinus);

    BinaryArithmeticExprCheck<int32_t, int64_t, int64_t>(
        ::fesql::type::kInt32, ::fesql::type::kInt64, ::fesql::type::kInt64, 1,
        8000000000L, 1L - 8000000000L, ::fesql::node::kFnOpMinus);

    BinaryArithmeticExprCheck<int32_t, float, float>(
        ::fesql::type::kInt32, ::fesql::type::kFloat, ::fesql::type::kFloat, 1,
        12345678.5f, 1.0f - 12345678.5f, ::fesql::node::kFnOpMinus);
    BinaryArithmeticExprCheck<int32_t, double, double>(
        ::fesql::type::kInt32, ::fesql::type::kDouble, ::fesql::type::kDouble,
        1, 12345678.5, 1.0 - 12345678.5, ::fesql::node::kFnOpMinus);
}

TEST_F(ArithmeticIRBuilderTest, test_sub_int64_x_expr) {
    BinaryArithmeticExprCheck<int64_t, int16_t, int64_t>(
        ::fesql::type::kInt64, ::fesql::type::kInt16, ::fesql::type::kInt64,
        8000000000L, 1, 8000000000L - 1L, ::fesql::node::kFnOpMinus);

    BinaryArithmeticExprCheck<int64_t, int32_t, int64_t>(
        ::fesql::type::kInt64, ::fesql::type::kInt32, ::fesql::type::kInt64,
        8000000000L, 1, 8000000000L - 1L, ::fesql::node::kFnOpMinus);

    BinaryArithmeticExprCheck<int64_t, int64_t, int64_t>(
        ::fesql::type::kInt64, ::fesql::type::kInt64, ::fesql::type::kInt64, 1L,
        8000000000L, 1L - 8000000000L, ::fesql::node::kFnOpMinus);

    BinaryArithmeticExprCheck<int64_t, float, float>(
        ::fesql::type::kInt64, ::fesql::type::kFloat, ::fesql::type::kFloat, 1L,
        12345678.5f, 1.0f - 12345678.5f, ::fesql::node::kFnOpMinus);
    BinaryArithmeticExprCheck<int64_t, double, double>(
        ::fesql::type::kInt64, ::fesql::type::kDouble, ::fesql::type::kDouble,
        1, 12345678.5, 1.0 - 12345678.5, ::fesql::node::kFnOpMinus);
}

TEST_F(ArithmeticIRBuilderTest, test_sub_float_x_expr) {
    BinaryArithmeticExprCheck<float, int16_t, float>(
        ::fesql::type::kFloat, ::fesql::type::kInt16, ::fesql::type::kFloat,
        2.0f, 1, 1.0f, ::fesql::node::kFnOpMinus);

    BinaryArithmeticExprCheck<float, int32_t, float>(
        ::fesql::type::kFloat, ::fesql::type::kInt32, ::fesql::type::kFloat,
        8000000000L, 1, 8000000000.0f - 1.0f, ::fesql::node::kFnOpMinus);

    BinaryArithmeticExprCheck<float, int64_t, float>(
        ::fesql::type::kFloat, ::fesql::type::kInt64, ::fesql::type::kFloat,
        1.0f, 200000L, 1.0f - 200000.0f, ::fesql::node::kFnOpMinus);

    BinaryArithmeticExprCheck<float, float, float>(
        ::fesql::type::kFloat, ::fesql::type::kFloat, ::fesql::type::kFloat,
        1.0f, 12345678.5f, 1.0f - 12345678.5f, ::fesql::node::kFnOpMinus);
    BinaryArithmeticExprCheck<float, double, double>(
        ::fesql::type::kFloat, ::fesql::type::kDouble, ::fesql::type::kDouble,
        1.0f, 12345678.5, 1.0 - 12345678.5, ::fesql::node::kFnOpMinus);
}

TEST_F(ArithmeticIRBuilderTest, test_sub_double_x_expr) {
    BinaryArithmeticExprCheck<double, int16_t, double>(
        ::fesql::type::kDouble, ::fesql::type::kInt16, ::fesql::type::kDouble,
        2.0, 1, 1.0, ::fesql::node::kFnOpMinus);

    BinaryArithmeticExprCheck<double, int32_t, double>(
        ::fesql::type::kDouble, ::fesql::type::kInt32, ::fesql::type::kDouble,
        8000000000L, 1, 8000000000.0 - 1.0, ::fesql::node::kFnOpMinus);

    BinaryArithmeticExprCheck<double, int64_t, double>(
        ::fesql::type::kDouble, ::fesql::type::kInt64, ::fesql::type::kDouble,
        1.0f, 200000L, 1.0 - 200000.0, ::fesql::node::kFnOpMinus);

    BinaryArithmeticExprCheck<double, float, double>(
        ::fesql::type::kDouble, ::fesql::type::kFloat, ::fesql::type::kDouble,
        1.0, 12345678.5f, 1.0 - static_cast<double>(12345678.5f),
        ::fesql::node::kFnOpMinus);
    BinaryArithmeticExprCheck<double, double, double>(
        ::fesql::type::kDouble, ::fesql::type::kDouble, ::fesql::type::kDouble,
        1.0, 12345678.5, 1.0 - 12345678.5, ::fesql::node::kFnOpMinus);
}

TEST_F(ArithmeticIRBuilderTest, test_mul_int16_x_expr) {
    BinaryArithmeticExprCheck<int16_t, int16_t, int16_t>(
        ::fesql::type::kInt16, ::fesql::type::kInt16, ::fesql::type::kInt16, 2,
        3, 2 * 3, ::fesql::node::kFnOpMulti);

    BinaryArithmeticExprCheck<int16_t, int32_t, int32_t>(
        ::fesql::type::kInt16, ::fesql::type::kInt32, ::fesql::type::kInt32, 2,
        3, 2 * 3, ::fesql::node::kFnOpMulti);

    BinaryArithmeticExprCheck<int16_t, int64_t, int64_t>(
        ::fesql::type::kInt16, ::fesql::type::kInt64, ::fesql::type::kInt64, 2,
        8000000000L, 2L * 8000000000L, ::fesql::node::kFnOpMulti);

    BinaryArithmeticExprCheck<int16_t, float, float>(
        ::fesql::type::kInt16, ::fesql::type::kFloat, ::fesql::type::kFloat, 2,
        12345678.5f, 2.0f * 12345678.5f, ::fesql::node::kFnOpMulti);
    BinaryArithmeticExprCheck<int16_t, double, double>(
        ::fesql::type::kInt16, ::fesql::type::kDouble, ::fesql::type::kDouble,
        2, 12345678.5, 2.0 * 12345678.5, ::fesql::node::kFnOpMulti);
}
TEST_F(ArithmeticIRBuilderTest, test_multi_int32_x_expr) {
    BinaryArithmeticExprCheck<int32_t, int16_t, int32_t>(
        ::fesql::type::kInt32, ::fesql::type::kInt16, ::fesql::type::kInt32, 2,
        3, 2 * 3, ::fesql::node::kFnOpMulti);

    BinaryArithmeticExprCheck<int32_t, int32_t, int32_t>(
        ::fesql::type::kInt32, ::fesql::type::kInt32, ::fesql::type::kInt32, 2,
        3, 2 * 3, ::fesql::node::kFnOpMulti);

    BinaryArithmeticExprCheck<int32_t, int64_t, int64_t>(
        ::fesql::type::kInt32, ::fesql::type::kInt64, ::fesql::type::kInt64, 2,
        8000000000L, 2L * 8000000000L, ::fesql::node::kFnOpMulti);

    BinaryArithmeticExprCheck<int32_t, float, float>(
        ::fesql::type::kInt32, ::fesql::type::kFloat, ::fesql::type::kFloat, 2,
        12345678.5f, 2.0f * 12345678.5f, ::fesql::node::kFnOpMulti);
    BinaryArithmeticExprCheck<int32_t, double, double>(
        ::fesql::type::kInt32, ::fesql::type::kDouble, ::fesql::type::kDouble,
        2, 12345678.5, 2.0 * 12345678.5, ::fesql::node::kFnOpMulti);
}
TEST_F(ArithmeticIRBuilderTest, test_multi_int64_x_expr) {
    BinaryArithmeticExprCheck<int64_t, int16_t, int64_t>(
        ::fesql::type::kInt64, ::fesql::type::kInt16, ::fesql::type::kInt64,
        8000000000L, 2L, 8000000000L * 2L, ::fesql::node::kFnOpMulti);

    BinaryArithmeticExprCheck<int64_t, int32_t, int64_t>(
        ::fesql::type::kInt64, ::fesql::type::kInt32, ::fesql::type::kInt64,
        8000000000L, 2, 8000000000L * 2L, ::fesql::node::kFnOpMulti);

    BinaryArithmeticExprCheck<int64_t, int64_t, int64_t>(
        ::fesql::type::kInt64, ::fesql::type::kInt64, ::fesql::type::kInt64, 2L,
        8000000000L, 2L * 8000000000L, ::fesql::node::kFnOpMulti);

    BinaryArithmeticExprCheck<int64_t, float, float>(
        ::fesql::type::kInt64, ::fesql::type::kFloat, ::fesql::type::kFloat, 2L,
        12345678.5f, 2.0f * 12345678.5f, ::fesql::node::kFnOpMulti);
    BinaryArithmeticExprCheck<int64_t, double, double>(
        ::fesql::type::kInt64, ::fesql::type::kDouble, ::fesql::type::kDouble,
        2, 12345678.5, 2.0 * 12345678.5, ::fesql::node::kFnOpMulti);
}

TEST_F(ArithmeticIRBuilderTest, test_multi_float_x_expr) {
    BinaryArithmeticExprCheck<float, int16_t, float>(
        ::fesql::type::kFloat, ::fesql::type::kInt16, ::fesql::type::kFloat,
        2.0f, 3.0f, 2.0f * 3.0f, ::fesql::node::kFnOpMulti);

    BinaryArithmeticExprCheck<float, int32_t, float>(
        ::fesql::type::kFloat, ::fesql::type::kInt32, ::fesql::type::kFloat,
        8000000000L, 2, 8000000000.0f * 2.0f, ::fesql::node::kFnOpMulti);

    BinaryArithmeticExprCheck<float, int64_t, float>(
        ::fesql::type::kFloat, ::fesql::type::kInt64, ::fesql::type::kFloat,
        2.0f, 200000L, 2.0f * 200000.0f, ::fesql::node::kFnOpMulti);

    BinaryArithmeticExprCheck<float, float, float>(
        ::fesql::type::kFloat, ::fesql::type::kFloat, ::fesql::type::kFloat,
        2.0f, 12345678.5f, 2.0f * 12345678.5f, ::fesql::node::kFnOpMulti);
    BinaryArithmeticExprCheck<float, double, double>(
        ::fesql::type::kFloat, ::fesql::type::kDouble, ::fesql::type::kDouble,
        2.0f, 12345678.5, 2.0 * 12345678.5, ::fesql::node::kFnOpMulti);
}
TEST_F(ArithmeticIRBuilderTest, test_multi_double_x_expr) {
    BinaryArithmeticExprCheck<double, int16_t, double>(
        ::fesql::type::kDouble, ::fesql::type::kInt16, ::fesql::type::kDouble,
        2.0, 3, 2.0 * 3.0, ::fesql::node::kFnOpMulti);

    BinaryArithmeticExprCheck<double, int32_t, double>(
        ::fesql::type::kDouble, ::fesql::type::kInt32, ::fesql::type::kDouble,
        8000000000L, 2, 8000000000.0 * 2.0, ::fesql::node::kFnOpMulti);

    BinaryArithmeticExprCheck<double, int64_t, double>(
        ::fesql::type::kDouble, ::fesql::type::kInt64, ::fesql::type::kDouble,
        2.0f, 200000L, 2.0 * 200000.0, ::fesql::node::kFnOpMulti);

    BinaryArithmeticExprCheck<double, float, double>(
        ::fesql::type::kDouble, ::fesql::type::kFloat, ::fesql::type::kDouble,
        2.0, 12345678.5f, 2.0 * static_cast<double>(12345678.5f),
        ::fesql::node::kFnOpMulti);
    BinaryArithmeticExprCheck<double, double, double>(
        ::fesql::type::kDouble, ::fesql::type::kDouble, ::fesql::type::kDouble,
        2.0, 12345678.5, 2.0 * 12345678.5, ::fesql::node::kFnOpMulti);
}

TEST_F(ArithmeticIRBuilderTest, test_fdiv_int32_x_expr) {
    BinaryArithmeticExprCheck<int32_t, int16_t, double>(
        ::fesql::type::kInt32, ::fesql::type::kInt16, ::fesql::type::kDouble, 2,
        3, 2.0/3.0, ::fesql::node::kFnOpDiv);

    BinaryArithmeticExprCheck<int32_t, int32_t, double>(
        ::fesql::type::kInt32, ::fesql::type::kInt32, ::fesql::type::kDouble, 2,
        3, 2.0/3.0, ::fesql::node::kFnOpDiv);

    BinaryArithmeticExprCheck<int32_t, int64_t, double>(
        ::fesql::type::kInt32, ::fesql::type::kInt64, ::fesql::type::kDouble, 2,
        8000000000L, 2.0 / 8000000000.0, ::fesql::node::kFnOpDiv);
//
    BinaryArithmeticExprCheck<int32_t, float, double>(
        ::fesql::type::kInt32, ::fesql::type::kFloat, ::fesql::type::kDouble, 2,
        3.0f, 2.0 / 3.0, ::fesql::node::kFnOpDiv);
    BinaryArithmeticExprCheck<int32_t, double, double>(
        ::fesql::type::kInt32, ::fesql::type::kDouble, ::fesql::type::kDouble,
        2, 12345678.5, 2.0 / 12345678.5, ::fesql::node::kFnOpDiv);
}


TEST_F(ArithmeticIRBuilderTest, test_mod_int32_x_expr) {
    BinaryArithmeticExprCheck<int32_t, int16_t, int32_t>(
        ::fesql::type::kInt32, ::fesql::type::kInt16, ::fesql::type::kInt32, 12,
        5, 2, ::fesql::node::kFnOpMod);

    BinaryArithmeticExprCheck<int32_t, int32_t, int32_t>(
        ::fesql::type::kInt32, ::fesql::type::kInt32, ::fesql::type::kInt32, 12,
        5, 2, ::fesql::node::kFnOpMod);

    BinaryArithmeticExprCheck<int32_t, int64_t, int64_t>(
        ::fesql::type::kInt32, ::fesql::type::kInt64, ::fesql::type::kInt64, 12,
        50000L, 12L, ::fesql::node::kFnOpMod);

//    BinaryArithmeticExprCheck<int32_t, float, float>(
//        ::fesql::type::kInt32, ::fesql::type::kFloat, ::fesql::type::kFloat, 12,
//        5.1f, 1.8f, ::fesql::node::kFnOpMod);

}
}  // namespace codegen
}  // namespace fesql
int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    InitializeNativeTarget();
    InitializeNativeTargetAsmPrinter();
    return RUN_ALL_TESTS();
}
