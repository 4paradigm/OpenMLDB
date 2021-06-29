/*
 * Copyright 2021 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "codegen/arithmetic_expr_ir_builder.h"
#include <memory>
#include <utility>
#include "case/sql_case.h"
#include "codegen/ir_base_builder.h"
#include "codegen/ir_base_builder_test.h"
#include "codegen/string_ir_builder.h"
#include "codegen/timestamp_ir_builder.h"
#include "gtest/gtest.h"
#include "llvm/ExecutionEngine/Orc/LLJIT.h"
#include "llvm/IR/Function.h"
#include "llvm/IR/IRBuilder.h"
#include "llvm/IR/LegacyPassManager.h"
#include "llvm/IR/Module.h"
#include "llvm/Support/InitLLVM.h"
#include "llvm/Support/TargetSelect.h"
#include "llvm/Support/raw_ostream.h"
#include "llvm/Transforms/Scalar.h"
#include "node/node_manager.h"
#include "udf/default_udf_library.h"
#include "udf/udf.h"

using namespace llvm;       // NOLINT
using namespace llvm::orc;  // NOLINT
ExitOnError ExitOnErr;
namespace hybridse {
namespace codegen {
using hybridse::codec::Timestamp;
using hybridse::udf::Nullable;
class ArithmeticIRBuilderTest : public ::testing::Test {
 public:
    ArithmeticIRBuilderTest() { manager_ = new node::NodeManager(); }
    ~ArithmeticIRBuilderTest() { delete manager_; }

 protected:
    node::NodeManager *manager_;
};

template <typename LHS, typename RHS, typename Ret>
void BinaryArithmeticErrorCheck(hybridse::node::FnOperator op) {
    auto compiled_func = BuildExprFunction<Ret, LHS, RHS>(
        [op](node::NodeManager *nm, node::ExprNode *left,
             node::ExprNode *right) {
            return nm->MakeBinaryExprNode(left, right, op);
        });
    ASSERT_FALSE(compiled_func.valid())
        << DataTypeTrait<LHS>::to_string() << " "
        << DataTypeTrait<RHS>::to_string() << " "
        << DataTypeTrait<Ret>::to_string();
}
template <typename LHS, typename RHS, typename Ret>
void BinaryArithmeticExprCheck(LHS left_val, RHS right_val, Ret expect,
                               hybridse::node::FnOperator op) {
    auto compiled_func = BuildExprFunction<Ret, LHS, RHS>(
        [op](node::NodeManager *nm, node::ExprNode *left,
             node::ExprNode *right) {
            return nm->MakeBinaryExprNode(left, right, op);
        });
    ASSERT_TRUE(compiled_func.valid())
        << "BinaryArithmeticExprCheck fail: " << DataTypeTrait<LHS>::to_string
        << " " << DataTypeTrait<RHS>::to_string() << " "
        << DataTypeTrait<Ret>::to_string() << " " << node::ExprOpTypeName(op);
    Ret result = compiled_func(left_val, right_val);
    ASSERT_EQ(expect, result);
}
template <class V1, class V2, class R>
void BinaryArithmeticExprCheck(::hybridse::node::DataType left_type,
                               ::hybridse::node::DataType right_type,
                               ::hybridse::node::DataType dist_type, V1 value1,
                               V2 value2, R expected,
                               hybridse::node::FnOperator op) {
    return BinaryArithmeticExprCheck<V1, V2, R>(value1, value2, expected, op);
}

TEST_F(ArithmeticIRBuilderTest, test_add_null) {
    BinaryArithmeticExprCheck<Nullable<int16_t>, Nullable<int16_t>,
                              Nullable<int16_t>>(nullptr, nullptr, nullptr,
                                                 ::hybridse::node::kFnOpAdd);
    BinaryArithmeticExprCheck<Nullable<int16_t>, Nullable<int32_t>,
                              Nullable<int32_t>>(nullptr, nullptr, nullptr,
                                                 ::hybridse::node::kFnOpAdd);
    BinaryArithmeticExprCheck<Nullable<int16_t>, Nullable<int64_t>,
                              Nullable<int64_t>>(nullptr, nullptr, nullptr,
                                                 ::hybridse::node::kFnOpAdd);
    BinaryArithmeticExprCheck<Nullable<int16_t>, Nullable<float>,
                              Nullable<float>>(nullptr, nullptr, nullptr,
                                               ::hybridse::node::kFnOpAdd);
    BinaryArithmeticExprCheck<Nullable<int16_t>, Nullable<double>,
                              Nullable<double>>(nullptr, nullptr, nullptr,
                                                ::hybridse::node::kFnOpAdd);
    BinaryArithmeticExprCheck<Nullable<int16_t>, Nullable<int16_t>,
                              Nullable<int16_t>>(nullptr, nullptr, nullptr,
                                                 ::hybridse::node::kFnOpAdd);
    BinaryArithmeticExprCheck<Nullable<int16_t>, Nullable<int16_t>,
                              Nullable<int16_t>>(1, nullptr, nullptr,
                                                 ::hybridse::node::kFnOpAdd);
    BinaryArithmeticExprCheck<Nullable<int16_t>, Nullable<int16_t>,
                              Nullable<int16_t>>(nullptr, 1, nullptr,
                                                 ::hybridse::node::kFnOpAdd);
    BinaryArithmeticExprCheck<Nullable<int32_t>, Nullable<int32_t>,
                              Nullable<int32_t>>(1, nullptr, nullptr,
                                                 ::hybridse::node::kFnOpAdd);
    BinaryArithmeticExprCheck<Nullable<int32_t>, Nullable<int32_t>,
                              Nullable<int32_t>>(nullptr, 1, nullptr,
                                                 ::hybridse::node::kFnOpAdd);
    BinaryArithmeticExprCheck<Nullable<int64_t>, Nullable<int64_t>,
                              Nullable<int64_t>>(1, nullptr, nullptr,
                                                 ::hybridse::node::kFnOpAdd);
    BinaryArithmeticExprCheck<Nullable<int64_t>, Nullable<int64_t>,
                              Nullable<int64_t>>(nullptr, 1, nullptr,
                                                 ::hybridse::node::kFnOpAdd);
    BinaryArithmeticExprCheck<Nullable<float>, Nullable<float>,
                              Nullable<float>>(1.0f, nullptr, nullptr,
                                               ::hybridse::node::kFnOpAdd);
    BinaryArithmeticExprCheck<Nullable<float>, Nullable<float>,
                              Nullable<float>>(nullptr, 1.0f, nullptr,
                                               ::hybridse::node::kFnOpAdd);
    BinaryArithmeticExprCheck<Nullable<double>, Nullable<double>,
                              Nullable<double>>(1.0, nullptr, nullptr,
                                                ::hybridse::node::kFnOpAdd);
    BinaryArithmeticExprCheck<Nullable<double>, Nullable<double>,
                              Nullable<double>>(nullptr, 1.0, nullptr,
                                                ::hybridse::node::kFnOpAdd);
    BinaryArithmeticExprCheck<Nullable<Timestamp>, Nullable<Timestamp>,
                              Nullable<Timestamp>>(
        codec::Timestamp(1590115420000L), nullptr, nullptr,
        ::hybridse::node::kFnOpAdd);
    BinaryArithmeticExprCheck<Nullable<Timestamp>, Nullable<Timestamp>,
                              Nullable<Timestamp>>(
        nullptr, codec::Timestamp(1590115420000L), nullptr,
        ::hybridse::node::kFnOpAdd);
}
TEST_F(ArithmeticIRBuilderTest, test_sub_null) {
    BinaryArithmeticExprCheck<Nullable<int16_t>, Nullable<int16_t>,
                              Nullable<int16_t>>(nullptr, nullptr, nullptr,
                                                 ::hybridse::node::kFnOpMinus);
    BinaryArithmeticExprCheck<Nullable<int16_t>, Nullable<int32_t>,
                              Nullable<int32_t>>(nullptr, nullptr, nullptr,
                                                 ::hybridse::node::kFnOpMinus);
    BinaryArithmeticExprCheck<Nullable<int16_t>, Nullable<int64_t>,
                              Nullable<int64_t>>(nullptr, nullptr, nullptr,
                                                 ::hybridse::node::kFnOpMinus);
    BinaryArithmeticExprCheck<Nullable<int16_t>, Nullable<float>,
                              Nullable<float>>(nullptr, nullptr, nullptr,
                                               ::hybridse::node::kFnOpMinus);
    BinaryArithmeticExprCheck<Nullable<int16_t>, Nullable<double>,
                              Nullable<double>>(nullptr, nullptr, nullptr,
                                                ::hybridse::node::kFnOpMinus);
    BinaryArithmeticExprCheck<Nullable<int16_t>, Nullable<int16_t>,
                              Nullable<int16_t>>(nullptr, nullptr, nullptr,
                                                 ::hybridse::node::kFnOpMinus);
    BinaryArithmeticExprCheck<Nullable<int16_t>, Nullable<int16_t>,
                              Nullable<int16_t>>(1, nullptr, nullptr,
                                                 ::hybridse::node::kFnOpMinus);
    BinaryArithmeticExprCheck<Nullable<int16_t>, Nullable<int16_t>,
                              Nullable<int16_t>>(nullptr, 1, nullptr,
                                                 ::hybridse::node::kFnOpMinus);
    BinaryArithmeticExprCheck<Nullable<int32_t>, Nullable<int32_t>,
                              Nullable<int32_t>>(1, nullptr, nullptr,
                                                 ::hybridse::node::kFnOpMinus);
    BinaryArithmeticExprCheck<Nullable<int32_t>, Nullable<int32_t>,
                              Nullable<int32_t>>(nullptr, 1, nullptr,
                                                 ::hybridse::node::kFnOpMinus);
    BinaryArithmeticExprCheck<Nullable<int64_t>, Nullable<int64_t>,
                              Nullable<int64_t>>(1, nullptr, nullptr,
                                                 ::hybridse::node::kFnOpMinus);
    BinaryArithmeticExprCheck<Nullable<int64_t>, Nullable<int64_t>,
                              Nullable<int64_t>>(nullptr, 1, nullptr,
                                                 ::hybridse::node::kFnOpMinus);
    BinaryArithmeticExprCheck<Nullable<float>, Nullable<float>,
                              Nullable<float>>(1.0f, nullptr, nullptr,
                                               ::hybridse::node::kFnOpMinus);
    BinaryArithmeticExprCheck<Nullable<float>, Nullable<float>,
                              Nullable<float>>(nullptr, 1.0f, nullptr,
                                               ::hybridse::node::kFnOpMinus);
    BinaryArithmeticExprCheck<Nullable<double>, Nullable<double>,
                              Nullable<double>>(1.0, nullptr, nullptr,
                                                ::hybridse::node::kFnOpMinus);
    BinaryArithmeticExprCheck<Nullable<double>, Nullable<double>,
                              Nullable<double>>(nullptr, 1.0, nullptr,
                                                ::hybridse::node::kFnOpMinus);
}
TEST_F(ArithmeticIRBuilderTest, test_sub_timestamp_null_0) {
    BinaryArithmeticExprCheck<Nullable<Timestamp>, Nullable<int32_t>,
                              Nullable<Timestamp>>(
        nullptr, 1, nullptr, ::hybridse::node::kFnOpMinus);
}
TEST_F(ArithmeticIRBuilderTest, test_sub_timestamp_null_1) {
    BinaryArithmeticExprCheck<Nullable<Timestamp>, Nullable<int64_t>,
                              Nullable<Timestamp>>(
        nullptr, 1, nullptr, ::hybridse::node::kFnOpMinus);
}
TEST_F(ArithmeticIRBuilderTest, test_sub_timestamp_null_2) {
    BinaryArithmeticExprCheck<Nullable<Timestamp>, Nullable<int16_t>,
                              Nullable<Timestamp>>(
        nullptr, 1, nullptr, ::hybridse::node::kFnOpMinus);
}
TEST_F(ArithmeticIRBuilderTest, test_sub_timestamp_null_3) {
    BinaryArithmeticExprCheck<Nullable<Timestamp>, Nullable<bool>,
                              Nullable<Timestamp>>(
        nullptr, true, nullptr, ::hybridse::node::kFnOpMinus);
}
TEST_F(ArithmeticIRBuilderTest, test_multi_null) {
    BinaryArithmeticExprCheck<Nullable<int16_t>, Nullable<int16_t>,
                              Nullable<int16_t>>(nullptr, nullptr, nullptr,
                                                 ::hybridse::node::kFnOpMulti);
    BinaryArithmeticExprCheck<Nullable<int16_t>, Nullable<int32_t>,
                              Nullable<int32_t>>(nullptr, nullptr, nullptr,
                                                 ::hybridse::node::kFnOpMulti);
    BinaryArithmeticExprCheck<Nullable<int16_t>, Nullable<int64_t>,
                              Nullable<int64_t>>(nullptr, nullptr, nullptr,
                                                 ::hybridse::node::kFnOpMulti);
    BinaryArithmeticExprCheck<Nullable<int16_t>, Nullable<float>,
                              Nullable<float>>(nullptr, nullptr, nullptr,
                                               ::hybridse::node::kFnOpMulti);
    BinaryArithmeticExprCheck<Nullable<int16_t>, Nullable<double>,
                              Nullable<double>>(nullptr, nullptr, nullptr,
                                                ::hybridse::node::kFnOpMulti);
    BinaryArithmeticExprCheck<Nullable<int16_t>, Nullable<int16_t>,
                              Nullable<int16_t>>(nullptr, nullptr, nullptr,
                                                 ::hybridse::node::kFnOpMulti);
    BinaryArithmeticExprCheck<Nullable<int16_t>, Nullable<int16_t>,
                              Nullable<int16_t>>(1, nullptr, nullptr,
                                                 ::hybridse::node::kFnOpMulti);
    BinaryArithmeticExprCheck<Nullable<int16_t>, Nullable<int16_t>,
                              Nullable<int16_t>>(nullptr, 1, nullptr,
                                                 ::hybridse::node::kFnOpMulti);
    BinaryArithmeticExprCheck<Nullable<int32_t>, Nullable<int32_t>,
                              Nullable<int32_t>>(1, nullptr, nullptr,
                                                 ::hybridse::node::kFnOpMulti);
    BinaryArithmeticExprCheck<Nullable<int32_t>, Nullable<int32_t>,
                              Nullable<int32_t>>(nullptr, 1, nullptr,
                                                 ::hybridse::node::kFnOpMulti);
    BinaryArithmeticExprCheck<Nullable<int64_t>, Nullable<int64_t>,
                              Nullable<int64_t>>(1, nullptr, nullptr,
                                                 ::hybridse::node::kFnOpMulti);
    BinaryArithmeticExprCheck<Nullable<int64_t>, Nullable<int64_t>,
                              Nullable<int64_t>>(nullptr, 1, nullptr,
                                                 ::hybridse::node::kFnOpMulti);
    BinaryArithmeticExprCheck<Nullable<float>, Nullable<float>,
                              Nullable<float>>(1.0f, nullptr, nullptr,
                                               ::hybridse::node::kFnOpMulti);
    BinaryArithmeticExprCheck<Nullable<float>, Nullable<float>,
                              Nullable<float>>(nullptr, 1.0f, nullptr,
                                               ::hybridse::node::kFnOpMulti);
    BinaryArithmeticExprCheck<Nullable<double>, Nullable<double>,
                              Nullable<double>>(1.0, nullptr, nullptr,
                                                ::hybridse::node::kFnOpMulti);
    BinaryArithmeticExprCheck<Nullable<double>, Nullable<double>,
                              Nullable<double>>(nullptr, 1.0, nullptr,
                                                ::hybridse::node::kFnOpMulti);
}
TEST_F(ArithmeticIRBuilderTest, test_int_div_null) {
    BinaryArithmeticExprCheck<Nullable<int16_t>, Nullable<int16_t>,
                              Nullable<int16_t>>(nullptr, nullptr, nullptr,
                                                 ::hybridse::node::kFnOpDiv);
    BinaryArithmeticExprCheck<Nullable<int16_t>, Nullable<int32_t>,
                              Nullable<int32_t>>(nullptr, nullptr, nullptr,
                                                 ::hybridse::node::kFnOpDiv);
    BinaryArithmeticExprCheck<Nullable<int16_t>, Nullable<int64_t>,
                              Nullable<int64_t>>(nullptr, nullptr, nullptr,
                                                 ::hybridse::node::kFnOpDiv);
    BinaryArithmeticExprCheck<Nullable<int16_t>, Nullable<int16_t>,
                              Nullable<int16_t>>(nullptr, nullptr, nullptr,
                                                 ::hybridse::node::kFnOpDiv);
    BinaryArithmeticExprCheck<Nullable<int16_t>, Nullable<int16_t>,
                              Nullable<int16_t>>(1, nullptr, nullptr,
                                                 ::hybridse::node::kFnOpDiv);
    BinaryArithmeticExprCheck<Nullable<int16_t>, Nullable<int16_t>,
                              Nullable<int16_t>>(nullptr, 1, nullptr,
                                                 ::hybridse::node::kFnOpDiv);
    BinaryArithmeticExprCheck<Nullable<int32_t>, Nullable<int32_t>,
                              Nullable<int32_t>>(1, nullptr, nullptr,
                                                 ::hybridse::node::kFnOpDiv);
    BinaryArithmeticExprCheck<Nullable<int32_t>, Nullable<int32_t>,
                              Nullable<int32_t>>(nullptr, 1, nullptr,
                                                 ::hybridse::node::kFnOpDiv);
    BinaryArithmeticExprCheck<Nullable<int64_t>, Nullable<int64_t>,
                              Nullable<int64_t>>(1, nullptr, nullptr,
                                                 ::hybridse::node::kFnOpDiv);
    BinaryArithmeticExprCheck<Nullable<int64_t>, Nullable<int64_t>,
                              Nullable<int64_t>>(nullptr, 1, nullptr,
                                                 ::hybridse::node::kFnOpDiv);
}
TEST_F(ArithmeticIRBuilderTest, test_fdiv_null) {
    BinaryArithmeticExprCheck<Nullable<int16_t>, Nullable<int16_t>,
                              Nullable<double>>(nullptr, nullptr, nullptr,
                                                ::hybridse::node::kFnOpFDiv);
    BinaryArithmeticExprCheck<Nullable<int16_t>, Nullable<int32_t>,
                              Nullable<double>>(nullptr, nullptr, nullptr,
                                                ::hybridse::node::kFnOpFDiv);
    BinaryArithmeticExprCheck<Nullable<int16_t>, Nullable<int64_t>,
                              Nullable<double>>(nullptr, nullptr, nullptr,
                                                ::hybridse::node::kFnOpFDiv);
    BinaryArithmeticExprCheck<Nullable<int16_t>, Nullable<float>,
                              Nullable<double>>(nullptr, nullptr, nullptr,
                                                ::hybridse::node::kFnOpFDiv);
    BinaryArithmeticExprCheck<Nullable<int16_t>, Nullable<double>,
                              Nullable<double>>(nullptr, nullptr, nullptr,
                                                ::hybridse::node::kFnOpFDiv);
    BinaryArithmeticExprCheck<Nullable<int16_t>, Nullable<int16_t>,
                              Nullable<double>>(nullptr, nullptr, nullptr,
                                                ::hybridse::node::kFnOpFDiv);
    BinaryArithmeticExprCheck<Nullable<int16_t>, Nullable<int16_t>,
                              Nullable<double>>(1, nullptr, nullptr,
                                                ::hybridse::node::kFnOpFDiv);
    BinaryArithmeticExprCheck<Nullable<int16_t>, Nullable<int16_t>,
                              Nullable<double>>(nullptr, 1, nullptr,
                                                ::hybridse::node::kFnOpFDiv);
    BinaryArithmeticExprCheck<Nullable<int32_t>, Nullable<int32_t>,
                              Nullable<double>>(1, nullptr, nullptr,
                                                ::hybridse::node::kFnOpFDiv);
    BinaryArithmeticExprCheck<Nullable<int32_t>, Nullable<int32_t>,
                              Nullable<double>>(nullptr, 1, nullptr,
                                                ::hybridse::node::kFnOpFDiv);
    BinaryArithmeticExprCheck<Nullable<int64_t>, Nullable<int64_t>,
                              Nullable<double>>(1, nullptr, nullptr,
                                                ::hybridse::node::kFnOpFDiv);
    BinaryArithmeticExprCheck<Nullable<int64_t>, Nullable<int64_t>,
                              Nullable<double>>(nullptr, 1, nullptr,
                                                ::hybridse::node::kFnOpFDiv);
    BinaryArithmeticExprCheck<Nullable<float>, Nullable<float>,
                              Nullable<double>>(1.0f, nullptr, nullptr,
                                                ::hybridse::node::kFnOpFDiv);
    BinaryArithmeticExprCheck<Nullable<float>, Nullable<float>,
                              Nullable<double>>(nullptr, 1.0f, nullptr,
                                                ::hybridse::node::kFnOpFDiv);
    BinaryArithmeticExprCheck<Nullable<double>, Nullable<double>,
                              Nullable<double>>(1.0, nullptr, nullptr,
                                                ::hybridse::node::kFnOpFDiv);
    BinaryArithmeticExprCheck<Nullable<double>, Nullable<double>,
                              Nullable<double>>(nullptr, 1.0, nullptr,
                                                ::hybridse::node::kFnOpFDiv);
}
TEST_F(ArithmeticIRBuilderTest, test_div_null) {
    BinaryArithmeticExprCheck<Nullable<int16_t>, Nullable<int16_t>,
                              Nullable<double>>(nullptr, nullptr, nullptr,
                                                ::hybridse::node::kFnOpFDiv);
    BinaryArithmeticExprCheck<Nullable<int16_t>, Nullable<int32_t>,
                              Nullable<double>>(nullptr, nullptr, nullptr,
                                                ::hybridse::node::kFnOpFDiv);
    BinaryArithmeticExprCheck<Nullable<int16_t>, Nullable<int64_t>,
                              Nullable<double>>(nullptr, nullptr, nullptr,
                                                ::hybridse::node::kFnOpFDiv);
    BinaryArithmeticExprCheck<Nullable<int16_t>, Nullable<float>,
                              Nullable<double>>(nullptr, nullptr, nullptr,
                                                ::hybridse::node::kFnOpFDiv);
    BinaryArithmeticExprCheck<Nullable<int16_t>, Nullable<double>,
                              Nullable<double>>(nullptr, nullptr, nullptr,
                                                ::hybridse::node::kFnOpFDiv);
    BinaryArithmeticExprCheck<Nullable<int16_t>, Nullable<int16_t>,
                              Nullable<double>>(nullptr, nullptr, nullptr,
                                                ::hybridse::node::kFnOpFDiv);
    BinaryArithmeticExprCheck<Nullable<int16_t>, Nullable<int16_t>,
                              Nullable<double>>(1, nullptr, nullptr,
                                                ::hybridse::node::kFnOpFDiv);
    BinaryArithmeticExprCheck<Nullable<int16_t>, Nullable<int16_t>,
                              Nullable<double>>(nullptr, 1, nullptr,
                                                ::hybridse::node::kFnOpFDiv);
    BinaryArithmeticExprCheck<Nullable<int32_t>, Nullable<int32_t>,
                              Nullable<double>>(1, nullptr, nullptr,
                                                ::hybridse::node::kFnOpFDiv);
    BinaryArithmeticExprCheck<Nullable<int32_t>, Nullable<int32_t>,
                              Nullable<double>>(nullptr, 1, nullptr,
                                                ::hybridse::node::kFnOpFDiv);
    BinaryArithmeticExprCheck<Nullable<int64_t>, Nullable<int64_t>,
                              Nullable<double>>(1, nullptr, nullptr,
                                                ::hybridse::node::kFnOpFDiv);
    BinaryArithmeticExprCheck<Nullable<int64_t>, Nullable<int64_t>,
                              Nullable<double>>(nullptr, 1, nullptr,
                                                ::hybridse::node::kFnOpFDiv);
    BinaryArithmeticExprCheck<Nullable<float>, Nullable<float>,
                              Nullable<double>>(1.0f, nullptr, nullptr,
                                                ::hybridse::node::kFnOpFDiv);
    BinaryArithmeticExprCheck<Nullable<float>, Nullable<float>,
                              Nullable<double>>(nullptr, 1.0f, nullptr,
                                                ::hybridse::node::kFnOpFDiv);
    BinaryArithmeticExprCheck<Nullable<double>, Nullable<double>,
                              Nullable<double>>(1.0, nullptr, nullptr,
                                                ::hybridse::node::kFnOpFDiv);
    BinaryArithmeticExprCheck<Nullable<double>, Nullable<double>,
                              Nullable<double>>(nullptr, 1.0, nullptr,
                                                ::hybridse::node::kFnOpFDiv);
}
TEST_F(ArithmeticIRBuilderTest, test_mod_null) {
    BinaryArithmeticExprCheck<Nullable<int16_t>, Nullable<int16_t>,
                              Nullable<int16_t>>(nullptr, nullptr, nullptr,
                                                 ::hybridse::node::kFnOpMod);
    BinaryArithmeticExprCheck<Nullable<int16_t>, Nullable<int32_t>,
                              Nullable<int32_t>>(nullptr, nullptr, nullptr,
                                                 ::hybridse::node::kFnOpMod);
    BinaryArithmeticExprCheck<Nullable<int16_t>, Nullable<int64_t>,
                              Nullable<int64_t>>(nullptr, nullptr, nullptr,
                                                 ::hybridse::node::kFnOpMod);
    BinaryArithmeticExprCheck<Nullable<int16_t>, Nullable<float>,
                              Nullable<float>>(nullptr, nullptr, nullptr,
                                               ::hybridse::node::kFnOpMod);
    BinaryArithmeticExprCheck<Nullable<int16_t>, Nullable<double>,
                              Nullable<double>>(nullptr, nullptr, nullptr,
                                                ::hybridse::node::kFnOpMod);
    BinaryArithmeticExprCheck<Nullable<int16_t>, Nullable<int16_t>,
                              Nullable<int16_t>>(nullptr, nullptr, nullptr,
                                                 ::hybridse::node::kFnOpMod);
    BinaryArithmeticExprCheck<Nullable<int16_t>, Nullable<int16_t>,
                              Nullable<int16_t>>(1, nullptr, nullptr,
                                                 ::hybridse::node::kFnOpMod);
    BinaryArithmeticExprCheck<Nullable<int16_t>, Nullable<int16_t>,
                              Nullable<int16_t>>(nullptr, 1, nullptr,
                                                 ::hybridse::node::kFnOpMod);
    BinaryArithmeticExprCheck<Nullable<int32_t>, Nullable<int32_t>,
                              Nullable<int32_t>>(1, nullptr, nullptr,
                                                 ::hybridse::node::kFnOpMod);
    BinaryArithmeticExprCheck<Nullable<int32_t>, Nullable<int32_t>,
                              Nullable<int32_t>>(nullptr, 1, nullptr,
                                                 ::hybridse::node::kFnOpMod);
    BinaryArithmeticExprCheck<Nullable<int64_t>, Nullable<int64_t>,
                              Nullable<int64_t>>(1, nullptr, nullptr,
                                                 ::hybridse::node::kFnOpMod);
    BinaryArithmeticExprCheck<Nullable<int64_t>, Nullable<int64_t>,
                              Nullable<int64_t>>(nullptr, 1, nullptr,
                                                 ::hybridse::node::kFnOpMod);
    BinaryArithmeticExprCheck<Nullable<float>, Nullable<float>,
                              Nullable<float>>(1.0f, nullptr, nullptr,
                                               ::hybridse::node::kFnOpMod);
    BinaryArithmeticExprCheck<Nullable<float>, Nullable<float>,
                              Nullable<float>>(nullptr, 1.0f, nullptr,
                                               ::hybridse::node::kFnOpMod);
    BinaryArithmeticExprCheck<Nullable<double>, Nullable<double>,
                              Nullable<double>>(1.0, nullptr, nullptr,
                                                ::hybridse::node::kFnOpMod);
    BinaryArithmeticExprCheck<Nullable<double>, Nullable<double>,
                              Nullable<double>>(nullptr, 1.0, nullptr,
                                                ::hybridse::node::kFnOpMod);
}
TEST_F(ArithmeticIRBuilderTest, test_error_expr_op_1) {
    BinaryArithmeticErrorCheck<Nullable<codec::StringRef>,
                               Nullable<codec::StringRef>,
                               Nullable<codec::StringRef>>(
        ::hybridse::node::kFnOpAdd);
}
TEST_F(ArithmeticIRBuilderTest, test_error_expr_op_2) {
    BinaryArithmeticErrorCheck<Nullable<bool>, Nullable<codec::StringRef>,
                               Nullable<codec::StringRef>>(
        ::hybridse::node::kFnOpAdd);
}
TEST_F(ArithmeticIRBuilderTest, test_error_expr_op_3) {
    BinaryArithmeticErrorCheck<Nullable<Timestamp>, Nullable<Timestamp>,
                               Nullable<Timestamp>>(
        ::hybridse::node::kFnOpMinus);
}
TEST_F(ArithmeticIRBuilderTest, test_error_expr_op_4) {
    BinaryArithmeticErrorCheck<Nullable<codec::StringRef>,
                               Nullable<codec::StringRef>,
                               Nullable<codec::StringRef>>(
        ::hybridse::node::kFnOpMinus);
}
TEST_F(ArithmeticIRBuilderTest, test_error_expr_op_5) {
    BinaryArithmeticErrorCheck<Nullable<codec::Date>, Nullable<codec::Date>,
                               Nullable<codec::Date>>(
        ::hybridse::node::kFnOpMinus);
}
TEST_F(ArithmeticIRBuilderTest, test_error_expr_op_6) {
    BinaryArithmeticErrorCheck<Nullable<Timestamp>, Nullable<Timestamp>,
                               Nullable<Timestamp>>(
        ::hybridse::node::kFnOpMulti);
}
TEST_F(ArithmeticIRBuilderTest, test_error_expr_op_7) {
    BinaryArithmeticErrorCheck<Nullable<codec::StringRef>,
                               Nullable<codec::StringRef>,
                               Nullable<codec::StringRef>>(
        ::hybridse::node::kFnOpMulti);
}
TEST_F(ArithmeticIRBuilderTest, test_error_expr_op_8) {
    BinaryArithmeticErrorCheck<Nullable<codec::Date>, Nullable<codec::Date>,
                               Nullable<codec::Date>>(
        ::hybridse::node::kFnOpMulti);
}
TEST_F(ArithmeticIRBuilderTest, test_error_expr_op_9) {
    BinaryArithmeticErrorCheck<Nullable<Timestamp>, Nullable<Timestamp>,
                               Nullable<Timestamp>>(
        ::hybridse::node::kFnOpFDiv);
}
TEST_F(ArithmeticIRBuilderTest, test_error_expr_op_10) {
    BinaryArithmeticErrorCheck<Nullable<codec::StringRef>,
                               Nullable<codec::StringRef>,
                               Nullable<codec::StringRef>>(
        ::hybridse::node::kFnOpFDiv);
}
TEST_F(ArithmeticIRBuilderTest, test_error_expr_op_11) {
    BinaryArithmeticErrorCheck<Nullable<codec::Date>, Nullable<codec::Date>,
                               Nullable<codec::Date>>(
        ::hybridse::node::kFnOpFDiv);
}
TEST_F(ArithmeticIRBuilderTest, test_error_expr_op_12) {
    BinaryArithmeticErrorCheck<Nullable<Timestamp>, Nullable<Timestamp>,
                               Nullable<Timestamp>>(::hybridse::node::kFnOpDiv);
}
TEST_F(ArithmeticIRBuilderTest, test_error_expr_op_13) {
    BinaryArithmeticErrorCheck<Nullable<codec::StringRef>,
                               Nullable<codec::StringRef>,
                               Nullable<codec::StringRef>>(
        ::hybridse::node::kFnOpDiv);
}
TEST_F(ArithmeticIRBuilderTest, test_error_expr_op_14) {
    BinaryArithmeticErrorCheck<Nullable<codec::Date>, Nullable<codec::Date>,
                               Nullable<codec::Date>>(
        ::hybridse::node::kFnOpDiv);
}
TEST_F(ArithmeticIRBuilderTest, test_error_expr_op_15) {
    BinaryArithmeticErrorCheck<Nullable<int16_t>, Nullable<float>,
                               Nullable<int16_t>>(::hybridse::node::kFnOpDiv);
}
TEST_F(ArithmeticIRBuilderTest, test_error_expr_op_16) {
    BinaryArithmeticErrorCheck<Nullable<int16_t>, Nullable<double>,
                               Nullable<int16_t>>(::hybridse::node::kFnOpDiv);
}

TEST_F(ArithmeticIRBuilderTest, test_add_int16_x_expr) {
    BinaryArithmeticExprCheck<int16_t, int16_t, int16_t>(
        1, 1, 2, ::hybridse::node::kFnOpAdd);

    BinaryArithmeticExprCheck<int16_t, int32_t, int32_t>(
        1, 1, 2, ::hybridse::node::kFnOpAdd);

    BinaryArithmeticExprCheck<int16_t, int64_t, int64_t>(
        1, 8000000000L, 8000000001L, ::hybridse::node::kFnOpAdd);

    BinaryArithmeticExprCheck<int16_t, float, float>(
        1, 12345678.5f, 12345678.5f + 1.0f, ::hybridse::node::kFnOpAdd);
    BinaryArithmeticExprCheck<int16_t, double, double>(
        1, 12345678.5, 12345678.5 + 1.0, ::hybridse::node::kFnOpAdd);
}
TEST_F(ArithmeticIRBuilderTest, test_add_bool_x_expr) {
    BinaryArithmeticExprCheck<bool, int16_t, int16_t>(
        true, 1, 2, ::hybridse::node::kFnOpAdd);

    BinaryArithmeticExprCheck<bool, int32_t, int32_t>(
        true, 1, 2, ::hybridse::node::kFnOpAdd);

    BinaryArithmeticExprCheck<bool, int64_t, int64_t>(
        true, 8000000000L, 8000000001L, ::hybridse::node::kFnOpAdd);

    BinaryArithmeticExprCheck<bool, float, float>(
        true, 12345678.5f, 12345678.5f + 1.0f, ::hybridse::node::kFnOpAdd);
    BinaryArithmeticExprCheck<bool, double, double>(
        true, 12345678.5, 12345678.5 + 1.0, ::hybridse::node::kFnOpAdd);
}
TEST_F(ArithmeticIRBuilderTest, test_add_int32_x_expr) {
    BinaryArithmeticExprCheck<int32_t, int16_t, int32_t>(
        ::hybridse::node::kInt32, ::hybridse::node::kInt16,
        ::hybridse::node::kInt32, 1, 1, 2, ::hybridse::node::kFnOpAdd);

    BinaryArithmeticExprCheck<int32_t, int32_t, int32_t>(
        ::hybridse::node::kInt32, ::hybridse::node::kInt32,
        ::hybridse::node::kInt32, 1, 1, 2, ::hybridse::node::kFnOpAdd);

    BinaryArithmeticExprCheck<int32_t, int64_t, int64_t>(
        ::hybridse::node::kInt32, ::hybridse::node::kInt64,
        ::hybridse::node::kInt64, 1, 8000000000L, 8000000001L,
        ::hybridse::node::kFnOpAdd);

    BinaryArithmeticExprCheck<int32_t, float, float>(
        ::hybridse::node::kInt32, ::hybridse::node::kFloat,
        ::hybridse::node::kFloat, 1, 12345678.5f, 12345678.5f + 1.0f,
        ::hybridse::node::kFnOpAdd);
    BinaryArithmeticExprCheck<int32_t, double, double>(
        ::hybridse::node::kInt32, ::hybridse::node::kDouble,
        ::hybridse::node::kDouble, 1, 12345678.5, 12345678.5 + 1.0,
        ::hybridse::node::kFnOpAdd);
}

TEST_F(ArithmeticIRBuilderTest, test_add_int64_x_expr) {
    BinaryArithmeticExprCheck<int64_t, int16_t, int64_t>(
        ::hybridse::node::kInt64, ::hybridse::node::kInt16,
        ::hybridse::node::kInt64, 8000000000L, 1, 8000000001L,
        ::hybridse::node::kFnOpAdd);

    BinaryArithmeticExprCheck<int64_t, int32_t, int64_t>(
        ::hybridse::node::kInt64, ::hybridse::node::kInt32,
        ::hybridse::node::kInt64, 8000000000L, 1, 8000000001L,
        ::hybridse::node::kFnOpAdd);

    BinaryArithmeticExprCheck<int64_t, int64_t, int64_t>(
        ::hybridse::node::kInt64, ::hybridse::node::kInt64,
        ::hybridse::node::kInt64, 1L, 8000000000L, 8000000001L,
        ::hybridse::node::kFnOpAdd);

    BinaryArithmeticExprCheck<int64_t, float, float>(
        ::hybridse::node::kInt64, ::hybridse::node::kFloat,
        ::hybridse::node::kFloat, 1L, 12345678.5f, 12345678.5f + 1.0f,
        ::hybridse::node::kFnOpAdd);
    BinaryArithmeticExprCheck<int64_t, double, double>(
        ::hybridse::node::kInt64, ::hybridse::node::kDouble,
        ::hybridse::node::kDouble, 1, 12345678.5, 12345678.5 + 1.0,
        ::hybridse::node::kFnOpAdd);
}

TEST_F(ArithmeticIRBuilderTest, test_add_timestamp_expr_0) {
    BinaryArithmeticExprCheck<Timestamp, Timestamp, Timestamp>(
        ::hybridse::node::kTimestamp, ::hybridse::node::kTimestamp,
        ::hybridse::node::kTimestamp, Timestamp(8000000000L), Timestamp(1L),
        Timestamp(8000000001L), ::hybridse::node::kFnOpAdd);
}
TEST_F(ArithmeticIRBuilderTest, test_add_timestamp_expr_1) {
    BinaryArithmeticExprCheck<Timestamp, int64_t, Timestamp>(
        ::hybridse::node::kTimestamp, ::hybridse::node::kInt64,
        ::hybridse::node::kTimestamp, Timestamp(8000000000L), 1L,
        Timestamp(8000000001L), ::hybridse::node::kFnOpAdd);
}
TEST_F(ArithmeticIRBuilderTest, test_add_timestamp_expr_2) {
    BinaryArithmeticExprCheck<Timestamp, int32_t, Timestamp>(
        ::hybridse::node::kTimestamp, ::hybridse::node::kInt32,
        ::hybridse::node::kTimestamp, Timestamp(8000000000L), 1,
        Timestamp(8000000001L), ::hybridse::node::kFnOpAdd);
}
TEST_F(ArithmeticIRBuilderTest, test_sub_timestamp_expr_1) {
    BinaryArithmeticExprCheck<Timestamp, int64_t, Timestamp>(
        ::hybridse::node::kTimestamp, ::hybridse::node::kInt64,
        ::hybridse::node::kTimestamp, Timestamp(8000000001L), 1L,
        Timestamp(8000000000L), ::hybridse::node::kFnOpMinus);
}
TEST_F(ArithmeticIRBuilderTest, test_sub_timestamp_expr_2) {
    BinaryArithmeticExprCheck<Timestamp, int32_t, Timestamp>(
        ::hybridse::node::kTimestamp, ::hybridse::node::kInt32,
        ::hybridse::node::kTimestamp, Timestamp(8000000001L), 1,
        Timestamp(8000000000L), ::hybridse::node::kFnOpMinus);
}

TEST_F(ArithmeticIRBuilderTest, test_add_float_x_expr) {
    BinaryArithmeticExprCheck<float, int16_t, float>(
        ::hybridse::node::kFloat, ::hybridse::node::kInt16,
        ::hybridse::node::kFloat, 1.0f, 1, 2.0f, ::hybridse::node::kFnOpAdd);

    BinaryArithmeticExprCheck<float, int32_t, float>(
        ::hybridse::node::kFloat, ::hybridse::node::kInt32,
        ::hybridse::node::kFloat, 8000000000L, 1, 8000000001L,
        ::hybridse::node::kFnOpAdd);

    BinaryArithmeticExprCheck<float, int64_t, float>(
        ::hybridse::node::kFloat, ::hybridse::node::kInt64,
        ::hybridse::node::kFloat, 1.0f, 200000L, 1.0f + 200000.0f,
        ::hybridse::node::kFnOpAdd);

    BinaryArithmeticExprCheck<float, float, float>(
        ::hybridse::node::kFloat, ::hybridse::node::kFloat,
        ::hybridse::node::kFloat, 1.0f, 12345678.5f, 12345678.5f + 1.0f,
        ::hybridse::node::kFnOpAdd);
    BinaryArithmeticExprCheck<float, double, double>(
        ::hybridse::node::kFloat, ::hybridse::node::kDouble,
        ::hybridse::node::kDouble, 1.0f, 12345678.5, 12345678.5 + 1.0,
        ::hybridse::node::kFnOpAdd);
}

TEST_F(ArithmeticIRBuilderTest, test_add_double_x_expr) {
    BinaryArithmeticExprCheck<double, int16_t, double>(
        ::hybridse::node::kDouble, ::hybridse::node::kInt16,
        ::hybridse::node::kDouble, 1.0, 1, 2.0, ::hybridse::node::kFnOpAdd);

    BinaryArithmeticExprCheck<double, int32_t, double>(
        ::hybridse::node::kDouble, ::hybridse::node::kInt32,
        ::hybridse::node::kDouble, 8000000000L, 1, 8000000001.0,
        ::hybridse::node::kFnOpAdd);

    BinaryArithmeticExprCheck<double, int64_t, double>(
        ::hybridse::node::kDouble, ::hybridse::node::kInt64,
        ::hybridse::node::kDouble, 1.0f, 200000L, 200001.0,
        ::hybridse::node::kFnOpAdd);

    BinaryArithmeticExprCheck<double, float, double>(
        ::hybridse::node::kDouble, ::hybridse::node::kFloat,
        ::hybridse::node::kDouble, 1.0, 12345678.5f,
        static_cast<double>(12345678.5f) + 1.0, ::hybridse::node::kFnOpAdd);
    BinaryArithmeticExprCheck<double, double, double>(
        ::hybridse::node::kDouble, ::hybridse::node::kDouble,
        ::hybridse::node::kDouble, 1.0, 12345678.5, 12345678.5 + 1.0,
        ::hybridse::node::kFnOpAdd);
}

TEST_F(ArithmeticIRBuilderTest, test_sub_int16_x_expr) {
    BinaryArithmeticExprCheck<int16_t, int16_t, int16_t>(
        ::hybridse::node::kInt16, ::hybridse::node::kInt16,
        ::hybridse::node::kInt16, 2, 1, 1, ::hybridse::node::kFnOpMinus);

    BinaryArithmeticExprCheck<int16_t, int32_t, int32_t>(
        ::hybridse::node::kInt16, ::hybridse::node::kInt32,
        ::hybridse::node::kInt32, 2, 1, 1, ::hybridse::node::kFnOpMinus);

    BinaryArithmeticExprCheck<int16_t, int64_t, int64_t>(
        ::hybridse::node::kInt16, ::hybridse::node::kInt64,
        ::hybridse::node::kInt64, 1, 8000000000L, 1L - 8000000000L,
        ::hybridse::node::kFnOpMinus);

    BinaryArithmeticExprCheck<int16_t, float, float>(
        ::hybridse::node::kInt16, ::hybridse::node::kFloat,
        ::hybridse::node::kFloat, 1, 12345678.5f, 1.0f - 12345678.5f,
        ::hybridse::node::kFnOpMinus);
    BinaryArithmeticExprCheck<int16_t, double, double>(
        ::hybridse::node::kInt16, ::hybridse::node::kDouble,
        ::hybridse::node::kDouble, 1, 12345678.5, 1.0 - 12345678.5,
        ::hybridse::node::kFnOpMinus);
}

TEST_F(ArithmeticIRBuilderTest, test_sub_int32_x_expr) {
    BinaryArithmeticExprCheck<int32_t, int16_t, int32_t>(
        ::hybridse::node::kInt32, ::hybridse::node::kInt16,
        ::hybridse::node::kInt32, 2, 1, 1, ::hybridse::node::kFnOpMinus);

    BinaryArithmeticExprCheck<int32_t, int32_t, int32_t>(
        ::hybridse::node::kInt32, ::hybridse::node::kInt32,
        ::hybridse::node::kInt32, 2, 1, 1, ::hybridse::node::kFnOpMinus);

    BinaryArithmeticExprCheck<int32_t, int64_t, int64_t>(
        ::hybridse::node::kInt32, ::hybridse::node::kInt64,
        ::hybridse::node::kInt64, 1, 8000000000L, 1L - 8000000000L,
        ::hybridse::node::kFnOpMinus);

    BinaryArithmeticExprCheck<int32_t, float, float>(
        ::hybridse::node::kInt32, ::hybridse::node::kFloat,
        ::hybridse::node::kFloat, 1, 12345678.5f, 1.0f - 12345678.5f,
        ::hybridse::node::kFnOpMinus);
    BinaryArithmeticExprCheck<int32_t, double, double>(
        ::hybridse::node::kInt32, ::hybridse::node::kDouble,
        ::hybridse::node::kDouble, 1, 12345678.5, 1.0 - 12345678.5,
        ::hybridse::node::kFnOpMinus);
}

TEST_F(ArithmeticIRBuilderTest, test_sub_int64_x_expr) {
    BinaryArithmeticExprCheck<int64_t, int16_t, int64_t>(
        ::hybridse::node::kInt64, ::hybridse::node::kInt16,
        ::hybridse::node::kInt64, 8000000000L, 1, 8000000000L - 1L,
        ::hybridse::node::kFnOpMinus);

    BinaryArithmeticExprCheck<int64_t, int32_t, int64_t>(
        ::hybridse::node::kInt64, ::hybridse::node::kInt32,
        ::hybridse::node::kInt64, 8000000000L, 1, 8000000000L - 1L,
        ::hybridse::node::kFnOpMinus);

    BinaryArithmeticExprCheck<int64_t, int64_t, int64_t>(
        ::hybridse::node::kInt64, ::hybridse::node::kInt64,
        ::hybridse::node::kInt64, 1L, 8000000000L, 1L - 8000000000L,
        ::hybridse::node::kFnOpMinus);

    BinaryArithmeticExprCheck<int64_t, float, float>(
        ::hybridse::node::kInt64, ::hybridse::node::kFloat,
        ::hybridse::node::kFloat, 1L, 12345678.5f, 1.0f - 12345678.5f,
        ::hybridse::node::kFnOpMinus);
    BinaryArithmeticExprCheck<int64_t, double, double>(
        ::hybridse::node::kInt64, ::hybridse::node::kDouble,
        ::hybridse::node::kDouble, 1, 12345678.5, 1.0 - 12345678.5,
        ::hybridse::node::kFnOpMinus);
}

TEST_F(ArithmeticIRBuilderTest, test_sub_float_x_expr) {
    BinaryArithmeticExprCheck<float, int16_t, float>(
        ::hybridse::node::kFloat, ::hybridse::node::kInt16,
        ::hybridse::node::kFloat, 2.0f, 1, 1.0f, ::hybridse::node::kFnOpMinus);

    BinaryArithmeticExprCheck<float, int32_t, float>(
        ::hybridse::node::kFloat, ::hybridse::node::kInt32,
        ::hybridse::node::kFloat, 8000000000L, 1, 8000000000.0f - 1.0f,
        ::hybridse::node::kFnOpMinus);

    BinaryArithmeticExprCheck<float, int64_t, float>(
        ::hybridse::node::kFloat, ::hybridse::node::kInt64,
        ::hybridse::node::kFloat, 1.0f, 200000L, 1.0f - 200000.0f,
        ::hybridse::node::kFnOpMinus);

    BinaryArithmeticExprCheck<float, float, float>(
        ::hybridse::node::kFloat, ::hybridse::node::kFloat,
        ::hybridse::node::kFloat, 1.0f, 12345678.5f, 1.0f - 12345678.5f,
        ::hybridse::node::kFnOpMinus);
    BinaryArithmeticExprCheck<float, double, double>(
        ::hybridse::node::kFloat, ::hybridse::node::kDouble,
        ::hybridse::node::kDouble, 1.0f, 12345678.5, 1.0 - 12345678.5,
        ::hybridse::node::kFnOpMinus);
}

TEST_F(ArithmeticIRBuilderTest, test_sub_double_x_expr) {
    BinaryArithmeticExprCheck<double, int16_t, double>(
        ::hybridse::node::kDouble, ::hybridse::node::kInt16,
        ::hybridse::node::kDouble, 2.0, 1, 1.0, ::hybridse::node::kFnOpMinus);

    BinaryArithmeticExprCheck<double, int32_t, double>(
        ::hybridse::node::kDouble, ::hybridse::node::kInt32,
        ::hybridse::node::kDouble, 8000000000L, 1, 8000000000.0 - 1.0,
        ::hybridse::node::kFnOpMinus);

    BinaryArithmeticExprCheck<double, int64_t, double>(
        ::hybridse::node::kDouble, ::hybridse::node::kInt64,
        ::hybridse::node::kDouble, 1.0f, 200000L, 1.0 - 200000.0,
        ::hybridse::node::kFnOpMinus);

    BinaryArithmeticExprCheck<double, float, double>(
        ::hybridse::node::kDouble, ::hybridse::node::kFloat,
        ::hybridse::node::kDouble, 1.0, 12345678.5f,
        1.0 - static_cast<double>(12345678.5f), ::hybridse::node::kFnOpMinus);
    BinaryArithmeticExprCheck<double, double, double>(
        ::hybridse::node::kDouble, ::hybridse::node::kDouble,
        ::hybridse::node::kDouble, 1.0, 12345678.5, 1.0 - 12345678.5,
        ::hybridse::node::kFnOpMinus);
}

TEST_F(ArithmeticIRBuilderTest, test_mul_int16_x_expr) {
    BinaryArithmeticExprCheck<int16_t, int16_t, int16_t>(
        ::hybridse::node::kInt16, ::hybridse::node::kInt16,
        ::hybridse::node::kInt16, 2, 3, 2 * 3, ::hybridse::node::kFnOpMulti);

    BinaryArithmeticExprCheck<int16_t, int32_t, int32_t>(
        ::hybridse::node::kInt16, ::hybridse::node::kInt32,
        ::hybridse::node::kInt32, 2, 3, 2 * 3, ::hybridse::node::kFnOpMulti);

    BinaryArithmeticExprCheck<int16_t, int64_t, int64_t>(
        ::hybridse::node::kInt16, ::hybridse::node::kInt64,
        ::hybridse::node::kInt64, 2, 8000000000L, 2L * 8000000000L,
        ::hybridse::node::kFnOpMulti);

    BinaryArithmeticExprCheck<int16_t, float, float>(
        ::hybridse::node::kInt16, ::hybridse::node::kFloat,
        ::hybridse::node::kFloat, 2, 12345678.5f, 2.0f * 12345678.5f,
        ::hybridse::node::kFnOpMulti);
    BinaryArithmeticExprCheck<int16_t, double, double>(
        ::hybridse::node::kInt16, ::hybridse::node::kDouble,
        ::hybridse::node::kDouble, 2, 12345678.5, 2.0 * 12345678.5,
        ::hybridse::node::kFnOpMulti);
}
TEST_F(ArithmeticIRBuilderTest, test_multi_int32_x_expr) {
    BinaryArithmeticExprCheck<int32_t, int16_t, int32_t>(
        ::hybridse::node::kInt32, ::hybridse::node::kInt16,
        ::hybridse::node::kInt32, 2, 3, 2 * 3, ::hybridse::node::kFnOpMulti);

    BinaryArithmeticExprCheck<int32_t, int32_t, int32_t>(
        ::hybridse::node::kInt32, ::hybridse::node::kInt32,
        ::hybridse::node::kInt32, 2, 3, 2 * 3, ::hybridse::node::kFnOpMulti);

    BinaryArithmeticExprCheck<int32_t, int64_t, int64_t>(
        ::hybridse::node::kInt32, ::hybridse::node::kInt64,
        ::hybridse::node::kInt64, 2, 8000000000L, 2L * 8000000000L,
        ::hybridse::node::kFnOpMulti);

    BinaryArithmeticExprCheck<int32_t, float, float>(
        ::hybridse::node::kInt32, ::hybridse::node::kFloat,
        ::hybridse::node::kFloat, 2, 12345678.5f, 2.0f * 12345678.5f,
        ::hybridse::node::kFnOpMulti);
    BinaryArithmeticExprCheck<int32_t, double, double>(
        ::hybridse::node::kInt32, ::hybridse::node::kDouble,
        ::hybridse::node::kDouble, 2, 12345678.5, 2.0 * 12345678.5,
        ::hybridse::node::kFnOpMulti);
}
TEST_F(ArithmeticIRBuilderTest, test_multi_int64_x_expr) {
    BinaryArithmeticExprCheck<int64_t, int16_t, int64_t>(
        ::hybridse::node::kInt64, ::hybridse::node::kInt16,
        ::hybridse::node::kInt64, 8000000000L, 2L, 8000000000L * 2L,
        ::hybridse::node::kFnOpMulti);

    BinaryArithmeticExprCheck<int64_t, int32_t, int64_t>(
        ::hybridse::node::kInt64, ::hybridse::node::kInt32,
        ::hybridse::node::kInt64, 8000000000L, 2, 8000000000L * 2L,
        ::hybridse::node::kFnOpMulti);

    BinaryArithmeticExprCheck<int64_t, int64_t, int64_t>(
        ::hybridse::node::kInt64, ::hybridse::node::kInt64,
        ::hybridse::node::kInt64, 2L, 8000000000L, 2L * 8000000000L,
        ::hybridse::node::kFnOpMulti);

    BinaryArithmeticExprCheck<int64_t, float, float>(
        ::hybridse::node::kInt64, ::hybridse::node::kFloat,
        ::hybridse::node::kFloat, 2L, 12345678.5f, 2.0f * 12345678.5f,
        ::hybridse::node::kFnOpMulti);
    BinaryArithmeticExprCheck<int64_t, double, double>(
        ::hybridse::node::kInt64, ::hybridse::node::kDouble,
        ::hybridse::node::kDouble, 2, 12345678.5, 2.0 * 12345678.5,
        ::hybridse::node::kFnOpMulti);
}

TEST_F(ArithmeticIRBuilderTest, test_multi_float_x_expr) {
    BinaryArithmeticExprCheck<float, int16_t, float>(
        ::hybridse::node::kFloat, ::hybridse::node::kInt16,
        ::hybridse::node::kFloat, 2.0f, 3.0f, 2.0f * 3.0f,
        ::hybridse::node::kFnOpMulti);

    BinaryArithmeticExprCheck<float, int32_t, float>(
        ::hybridse::node::kFloat, ::hybridse::node::kInt32,
        ::hybridse::node::kFloat, 8000000000L, 2, 8000000000.0f * 2.0f,
        ::hybridse::node::kFnOpMulti);

    BinaryArithmeticExprCheck<float, int64_t, float>(
        ::hybridse::node::kFloat, ::hybridse::node::kInt64,
        ::hybridse::node::kFloat, 2.0f, 200000L, 2.0f * 200000.0f,
        ::hybridse::node::kFnOpMulti);

    BinaryArithmeticExprCheck<float, float, float>(
        ::hybridse::node::kFloat, ::hybridse::node::kFloat,
        ::hybridse::node::kFloat, 2.0f, 12345678.5f, 2.0f * 12345678.5f,
        ::hybridse::node::kFnOpMulti);
    BinaryArithmeticExprCheck<float, double, double>(
        ::hybridse::node::kFloat, ::hybridse::node::kDouble,
        ::hybridse::node::kDouble, 2.0f, 12345678.5, 2.0 * 12345678.5,
        ::hybridse::node::kFnOpMulti);
}
TEST_F(ArithmeticIRBuilderTest, test_multi_double_x_expr) {
    BinaryArithmeticExprCheck<double, int16_t, double>(
        ::hybridse::node::kDouble, ::hybridse::node::kInt16,
        ::hybridse::node::kDouble, 2.0, 3, 2.0 * 3.0,
        ::hybridse::node::kFnOpMulti);

    BinaryArithmeticExprCheck<double, int32_t, double>(
        ::hybridse::node::kDouble, ::hybridse::node::kInt32,
        ::hybridse::node::kDouble, 8000000000L, 2, 8000000000.0 * 2.0,
        ::hybridse::node::kFnOpMulti);

    BinaryArithmeticExprCheck<double, int64_t, double>(
        ::hybridse::node::kDouble, ::hybridse::node::kInt64,
        ::hybridse::node::kDouble, 2.0f, 200000L, 2.0 * 200000.0,
        ::hybridse::node::kFnOpMulti);

    BinaryArithmeticExprCheck<double, float, double>(
        ::hybridse::node::kDouble, ::hybridse::node::kFloat,
        ::hybridse::node::kDouble, 2.0, 12345678.5f,
        2.0 * static_cast<double>(12345678.5f), ::hybridse::node::kFnOpMulti);
    BinaryArithmeticExprCheck<double, double, double>(
        ::hybridse::node::kDouble, ::hybridse::node::kDouble,
        ::hybridse::node::kDouble, 2.0, 12345678.5, 2.0 * 12345678.5,
        ::hybridse::node::kFnOpMulti);
}

TEST_F(ArithmeticIRBuilderTest, test_fdiv_zero) {
    BinaryArithmeticExprCheck<int32_t, int16_t, double>(
        ::hybridse::node::kInt32, ::hybridse::node::kInt16,
        ::hybridse::node::kDouble, 2, 0, 2.0 / 0.0,
        ::hybridse::node::kFnOpFDiv);
    BinaryArithmeticExprCheck<int32_t, int16_t, double>(
        ::hybridse::node::kInt32, ::hybridse::node::kInt32,
        ::hybridse::node::kDouble, 2, 0, 2.0 / 0.0,
        ::hybridse::node::kFnOpFDiv);

    BinaryArithmeticExprCheck<int64_t, int16_t, double>(
        ::hybridse::node::kInt64, ::hybridse::node::kInt32,
        ::hybridse::node::kDouble, 99999999L, 0, 99999999.0 / 0.0,
        ::hybridse::node::kFnOpFDiv);
    BinaryArithmeticExprCheck<int32_t, float, double>(
        ::hybridse::node::kInt32, ::hybridse::node::kFloat,
        ::hybridse::node::kDouble, 2, 0.0f, 2.0 / 0.0,
        ::hybridse::node::kFnOpFDiv);
    BinaryArithmeticExprCheck<int32_t, double, double>(
        ::hybridse::node::kInt32, ::hybridse::node::kDouble,
        ::hybridse::node::kDouble, 2, 0.0, 2.0 / 0.0,
        ::hybridse::node::kFnOpFDiv);
    std::cout << std::to_string(1 / 0.0) << std::endl;
}

TEST_F(ArithmeticIRBuilderTest, test_fdiv_int32_x_expr) {
    BinaryArithmeticExprCheck<int32_t, int16_t, double>(
        ::hybridse::node::kInt32, ::hybridse::node::kInt16,
        ::hybridse::node::kDouble, 2, 3, 2.0 / 3.0,
        ::hybridse::node::kFnOpFDiv);

    BinaryArithmeticExprCheck<int32_t, int32_t, double>(
        ::hybridse::node::kInt32, ::hybridse::node::kInt32,
        ::hybridse::node::kDouble, 2, 3, 2.0 / 3.0,
        ::hybridse::node::kFnOpFDiv);

    BinaryArithmeticExprCheck<int32_t, int64_t, double>(
        ::hybridse::node::kInt32, ::hybridse::node::kInt64,
        ::hybridse::node::kDouble, 2, 8000000000L, 2.0 / 8000000000.0,
        ::hybridse::node::kFnOpFDiv);
    //
    BinaryArithmeticExprCheck<int32_t, float, double>(
        ::hybridse::node::kInt32, ::hybridse::node::kFloat,
        ::hybridse::node::kDouble, 2, 3.0f, 2.0 / 3.0,
        ::hybridse::node::kFnOpFDiv);
    BinaryArithmeticExprCheck<int32_t, double, double>(
        ::hybridse::node::kInt32, ::hybridse::node::kDouble,
        ::hybridse::node::kDouble, 2, 12345678.5, 2.0 / 12345678.5,
        ::hybridse::node::kFnOpFDiv);
}

TEST_F(ArithmeticIRBuilderTest, test_mod_int32_x_expr) {
    BinaryArithmeticExprCheck<int32_t, int16_t, int32_t>(
        ::hybridse::node::kInt32, ::hybridse::node::kInt16,
        ::hybridse::node::kInt32, 12, 5, 2, ::hybridse::node::kFnOpMod);

    BinaryArithmeticExprCheck<int32_t, int32_t, int32_t>(
        ::hybridse::node::kInt32, ::hybridse::node::kInt32,
        ::hybridse::node::kInt32, 12, 5, 2, ::hybridse::node::kFnOpMod);

    BinaryArithmeticExprCheck<int32_t, int64_t, int64_t>(
        ::hybridse::node::kInt32, ::hybridse::node::kInt64,
        ::hybridse::node::kInt64, 12, 50000L, 12L, ::hybridse::node::kFnOpMod);

    BinaryArithmeticExprCheck<int32_t, float, float>(
        ::hybridse::node::kInt32, ::hybridse::node::kFloat,
        ::hybridse::node::kFloat, 12, 5.1f, fmod(12.0f, 5.1f),
        ::hybridse::node::kFnOpMod);
    BinaryArithmeticExprCheck<int32_t, double, double>(
        ::hybridse::node::kInt32, ::hybridse::node::kDouble,
        ::hybridse::node::kDouble, 12, 5.1, fmod(12.0, 5.1),
        ::hybridse::node::kFnOpMod);
}

TEST_F(ArithmeticIRBuilderTest, test_mod_float_x_expr) {
    BinaryArithmeticExprCheck<int16_t, float, float>(
        ::hybridse::node::kInt16, ::hybridse::node::kFloat,
        ::hybridse::node::kFloat, 12, 5.1f, fmod(12.0f, 5.1f),
        ::hybridse::node::kFnOpMod);

    BinaryArithmeticExprCheck<int32_t, float, float>(
        ::hybridse::node::kInt32, ::hybridse::node::kFloat,
        ::hybridse::node::kFloat, 12, 5.1f, fmod(12.0f, 5.1f),
        ::hybridse::node::kFnOpMod);

    BinaryArithmeticExprCheck<int64_t, float, float>(
        ::hybridse::node::kInt64, ::hybridse::node::kFloat,
        ::hybridse::node::kFloat, 12L, 5.1f, fmod(12.0f, 5.1f),
        ::hybridse::node::kFnOpMod);

    BinaryArithmeticExprCheck<float, float, float>(
        ::hybridse::node::kFloat, ::hybridse::node::kFloat,
        ::hybridse::node::kFloat, 12.0f, 5.1f, fmod(12.0f, 5.1f),
        ::hybridse::node::kFnOpMod);
}

TEST_F(ArithmeticIRBuilderTest, test_mod_double_x_expr) {
    BinaryArithmeticExprCheck<int16_t, double, double>(
        ::hybridse::node::kInt16, ::hybridse::node::kDouble,
        ::hybridse::node::kDouble, 12, 5.1, fmod(12.0, 5.1),
        ::hybridse::node::kFnOpMod);

    BinaryArithmeticExprCheck<int32_t, double, double>(
        ::hybridse::node::kInt32, ::hybridse::node::kDouble,
        ::hybridse::node::kDouble, 12, 5.1, fmod(12.0, 5.1),
        ::hybridse::node::kFnOpMod);

    BinaryArithmeticExprCheck<int64_t, double, double>(
        ::hybridse::node::kInt64, ::hybridse::node::kDouble,
        ::hybridse::node::kDouble, 12L, 5.1, fmod(12.0, 5.1),
        ::hybridse::node::kFnOpMod);

    BinaryArithmeticExprCheck<float, double, double>(
        ::hybridse::node::kFloat, ::hybridse::node::kDouble,
        ::hybridse::node::kDouble, 12.0f, 5.1, fmod(12.0, 5.1),
        ::hybridse::node::kFnOpMod);
    BinaryArithmeticExprCheck<double, double, double>(
        ::hybridse::node::kDouble, ::hybridse::node::kDouble,
        ::hybridse::node::kDouble, 12.0, 5.1, fmod(12.0, 5.1),
        ::hybridse::node::kFnOpMod);
}
}  // namespace codegen
}  // namespace hybridse
int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    InitializeNativeTarget();
    InitializeNativeTargetAsmPrinter();
    return RUN_ALL_TESTS();
}
