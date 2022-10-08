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
#include <random>
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

using openmldb::base::Timestamp;
using openmldb::base::Date;
using hybridse::udf::Nullable;

class ArithmeticIRBuilderTest : public ::testing::Test {
 public:
    ArithmeticIRBuilderTest() { manager_ = new node::NodeManager(); }
    ~ArithmeticIRBuilderTest() { delete manager_; }

 protected:
    node::NodeManager *manager_;
};

template <typename RHS, typename Ret>
void UnaryArithmeticExprCheck(RHS rhs, Ret expect, ::hybridse::node::FnOperator op) {
    auto compiled_func = BuildExprFunction<Ret, RHS>([op](node::NodeManager* nm, node::ExprNode* input) {
        return nm->MakeUnaryExprNode(input, op);
    });
    ASSERT_TRUE(compiled_func.valid())
        << "UnaryArithmeticExprCheck failed: " << DataTypeTrait<RHS>::to_string()
        << " " << DataTypeTrait<Ret>::to_string()
        << " " << node::ExprOpTypeName(op);
    auto result = compiled_func(rhs);
    EXPECT_EQ(expect, result);
}

template <typename RHS, typename Ret>
void UnaryArithmeticErrorCheck(::hybridse::node::FnOperator op) {
    auto compiled_func = BuildExprFunction<Ret, RHS>([op](node::NodeManager* nm, node::ExprNode* input) {
        return nm->MakeUnaryExprNode(input, op);
    });
    EXPECT_FALSE(compiled_func.valid())
        << "UnaryArithmeticExprCheck succeed but expect fail: " << DataTypeTrait<RHS>::to_string()
        << " " << DataTypeTrait<Ret>::to_string()
        << " " << node::ExprOpTypeName(op);
}

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

TEST_F(ArithmeticIRBuilderTest, TestAddNull) {
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
        Timestamp(1590115420000L), nullptr, nullptr,
        ::hybridse::node::kFnOpAdd);
    BinaryArithmeticExprCheck<Nullable<Timestamp>, Nullable<Timestamp>,
                              Nullable<Timestamp>>(
        nullptr, Timestamp(1590115420000L), nullptr,
        ::hybridse::node::kFnOpAdd);
}
TEST_F(ArithmeticIRBuilderTest, TestSubNull) {
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
TEST_F(ArithmeticIRBuilderTest, TestSubTimestampNull0) {
    BinaryArithmeticExprCheck<Nullable<Timestamp>, Nullable<int32_t>,
                              Nullable<Timestamp>>(
        nullptr, 1, nullptr, ::hybridse::node::kFnOpMinus);
}
TEST_F(ArithmeticIRBuilderTest, TestSubTimestampNull1) {
    BinaryArithmeticExprCheck<Nullable<Timestamp>, Nullable<int64_t>,
                              Nullable<Timestamp>>(
        nullptr, 1, nullptr, ::hybridse::node::kFnOpMinus);
}
TEST_F(ArithmeticIRBuilderTest, TestSubTimestampNull2) {
    BinaryArithmeticExprCheck<Nullable<Timestamp>, Nullable<int16_t>,
                              Nullable<Timestamp>>(
        nullptr, 1, nullptr, ::hybridse::node::kFnOpMinus);
}
TEST_F(ArithmeticIRBuilderTest, TestSubTimestampNull3) {
    BinaryArithmeticExprCheck<Nullable<Timestamp>, Nullable<bool>,
                              Nullable<Timestamp>>(
        nullptr, true, nullptr, ::hybridse::node::kFnOpMinus);
}
TEST_F(ArithmeticIRBuilderTest, TestMultiNull) {
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
TEST_F(ArithmeticIRBuilderTest, TestIntDivNull) {
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
    BinaryArithmeticExprCheck<int64_t, int64_t, Nullable<int64_t>>(10, 0L, nullptr, ::hybridse::node::kFnOpDiv);
}
TEST_F(ArithmeticIRBuilderTest, TestFdivNull) {
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
TEST_F(ArithmeticIRBuilderTest, TestDivNull) {
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
TEST_F(ArithmeticIRBuilderTest, TestModNull) {
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
TEST_F(ArithmeticIRBuilderTest, TestErrorExprOp1) {
    BinaryArithmeticErrorCheck<Nullable<codec::StringRef>,
                               Nullable<codec::StringRef>,
                               Nullable<codec::StringRef>>(
        ::hybridse::node::kFnOpAdd);
}
TEST_F(ArithmeticIRBuilderTest, TestErrorExprOp2) {
    BinaryArithmeticErrorCheck<Nullable<bool>, Nullable<codec::StringRef>,
                               Nullable<codec::StringRef>>(
        ::hybridse::node::kFnOpAdd);
}
TEST_F(ArithmeticIRBuilderTest, TestErrorExprOp3) {
    BinaryArithmeticErrorCheck<Nullable<Timestamp>, Nullable<Timestamp>,
                               Nullable<Timestamp>>(
        ::hybridse::node::kFnOpMinus);
}
TEST_F(ArithmeticIRBuilderTest, TestErrorExprOp4) {
    BinaryArithmeticErrorCheck<Nullable<codec::StringRef>,
                               Nullable<codec::StringRef>,
                               Nullable<codec::StringRef>>(
        ::hybridse::node::kFnOpMinus);
}
TEST_F(ArithmeticIRBuilderTest, TestErrorExprOp5) {
    BinaryArithmeticErrorCheck<Nullable<Date>, Nullable<Date>,
                               Nullable<Date>>(
        ::hybridse::node::kFnOpMinus);
}
TEST_F(ArithmeticIRBuilderTest, TestErrorExprOp6) {
    BinaryArithmeticErrorCheck<Nullable<Timestamp>, Nullable<Timestamp>,
                               Nullable<Timestamp>>(
        ::hybridse::node::kFnOpMulti);
}
TEST_F(ArithmeticIRBuilderTest, TestErrorExprOp7) {
    BinaryArithmeticErrorCheck<Nullable<codec::StringRef>,
                               Nullable<codec::StringRef>,
                               Nullable<codec::StringRef>>(
        ::hybridse::node::kFnOpMulti);
}
TEST_F(ArithmeticIRBuilderTest, TestErrorExprOp8) {
    BinaryArithmeticErrorCheck<Nullable<Date>, Nullable<Date>,
                               Nullable<Date>>(
        ::hybridse::node::kFnOpMulti);
}
TEST_F(ArithmeticIRBuilderTest, TestErrorExprOp9) {
    BinaryArithmeticErrorCheck<Nullable<Timestamp>, Nullable<Timestamp>,
                               Nullable<Timestamp>>(
        ::hybridse::node::kFnOpFDiv);
}
TEST_F(ArithmeticIRBuilderTest, TestErrorExprOp10) {
    BinaryArithmeticErrorCheck<Nullable<codec::StringRef>,
                               Nullable<codec::StringRef>,
                               Nullable<codec::StringRef>>(
        ::hybridse::node::kFnOpFDiv);
}
TEST_F(ArithmeticIRBuilderTest, TestErrorExprOp11) {
    BinaryArithmeticErrorCheck<Nullable<Date>, Nullable<Date>,
                               Nullable<Date>>(
        ::hybridse::node::kFnOpFDiv);
}
TEST_F(ArithmeticIRBuilderTest, TestErrorExprOp12) {
    BinaryArithmeticErrorCheck<Nullable<Timestamp>, Nullable<Timestamp>,
                               Nullable<Timestamp>>(::hybridse::node::kFnOpDiv);
}
TEST_F(ArithmeticIRBuilderTest, TestErrorExprOp13) {
    BinaryArithmeticErrorCheck<Nullable<codec::StringRef>,
                               Nullable<codec::StringRef>,
                               Nullable<codec::StringRef>>(
        ::hybridse::node::kFnOpDiv);
}
TEST_F(ArithmeticIRBuilderTest, TestErrorExprOp14) {
    BinaryArithmeticErrorCheck<Nullable<Date>, Nullable<Date>,
                               Nullable<Date>>(
        ::hybridse::node::kFnOpDiv);
}
TEST_F(ArithmeticIRBuilderTest, TestErrorExprOp15) {
    BinaryArithmeticErrorCheck<Nullable<int16_t>, Nullable<float>,
                               Nullable<int16_t>>(::hybridse::node::kFnOpDiv);
}
TEST_F(ArithmeticIRBuilderTest, TestErrorExprOp16) {
    BinaryArithmeticErrorCheck<Nullable<int16_t>, Nullable<double>,
                               Nullable<int16_t>>(::hybridse::node::kFnOpDiv);
}

TEST_F(ArithmeticIRBuilderTest, TestAddInt16XExpr) {
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
TEST_F(ArithmeticIRBuilderTest, TestAddBoolXExpr) {
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
TEST_F(ArithmeticIRBuilderTest, TestAddInt32XExpr) {
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

TEST_F(ArithmeticIRBuilderTest, TestAddInt64XExpr) {
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

TEST_F(ArithmeticIRBuilderTest, TestAddTimestampExpr0) {
    BinaryArithmeticExprCheck<Timestamp, Timestamp, Timestamp>(
        ::hybridse::node::kTimestamp, ::hybridse::node::kTimestamp,
        ::hybridse::node::kTimestamp, Timestamp(8000000000L), Timestamp(1L),
        Timestamp(8000000001L), ::hybridse::node::kFnOpAdd);
}
TEST_F(ArithmeticIRBuilderTest, TestAddTimestampExpr1) {
    BinaryArithmeticExprCheck<Timestamp, int64_t, Timestamp>(
        ::hybridse::node::kTimestamp, ::hybridse::node::kInt64,
        ::hybridse::node::kTimestamp, Timestamp(8000000000L), 1L,
        Timestamp(8000000001L), ::hybridse::node::kFnOpAdd);
}
TEST_F(ArithmeticIRBuilderTest, TestAddTimestampExpr2) {
    BinaryArithmeticExprCheck<Timestamp, int32_t, Timestamp>(
        ::hybridse::node::kTimestamp, ::hybridse::node::kInt32,
        ::hybridse::node::kTimestamp, Timestamp(8000000000L), 1,
        Timestamp(8000000001L), ::hybridse::node::kFnOpAdd);
}
TEST_F(ArithmeticIRBuilderTest, TestSubTimestampExpr1) {
    BinaryArithmeticExprCheck<Timestamp, int64_t, Timestamp>(
        ::hybridse::node::kTimestamp, ::hybridse::node::kInt64,
        ::hybridse::node::kTimestamp, Timestamp(8000000001L), 1L,
        Timestamp(8000000000L), ::hybridse::node::kFnOpMinus);
}
TEST_F(ArithmeticIRBuilderTest, TestSubTimestampExpr2) {
    BinaryArithmeticExprCheck<Timestamp, int32_t, Timestamp>(
        ::hybridse::node::kTimestamp, ::hybridse::node::kInt32,
        ::hybridse::node::kTimestamp, Timestamp(8000000001L), 1,
        Timestamp(8000000000L), ::hybridse::node::kFnOpMinus);
}

TEST_F(ArithmeticIRBuilderTest, TestAddFloatXExpr) {
    BinaryArithmeticExprCheck<float, int16_t, float>(
        ::hybridse::node::kFloat, ::hybridse::node::kInt16,
        ::hybridse::node::kFloat, 1.0f, 1, 2.0f, ::hybridse::node::kFnOpAdd);

    BinaryArithmeticExprCheck<float, int32_t, float>(
        ::hybridse::node::kFloat, ::hybridse::node::kInt32,
        ::hybridse::node::kFloat, 8000000000.0f, 1, 8000000001.0f,
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

TEST_F(ArithmeticIRBuilderTest, TestAddDoubleXExpr) {
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

TEST_F(ArithmeticIRBuilderTest, TestSubInt16XExpr) {
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

TEST_F(ArithmeticIRBuilderTest, TestSubInt32XExpr) {
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

TEST_F(ArithmeticIRBuilderTest, TestSubInt64XExpr) {
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

TEST_F(ArithmeticIRBuilderTest, TestSubFloatXExpr) {
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

TEST_F(ArithmeticIRBuilderTest, TestSubDoubleXExpr) {
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

TEST_F(ArithmeticIRBuilderTest, TestMulInt16XExpr) {
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
TEST_F(ArithmeticIRBuilderTest, TestMultiInt32XExpr) {
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
TEST_F(ArithmeticIRBuilderTest, TestMultiInt64XExpr) {
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

TEST_F(ArithmeticIRBuilderTest, TestMultiFloatXExpr) {
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
TEST_F(ArithmeticIRBuilderTest, TestMultiDoubleXExpr) {
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

TEST_F(ArithmeticIRBuilderTest, TestFdivZero) {
    BinaryArithmeticExprCheck<int32_t, int16_t, Nullable<double>>(
        ::hybridse::node::kInt32, ::hybridse::node::kInt16,
        ::hybridse::node::kDouble, 2, 0, nullptr,
        ::hybridse::node::kFnOpFDiv);
    BinaryArithmeticExprCheck<int32_t, int16_t, Nullable<double>>(
        ::hybridse::node::kInt32, ::hybridse::node::kInt32,
        ::hybridse::node::kDouble, 2, 0, nullptr,
        ::hybridse::node::kFnOpFDiv);

    BinaryArithmeticExprCheck<int64_t, int16_t, Nullable<double>>(
        ::hybridse::node::kInt64, ::hybridse::node::kInt32,
        ::hybridse::node::kDouble, 99999999L, 0, nullptr,
        ::hybridse::node::kFnOpFDiv);
    BinaryArithmeticExprCheck<int32_t, float, Nullable<double>>(
        ::hybridse::node::kInt32, ::hybridse::node::kFloat,
        ::hybridse::node::kDouble, 2, 0.0f, nullptr,
        ::hybridse::node::kFnOpFDiv);
    BinaryArithmeticExprCheck<int32_t, double, Nullable<double>>(
        ::hybridse::node::kInt32, ::hybridse::node::kDouble,
        ::hybridse::node::kDouble, 2, 0.0, nullptr,
        ::hybridse::node::kFnOpFDiv);
}

TEST_F(ArithmeticIRBuilderTest, TestFdivInt32XExpr) {
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

TEST_F(ArithmeticIRBuilderTest, TestModInt32XExpr) {
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

    BinaryArithmeticExprCheck<int32_t, double, Nullable<double>>(12, 0.0, nullptr, ::hybridse::node::kFnOpMod);
}

TEST_F(ArithmeticIRBuilderTest, TestModFloatXExpr) {
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

TEST_F(ArithmeticIRBuilderTest, TestModDoubleXExpr) {
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

TEST_F(ArithmeticIRBuilderTest, BitwiseAnd) {
    auto op = ::hybridse::node::kFnOpBitwiseAnd;
    // 0x0011 & 0x0110 = 0x0010
    BinaryArithmeticExprCheck<int16_t, int16_t, int16_t>(
        ::hybridse::node::kInt16, ::hybridse::node::kInt16, ::hybridse::node::kInt16,
        3, 6, 2, op);
    BinaryArithmeticExprCheck<int32_t, int32_t, int32_t>(
        ::hybridse::node::kInt32, ::hybridse::node::kInt32, ::hybridse::node::kInt32,
        3, 6, 2, op);
    BinaryArithmeticExprCheck<int32_t, int16_t, int32_t>(
        ::hybridse::node::kInt32, ::hybridse::node::kInt16, ::hybridse::node::kInt32,
        3, 6, 2, op);
    BinaryArithmeticExprCheck<int16_t, int32_t, int32_t>(
        ::hybridse::node::kInt16, ::hybridse::node::kInt32, ::hybridse::node::kInt32,
        3, 6, 2, op);
    BinaryArithmeticExprCheck<int16_t, int64_t, int64_t>(
        ::hybridse::node::kInt16, ::hybridse::node::kInt64, ::hybridse::node::kInt64,
        3, 6L, 2L, op);
    BinaryArithmeticExprCheck<int64_t, int32_t, int64_t>(
        ::hybridse::node::kInt64, ::hybridse::node::kInt32, ::hybridse::node::kInt64,
        3L, 6, 2L, op);
    BinaryArithmeticExprCheck<int64_t, int64_t, int64_t>(
        ::hybridse::node::kInt64, ::hybridse::node::kInt64, ::hybridse::node::kInt64,
        3L, 6L, 2L, op);

    // randomize check
    std::mt19937 rng;
    std::uniform_int_distribution<int64_t> gen(INT64_MIN, INT64_MAX);
    for (int i = 0; i < 10; ++i) {
        auto left = gen(rng);
        auto right = gen(rng);
        BinaryArithmeticExprCheck<int64_t, int64_t, int64_t>(
            ::hybridse::node::kInt64, ::hybridse::node::kInt64, ::hybridse::node::kInt64,
            left, right, left & right, op);
    }
}

TEST_F(ArithmeticIRBuilderTest, BitwiseAndFail) {
    auto op = ::hybridse::node::kFnOpBitwiseAnd;
    BinaryArithmeticErrorCheck<double, int32_t, int32_t>(op);
    BinaryArithmeticErrorCheck<bool, int32_t, int32_t>(op);
    BinaryArithmeticErrorCheck<int64_t, float, int64_t>(op);
    BinaryArithmeticErrorCheck<int16_t, Nullable<codec::StringRef>, int16_t>(op);
}

TEST_F(ArithmeticIRBuilderTest, BitwiseAndNull) {
    auto op = ::hybridse::node::kFnOpBitwiseAnd;
    BinaryArithmeticExprCheck<Nullable<int32_t>, Nullable<int32_t>, Nullable<int32_t>>(
        nullptr, nullptr, nullptr, op);
    BinaryArithmeticExprCheck<Nullable<int32_t>, Nullable<int32_t>, Nullable<int32_t>>(
        nullptr, 10, nullptr, op);
    BinaryArithmeticExprCheck<Nullable<int32_t>, Nullable<int32_t>, Nullable<int32_t>>(
        10, nullptr, nullptr, op);
    BinaryArithmeticExprCheck<Nullable<int16_t>, Nullable<int32_t>, Nullable<int32_t>>(
        10, nullptr, nullptr, op);
    BinaryArithmeticExprCheck<Nullable<int64_t>, Nullable<int32_t>, Nullable<int64_t>>(
        10L, nullptr, nullptr, op);
    BinaryArithmeticExprCheck<Nullable<int64_t>, Nullable<int16_t>, Nullable<int64_t>>(
        nullptr, 20, nullptr, op);
    BinaryArithmeticExprCheck<Nullable<int64_t>, Nullable<int64_t>, Nullable<int64_t>>(
        nullptr, 20L, nullptr, op);
}

TEST_F(ArithmeticIRBuilderTest, BitwiseOr) {
    auto op = ::hybridse::node::kFnOpBitwiseOr;
    // 0x0011 | 0x0110 = 0x0111
    BinaryArithmeticExprCheck<int16_t, int16_t, int16_t>(
        ::hybridse::node::kInt16, ::hybridse::node::kInt16, ::hybridse::node::kInt16,
        3, 6, 7, op);
    BinaryArithmeticExprCheck<int32_t, int32_t, int32_t>(
        ::hybridse::node::kInt32, ::hybridse::node::kInt32, ::hybridse::node::kInt32,
        3, 6, 7, op);
    BinaryArithmeticExprCheck<int32_t, int16_t, int32_t>(
        ::hybridse::node::kInt32, ::hybridse::node::kInt16, ::hybridse::node::kInt32,
        3, 6, 7, op);
    BinaryArithmeticExprCheck<int16_t, int32_t, int32_t>(
        ::hybridse::node::kInt16, ::hybridse::node::kInt32, ::hybridse::node::kInt32,
        3, 6, 7, op);
    BinaryArithmeticExprCheck<int16_t, int64_t, int64_t>(
        ::hybridse::node::kInt16, ::hybridse::node::kInt64, ::hybridse::node::kInt64,
        3, 6L, 7L, op);
    BinaryArithmeticExprCheck<int64_t, int32_t, int64_t>(
        ::hybridse::node::kInt64, ::hybridse::node::kInt32, ::hybridse::node::kInt64,
        3L, 6, 7L, op);
    BinaryArithmeticExprCheck<int64_t, int64_t, int64_t>(
        ::hybridse::node::kInt64, ::hybridse::node::kInt64, ::hybridse::node::kInt64,
        3L, 6L, 7L, op);

    // randomize check
    std::mt19937 rng;
    std::uniform_int_distribution<int64_t> gen(INT64_MIN, INT64_MAX);
    for (int i = 0; i < 10; ++i) {
        auto left = gen(rng);
        auto right = gen(rng);
        BinaryArithmeticExprCheck<int64_t, int64_t, int64_t>(
            ::hybridse::node::kInt64, ::hybridse::node::kInt64, ::hybridse::node::kInt64,
            left, right, left | right, op);
    }
}

TEST_F(ArithmeticIRBuilderTest, BitwiseOrFail) {
    auto op = ::hybridse::node::kFnOpBitwiseOr;
    BinaryArithmeticErrorCheck<double, int32_t, int32_t>(op);
    BinaryArithmeticErrorCheck<bool, int32_t, int32_t>(op);
    BinaryArithmeticErrorCheck<int64_t, float, int64_t>(op);
    BinaryArithmeticErrorCheck<int16_t, Nullable<codec::StringRef>, int16_t>(op);
}

TEST_F(ArithmeticIRBuilderTest, BitwiseOrNull) {
    auto op = ::hybridse::node::kFnOpBitwiseOr;
    BinaryArithmeticExprCheck<Nullable<int32_t>, Nullable<int32_t>, Nullable<int32_t>>(
        nullptr, nullptr, nullptr, op);
    BinaryArithmeticExprCheck<Nullable<int32_t>, Nullable<int32_t>, Nullable<int32_t>>(
        nullptr, 10, nullptr, op);
    BinaryArithmeticExprCheck<Nullable<int32_t>, Nullable<int32_t>, Nullable<int32_t>>(
        10, nullptr, nullptr, op);
    BinaryArithmeticExprCheck<Nullable<int16_t>, Nullable<int32_t>, Nullable<int32_t>>(
        10, nullptr, nullptr, op);
    BinaryArithmeticExprCheck<Nullable<int64_t>, Nullable<int32_t>, Nullable<int64_t>>(
        10L, nullptr, nullptr, op);
    BinaryArithmeticExprCheck<Nullable<int64_t>, Nullable<int16_t>, Nullable<int64_t>>(
        nullptr, 20, nullptr, op);
    BinaryArithmeticExprCheck<Nullable<int64_t>, Nullable<int64_t>, Nullable<int64_t>>(
        nullptr, 20L, nullptr, op);
}

TEST_F(ArithmeticIRBuilderTest, BitwiseXor) {
    auto op = ::hybridse::node::kFnOpBitwiseXor;
    // 0x0011 ^ 0x0110 = 0x0101
    BinaryArithmeticExprCheck<int16_t, int16_t, int16_t>(
        ::hybridse::node::kInt16, ::hybridse::node::kInt16, ::hybridse::node::kInt16,
        3, 6, 5, op);
    BinaryArithmeticExprCheck<int32_t, int32_t, int32_t>(
        ::hybridse::node::kInt32, ::hybridse::node::kInt32, ::hybridse::node::kInt32,
        3, 6, 5, op);
    BinaryArithmeticExprCheck<int32_t, int16_t, int32_t>(
        ::hybridse::node::kInt32, ::hybridse::node::kInt16, ::hybridse::node::kInt32,
        3, 6, 5, op);
    BinaryArithmeticExprCheck<int16_t, int32_t, int32_t>(
        ::hybridse::node::kInt16, ::hybridse::node::kInt32, ::hybridse::node::kInt32,
        3, 6, 5, op);
    BinaryArithmeticExprCheck<int16_t, int64_t, int64_t>(
        ::hybridse::node::kInt16, ::hybridse::node::kInt64, ::hybridse::node::kInt64,
        3, 6L, 5L, op);
    BinaryArithmeticExprCheck<int64_t, int32_t, int64_t>(
        ::hybridse::node::kInt64, ::hybridse::node::kInt32, ::hybridse::node::kInt64,
        3L, 6, 5L, op);
    BinaryArithmeticExprCheck<int64_t, int64_t, int64_t>(
        ::hybridse::node::kInt64, ::hybridse::node::kInt64, ::hybridse::node::kInt64,
        3L, 6L, 5L, op);

    // randomize check
    std::mt19937 rng;
    std::uniform_int_distribution<int64_t> gen(INT64_MIN, INT64_MAX);
    for (int i = 0; i < 10; ++i) {
        auto left = gen(rng);
        auto right = gen(rng);
        BinaryArithmeticExprCheck<int64_t, int64_t, int64_t>(
            ::hybridse::node::kInt64, ::hybridse::node::kInt64, ::hybridse::node::kInt64,
            left, right, left ^ right, op);
    }
}

TEST_F(ArithmeticIRBuilderTest, BitwiseXorFail) {
    auto op = ::hybridse::node::kFnOpBitwiseXor;
    BinaryArithmeticErrorCheck<double, int32_t, int32_t>(op);
    BinaryArithmeticErrorCheck<bool, int32_t, int32_t>(op);
    BinaryArithmeticErrorCheck<int64_t, float, int64_t>(op);
    BinaryArithmeticErrorCheck<int16_t, Nullable<codec::StringRef>, int16_t>(op);
}

TEST_F(ArithmeticIRBuilderTest, BitwiseXorNull) {
    auto op = ::hybridse::node::kFnOpBitwiseXor;
    BinaryArithmeticExprCheck<Nullable<int32_t>, Nullable<int32_t>, Nullable<int32_t>>(
        nullptr, nullptr, nullptr, op);
    BinaryArithmeticExprCheck<Nullable<int32_t>, Nullable<int32_t>, Nullable<int32_t>>(
        nullptr, 10, nullptr, op);
    BinaryArithmeticExprCheck<Nullable<int32_t>, Nullable<int32_t>, Nullable<int32_t>>(
        10, nullptr, nullptr, op);
    BinaryArithmeticExprCheck<Nullable<int16_t>, Nullable<int32_t>, Nullable<int32_t>>(
        10, nullptr, nullptr, op);
    BinaryArithmeticExprCheck<Nullable<int64_t>, Nullable<int32_t>, Nullable<int64_t>>(
        10L, nullptr, nullptr, op);
    BinaryArithmeticExprCheck<Nullable<int64_t>, Nullable<int16_t>, Nullable<int64_t>>(
        nullptr, 20, nullptr, op);
    BinaryArithmeticExprCheck<Nullable<int64_t>, Nullable<int64_t>, Nullable<int64_t>>(
        nullptr, 20L, nullptr, op);
}

TEST_F(ArithmeticIRBuilderTest, BitwiseNot) {
    auto op = ::hybridse::node::kFnOpBitwiseNot;
    UnaryArithmeticExprCheck<int16_t, int16_t>(0, -1, op);
    UnaryArithmeticExprCheck<int32_t, int32_t>(0, -1, op);
    UnaryArithmeticExprCheck<int64_t, int64_t>(0L, -1L, op);
    // randomize check
    std::mt19937 rng;
    std::uniform_int_distribution<int64_t> gen(INT64_MIN, INT64_MAX);
    for (int i = 0; i < 10; ++i) {
        auto input = gen(rng);
        UnaryArithmeticExprCheck<int64_t, int64_t>(input, -1 - input, op);
    }
}

TEST_F(ArithmeticIRBuilderTest, BitwiseNotFail) {
    auto op = ::hybridse::node::kFnOpBitwiseNot;
    UnaryArithmeticErrorCheck<bool, bool>(op);
    UnaryArithmeticErrorCheck<float, float>(op);
    UnaryArithmeticErrorCheck<double, double>(op);
    UnaryArithmeticErrorCheck<Nullable<codec::StringRef>, Nullable<codec::StringRef>>(op);
}

TEST_F(ArithmeticIRBuilderTest, BitwiseNotNull) {
    auto op = ::hybridse::node::kFnOpBitwiseNot;
    UnaryArithmeticExprCheck<Nullable<int16_t>, Nullable<int16_t>>(nullptr, nullptr, op);
    UnaryArithmeticExprCheck<Nullable<int32_t>, Nullable<int32_t>>(nullptr, nullptr, op);
    UnaryArithmeticExprCheck<Nullable<int64_t>, Nullable<int64_t>>(nullptr, nullptr, op);
}

}  // namespace codegen
}  // namespace hybridse
int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    InitializeNativeTarget();
    InitializeNativeTargetAsmPrinter();
    return RUN_ALL_TESTS();
}
