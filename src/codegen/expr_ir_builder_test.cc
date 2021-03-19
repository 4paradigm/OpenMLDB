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

#include "codegen/expr_ir_builder.h"
#include <memory>
#include <utility>
#include "case/sql_case.h"
#include "codec/type_codec.h"
#include "codegen/ir_base_builder.h"
#include "codegen/ir_base_builder_test.h"
#include "gtest/gtest.h"
#include "llvm/ExecutionEngine/Orc/LLJIT.h"
#include "llvm/IR/Function.h"
#include "llvm/IR/IRBuilder.h"
#include "llvm/IR/InstrTypes.h"
#include "llvm/IR/LegacyPassManager.h"
#include "llvm/IR/Module.h"
#include "llvm/Support/InitLLVM.h"
#include "llvm/Support/TargetSelect.h"
#include "llvm/Support/raw_ostream.h"
#include "llvm/Transforms/AggressiveInstCombine/AggressiveInstCombine.h"
#include "llvm/Transforms/InstCombine/InstCombine.h"
#include "llvm/Transforms/Scalar.h"
#include "llvm/Transforms/Scalar/GVN.h"
#include "node/node_manager.h"
using namespace llvm;       // NOLINT
using namespace llvm::orc;  // NOLINT

using hybridse::codec::Date;
using hybridse::codec::StringRef;
using hybridse::codec::Timestamp;
using hybridse::node::ExprNode;
ExitOnError ExitOnErr;

namespace hybridse {
namespace codegen {

class ExprIRBuilderTest : public ::testing::Test {
 public:
    ExprIRBuilderTest() { manager_ = new node::NodeManager(); }
    ~ExprIRBuilderTest() { delete manager_; }

 protected:
    node::NodeManager *manager_;
};

template <typename Ret, typename... Args>
void ExprCheck(
    const std::function<node::ExprNode *(
        node::NodeManager *,
        typename std::pair<Args, node::ExprNode *>::second_type...)> &expr_func,
    Ret expect, Args... args) {
    auto compiled_func = BuildExprFunction<Ret, Args...>(expr_func);
    ASSERT_TRUE(compiled_func.valid())
        << "Fail Expr Check: "
        << "Ret: " << DataTypeTrait<Ret>::to_string << "Args: ...";

    std::ostringstream oss;
    Ret result = compiled_func(args...);
    ASSERT_EQ(expect, result);
}
template <typename Ret, typename... Args>
void ExprErrorCheck(
    const std::function<node::ExprNode *(
        node::NodeManager *,
        typename std::pair<Args, node::ExprNode *>::second_type...)>
        &expr_func) {
    auto compiled_func = BuildExprFunction<Ret, Args...>(expr_func);
    ASSERT_FALSE(compiled_func.valid());
}

void GenAddExpr(node::NodeManager *manager, ::hybridse::node::ExprNode **expr) {
    // TODO(wangtaize) free
    new ::hybridse::node::BinaryExpr(::hybridse::node::kFnOpAdd);

    ::hybridse::node::ExprNode *i32_node = (manager->MakeConstNode(1));
    ::hybridse::node::ExprNode *id_node = (manager->MakeExprIdNode("a"));
    ::hybridse::node::ExprNode *bexpr = (manager->MakeBinaryExprNode(
        i32_node, id_node, hybridse::node::kFnOpAdd));
    *expr = bexpr;
}

TEST_F(ExprIRBuilderTest, test_add_int32) {
    ExprCheck(
        [](node::NodeManager *nm, node::ExprNode *input) {
            return nm->MakeBinaryExprNode(input, nm->MakeConstNode(1),
                                          node::kFnOpAdd);
        },
        2, 1);
}

template <class V1, class V2, class R>
void BinaryExprCheck(V1 v1, V2 v2, R r, ::hybridse::node::FnOperator op) {
    ExprCheck(
        [=](node::NodeManager *nm, node::ExprNode *l, node::ExprNode *r) {
            return nm->MakeBinaryExprNode(nm->MakeConstNode(v1),
                                          nm->MakeConstNode(v2), op);
        },
        r, v1, v2);
}

TEST_F(ExprIRBuilderTest, test_add_int16_x_expr) {
    BinaryExprCheck<int16_t, int16_t, int16_t>(1, 1, 2,
                                               ::hybridse::node::kFnOpAdd);

    BinaryExprCheck<int16_t, int32_t, int32_t>(1, 1, 2,
                                               ::hybridse::node::kFnOpAdd);

    BinaryExprCheck<int16_t, int64_t, int64_t>(1, 8000000000L, 8000000001L,
                                               ::hybridse::node::kFnOpAdd);

    BinaryExprCheck<int16_t, float, float>(1, 12345678.5f, 12345678.5f + 1.0f,
                                           ::hybridse::node::kFnOpAdd);
    BinaryExprCheck<int16_t, double, double>(1, 12345678.5, 12345678.5 + 1.0,
                                             ::hybridse::node::kFnOpAdd);
}

TEST_F(ExprIRBuilderTest, test_add_int32_x_expr) {
    BinaryExprCheck<int32_t, int16_t, int32_t>(1, 1, 2,
                                               ::hybridse::node::kFnOpAdd);

    BinaryExprCheck<int32_t, int32_t, int32_t>(1, 1, 2,
                                               ::hybridse::node::kFnOpAdd);

    BinaryExprCheck<int32_t, int64_t, int64_t>(1, 8000000000L, 8000000001L,
                                               ::hybridse::node::kFnOpAdd);

    BinaryExprCheck<int32_t, float, float>(1, 12345678.5f, 12345678.5f + 1.0f,
                                           ::hybridse::node::kFnOpAdd);
    BinaryExprCheck<int32_t, double, double>(1, 12345678.5, 12345678.5 + 1.0,
                                             ::hybridse::node::kFnOpAdd);
}

TEST_F(ExprIRBuilderTest, test_add_int64_x_expr) {
    BinaryExprCheck<int64_t, int16_t, int64_t>(8000000000L, 1, 8000000001L,
                                               ::hybridse::node::kFnOpAdd);

    BinaryExprCheck<int64_t, int32_t, int64_t>(8000000000L, 1, 8000000001L,
                                               ::hybridse::node::kFnOpAdd);

    BinaryExprCheck<int64_t, int64_t, int64_t>(1L, 8000000000L, 8000000001L,
                                               ::hybridse::node::kFnOpAdd);

    BinaryExprCheck<int64_t, float, float>(1L, 12345678.5f, 12345678.5f + 1.0f,
                                           ::hybridse::node::kFnOpAdd);
    BinaryExprCheck<int64_t, double, double>(1, 12345678.5, 12345678.5 + 1.0,
                                             ::hybridse::node::kFnOpAdd);
}

TEST_F(ExprIRBuilderTest, test_add_float_x_expr) {
    BinaryExprCheck<float, int16_t, float>(1.0f, 1, 2.0f,
                                           ::hybridse::node::kFnOpAdd);

    BinaryExprCheck<float, int32_t, float>(8000000000L, 1, 8000000001L,
                                           ::hybridse::node::kFnOpAdd);

    BinaryExprCheck<float, int64_t, float>(1.0f, 200000L, 1.0f + 200000.0f,
                                           ::hybridse::node::kFnOpAdd);

    BinaryExprCheck<float, float, float>(1.0f, 12345678.5f, 12345678.5f + 1.0f,
                                         ::hybridse::node::kFnOpAdd);
    BinaryExprCheck<float, double, double>(1.0f, 12345678.5, 12345678.5 + 1.0,
                                           ::hybridse::node::kFnOpAdd);
}

TEST_F(ExprIRBuilderTest, test_add_double_x_expr) {
    BinaryExprCheck<double, int16_t, double>(1.0, 1, 2.0,
                                             ::hybridse::node::kFnOpAdd);

    BinaryExprCheck<double, int32_t, double>(8000000000L, 1, 8000000001.0,
                                             ::hybridse::node::kFnOpAdd);

    BinaryExprCheck<double, int64_t, double>(1.0f, 200000L, 200001.0,
                                             ::hybridse::node::kFnOpAdd);

    BinaryExprCheck<double, float, double>(
        1.0, 12345678.5f, static_cast<double>(12345678.5f) + 1.0,
        ::hybridse::node::kFnOpAdd);
    BinaryExprCheck<double, double, double>(1.0, 12345678.5, 12345678.5 + 1.0,
                                            ::hybridse::node::kFnOpAdd);
}

TEST_F(ExprIRBuilderTest, test_sub_int16_x_expr) {
    BinaryExprCheck<int16_t, int16_t, int16_t>(2, 1, 1,
                                               ::hybridse::node::kFnOpMinus);

    BinaryExprCheck<int16_t, int32_t, int32_t>(2, 1, 1,
                                               ::hybridse::node::kFnOpMinus);

    BinaryExprCheck<int16_t, int64_t, int64_t>(1, 8000000000L, 1L - 8000000000L,
                                               ::hybridse::node::kFnOpMinus);

    BinaryExprCheck<int16_t, float, float>(1, 12345678.5f, 1.0f - 12345678.5f,
                                           ::hybridse::node::kFnOpMinus);
    BinaryExprCheck<int16_t, double, double>(1, 12345678.5, 1.0 - 12345678.5,
                                             ::hybridse::node::kFnOpMinus);
}

TEST_F(ExprIRBuilderTest, test_sub_int32_x_expr) {
    BinaryExprCheck<int32_t, int16_t, int32_t>(2, 1, 1,
                                               ::hybridse::node::kFnOpMinus);

    BinaryExprCheck<int32_t, int32_t, int32_t>(2, 1, 1,
                                               ::hybridse::node::kFnOpMinus);

    BinaryExprCheck<int32_t, int64_t, int64_t>(1, 8000000000L, 1L - 8000000000L,
                                               ::hybridse::node::kFnOpMinus);

    BinaryExprCheck<int32_t, float, float>(1, 12345678.5f, 1.0f - 12345678.5f,
                                           ::hybridse::node::kFnOpMinus);
    BinaryExprCheck<int32_t, double, double>(1, 12345678.5, 1.0 - 12345678.5,
                                             ::hybridse::node::kFnOpMinus);
}

TEST_F(ExprIRBuilderTest, test_sub_int64_x_expr) {
    BinaryExprCheck<int64_t, int16_t, int64_t>(8000000000L, 1, 8000000000L - 1L,
                                               ::hybridse::node::kFnOpMinus);

    BinaryExprCheck<int64_t, int32_t, int64_t>(8000000000L, 1, 8000000000L - 1L,
                                               ::hybridse::node::kFnOpMinus);

    BinaryExprCheck<int64_t, int64_t, int64_t>(
        1L, 8000000000L, 1L - 8000000000L, ::hybridse::node::kFnOpMinus);

    BinaryExprCheck<int64_t, float, float>(1L, 12345678.5f, 1.0f - 12345678.5f,
                                           ::hybridse::node::kFnOpMinus);
    BinaryExprCheck<int64_t, double, double>(1, 12345678.5, 1.0 - 12345678.5,
                                             ::hybridse::node::kFnOpMinus);
}

TEST_F(ExprIRBuilderTest, test_sub_float_x_expr) {
    BinaryExprCheck<float, int16_t, float>(2.0f, 1, 1.0f,
                                           ::hybridse::node::kFnOpMinus);

    BinaryExprCheck<float, int32_t, float>(8000000000L, 1, 8000000000.0f - 1.0f,
                                           ::hybridse::node::kFnOpMinus);

    BinaryExprCheck<float, int64_t, float>(1.0f, 200000L, 1.0f - 200000.0f,
                                           ::hybridse::node::kFnOpMinus);

    BinaryExprCheck<float, float, float>(1.0f, 12345678.5f, 1.0f - 12345678.5f,
                                         ::hybridse::node::kFnOpMinus);
    BinaryExprCheck<float, double, double>(1.0f, 12345678.5, 1.0 - 12345678.5,
                                           ::hybridse::node::kFnOpMinus);
}

TEST_F(ExprIRBuilderTest, test_sub_double_x_expr) {
    BinaryExprCheck<double, int16_t, double>(2.0, 1, 1.0,
                                             ::hybridse::node::kFnOpMinus);

    BinaryExprCheck<double, int32_t, double>(8000000000L, 1, 8000000000.0 - 1.0,
                                             ::hybridse::node::kFnOpMinus);

    BinaryExprCheck<double, int64_t, double>(1.0f, 200000L, 1.0 - 200000.0,
                                             ::hybridse::node::kFnOpMinus);

    BinaryExprCheck<double, float, double>(
        1.0, 12345678.5f, 1.0 - static_cast<double>(12345678.5f),
        ::hybridse::node::kFnOpMinus);
    BinaryExprCheck<double, double, double>(1.0, 12345678.5, 1.0 - 12345678.5,
                                            ::hybridse::node::kFnOpMinus);
}

TEST_F(ExprIRBuilderTest, test_mul_int16_x_expr) {
    BinaryExprCheck<int16_t, int16_t, int16_t>(2, 3, 2 * 3,
                                               ::hybridse::node::kFnOpMulti);

    BinaryExprCheck<int16_t, int32_t, int32_t>(2, 3, 2 * 3,
                                               ::hybridse::node::kFnOpMulti);

    BinaryExprCheck<int16_t, int64_t, int64_t>(2, 8000000000L, 2L * 8000000000L,
                                               ::hybridse::node::kFnOpMulti);

    BinaryExprCheck<int16_t, float, float>(2, 12345678.5f, 2.0f * 12345678.5f,
                                           ::hybridse::node::kFnOpMulti);
    BinaryExprCheck<int16_t, double, double>(2, 12345678.5, 2.0 * 12345678.5,
                                             ::hybridse::node::kFnOpMulti);
}
TEST_F(ExprIRBuilderTest, test_multi_int32_x_expr) {
    BinaryExprCheck<int32_t, int16_t, int32_t>(2, 3, 2 * 3,
                                               ::hybridse::node::kFnOpMulti);

    BinaryExprCheck<int32_t, int32_t, int32_t>(2, 3, 2 * 3,
                                               ::hybridse::node::kFnOpMulti);

    BinaryExprCheck<int32_t, int64_t, int64_t>(2, 8000000000L, 2L * 8000000000L,
                                               ::hybridse::node::kFnOpMulti);

    BinaryExprCheck<int32_t, float, float>(2, 12345678.5f, 2.0f * 12345678.5f,
                                           ::hybridse::node::kFnOpMulti);
    BinaryExprCheck<int32_t, double, double>(2, 12345678.5, 2.0 * 12345678.5,
                                             ::hybridse::node::kFnOpMulti);
}
TEST_F(ExprIRBuilderTest, test_multi_int64_x_expr) {
    BinaryExprCheck<int64_t, int16_t, int64_t>(
        8000000000L, 2L, 8000000000L * 2L, ::hybridse::node::kFnOpMulti);

    BinaryExprCheck<int64_t, int32_t, int64_t>(8000000000L, 2, 8000000000L * 2L,
                                               ::hybridse::node::kFnOpMulti);

    BinaryExprCheck<int64_t, int64_t, int64_t>(
        2L, 8000000000L, 2L * 8000000000L, ::hybridse::node::kFnOpMulti);

    BinaryExprCheck<int64_t, float, float>(2L, 12345678.5f, 2.0f * 12345678.5f,
                                           ::hybridse::node::kFnOpMulti);
    BinaryExprCheck<int64_t, double, double>(2, 12345678.5, 2.0 * 12345678.5,
                                             ::hybridse::node::kFnOpMulti);
}

TEST_F(ExprIRBuilderTest, test_multi_float_x_expr) {
    BinaryExprCheck<float, int16_t, float>(2.0f, 3.0f, 2.0f * 3.0f,
                                           ::hybridse::node::kFnOpMulti);

    BinaryExprCheck<float, int32_t, float>(8000000000L, 2, 8000000000.0f * 2.0f,
                                           ::hybridse::node::kFnOpMulti);

    BinaryExprCheck<float, int64_t, float>(2.0f, 200000L, 2.0f * 200000.0f,
                                           ::hybridse::node::kFnOpMulti);

    BinaryExprCheck<float, float, float>(2.0f, 12345678.5f, 2.0f * 12345678.5f,
                                         ::hybridse::node::kFnOpMulti);
    BinaryExprCheck<float, double, double>(2.0f, 12345678.5, 2.0 * 12345678.5,
                                           ::hybridse::node::kFnOpMulti);
}
TEST_F(ExprIRBuilderTest, test_multi_double_x_expr) {
    BinaryExprCheck<double, int16_t, double>(2.0, 3, 2.0 * 3.0,
                                             ::hybridse::node::kFnOpMulti);

    BinaryExprCheck<double, int32_t, double>(8000000000L, 2, 8000000000.0 * 2.0,
                                             ::hybridse::node::kFnOpMulti);

    BinaryExprCheck<double, int64_t, double>(2.0f, 200000L, 2.0 * 200000.0,
                                             ::hybridse::node::kFnOpMulti);

    BinaryExprCheck<double, float, double>(
        2.0, 12345678.5f, 2.0 * static_cast<double>(12345678.5f),
        ::hybridse::node::kFnOpMulti);
    BinaryExprCheck<double, double, double>(2.0, 12345678.5, 2.0 * 12345678.5,
                                            ::hybridse::node::kFnOpMulti);
}

TEST_F(ExprIRBuilderTest, test_fdiv_int32_x_expr) {
    BinaryExprCheck<int32_t, int16_t, double>(2, 3, 2.0 / 3.0,
                                              ::hybridse::node::kFnOpFDiv);

    BinaryExprCheck<int32_t, int32_t, double>(2, 3, 2.0 / 3.0,
                                              ::hybridse::node::kFnOpFDiv);

    BinaryExprCheck<int32_t, int64_t, double>(
        2, 8000000000L, 2.0 / 8000000000.0, ::hybridse::node::kFnOpFDiv);

    BinaryExprCheck<int32_t, float, double>(2, 3.0f, 2.0 / 3.0,
                                            ::hybridse::node::kFnOpFDiv);
    BinaryExprCheck<int32_t, double, double>(2, 12345678.5, 2.0 / 12345678.5,
                                             ::hybridse::node::kFnOpFDiv);
}

TEST_F(ExprIRBuilderTest, test_mod_int32_x_expr) {
    BinaryExprCheck<int32_t, int16_t, int32_t>(12, 5, 2,
                                               ::hybridse::node::kFnOpMod);

    BinaryExprCheck<int32_t, int32_t, int32_t>(12, 5, 2,
                                               ::hybridse::node::kFnOpMod);

    BinaryExprCheck<int32_t, int64_t, int64_t>(12, 50000L, 12L,
                                               ::hybridse::node::kFnOpMod);

    BinaryExprCheck<int32_t, float, float>(12, 5.1f, fmod(12.0f, 5.1f),
                                           ::hybridse::node::kFnOpMod);
    BinaryExprCheck<int32_t, double, double>(12, 5.1, fmod(12.0, 5.1),
                                             ::hybridse::node::kFnOpMod);
}

TEST_F(ExprIRBuilderTest, test_mod_float_x_expr) {
    BinaryExprCheck<int16_t, float, float>(12, 5.1f, fmod(12.0f, 5.1f),
                                           ::hybridse::node::kFnOpMod);

    BinaryExprCheck<int32_t, float, float>(12, 5.1f, fmod(12.0f, 5.1f),
                                           ::hybridse::node::kFnOpMod);

    BinaryExprCheck<int64_t, float, float>(12L, 5.1f, fmod(12.0f, 5.1f),
                                           ::hybridse::node::kFnOpMod);

    BinaryExprCheck<float, float, float>(12.0f, 5.1f, fmod(12.0f, 5.1f),
                                         ::hybridse::node::kFnOpMod);
}

TEST_F(ExprIRBuilderTest, test_mod_double_x_expr) {
    BinaryExprCheck<int16_t, double, double>(12, 5.1, fmod(12.0, 5.1),
                                             ::hybridse::node::kFnOpMod);

    BinaryExprCheck<int32_t, double, double>(12, 5.1, fmod(12.0, 5.1),
                                             ::hybridse::node::kFnOpMod);

    BinaryExprCheck<int64_t, double, double>(12L, 5.1, fmod(12.0, 5.1),
                                             ::hybridse::node::kFnOpMod);

    BinaryExprCheck<float, double, double>(12.0f, 5.1, fmod(12.0, 5.1),
                                           ::hybridse::node::kFnOpMod);
    BinaryExprCheck<double, double, double>(12.0, 5.1, fmod(12.0, 5.1),
                                            ::hybridse::node::kFnOpMod);
}

TEST_F(ExprIRBuilderTest, test_eq_expr_true) {
    BinaryExprCheck<int16_t, int16_t, bool>(1, 1, true,
                                            ::hybridse::node::kFnOpEq);

    BinaryExprCheck<int32_t, int32_t, bool>(1, 1, true,
                                            ::hybridse::node::kFnOpEq);

    BinaryExprCheck<int64_t, int64_t, bool>(1, 1, true,
                                            ::hybridse::node::kFnOpEq);

    BinaryExprCheck<float, float, bool>(1.0f, 1.0f, true,
                                        ::hybridse::node::kFnOpEq);

    BinaryExprCheck<double, double, bool>(1.0, 1.0, true,
                                          ::hybridse::node::kFnOpEq);

    BinaryExprCheck<int32_t, float, bool>(1, 1.0f, true,
                                          ::hybridse::node::kFnOpEq);

    BinaryExprCheck<int32_t, double, bool>(1, 1.0, true,
                                           ::hybridse::node::kFnOpEq);
}

TEST_F(ExprIRBuilderTest, test_eq_expr_false) {
    BinaryExprCheck<int16_t, int16_t, bool>(1, 2, false,
                                            ::hybridse::node::kFnOpEq);

    BinaryExprCheck<int32_t, int32_t, bool>(1, 2, false,
                                            ::hybridse::node::kFnOpEq);

    BinaryExprCheck<int64_t, int64_t, bool>(1, 2, false,
                                            ::hybridse::node::kFnOpEq);

    BinaryExprCheck<float, float, bool>(1.0f, 1.1f, false,
                                        ::hybridse::node::kFnOpEq);

    BinaryExprCheck<double, double, bool>(1.0, 1.1, false,
                                          ::hybridse::node::kFnOpEq);
}

TEST_F(ExprIRBuilderTest, test_neq_expr_true) {
    BinaryExprCheck<int16_t, int16_t, bool>(1, 2, true,
                                            ::hybridse::node::kFnOpNeq);

    BinaryExprCheck<int32_t, int32_t, bool>(1, 2, true,
                                            ::hybridse::node::kFnOpNeq);

    BinaryExprCheck<int64_t, int64_t, bool>(1, 2, true,
                                            ::hybridse::node::kFnOpNeq);

    BinaryExprCheck<float, float, bool>(1.0f, 1.1f, true,
                                        ::hybridse::node::kFnOpNeq);

    BinaryExprCheck<double, double, bool>(1.0, 1.1, true,
                                          ::hybridse::node::kFnOpNeq);
}

TEST_F(ExprIRBuilderTest, test_neq_expr_false) {
    BinaryExprCheck<int16_t, int16_t, bool>(1, 1, false,
                                            ::hybridse::node::kFnOpNeq);

    BinaryExprCheck<int32_t, int32_t, bool>(1, 1, false,
                                            ::hybridse::node::kFnOpNeq);

    BinaryExprCheck<int64_t, int64_t, bool>(1, 1, false,
                                            ::hybridse::node::kFnOpNeq);

    BinaryExprCheck<float, float, bool>(1.0f, 1.0f, false,
                                        ::hybridse::node::kFnOpNeq);

    BinaryExprCheck<double, double, bool>(1.0, 1.0, false,
                                          ::hybridse::node::kFnOpNeq);

    BinaryExprCheck<int32_t, float, bool>(1, 1.0f, false,
                                          ::hybridse::node::kFnOpNeq);

    BinaryExprCheck<int32_t, double, bool>(1, 1.0, false,
                                           ::hybridse::node::kFnOpNeq);
}

TEST_F(ExprIRBuilderTest, test_gt_expr_true) {
    BinaryExprCheck<int16_t, int16_t, bool>(2, 1, true,
                                            ::hybridse::node::kFnOpGt);

    BinaryExprCheck<int32_t, int32_t, bool>(2, 1, true,
                                            ::hybridse::node::kFnOpGt);

    BinaryExprCheck<int64_t, int64_t, bool>(2, 1, true,
                                            ::hybridse::node::kFnOpGt);

    BinaryExprCheck<float, float, bool>(1.1f, 1.0f, true,
                                        ::hybridse::node::kFnOpGt);

    BinaryExprCheck<double, double, bool>(1.1, 1.0, true,
                                          ::hybridse::node::kFnOpGt);

    BinaryExprCheck<int32_t, float, bool>(2, 1.9f, true,
                                          ::hybridse::node::kFnOpGt);

    BinaryExprCheck<int32_t, double, bool>(2, 1.9, true,
                                           ::hybridse::node::kFnOpGt);
}

TEST_F(ExprIRBuilderTest, test_gt_expr_false) {
    BinaryExprCheck<int16_t, int16_t, bool>(2, 2, false,
                                            ::hybridse::node::kFnOpGt);
    BinaryExprCheck<int16_t, int16_t, bool>(2, 3, false,
                                            ::hybridse::node::kFnOpGt);

    BinaryExprCheck<int32_t, int32_t, bool>(2, 2, false,
                                            ::hybridse::node::kFnOpGt);
    BinaryExprCheck<int32_t, int32_t, bool>(2, 3, false,
                                            ::hybridse::node::kFnOpGt);

    BinaryExprCheck<int64_t, int64_t, bool>(2, 2, false,
                                            ::hybridse::node::kFnOpGt);
    BinaryExprCheck<int64_t, int64_t, bool>(2, 3, false,
                                            ::hybridse::node::kFnOpGt);

    BinaryExprCheck<float, float, bool>(1.1f, 1.2f, false,
                                        ::hybridse::node::kFnOpGt);

    BinaryExprCheck<double, double, bool>(1.1, 1.2, false,
                                          ::hybridse::node::kFnOpGt);

    BinaryExprCheck<int32_t, float, bool>(2, 2.1f, false,
                                          ::hybridse::node::kFnOpGt);

    BinaryExprCheck<int32_t, double, bool>(2, 2.1, false,
                                           ::hybridse::node::kFnOpGt);
}
TEST_F(ExprIRBuilderTest, test_ge_expr_true) {
    BinaryExprCheck<int16_t, int16_t, bool>(2, 1, true,
                                            ::hybridse::node::kFnOpGe);
    BinaryExprCheck<int16_t, int16_t, bool>(2, 2, true,
                                            ::hybridse::node::kFnOpGe);

    BinaryExprCheck<int32_t, int32_t, bool>(2, 1, true,
                                            ::hybridse::node::kFnOpGe);
    BinaryExprCheck<int32_t, int32_t, bool>(2, 2, true,
                                            ::hybridse::node::kFnOpGe);

    BinaryExprCheck<int64_t, int64_t, bool>(2, 1, true,
                                            ::hybridse::node::kFnOpGe);
    BinaryExprCheck<int64_t, int64_t, bool>(2, 2, true,
                                            ::hybridse::node::kFnOpGe);

    BinaryExprCheck<float, float, bool>(1.1f, 1.0f, true,
                                        ::hybridse::node::kFnOpGe);
    BinaryExprCheck<float, float, bool>(1.1f, 1.1f, true,
                                        ::hybridse::node::kFnOpGe);

    BinaryExprCheck<double, double, bool>(1.1, 1.0, true,
                                          ::hybridse::node::kFnOpGe);

    BinaryExprCheck<double, double, bool>(1.1, 1.1, true,
                                          ::hybridse::node::kFnOpGe);
    BinaryExprCheck<int32_t, float, bool>(2, 1.9f, true,
                                          ::hybridse::node::kFnOpGe);
    BinaryExprCheck<int32_t, float, bool>(2, 2.0f, true,
                                          ::hybridse::node::kFnOpGe);

    BinaryExprCheck<int32_t, double, bool>(2, 1.9, true,
                                           ::hybridse::node::kFnOpGe);
    BinaryExprCheck<int32_t, double, bool>(2, 2.0, true,
                                           ::hybridse::node::kFnOpGe);
}

TEST_F(ExprIRBuilderTest, test_ge_expr_false) {
    BinaryExprCheck<int16_t, int16_t, bool>(2, 3, false,
                                            ::hybridse::node::kFnOpGe);

    BinaryExprCheck<int32_t, int32_t, bool>(2, 3, false,
                                            ::hybridse::node::kFnOpGe);

    BinaryExprCheck<int64_t, int64_t, bool>(2, 3, false,
                                            ::hybridse::node::kFnOpGe);

    BinaryExprCheck<float, float, bool>(1.1f, 1.2f, false,
                                        ::hybridse::node::kFnOpGe);

    BinaryExprCheck<double, double, bool>(1.1, 1.2, false,
                                          ::hybridse::node::kFnOpGe);

    BinaryExprCheck<int32_t, float, bool>(2, 2.1f, false,
                                          ::hybridse::node::kFnOpGe);
}

TEST_F(ExprIRBuilderTest, test_lt_expr_true) {
    BinaryExprCheck<int16_t, int16_t, bool>(2, 3, true,
                                            ::hybridse::node::kFnOpLt);

    BinaryExprCheck<int32_t, int32_t, bool>(2, 3, true,
                                            ::hybridse::node::kFnOpLt);

    BinaryExprCheck<int64_t, int64_t, bool>(2, 3, true,
                                            ::hybridse::node::kFnOpLt);

    BinaryExprCheck<float, float, bool>(1.1f, 1.2f, true,
                                        ::hybridse::node::kFnOpLt);

    BinaryExprCheck<double, double, bool>(1.1, 1.2, true,
                                          ::hybridse::node::kFnOpLt);

    BinaryExprCheck<int32_t, float, bool>(2, 2.1f, true,
                                          ::hybridse::node::kFnOpLt);
}

TEST_F(ExprIRBuilderTest, test_lt_expr_false) {
    BinaryExprCheck<int16_t, int16_t, bool>(2, 1, false,
                                            ::hybridse::node::kFnOpLt);
    BinaryExprCheck<int16_t, int16_t, bool>(2, 2, false,
                                            ::hybridse::node::kFnOpLt);

    BinaryExprCheck<int32_t, int32_t, bool>(2, 1, false,
                                            ::hybridse::node::kFnOpLt);
    BinaryExprCheck<int32_t, int32_t, bool>(2, 2, false,
                                            ::hybridse::node::kFnOpLt);

    BinaryExprCheck<int64_t, int64_t, bool>(2, 1, false,
                                            ::hybridse::node::kFnOpLt);
    BinaryExprCheck<int64_t, int64_t, bool>(2, 2, false,
                                            ::hybridse::node::kFnOpLt);

    BinaryExprCheck<float, float, bool>(1.1f, 1.0f, false,
                                        ::hybridse::node::kFnOpLt);
    BinaryExprCheck<float, float, bool>(1.1f, 1.1f, false,
                                        ::hybridse::node::kFnOpLt);

    BinaryExprCheck<double, double, bool>(1.1, 1.0, false,
                                          ::hybridse::node::kFnOpLt);

    BinaryExprCheck<double, double, bool>(1.1, 1.1, false,
                                          ::hybridse::node::kFnOpLt);
    BinaryExprCheck<int32_t, float, bool>(2, 1.9f, false,
                                          ::hybridse::node::kFnOpLt);
    BinaryExprCheck<int32_t, float, bool>(2, 2.0f, false,
                                          ::hybridse::node::kFnOpLt);

    BinaryExprCheck<int32_t, double, bool>(2, 1.9, false,
                                           ::hybridse::node::kFnOpLt);
    BinaryExprCheck<int32_t, double, bool>(2, 2.0, false,
                                           ::hybridse::node::kFnOpLt);
}

TEST_F(ExprIRBuilderTest, test_and_expr_true) {
    BinaryExprCheck<bool, bool, bool>(true, true, true,
                                      ::hybridse::node::kFnOpAnd);
}

TEST_F(ExprIRBuilderTest, test_and_expr_false) {
    BinaryExprCheck<bool, bool, bool>(false, true, false,
                                      ::hybridse::node::kFnOpAnd);
    BinaryExprCheck<bool, bool, bool>(false, false, false,
                                      ::hybridse::node::kFnOpAnd);
    BinaryExprCheck<bool, bool, bool>(true, false, false,
                                      ::hybridse::node::kFnOpAnd);
}

TEST_F(ExprIRBuilderTest, test_or_expr_true) {
    BinaryExprCheck<bool, bool, bool>(true, true, true,
                                      ::hybridse::node::kFnOpOr);
    BinaryExprCheck<bool, bool, bool>(true, false, true,
                                      ::hybridse::node::kFnOpOr);
    BinaryExprCheck<bool, bool, bool>(false, true, true,
                                      ::hybridse::node::kFnOpOr);
}

TEST_F(ExprIRBuilderTest, test_or_expr_false) {
    BinaryExprCheck<bool, bool, bool>(false, false, false,
                                      ::hybridse::node::kFnOpOr);
}

TEST_F(ExprIRBuilderTest, test_get_field) {
    auto schema = udf::MakeLiteralSchema<int16_t, int32_t, int64_t, float,
                                         double, codec::Timestamp, codec::Date,
                                         codec::StringRef>();
    codec::RowBuilder row_builder(schema);
    size_t row_size = row_builder.CalTotalLength(5);
    int8_t *buf = reinterpret_cast<int8_t *>(malloc(row_size));
    row_builder.SetBuffer(buf, row_size);
    row_builder.AppendInt16(16);
    row_builder.AppendInt32(32);
    row_builder.AppendInt64(64);
    row_builder.AppendFloat(3.14f);
    row_builder.AppendDouble(1.0);
    row_builder.AppendTimestamp(1590115420000L);
    row_builder.AppendDate(2009, 7, 1);
    row_builder.AppendString("hello", 5);
    codec::Row *row =
        new codec::Row(base::RefCountedSlice::Create(buf, row_size));
    udf::LiteralTypedRow<int16_t, int32_t, int64_t, float, double,
                         codec::Timestamp, codec::Date, codec::StringRef>
        typed_row(reinterpret_cast<int8_t *>(row));

    auto make_get_field = [](node::NodeManager *nm, node::ExprNode *input,
                             size_t idx) {
        auto row_type =
            dynamic_cast<const node::RowTypeNode *>(input->GetOutputType());
        auto source = row_type->schemas_ctx()->GetSchemaSource(0);
        auto column_name = source->GetColumnName(idx);
        auto column_id = source->GetColumnID(idx);
        return nm->MakeGetFieldExpr(input, column_name, column_id);
    };

    ExprCheck([&](node::NodeManager *nm,
                  ExprNode *input) { return make_get_field(nm, input, 0); },
              (int16_t)16, typed_row);
    ExprCheck([&](node::NodeManager *nm,
                  ExprNode *input) { return make_get_field(nm, input, 1); },
              32, typed_row);
    ExprCheck([&](node::NodeManager *nm,
                  ExprNode *input) { return make_get_field(nm, input, 2); },
              (int64_t)64L, typed_row);
    ExprCheck([&](node::NodeManager *nm,
                  ExprNode *input) { return make_get_field(nm, input, 3); },
              3.14f, typed_row);
    ExprCheck([&](node::NodeManager *nm,
                  ExprNode *input) { return make_get_field(nm, input, 4); },
              1.0, typed_row);
    ExprCheck([&](node::NodeManager *nm,
                  ExprNode *input) { return make_get_field(nm, input, 5); },
              codec::Timestamp(1590115420000L), typed_row);
    ExprCheck([&](node::NodeManager *nm,
                  ExprNode *input) { return make_get_field(nm, input, 6); },
              codec::Date(2009, 7, 1), typed_row);
    ExprCheck([&](node::NodeManager *nm,
                  ExprNode *input) { return make_get_field(nm, input, 7); },
              codec::StringRef(5, "hello"), typed_row);
}

TEST_F(ExprIRBuilderTest, test_build_lambda) {
    ExprCheck(
        [](node::NodeManager *nm, ExprNode *x, ExprNode *y) {
            auto arg1 = nm->MakeExprIdNode("x");
            auto arg2 = nm->MakeExprIdNode("y");
            arg1->SetOutputType(nm->MakeTypeNode(node::kInt32));
            arg2->SetOutputType(nm->MakeTypeNode(node::kInt32));
            auto body = nm->MakeBinaryExprNode(arg1, arg2, node::kFnOpAdd);
            auto lambda = nm->MakeLambdaNode({arg1, arg2}, body);
            return nm->MakeFuncNode(lambda, {x, y}, nullptr);
        },
        11, 1, 10);
}

void CondExprCheck(const udf::Nullable<bool> &cond_val,
                   const udf::Nullable<codec::StringRef> &left_val,
                   const udf::Nullable<codec::StringRef> &right_val,
                   const udf::Nullable<codec::StringRef> &result) {
    ExprCheck(
        [=](node::NodeManager *nm, node::ExprNode *cond, node::ExprNode *l,
            node::ExprNode *r) { return nm->MakeCondExpr(cond, l, r); },
        result, cond_val, left_val, right_val);
}

TEST_F(ExprIRBuilderTest, test_cond_expr) {
    CondExprCheck(true, codec::StringRef("left"), codec::StringRef("right"),
                  codec::StringRef("left"));
    CondExprCheck(true, codec::StringRef("left"), nullptr,
                  codec::StringRef("left"));
    CondExprCheck(true, nullptr, codec::StringRef("right"), nullptr);
    CondExprCheck(true, nullptr, nullptr, nullptr);

    CondExprCheck(false, codec::StringRef("left"), codec::StringRef("right"),
                  codec::StringRef("right"));
    CondExprCheck(false, codec::StringRef("left"), nullptr, nullptr);
    CondExprCheck(false, nullptr, codec::StringRef("right"),
                  codec::StringRef("right"));
    CondExprCheck(false, nullptr, nullptr, nullptr);

    CondExprCheck(nullptr, codec::StringRef("left"), codec::StringRef("right"),
                  codec::StringRef("right"));
    CondExprCheck(nullptr, codec::StringRef("left"), nullptr, nullptr);
    CondExprCheck(nullptr, nullptr, codec::StringRef("right"),
                  codec::StringRef("right"));
    CondExprCheck(nullptr, nullptr, nullptr, nullptr);
}

void CaseWhenExprCheck(const udf::Nullable<bool> &cond_val,
                       const udf::Nullable<codec::StringRef> &left_val,
                       const udf::Nullable<codec::StringRef> &right_val,
                       const udf::Nullable<codec::StringRef> &result) {
    ExprCheck(
        [=](node::NodeManager *nm, node::ExprNode *cond, node::ExprNode *l,
            node::ExprNode *r) {
            return nm->MakeSearchedCaseWhenNode(
                nm->MakeExprList(nm->MakeWhenNode(cond, l)), r);
        },
        result, cond_val, left_val, right_val);
}

TEST_F(ExprIRBuilderTest, test_case_when_expr) {
    CaseWhenExprCheck(true, codec::StringRef("left"), codec::StringRef("right"),
                      codec::StringRef("left"));
    CaseWhenExprCheck(true, codec::StringRef("left"), nullptr,
                      codec::StringRef("left"));
    CaseWhenExprCheck(true, nullptr, codec::StringRef("right"), nullptr);
    CaseWhenExprCheck(true, nullptr, nullptr, nullptr);

    CaseWhenExprCheck(false, codec::StringRef("left"),
                      codec::StringRef("right"), codec::StringRef("right"));
    CaseWhenExprCheck(false, codec::StringRef("left"), nullptr, nullptr);
    CaseWhenExprCheck(false, nullptr, codec::StringRef("right"),
                      codec::StringRef("right"));
    CaseWhenExprCheck(false, nullptr, nullptr, nullptr);

    CaseWhenExprCheck(nullptr, codec::StringRef("left"),
                      codec::StringRef("right"), codec::StringRef("right"));
    CaseWhenExprCheck(nullptr, codec::StringRef("left"), nullptr, nullptr);
    CaseWhenExprCheck(nullptr, nullptr, codec::StringRef("right"),
                      codec::StringRef("right"));
    CaseWhenExprCheck(nullptr, nullptr, nullptr, nullptr);
}

TEST_F(ExprIRBuilderTest, test_is_null_expr) {
    auto make_if_null = [](node::NodeManager *nm, node::ExprNode *input) {
        return nm->MakeUnaryExprNode(input, node::kFnOpIsNull);
    };
    ExprCheck<bool, udf::Nullable<int32_t>>(make_if_null, false, 1);
    ExprCheck<bool, udf::Nullable<int32_t>>(make_if_null, true, nullptr);
}
}  // namespace codegen
}  // namespace hybridse

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    InitializeNativeTarget();
    InitializeNativeTargetAsmPrinter();
    return RUN_ALL_TESTS();
}
