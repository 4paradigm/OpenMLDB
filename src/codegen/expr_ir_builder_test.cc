/*
 * ir_base_builder_test.cc
 * Copyright (C) 4paradigm.com 2019 wangtaize <wangtaize@4paradigm.com>
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

using fesql::codec::Date;
using fesql::codec::StringRef;
using fesql::codec::Timestamp;
using fesql::node::ExprNode;
ExitOnError ExitOnErr;

namespace fesql {
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
    Ret result = compiled_func(args...);
    ASSERT_EQ(expect, result);
}

void GenAddExpr(node::NodeManager *manager, ::fesql::node::ExprNode **expr) {
    // TODO(wangtaize) free
    new ::fesql::node::BinaryExpr(::fesql::node::kFnOpAdd);

    ::fesql::node::ExprNode *i32_node = (manager->MakeConstNode(1));
    ::fesql::node::ExprNode *id_node = (manager->MakeExprIdNode("a", 0));
    ::fesql::node::ExprNode *bexpr =
        (manager->MakeBinaryExprNode(i32_node, id_node, fesql::node::kFnOpAdd));
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
void BinaryExprCheck(V1 v1, V2 v2, R r, ::fesql::node::FnOperator op) {
    ExprCheck(
        [=](node::NodeManager *nm, node::ExprNode *l, node::ExprNode *r) {
            return nm->MakeBinaryExprNode(nm->MakeConstNode(v1),
                                          nm->MakeConstNode(v2), op);
        },
        r, v1, v2);
}

TEST_F(ExprIRBuilderTest, test_add_int16_x_expr) {
    BinaryExprCheck<int16_t, int16_t, int16_t>(1, 1, 2,
                                               ::fesql::node::kFnOpAdd);

    BinaryExprCheck<int16_t, int32_t, int32_t>(1, 1, 2,
                                               ::fesql::node::kFnOpAdd);

    BinaryExprCheck<int16_t, int64_t, int64_t>(1, 8000000000L, 8000000001L,
                                               ::fesql::node::kFnOpAdd);

    BinaryExprCheck<int16_t, float, float>(1, 12345678.5f, 12345678.5f + 1.0f,
                                           ::fesql::node::kFnOpAdd);
    BinaryExprCheck<int16_t, double, double>(1, 12345678.5, 12345678.5 + 1.0,
                                             ::fesql::node::kFnOpAdd);
}

TEST_F(ExprIRBuilderTest, test_add_int32_x_expr) {
    BinaryExprCheck<int32_t, int16_t, int32_t>(1, 1, 2,
                                               ::fesql::node::kFnOpAdd);

    BinaryExprCheck<int32_t, int32_t, int32_t>(1, 1, 2,
                                               ::fesql::node::kFnOpAdd);

    BinaryExprCheck<int32_t, int64_t, int64_t>(1, 8000000000L, 8000000001L,
                                               ::fesql::node::kFnOpAdd);

    BinaryExprCheck<int32_t, float, float>(1, 12345678.5f, 12345678.5f + 1.0f,
                                           ::fesql::node::kFnOpAdd);
    BinaryExprCheck<int32_t, double, double>(1, 12345678.5, 12345678.5 + 1.0,
                                             ::fesql::node::kFnOpAdd);
}

TEST_F(ExprIRBuilderTest, test_add_int64_x_expr) {
    BinaryExprCheck<int64_t, int16_t, int64_t>(8000000000L, 1, 8000000001L,
                                               ::fesql::node::kFnOpAdd);

    BinaryExprCheck<int64_t, int32_t, int64_t>(8000000000L, 1, 8000000001L,
                                               ::fesql::node::kFnOpAdd);

    BinaryExprCheck<int64_t, int64_t, int64_t>(1L, 8000000000L, 8000000001L,
                                               ::fesql::node::kFnOpAdd);

    BinaryExprCheck<int64_t, float, float>(1L, 12345678.5f, 12345678.5f + 1.0f,
                                           ::fesql::node::kFnOpAdd);
    BinaryExprCheck<int64_t, double, double>(1, 12345678.5, 12345678.5 + 1.0,
                                             ::fesql::node::kFnOpAdd);
}

TEST_F(ExprIRBuilderTest, test_add_float_x_expr) {
    BinaryExprCheck<float, int16_t, float>(1.0f, 1, 2.0f,
                                           ::fesql::node::kFnOpAdd);

    BinaryExprCheck<float, int32_t, float>(8000000000L, 1, 8000000001L,
                                           ::fesql::node::kFnOpAdd);

    BinaryExprCheck<float, int64_t, float>(1.0f, 200000L, 1.0f + 200000.0f,
                                           ::fesql::node::kFnOpAdd);

    BinaryExprCheck<float, float, float>(1.0f, 12345678.5f, 12345678.5f + 1.0f,
                                         ::fesql::node::kFnOpAdd);
    BinaryExprCheck<float, double, double>(1.0f, 12345678.5, 12345678.5 + 1.0,
                                           ::fesql::node::kFnOpAdd);
}

TEST_F(ExprIRBuilderTest, test_add_double_x_expr) {
    BinaryExprCheck<double, int16_t, double>(1.0, 1, 2.0,
                                             ::fesql::node::kFnOpAdd);

    BinaryExprCheck<double, int32_t, double>(8000000000L, 1, 8000000001.0,
                                             ::fesql::node::kFnOpAdd);

    BinaryExprCheck<double, int64_t, double>(1.0f, 200000L, 200001.0,
                                             ::fesql::node::kFnOpAdd);

    BinaryExprCheck<double, float, double>(
        1.0, 12345678.5f, static_cast<double>(12345678.5f) + 1.0,
        ::fesql::node::kFnOpAdd);
    BinaryExprCheck<double, double, double>(1.0, 12345678.5, 12345678.5 + 1.0,
                                            ::fesql::node::kFnOpAdd);
}

TEST_F(ExprIRBuilderTest, test_sub_int16_x_expr) {
    BinaryExprCheck<int16_t, int16_t, int16_t>(2, 1, 1,
                                               ::fesql::node::kFnOpMinus);

    BinaryExprCheck<int16_t, int32_t, int32_t>(2, 1, 1,
                                               ::fesql::node::kFnOpMinus);

    BinaryExprCheck<int16_t, int64_t, int64_t>(1, 8000000000L, 1L - 8000000000L,
                                               ::fesql::node::kFnOpMinus);

    BinaryExprCheck<int16_t, float, float>(1, 12345678.5f, 1.0f - 12345678.5f,
                                           ::fesql::node::kFnOpMinus);
    BinaryExprCheck<int16_t, double, double>(1, 12345678.5, 1.0 - 12345678.5,
                                             ::fesql::node::kFnOpMinus);
}

TEST_F(ExprIRBuilderTest, test_sub_int32_x_expr) {
    BinaryExprCheck<int32_t, int16_t, int32_t>(2, 1, 1,
                                               ::fesql::node::kFnOpMinus);

    BinaryExprCheck<int32_t, int32_t, int32_t>(2, 1, 1,
                                               ::fesql::node::kFnOpMinus);

    BinaryExprCheck<int32_t, int64_t, int64_t>(1, 8000000000L, 1L - 8000000000L,
                                               ::fesql::node::kFnOpMinus);

    BinaryExprCheck<int32_t, float, float>(1, 12345678.5f, 1.0f - 12345678.5f,
                                           ::fesql::node::kFnOpMinus);
    BinaryExprCheck<int32_t, double, double>(1, 12345678.5, 1.0 - 12345678.5,
                                             ::fesql::node::kFnOpMinus);
}

TEST_F(ExprIRBuilderTest, test_sub_int64_x_expr) {
    BinaryExprCheck<int64_t, int16_t, int64_t>(8000000000L, 1, 8000000000L - 1L,
                                               ::fesql::node::kFnOpMinus);

    BinaryExprCheck<int64_t, int32_t, int64_t>(8000000000L, 1, 8000000000L - 1L,
                                               ::fesql::node::kFnOpMinus);

    BinaryExprCheck<int64_t, int64_t, int64_t>(
        1L, 8000000000L, 1L - 8000000000L, ::fesql::node::kFnOpMinus);

    BinaryExprCheck<int64_t, float, float>(1L, 12345678.5f, 1.0f - 12345678.5f,
                                           ::fesql::node::kFnOpMinus);
    BinaryExprCheck<int64_t, double, double>(1, 12345678.5, 1.0 - 12345678.5,
                                             ::fesql::node::kFnOpMinus);
}

TEST_F(ExprIRBuilderTest, test_sub_float_x_expr) {
    BinaryExprCheck<float, int16_t, float>(2.0f, 1, 1.0f,
                                           ::fesql::node::kFnOpMinus);

    BinaryExprCheck<float, int32_t, float>(8000000000L, 1, 8000000000.0f - 1.0f,
                                           ::fesql::node::kFnOpMinus);

    BinaryExprCheck<float, int64_t, float>(1.0f, 200000L, 1.0f - 200000.0f,
                                           ::fesql::node::kFnOpMinus);

    BinaryExprCheck<float, float, float>(1.0f, 12345678.5f, 1.0f - 12345678.5f,
                                         ::fesql::node::kFnOpMinus);
    BinaryExprCheck<float, double, double>(1.0f, 12345678.5, 1.0 - 12345678.5,
                                           ::fesql::node::kFnOpMinus);
}

TEST_F(ExprIRBuilderTest, test_sub_double_x_expr) {
    BinaryExprCheck<double, int16_t, double>(2.0, 1, 1.0,
                                             ::fesql::node::kFnOpMinus);

    BinaryExprCheck<double, int32_t, double>(8000000000L, 1, 8000000000.0 - 1.0,
                                             ::fesql::node::kFnOpMinus);

    BinaryExprCheck<double, int64_t, double>(1.0f, 200000L, 1.0 - 200000.0,
                                             ::fesql::node::kFnOpMinus);

    BinaryExprCheck<double, float, double>(
        1.0, 12345678.5f, 1.0 - static_cast<double>(12345678.5f),
        ::fesql::node::kFnOpMinus);
    BinaryExprCheck<double, double, double>(1.0, 12345678.5, 1.0 - 12345678.5,
                                            ::fesql::node::kFnOpMinus);
}

TEST_F(ExprIRBuilderTest, test_mul_int16_x_expr) {
    BinaryExprCheck<int16_t, int16_t, int16_t>(2, 3, 2 * 3,
                                               ::fesql::node::kFnOpMulti);

    BinaryExprCheck<int16_t, int32_t, int32_t>(2, 3, 2 * 3,
                                               ::fesql::node::kFnOpMulti);

    BinaryExprCheck<int16_t, int64_t, int64_t>(2, 8000000000L, 2L * 8000000000L,
                                               ::fesql::node::kFnOpMulti);

    BinaryExprCheck<int16_t, float, float>(2, 12345678.5f, 2.0f * 12345678.5f,
                                           ::fesql::node::kFnOpMulti);
    BinaryExprCheck<int16_t, double, double>(2, 12345678.5, 2.0 * 12345678.5,
                                             ::fesql::node::kFnOpMulti);
}
TEST_F(ExprIRBuilderTest, test_multi_int32_x_expr) {
    BinaryExprCheck<int32_t, int16_t, int32_t>(2, 3, 2 * 3,
                                               ::fesql::node::kFnOpMulti);

    BinaryExprCheck<int32_t, int32_t, int32_t>(2, 3, 2 * 3,
                                               ::fesql::node::kFnOpMulti);

    BinaryExprCheck<int32_t, int64_t, int64_t>(2, 8000000000L, 2L * 8000000000L,
                                               ::fesql::node::kFnOpMulti);

    BinaryExprCheck<int32_t, float, float>(2, 12345678.5f, 2.0f * 12345678.5f,
                                           ::fesql::node::kFnOpMulti);
    BinaryExprCheck<int32_t, double, double>(2, 12345678.5, 2.0 * 12345678.5,
                                             ::fesql::node::kFnOpMulti);
}
TEST_F(ExprIRBuilderTest, test_multi_int64_x_expr) {
    BinaryExprCheck<int64_t, int16_t, int64_t>(
        8000000000L, 2L, 8000000000L * 2L, ::fesql::node::kFnOpMulti);

    BinaryExprCheck<int64_t, int32_t, int64_t>(8000000000L, 2, 8000000000L * 2L,
                                               ::fesql::node::kFnOpMulti);

    BinaryExprCheck<int64_t, int64_t, int64_t>(
        2L, 8000000000L, 2L * 8000000000L, ::fesql::node::kFnOpMulti);

    BinaryExprCheck<int64_t, float, float>(2L, 12345678.5f, 2.0f * 12345678.5f,
                                           ::fesql::node::kFnOpMulti);
    BinaryExprCheck<int64_t, double, double>(2, 12345678.5, 2.0 * 12345678.5,
                                             ::fesql::node::kFnOpMulti);
}

TEST_F(ExprIRBuilderTest, test_multi_float_x_expr) {
    BinaryExprCheck<float, int16_t, float>(2.0f, 3.0f, 2.0f * 3.0f,
                                           ::fesql::node::kFnOpMulti);

    BinaryExprCheck<float, int32_t, float>(8000000000L, 2, 8000000000.0f * 2.0f,
                                           ::fesql::node::kFnOpMulti);

    BinaryExprCheck<float, int64_t, float>(2.0f, 200000L, 2.0f * 200000.0f,
                                           ::fesql::node::kFnOpMulti);

    BinaryExprCheck<float, float, float>(2.0f, 12345678.5f, 2.0f * 12345678.5f,
                                         ::fesql::node::kFnOpMulti);
    BinaryExprCheck<float, double, double>(2.0f, 12345678.5, 2.0 * 12345678.5,
                                           ::fesql::node::kFnOpMulti);
}
TEST_F(ExprIRBuilderTest, test_multi_double_x_expr) {
    BinaryExprCheck<double, int16_t, double>(2.0, 3, 2.0 * 3.0,
                                             ::fesql::node::kFnOpMulti);

    BinaryExprCheck<double, int32_t, double>(8000000000L, 2, 8000000000.0 * 2.0,
                                             ::fesql::node::kFnOpMulti);

    BinaryExprCheck<double, int64_t, double>(2.0f, 200000L, 2.0 * 200000.0,
                                             ::fesql::node::kFnOpMulti);

    BinaryExprCheck<double, float, double>(
        2.0, 12345678.5f, 2.0 * static_cast<double>(12345678.5f),
        ::fesql::node::kFnOpMulti);
    BinaryExprCheck<double, double, double>(2.0, 12345678.5, 2.0 * 12345678.5,
                                            ::fesql::node::kFnOpMulti);
}

TEST_F(ExprIRBuilderTest, test_fdiv_int32_x_expr) {
    BinaryExprCheck<int32_t, int16_t, double>(2, 3, 2.0 / 3.0,
                                              ::fesql::node::kFnOpFDiv);

    BinaryExprCheck<int32_t, int32_t, double>(2, 3, 2.0 / 3.0,
                                              ::fesql::node::kFnOpFDiv);

    BinaryExprCheck<int32_t, int64_t, double>(
        2, 8000000000L, 2.0 / 8000000000.0, ::fesql::node::kFnOpFDiv);

    BinaryExprCheck<int32_t, float, double>(2, 3.0f, 2.0 / 3.0,
                                            ::fesql::node::kFnOpFDiv);
    BinaryExprCheck<int32_t, double, double>(2, 12345678.5, 2.0 / 12345678.5,
                                             ::fesql::node::kFnOpFDiv);
}

TEST_F(ExprIRBuilderTest, test_mod_int32_x_expr) {
    BinaryExprCheck<int32_t, int16_t, int32_t>(12, 5, 2,
                                               ::fesql::node::kFnOpMod);

    BinaryExprCheck<int32_t, int32_t, int32_t>(12, 5, 2,
                                               ::fesql::node::kFnOpMod);

    BinaryExprCheck<int32_t, int64_t, int64_t>(12, 50000L, 12L,
                                               ::fesql::node::kFnOpMod);

    BinaryExprCheck<int32_t, float, float>(12, 5.1f, fmod(12.0f, 5.1f),
                                           ::fesql::node::kFnOpMod);
    BinaryExprCheck<int32_t, double, double>(12, 5.1, fmod(12.0, 5.1),
                                             ::fesql::node::kFnOpMod);
}

TEST_F(ExprIRBuilderTest, test_mod_float_x_expr) {
    BinaryExprCheck<int16_t, float, float>(12, 5.1f, fmod(12.0f, 5.1f),
                                           ::fesql::node::kFnOpMod);

    BinaryExprCheck<int32_t, float, float>(12, 5.1f, fmod(12.0f, 5.1f),
                                           ::fesql::node::kFnOpMod);

    BinaryExprCheck<int64_t, float, float>(12L, 5.1f, fmod(12.0f, 5.1f),
                                           ::fesql::node::kFnOpMod);

    BinaryExprCheck<float, float, float>(12.0f, 5.1f, fmod(12.0f, 5.1f),
                                         ::fesql::node::kFnOpMod);
}

TEST_F(ExprIRBuilderTest, test_mod_double_x_expr) {
    BinaryExprCheck<int16_t, double, double>(12, 5.1, fmod(12.0, 5.1),
                                             ::fesql::node::kFnOpMod);

    BinaryExprCheck<int32_t, double, double>(12, 5.1, fmod(12.0, 5.1),
                                             ::fesql::node::kFnOpMod);

    BinaryExprCheck<int64_t, double, double>(12L, 5.1, fmod(12.0, 5.1),
                                             ::fesql::node::kFnOpMod);

    BinaryExprCheck<float, double, double>(12.0f, 5.1, fmod(12.0, 5.1),
                                           ::fesql::node::kFnOpMod);
    BinaryExprCheck<double, double, double>(12.0, 5.1, fmod(12.0, 5.1),
                                            ::fesql::node::kFnOpMod);
}

TEST_F(ExprIRBuilderTest, test_eq_expr_true) {
    BinaryExprCheck<int16_t, int16_t, bool>(1, 1, true, ::fesql::node::kFnOpEq);

    BinaryExprCheck<int32_t, int32_t, bool>(1, 1, true, ::fesql::node::kFnOpEq);

    BinaryExprCheck<int64_t, int64_t, bool>(1, 1, true, ::fesql::node::kFnOpEq);

    BinaryExprCheck<float, float, bool>(1.0f, 1.0f, true,
                                        ::fesql::node::kFnOpEq);

    BinaryExprCheck<double, double, bool>(1.0, 1.0, true,
                                          ::fesql::node::kFnOpEq);

    BinaryExprCheck<int32_t, float, bool>(1, 1.0f, true,
                                          ::fesql::node::kFnOpEq);

    BinaryExprCheck<int32_t, double, bool>(1, 1.0, true,
                                           ::fesql::node::kFnOpEq);
}

TEST_F(ExprIRBuilderTest, test_eq_expr_false) {
    BinaryExprCheck<int16_t, int16_t, bool>(1, 2, false,
                                            ::fesql::node::kFnOpEq);

    BinaryExprCheck<int32_t, int32_t, bool>(1, 2, false,
                                            ::fesql::node::kFnOpEq);

    BinaryExprCheck<int64_t, int64_t, bool>(1, 2, false,
                                            ::fesql::node::kFnOpEq);

    BinaryExprCheck<float, float, bool>(1.0f, 1.1f, false,
                                        ::fesql::node::kFnOpEq);

    BinaryExprCheck<double, double, bool>(1.0, 1.1, false,
                                          ::fesql::node::kFnOpEq);
}

TEST_F(ExprIRBuilderTest, test_neq_expr_true) {
    BinaryExprCheck<int16_t, int16_t, bool>(1, 2, true,
                                            ::fesql::node::kFnOpNeq);

    BinaryExprCheck<int32_t, int32_t, bool>(1, 2, true,
                                            ::fesql::node::kFnOpNeq);

    BinaryExprCheck<int64_t, int64_t, bool>(1, 2, true,
                                            ::fesql::node::kFnOpNeq);

    BinaryExprCheck<float, float, bool>(1.0f, 1.1f, true,
                                        ::fesql::node::kFnOpNeq);

    BinaryExprCheck<double, double, bool>(1.0, 1.1, true,
                                          ::fesql::node::kFnOpNeq);
}

TEST_F(ExprIRBuilderTest, test_neq_expr_false) {
    BinaryExprCheck<int16_t, int16_t, bool>(1, 1, false,
                                            ::fesql::node::kFnOpNeq);

    BinaryExprCheck<int32_t, int32_t, bool>(1, 1, false,
                                            ::fesql::node::kFnOpNeq);

    BinaryExprCheck<int64_t, int64_t, bool>(1, 1, false,
                                            ::fesql::node::kFnOpNeq);

    BinaryExprCheck<float, float, bool>(1.0f, 1.0f, false,
                                        ::fesql::node::kFnOpNeq);

    BinaryExprCheck<double, double, bool>(1.0, 1.0, false,
                                          ::fesql::node::kFnOpNeq);

    BinaryExprCheck<int32_t, float, bool>(1, 1.0f, false,
                                          ::fesql::node::kFnOpNeq);

    BinaryExprCheck<int32_t, double, bool>(1, 1.0, false,
                                           ::fesql::node::kFnOpNeq);
}

TEST_F(ExprIRBuilderTest, test_gt_expr_true) {
    BinaryExprCheck<int16_t, int16_t, bool>(2, 1, true, ::fesql::node::kFnOpGt);

    BinaryExprCheck<int32_t, int32_t, bool>(2, 1, true, ::fesql::node::kFnOpGt);

    BinaryExprCheck<int64_t, int64_t, bool>(2, 1, true, ::fesql::node::kFnOpGt);

    BinaryExprCheck<float, float, bool>(1.1f, 1.0f, true,
                                        ::fesql::node::kFnOpGt);

    BinaryExprCheck<double, double, bool>(1.1, 1.0, true,
                                          ::fesql::node::kFnOpGt);

    BinaryExprCheck<int32_t, float, bool>(2, 1.9f, true,
                                          ::fesql::node::kFnOpGt);

    BinaryExprCheck<int32_t, double, bool>(2, 1.9, true,
                                           ::fesql::node::kFnOpGt);
}

TEST_F(ExprIRBuilderTest, test_gt_expr_false) {
    BinaryExprCheck<int16_t, int16_t, bool>(2, 2, false,
                                            ::fesql::node::kFnOpGt);
    BinaryExprCheck<int16_t, int16_t, bool>(2, 3, false,
                                            ::fesql::node::kFnOpGt);

    BinaryExprCheck<int32_t, int32_t, bool>(2, 2, false,
                                            ::fesql::node::kFnOpGt);
    BinaryExprCheck<int32_t, int32_t, bool>(2, 3, false,
                                            ::fesql::node::kFnOpGt);

    BinaryExprCheck<int64_t, int64_t, bool>(2, 2, false,
                                            ::fesql::node::kFnOpGt);
    BinaryExprCheck<int64_t, int64_t, bool>(2, 3, false,
                                            ::fesql::node::kFnOpGt);

    BinaryExprCheck<float, float, bool>(1.1f, 1.2f, false,
                                        ::fesql::node::kFnOpGt);

    BinaryExprCheck<double, double, bool>(1.1, 1.2, false,
                                          ::fesql::node::kFnOpGt);

    BinaryExprCheck<int32_t, float, bool>(2, 2.1f, false,
                                          ::fesql::node::kFnOpGt);

    BinaryExprCheck<int32_t, double, bool>(2, 2.1, false,
                                           ::fesql::node::kFnOpGt);
}
TEST_F(ExprIRBuilderTest, test_ge_expr_true) {
    BinaryExprCheck<int16_t, int16_t, bool>(2, 1, true, ::fesql::node::kFnOpGe);
    BinaryExprCheck<int16_t, int16_t, bool>(2, 2, true, ::fesql::node::kFnOpGe);

    BinaryExprCheck<int32_t, int32_t, bool>(2, 1, true, ::fesql::node::kFnOpGe);
    BinaryExprCheck<int32_t, int32_t, bool>(2, 2, true, ::fesql::node::kFnOpGe);

    BinaryExprCheck<int64_t, int64_t, bool>(2, 1, true, ::fesql::node::kFnOpGe);
    BinaryExprCheck<int64_t, int64_t, bool>(2, 2, true, ::fesql::node::kFnOpGe);

    BinaryExprCheck<float, float, bool>(1.1f, 1.0f, true,
                                        ::fesql::node::kFnOpGe);
    BinaryExprCheck<float, float, bool>(1.1f, 1.1f, true,
                                        ::fesql::node::kFnOpGe);

    BinaryExprCheck<double, double, bool>(1.1, 1.0, true,
                                          ::fesql::node::kFnOpGe);

    BinaryExprCheck<double, double, bool>(1.1, 1.1, true,
                                          ::fesql::node::kFnOpGe);
    BinaryExprCheck<int32_t, float, bool>(2, 1.9f, true,
                                          ::fesql::node::kFnOpGe);
    BinaryExprCheck<int32_t, float, bool>(2, 2.0f, true,
                                          ::fesql::node::kFnOpGe);

    BinaryExprCheck<int32_t, double, bool>(2, 1.9, true,
                                           ::fesql::node::kFnOpGe);
    BinaryExprCheck<int32_t, double, bool>(2, 2.0, true,
                                           ::fesql::node::kFnOpGe);
}

TEST_F(ExprIRBuilderTest, test_ge_expr_false) {
    BinaryExprCheck<int16_t, int16_t, bool>(2, 3, false,
                                            ::fesql::node::kFnOpGe);

    BinaryExprCheck<int32_t, int32_t, bool>(2, 3, false,
                                            ::fesql::node::kFnOpGe);

    BinaryExprCheck<int64_t, int64_t, bool>(2, 3, false,
                                            ::fesql::node::kFnOpGe);

    BinaryExprCheck<float, float, bool>(1.1f, 1.2f, false,
                                        ::fesql::node::kFnOpGe);

    BinaryExprCheck<double, double, bool>(1.1, 1.2, false,
                                          ::fesql::node::kFnOpGe);

    BinaryExprCheck<int32_t, float, bool>(2, 2.1f, false,
                                          ::fesql::node::kFnOpGe);
}

TEST_F(ExprIRBuilderTest, test_lt_expr_true) {
    BinaryExprCheck<int16_t, int16_t, bool>(2, 3, true, ::fesql::node::kFnOpLt);

    BinaryExprCheck<int32_t, int32_t, bool>(2, 3, true, ::fesql::node::kFnOpLt);

    BinaryExprCheck<int64_t, int64_t, bool>(2, 3, true, ::fesql::node::kFnOpLt);

    BinaryExprCheck<float, float, bool>(1.1f, 1.2f, true,
                                        ::fesql::node::kFnOpLt);

    BinaryExprCheck<double, double, bool>(1.1, 1.2, true,
                                          ::fesql::node::kFnOpLt);

    BinaryExprCheck<int32_t, float, bool>(2, 2.1f, true,
                                          ::fesql::node::kFnOpLt);
}

TEST_F(ExprIRBuilderTest, test_lt_expr_false) {
    BinaryExprCheck<int16_t, int16_t, bool>(2, 1, false,
                                            ::fesql::node::kFnOpLt);
    BinaryExprCheck<int16_t, int16_t, bool>(2, 2, false,
                                            ::fesql::node::kFnOpLt);

    BinaryExprCheck<int32_t, int32_t, bool>(2, 1, false,
                                            ::fesql::node::kFnOpLt);
    BinaryExprCheck<int32_t, int32_t, bool>(2, 2, false,
                                            ::fesql::node::kFnOpLt);

    BinaryExprCheck<int64_t, int64_t, bool>(2, 1, false,
                                            ::fesql::node::kFnOpLt);
    BinaryExprCheck<int64_t, int64_t, bool>(2, 2, false,
                                            ::fesql::node::kFnOpLt);

    BinaryExprCheck<float, float, bool>(1.1f, 1.0f, false,
                                        ::fesql::node::kFnOpLt);
    BinaryExprCheck<float, float, bool>(1.1f, 1.1f, false,
                                        ::fesql::node::kFnOpLt);

    BinaryExprCheck<double, double, bool>(1.1, 1.0, false,
                                          ::fesql::node::kFnOpLt);

    BinaryExprCheck<double, double, bool>(1.1, 1.1, false,
                                          ::fesql::node::kFnOpLt);
    BinaryExprCheck<int32_t, float, bool>(2, 1.9f, false,
                                          ::fesql::node::kFnOpLt);
    BinaryExprCheck<int32_t, float, bool>(2, 2.0f, false,
                                          ::fesql::node::kFnOpLt);

    BinaryExprCheck<int32_t, double, bool>(2, 1.9, false,
                                           ::fesql::node::kFnOpLt);
    BinaryExprCheck<int32_t, double, bool>(2, 2.0, false,
                                           ::fesql::node::kFnOpLt);
}

TEST_F(ExprIRBuilderTest, test_and_expr_true) {
    BinaryExprCheck<bool, bool, bool>(true, true, true,
                                      ::fesql::node::kFnOpAnd);
}

TEST_F(ExprIRBuilderTest, test_and_expr_false) {
    BinaryExprCheck<bool, bool, bool>(false, true, false,
                                      ::fesql::node::kFnOpAnd);
    BinaryExprCheck<bool, bool, bool>(false, false, false,
                                      ::fesql::node::kFnOpAnd);
    BinaryExprCheck<bool, bool, bool>(true, false, false,
                                      ::fesql::node::kFnOpAnd);
}

TEST_F(ExprIRBuilderTest, test_or_expr_true) {
    BinaryExprCheck<bool, bool, bool>(true, true, true, ::fesql::node::kFnOpOr);
    BinaryExprCheck<bool, bool, bool>(true, false, true,
                                      ::fesql::node::kFnOpOr);
    BinaryExprCheck<bool, bool, bool>(false, true, true,
                                      ::fesql::node::kFnOpOr);
}

TEST_F(ExprIRBuilderTest, test_or_expr_false) {
    BinaryExprCheck<bool, bool, bool>(false, false, false,
                                      ::fesql::node::kFnOpOr);
}

TEST_F(ExprIRBuilderTest, test_get_field) {
    auto schema = udf::MakeLiteralSchema<int16_t, int32_t, int64_t, float,
                                         double, codec::Timestamp, codec::Date,
                                         codec::StringRef>();
    vm::SchemaSourceList schema_sources;
    schema_sources.AddSchemaSource("t", &schema);
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

    ExprCheck(
        [](node::NodeManager *nm, ExprNode *input) {
            return nm->MakeGetFieldExpr(input, "col_0", "t");
        },
        (int16_t)16, typed_row);
    ExprCheck(
        [](node::NodeManager *nm, ExprNode *input) {
            return nm->MakeGetFieldExpr(input, "col_1", "t");
        },
        32, typed_row);
    ExprCheck(
        [](node::NodeManager *nm, ExprNode *input) {
            return nm->MakeGetFieldExpr(input, "col_2", "t");
        },
        (int64_t)64L, typed_row);
    ExprCheck(
        [](node::NodeManager *nm, ExprNode *input) {
            return nm->MakeGetFieldExpr(input, "col_3", "t");
        },
        3.14f, typed_row);
    ExprCheck(
        [](node::NodeManager *nm, ExprNode *input) {
            return nm->MakeGetFieldExpr(input, "col_4", "t");
        },
        1.0, typed_row);
    ExprCheck(
        [](node::NodeManager *nm, ExprNode *input) {
            return nm->MakeGetFieldExpr(input, "col_5", "t");
        },
        codec::Timestamp(1590115420000L), typed_row);
    ExprCheck(
        [](node::NodeManager *nm, ExprNode *input) {
            return nm->MakeGetFieldExpr(input, "col_6", "t");
        },
        codec::Date(2009, 7, 1), typed_row);
    ExprCheck(
        [](node::NodeManager *nm, ExprNode *input) {
            return nm->MakeGetFieldExpr(input, "col_7", "t");
        },
        codec::StringRef(5, "hello"), typed_row);
}

}  // namespace codegen
}  // namespace fesql

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    InitializeNativeTarget();
    InitializeNativeTargetAsmPrinter();
    return RUN_ALL_TESTS();
}
