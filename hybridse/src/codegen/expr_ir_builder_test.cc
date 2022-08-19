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

using openmldb::base::Date;
using openmldb::base::StringRef;
using openmldb::base::Timestamp;
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

TEST_F(ExprIRBuilderTest, TestAddInt32) {
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

TEST_F(ExprIRBuilderTest, TestAddInt16XExpr) {
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

TEST_F(ExprIRBuilderTest, TestAddInt32XExpr) {
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

TEST_F(ExprIRBuilderTest, TestAddInt64XExpr) {
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

TEST_F(ExprIRBuilderTest, TestAddFloatXExpr) {
    BinaryExprCheck<float, int16_t, float>(1.0f, 1, 2.0f,
                                           ::hybridse::node::kFnOpAdd);

    BinaryExprCheck<float, int32_t, float>(8000000000.0f, 1, 8000000001.0f,
                                           ::hybridse::node::kFnOpAdd);

    BinaryExprCheck<float, int64_t, float>(1.0f, 200000L, 1.0f + 200000.0f,
                                           ::hybridse::node::kFnOpAdd);

    BinaryExprCheck<float, float, float>(1.0f, 12345678.5f, 12345678.5f + 1.0f,
                                         ::hybridse::node::kFnOpAdd);
    BinaryExprCheck<float, double, double>(1.0f, 12345678.5, 12345678.5 + 1.0,
                                           ::hybridse::node::kFnOpAdd);
}

TEST_F(ExprIRBuilderTest, TestAddDoubleXExpr) {
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

TEST_F(ExprIRBuilderTest, TestSubInt16XExpr) {
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

TEST_F(ExprIRBuilderTest, TestSubInt32XExpr) {
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

TEST_F(ExprIRBuilderTest, TestSubInt64XExpr) {
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

TEST_F(ExprIRBuilderTest, TestSubFloatXExpr) {
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

TEST_F(ExprIRBuilderTest, TestSubDoubleXExpr) {
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

TEST_F(ExprIRBuilderTest, TestMulInt16XExpr) {
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
TEST_F(ExprIRBuilderTest, TestMultiInt32XExpr) {
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
TEST_F(ExprIRBuilderTest, TestMultiInt64XExpr) {
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

TEST_F(ExprIRBuilderTest, TestMultiFloatXExpr) {
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
TEST_F(ExprIRBuilderTest, TestMultiDoubleXExpr) {
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

TEST_F(ExprIRBuilderTest, TestFdivInt32XExpr) {
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

TEST_F(ExprIRBuilderTest, TestModInt32XExpr) {
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

TEST_F(ExprIRBuilderTest, TestModFloatXExpr) {
    BinaryExprCheck<int16_t, float, float>(12, 5.1f, fmod(12.0f, 5.1f),
                                           ::hybridse::node::kFnOpMod);

    BinaryExprCheck<int32_t, float, float>(12, 5.1f, fmod(12.0f, 5.1f),
                                           ::hybridse::node::kFnOpMod);

    BinaryExprCheck<int64_t, float, float>(12L, 5.1f, fmod(12.0f, 5.1f),
                                           ::hybridse::node::kFnOpMod);

    BinaryExprCheck<float, float, float>(12.0f, 5.1f, fmod(12.0f, 5.1f),
                                         ::hybridse::node::kFnOpMod);
}

TEST_F(ExprIRBuilderTest, TestModDoubleXExpr) {
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

TEST_F(ExprIRBuilderTest, TestEqExprTrue) {
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

TEST_F(ExprIRBuilderTest, TestEqExprFalse) {
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

TEST_F(ExprIRBuilderTest, TestNeqExprTrue) {
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

TEST_F(ExprIRBuilderTest, TestNeqExprFalse) {
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

TEST_F(ExprIRBuilderTest, TestGtExprTrue) {
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

TEST_F(ExprIRBuilderTest, TestGtExprFalse) {
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
TEST_F(ExprIRBuilderTest, TestGeExprTrue) {
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

TEST_F(ExprIRBuilderTest, TestGeExprFalse) {
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

TEST_F(ExprIRBuilderTest, TestLtExprTrue) {
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

TEST_F(ExprIRBuilderTest, TestLtExprFalse) {
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

TEST_F(ExprIRBuilderTest, TestAndExprTrue) {
    BinaryExprCheck<bool, bool, bool>(true, true, true,
                                      ::hybridse::node::kFnOpAnd);
}

TEST_F(ExprIRBuilderTest, TestAndExprFalse) {
    BinaryExprCheck<bool, bool, bool>(false, true, false,
                                      ::hybridse::node::kFnOpAnd);
    BinaryExprCheck<bool, bool, bool>(false, false, false,
                                      ::hybridse::node::kFnOpAnd);
    BinaryExprCheck<bool, bool, bool>(true, false, false,
                                      ::hybridse::node::kFnOpAnd);
}

TEST_F(ExprIRBuilderTest, TestOrExprTrue) {
    BinaryExprCheck<bool, bool, bool>(true, true, true,
                                      ::hybridse::node::kFnOpOr);
    BinaryExprCheck<bool, bool, bool>(true, false, true,
                                      ::hybridse::node::kFnOpOr);
    BinaryExprCheck<bool, bool, bool>(false, true, true,
                                      ::hybridse::node::kFnOpOr);
}

TEST_F(ExprIRBuilderTest, TestOrExprFalse) {
    BinaryExprCheck<bool, bool, bool>(false, false, false,
                                      ::hybridse::node::kFnOpOr);
}

TEST_F(ExprIRBuilderTest, TestGetField) {
    auto schema = udf::MakeLiteralSchema<int16_t, int32_t, int64_t, float,
                                         double, Timestamp, Date,
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
                         Timestamp, Date, codec::StringRef>
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
              static_cast<int16_t>(16), typed_row);
    ExprCheck([&](node::NodeManager *nm,
                  ExprNode *input) { return make_get_field(nm, input, 1); },
              32, typed_row);
    ExprCheck([&](node::NodeManager *nm,
                  ExprNode *input) { return make_get_field(nm, input, 2); },
              static_cast<int64_t>(64L), typed_row);
    ExprCheck([&](node::NodeManager *nm,
                  ExprNode *input) { return make_get_field(nm, input, 3); },
              3.14f, typed_row);
    ExprCheck([&](node::NodeManager *nm,
                  ExprNode *input) { return make_get_field(nm, input, 4); },
              1.0, typed_row);
    ExprCheck([&](node::NodeManager *nm,
                  ExprNode *input) { return make_get_field(nm, input, 5); },
              Timestamp(1590115420000L), typed_row);
    ExprCheck([&](node::NodeManager *nm,
                  ExprNode *input) { return make_get_field(nm, input, 6); },
              Date(2009, 7, 1), typed_row);
    ExprCheck([&](node::NodeManager *nm,
                  ExprNode *input) { return make_get_field(nm, input, 7); },
              codec::StringRef(5, "hello"), typed_row);
}

TEST_F(ExprIRBuilderTest, TestBuildLambda) {
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

TEST_F(ExprIRBuilderTest, TestCondExpr) {
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

TEST_F(ExprIRBuilderTest, TestCaseWhenExpr) {
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

TEST_F(ExprIRBuilderTest, TestIsNullExpr) {
    auto make_if_null = [](node::NodeManager *nm, node::ExprNode *input) {
        return nm->MakeUnaryExprNode(input, node::kFnOpIsNull);
    };
    ExprCheck<bool, udf::Nullable<int32_t>>(make_if_null, false, 1);
    ExprCheck<bool, udf::Nullable<int32_t>>(make_if_null, true, nullptr);
}

TEST_F(ExprIRBuilderTest, TestBitwiseAndExpr) {
    auto op = ::hybridse::node::kFnOpBitwiseAnd;
    BinaryExprCheck<int16_t, int16_t, int16_t>(3, 6, 2, op);
    BinaryExprCheck<int16_t, int32_t, int32_t>(3, 6, 2, op);
    BinaryExprCheck<int32_t, int32_t, int32_t>(3, 6, 2, op);
    BinaryExprCheck<int64_t, int16_t, int64_t>(3L, 6, 2L, op);
    BinaryExprCheck<int64_t, int32_t, int64_t>(3L, 6, 2L, op);
    BinaryExprCheck<int64_t, int64_t, int64_t>(3L, 6L, 2L, op);
}

TEST_F(ExprIRBuilderTest, TestBitwiseOrExpr) {
    auto op = ::hybridse::node::kFnOpBitwiseOr;
    BinaryExprCheck<int16_t, int16_t, int16_t>(3, 6, 7, op);
    BinaryExprCheck<int16_t, int32_t, int32_t>(3, 6, 7, op);
    BinaryExprCheck<int32_t, int32_t, int32_t>(3, 6, 7, op);
    BinaryExprCheck<int64_t, int16_t, int64_t>(3L, 6, 7L, op);
    BinaryExprCheck<int64_t, int32_t, int64_t>(3L, 6, 7L, op);
    BinaryExprCheck<int64_t, int64_t, int64_t>(3L, 6L, 7L, op);
}
TEST_F(ExprIRBuilderTest, TestBitwiseXorExpr) {
    auto op = ::hybridse::node::kFnOpBitwiseXor;
    BinaryExprCheck<int16_t, int16_t, int16_t>(3, 6, 5, op);
    BinaryExprCheck<int16_t, int32_t, int32_t>(3, 6, 5, op);
    BinaryExprCheck<int32_t, int32_t, int32_t>(3, 6, 5, op);
    BinaryExprCheck<int64_t, int16_t, int64_t>(3L, 6, 5L, op);
    BinaryExprCheck<int64_t, int32_t, int64_t>(3L, 6, 5L, op);
    BinaryExprCheck<int64_t, int64_t, int64_t>(3L, 6L, 5L, op);
}
TEST_F(ExprIRBuilderTest, TestBitwiseNotExpr) {
    auto not_expr = [](node::NodeManager* nm, node::ExprNode* input) {
        return nm->MakeUnaryExprNode(input, ::hybridse::node::kFnOpBitwiseNot);
    };
    ExprCheck<int16_t, int16_t>(not_expr, static_cast<int16_t>(-1), static_cast<int16_t>(0));
    ExprCheck<int32_t, int32_t>(not_expr, 0, -1);
    ExprCheck<int64_t, int64_t>(not_expr, 0L, -1L);

    ExprErrorCheck<codec::StringRef, codec::StringRef>(not_expr);
}

TEST_F(ExprIRBuilderTest, TestBetweenExpr) {
    auto between_expr = [](node::NodeManager *nm, node::ExprNode *lhs, node::ExprNode *low, node::ExprNode *high) {
        return nm->MakeBetweenExpr(lhs, low, high, false);
    };
    // number between
    ExprCheck<bool, int32_t, int32_t, int32_t>(between_expr, true, 3, 1, 5);
    ExprCheck<bool, int16_t, int32_t, int64_t>(between_expr, true, static_cast<int16_t>(3), 3, 5L);
    ExprCheck<bool, int32_t, int32_t, int32_t>(between_expr, false, 3, 6, 9);
    ExprCheck<bool, float, double, double>(between_expr, true, 3.0f, 1.0, 5.0);
    ExprCheck<bool, double, double, double>(between_expr, false, 3.0, 9.0, 11.0);
    ExprCheck<bool, int32_t, double, double>(between_expr, false, 3, 9.0, 11.0);

    // string between
    ExprCheck<bool, codec::StringRef, codec::StringRef, codec::StringRef>(
        between_expr, true, codec::StringRef("def"), codec::StringRef("aaa"), codec::StringRef("fgc"));
    ExprCheck<bool, codec::StringRef, codec::StringRef, codec::StringRef>(
        between_expr, false, codec::StringRef("aaa"), codec::StringRef("ddd"), codec::StringRef("fgc"));
    // timestamp between
    ExprCheck<bool, Timestamp, Timestamp, Timestamp>(
        between_expr, true, Timestamp(1629521917000), Timestamp(1629521917000),
        Timestamp(1629521919000));
}
TEST_F(ExprIRBuilderTest, TestNotBetweenExpr) {
    auto not_between_expr = [](node::NodeManager *nm, node::ExprNode *lhs, node::ExprNode *low, node::ExprNode *high) {
        return nm->MakeBetweenExpr(lhs, low, high, true);
    };
    // number between
    ExprCheck<bool, int32_t, int32_t, int32_t>(not_between_expr, false, 3, 1, 5);
    ExprCheck<bool, int16_t, int32_t, int64_t>(not_between_expr, false, static_cast<int16_t>(3), 3, 5L);
    ExprCheck<bool, int32_t, int32_t, int32_t>(not_between_expr, true, 3, 6, 9);
    ExprCheck<bool, float, double, double>(not_between_expr, false, 3.0f, 1.0, 5.0);
    ExprCheck<bool, double, double, double>(not_between_expr, true, 3.0, 9.0, 11.0);
    ExprCheck<bool, int32_t, double, double>(not_between_expr, true, 3, 9.0, 11.0);

    // string between
    ExprCheck<bool, codec::StringRef, codec::StringRef, codec::StringRef>(
        not_between_expr, false, codec::StringRef("def"), codec::StringRef("aaa"), codec::StringRef("fgc"));
    ExprCheck<bool, codec::StringRef, codec::StringRef, codec::StringRef>(
        not_between_expr, true, codec::StringRef("aaa"), codec::StringRef("ddd"), codec::StringRef("fgc"));
    // timestamp between
    ExprCheck<bool, Timestamp, Timestamp, Timestamp>(
        not_between_expr, false, Timestamp(1629521917000), Timestamp(1629521917000),
        Timestamp(1629521919000));
}
TEST_F(ExprIRBuilderTest, TestBetweenExprFail) {
    auto between_expr = [](node::NodeManager *nm, node::ExprNode *lhs, node::ExprNode *low, node::ExprNode *high) {
        return nm->MakeBetweenExpr(lhs, low, high, false);
    };
    ExprErrorCheck<bool, int64_t, Timestamp, Timestamp>(between_expr);
    ExprErrorCheck<bool, double, Timestamp, Timestamp>(between_expr);
}

template <typename T>
codec::ListRef<T> MakeList(const std::initializer_list<T>& vec) {
    codec::ArrayListV<T>* list =
        new codec::ArrayListV<T>(new std::vector<T>(vec));
    codec::ListRef<T> list_ref;
    list_ref.list = reinterpret_cast<int8_t*>(list);
    return list_ref;
}

TEST_F(ExprIRBuilderTest, TestInExprNormal) {
    auto in_expr = [](node::NodeManager *nm, node::ExprNode *lhs, node::ExprNode *first, node::ExprNode* second) {
        auto in_list = nm->MakeExprList();
        in_list->AddChild(first);
        in_list->AddChild(second);
        return nm->MakeInExpr(lhs, in_list, false);
    };

    ExprCheck<bool, int32_t, int32_t, int32_t>(in_expr, true, 1, 1, 10);
    ExprCheck<bool, int32_t, int32_t, int32_t>(in_expr, false, 2, 1, 10);

    ExprCheck<bool, int32_t, codec::StringRef, float>(in_expr, true, 1, codec::StringRef("1"), 10.0);
    ExprCheck<bool, int32_t, codec::StringRef, float>(in_expr, false, 2, codec::StringRef("1"), 10.0);
}

TEST_F(ExprIRBuilderTest, TestNotInExprNormal) {
    auto in_expr = [](node::NodeManager *nm, node::ExprNode *lhs, node::ExprNode *first, node::ExprNode* second) {
        auto in_list = nm->MakeExprList();
        in_list->AddChild(first);
        in_list->AddChild(second);
        return nm->MakeInExpr(lhs, in_list, true);
    };

    ExprCheck<bool, int32_t, int32_t, int32_t>(in_expr, false, 1, 1, 10);
    ExprCheck<bool, int32_t, int32_t, int32_t>(in_expr, true, 2, 1, 10);

    ExprCheck<bool, int32_t, codec::StringRef, float>(in_expr, false, 1, codec::StringRef("1"), 10.0);
    ExprCheck<bool, int32_t, codec::StringRef, float>(in_expr, true, 2, codec::StringRef("1"), 10.0);
}

TEST_F(ExprIRBuilderTest, TestInExprWithSubExprInInList) {
    auto in_expr = [](node::NodeManager *nm, node::ExprNode *lhs, node::ExprNode *first1, node::ExprNode *first2,
                      node::ExprNode *second) {
        auto in_list = nm->MakeExprList();
        auto first = nm->MakeBinaryExprNode(first1, first2, node::FnOperator::kFnOpAdd);
        in_list->AddChild(first);
        in_list->AddChild(second);
        return nm->MakeInExpr(lhs, in_list, false);
    };

    ExprCheck<bool, int32_t, int32_t, int32_t, int32_t>(in_expr, true, 2, 1, 1, 10);
}

TEST_F(ExprIRBuilderTest, TestInExprNotSupport) {
    auto in_expr = [](node::NodeManager *nm, node::ExprNode *lhs, node::ExprNode *in_list) {
        return nm->MakeInExpr(lhs, in_list, false);
    };
    ExprErrorCheck<bool, int32_t, codec::StringRef>(in_expr);
    ExprErrorCheck<bool, codec::StringRef, codec::StringRef>(in_expr);
}

TEST_F(ExprIRBuilderTest, LikeExpr) {
    auto assert_like = [&](const udf::Nullable<bool> &ret, const udf::Nullable<codec::StringRef> &lhs,
                           const udf::Nullable<codec::StringRef> &rhs) {
        ExprCheck([](node::NodeManager *nm, node::ExprNode *lhs,
                     node::ExprNode *rhs) { return nm->MakeBinaryExprNode(lhs, rhs, node::FnOperator::kFnOpLike); },
                  ret, lhs, rhs);
    };


    assert_like(true, "abc", "abc");
    assert_like(false, "abc", "def");
    assert_like(true, "abc", "a_c");
    assert_like(false, "a_c", "abc");
    assert_like(true, "Mary", "M%");
    assert_like(false, "Mary", "m%");
}

TEST_F(ExprIRBuilderTest, RLikeExpr) {
    auto assert_rlike = [&](const udf::Nullable<bool> &ret, const udf::Nullable<codec::StringRef> &lhs,
                           const udf::Nullable<codec::StringRef> &rhs) {
        ExprCheck([](node::NodeManager *nm, node::ExprNode *lhs,
                     node::ExprNode *rhs) { return nm->MakeBinaryExprNode(lhs, rhs, node::FnOperator::kFnOpRLike); },
                  ret, lhs, rhs);
    };

    assert_rlike(true, "The Lord of the Rings", "The Lord .f the Rings");
    assert_rlike(false, "The Lord of the Rings", "the Lord .f the Rings");
    assert_rlike(false, "The Lord of the Rings\nJ. R. R. Tolkien", "The Lord of the Rings.J\\. R\\. R\\. Tolkien");
    assert_rlike(true, "contact@openmldb.ai", "[A-Za-z0-9+_.-]+@[A-Za-z0-9+_.-]+");
}

}  // namespace codegen
}  // namespace hybridse

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    InitializeNativeTarget();
    InitializeNativeTargetAsmPrinter();
    return RUN_ALL_TESTS();
}
