/*
 * node/expr_node_test.cc
 * Copyright (C) 2019 chenjing <chenjing@4paradigm.com>
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
#include "node/expr_node.h"
#include <utility>
#include <vector>
#include "gtest/gtest.h"
#include "node/node_manager.h"
#include "node/sql_node.h"
#include "udf/default_udf_library.h"
#include "udf/literal_traits.h"

namespace fesql {
namespace node {

using codec::Timestamp;
using udf::Nullable;

class ExprNodeTest : public ::testing::Test {
 public:
    ExprNodeTest() { node_manager_ = new NodeManager(); }

    ~ExprNodeTest() { delete node_manager_; }

 protected:
    NodeManager* node_manager_;
};

template <typename F, size_t... I>
ExprNode* DoBuildExpr(const F& build_expr, NodeManager* node_manager,
                      const std::vector<ExprNode*>& args,
                      const std::index_sequence<I...>&) {
    return build_expr(node_manager, args[I]...);
}

template <typename Ret, typename... T>
void CheckInfer(
    const std::function<ExprNode*(
        NodeManager*, typename std::pair<T, ExprNode*>::second_type...)>&
        build_expr) {
    NodeManager nm;
    std::vector<node::TypeNode*> arg_types = {
        udf::DataTypeTrait<T>::to_type_node(&nm)...};
    std::vector<int> arg_nullable = {udf::IsNullableTrait<T>::value...};
    std::vector<ExprNode*> args;
    for (size_t i = 0; i < sizeof...(T); ++i) {
        auto expr_id = nm.MakeExprIdNode("arg_" + std::to_string(i),
                                         ExprIdNode::GetNewId());
        expr_id->SetOutputType(arg_types[i]);
        expr_id->SetNullable(arg_nullable[i]);
        args.push_back(expr_id);
    }

    ExprNode* expr =
        DoBuildExpr(build_expr, &nm, args, std::index_sequence_for<T...>());

    auto library = udf::DefaultUDFLibrary::get();
    ExprAnalysisContext ctx(&nm, library, nullptr);
    auto status = expr->InferAttr(&ctx);
    if (!status.isOK()) {
        LOG(INFO) << "Infer expr status: " << status;
    }
    ASSERT_TRUE(status.isOK());

    std::stringstream ss;
    ss << "<";
    for (size_t i = 0; i < arg_types.size(); ++i) {
        if (arg_nullable[i]) {
            ss << "nullable ";
        }
        ss << arg_types[i]->GetName();
        if (i < arg_types.size() - 1) {
            ss << ", ";
        }
    }
    ss << "> -> ";
    if (expr->nullable()) {
        ss << "nullable ";
    }
    ss << expr->GetOutputType()->GetName();
    LOG(INFO) << ss.str();

    ASSERT_EQ(udf::IsNullableTrait<Ret>::value, expr->nullable());
    ASSERT_TRUE(TypeEquals(udf::DataTypeTrait<Ret>::to_type_node(&nm),
                           expr->GetOutputType()));
}

template <typename... T>
void CheckInferError(
    const std::function<ExprNode*(
        NodeManager*, typename std::pair<T, ExprNode*>::second_type...)>&
        build_expr) {
    NodeManager nm;
    std::vector<node::TypeNode*> arg_types = {
        udf::DataTypeTrait<T>::to_type_node(&nm)...};
    std::vector<int> arg_nullable = {udf::IsNullableTrait<T>::value...};
    std::vector<ExprNode*> args;
    for (size_t i = 0; i < sizeof...(T); ++i) {
        auto expr_id = nm.MakeExprIdNode("arg_" + std::to_string(i),
                                         ExprIdNode::GetNewId());
        expr_id->SetOutputType(arg_types[i]);
        expr_id->SetNullable(arg_nullable[i]);
        args.push_back(expr_id);
    }

    ExprNode* expr =
        DoBuildExpr(build_expr, &nm, args, std::index_sequence_for<T...>());

    auto library = udf::DefaultUDFLibrary::get();
    ExprAnalysisContext ctx(&nm, library, nullptr);
    auto status = expr->InferAttr(&ctx);
    LOG(INFO) << "Infer expr status: " << status;
    ASSERT_TRUE(!status.isOK());
}

TEST_F(ExprNodeTest, CondExprNodeTest) {
    auto do_build = [](NodeManager* nm, ExprNode* cond, ExprNode* left,
                       ExprNode* right) {
        return nm->MakeCondExpr(cond, left, right);
    };

    CheckInfer<float, bool, float, float>(do_build);
    CheckInfer<Nullable<float>, bool, Nullable<float>, float>(do_build);
    CheckInfer<Nullable<float>, bool, float, Nullable<float>>(do_build);

    CheckInferError<int32_t, float, float>(do_build);
    CheckInferError<bool, double, float>(do_build);
}

void TestInferBinaryArithmetic(FnOperator op) {
    auto do_build = [op](NodeManager* nm, ExprNode* left, ExprNode* right) {
        return nm->MakeBinaryExprNode(left, right, op);
    };
    CheckInfer<bool, bool, bool>(do_build);
    CheckInfer<int16_t, bool, int16_t>(do_build);
    CheckInfer<int32_t, bool, int32_t>(do_build);
    CheckInfer<int64_t, bool, int64_t>(do_build);
    CheckInfer<float, bool, float>(do_build);
    CheckInfer<double, bool, double>(do_build);
    //    CheckInferError<bool, Timestamp>(do_build);

    CheckInfer<int16_t, int16_t, int16_t>(do_build);
    CheckInfer<int32_t, int16_t, int32_t>(do_build);
    CheckInfer<int64_t, int16_t, int64_t>(do_build);
    CheckInfer<float, int16_t, float>(do_build);
    CheckInfer<double, int16_t, double>(do_build);
    CheckInfer<Timestamp, int16_t, Timestamp>(do_build);
    CheckInfer<int32_t, int32_t, int16_t>(do_build);
    CheckInfer<int32_t, int32_t, int32_t>(do_build);
    CheckInfer<int64_t, int32_t, int64_t>(do_build);
    CheckInfer<float, int32_t, float>(do_build);
    CheckInfer<double, int32_t, double>(do_build);
    CheckInfer<Timestamp, int32_t, Timestamp>(do_build);
    CheckInfer<int64_t, int64_t, int16_t>(do_build);
    CheckInfer<int64_t, int64_t, int32_t>(do_build);
    CheckInfer<int64_t, int64_t, int64_t>(do_build);
    CheckInfer<float, int64_t, float>(do_build);
    CheckInfer<double, int64_t, double>(do_build);
    CheckInfer<Timestamp, int64_t, Timestamp>(do_build);
    CheckInfer<float, float, int16_t>(do_build);
    CheckInfer<float, float, int32_t>(do_build);
    CheckInfer<float, float, int64_t>(do_build);
    CheckInfer<float, float, float>(do_build);
    CheckInfer<double, float, double>(do_build);
    //    CheckInferError<float, Timestamp>(do_build);
    CheckInfer<double, double, int16_t>(do_build);
    CheckInfer<double, double, int32_t>(do_build);
    CheckInfer<double, double, int64_t>(do_build);
    CheckInfer<double, double, float>(do_build);
    CheckInfer<double, double, double>(do_build);
    CheckInferError<double, Timestamp>(do_build);
    CheckInfer<Timestamp, Timestamp, int16_t>(do_build);
    CheckInfer<Timestamp, Timestamp, int32_t>(do_build);
    CheckInfer<Timestamp, Timestamp, int64_t>(do_build);
    //    CheckInferError<Timestamp, float>(do_build);
    //    CheckInferError<Timestamp, double>(do_build);
    CheckInfer<Timestamp, Timestamp, Timestamp>(do_build);
}

void TestInferBinaryCompare(FnOperator op) {
    auto do_build = [op](NodeManager* nm, ExprNode* left, ExprNode* right) {
        return nm->MakeBinaryExprNode(left, right, op);
    };
    CheckInfer<bool, bool, bool>(do_build);
    CheckInfer<bool, bool, int16_t>(do_build);
    CheckInfer<bool, bool, int32_t>(do_build);
    CheckInfer<bool, bool, int64_t>(do_build);
    CheckInfer<bool, bool, float>(do_build);
    CheckInfer<bool, bool, double>(do_build);
    //    CheckInferError<bool, Timestamp>(do_build);

    CheckInfer<bool, int16_t, int16_t>(do_build);
    CheckInfer<bool, int16_t, int32_t>(do_build);
    CheckInfer<bool, int16_t, int64_t>(do_build);
    CheckInfer<bool, int16_t, float>(do_build);
    CheckInfer<bool, int16_t, double>(do_build);
    CheckInfer<bool, int16_t, Timestamp>(do_build);
    CheckInfer<bool, int32_t, int16_t>(do_build);
    CheckInfer<bool, int32_t, int32_t>(do_build);
    CheckInfer<bool, int32_t, int64_t>(do_build);
    CheckInfer<bool, int32_t, float>(do_build);
    CheckInfer<bool, int32_t, double>(do_build);
    CheckInfer<bool, int32_t, Timestamp>(do_build);
    CheckInfer<bool, int64_t, int16_t>(do_build);
    CheckInfer<bool, int64_t, int32_t>(do_build);
    CheckInfer<bool, int64_t, int64_t>(do_build);
    CheckInfer<bool, int64_t, float>(do_build);
    CheckInfer<bool, int64_t, double>(do_build);
    CheckInfer<bool, int64_t, Timestamp>(do_build);
    CheckInfer<bool, float, int16_t>(do_build);
    CheckInfer<bool, float, int32_t>(do_build);
    CheckInfer<bool, float, int64_t>(do_build);
    CheckInfer<bool, float, float>(do_build);
    CheckInfer<bool, float, double>(do_build);
    //    CheckInferError<float, Timestamp>(do_build);
    CheckInfer<bool, double, int16_t>(do_build);
    CheckInfer<bool, double, int32_t>(do_build);
    CheckInfer<bool, double, int64_t>(do_build);
    CheckInfer<bool, double, float>(do_build);
    CheckInfer<bool, double, double>(do_build);
    //    CheckInferError<double, Timestamp>(do_build);
    CheckInfer<bool, Timestamp, int16_t>(do_build);
    CheckInfer<bool, Timestamp, int32_t>(do_build);
    CheckInfer<bool, Timestamp, int64_t>(do_build);
    //    CheckInferError<Timestamp, float>(do_build);
    //    CheckInferError<Timestamp, double>(do_build);
    CheckInfer<bool, Timestamp, Timestamp>(do_build);
}

void TestInferBinaryLogical(FnOperator op) {
    auto do_build = [op](NodeManager* nm, ExprNode* left, ExprNode* right) {
        return nm->MakeBinaryExprNode(left, right, op);
    };
    CheckInfer<bool, bool, bool>(do_build);
    CheckInfer<bool, bool, int32_t>(do_build);
    CheckInfer<bool, int16_t, int64_t>(do_build);
    CheckInfer<bool, int32_t, int32_t>(do_build);
    CheckInfer<bool, float, bool>(do_build);
}

TEST_F(ExprNodeTest, InferBinaryArithmeticTypeTest) {
    TestInferBinaryArithmetic(kFnOpAdd);
    TestInferBinaryArithmetic(kFnOpMinus);
    TestInferBinaryArithmetic(kFnOpMulti);
    TestInferBinaryArithmetic(kFnOpDiv);
    TestInferBinaryArithmetic(kFnOpMod);
}

TEST_F(ExprNodeTest, InferBinaryCompareTypeTest) {
    TestInferBinaryCompare(kFnOpEq);
    TestInferBinaryCompare(kFnOpNeq);
    TestInferBinaryCompare(kFnOpLt);
    TestInferBinaryCompare(kFnOpLe);
    TestInferBinaryCompare(kFnOpGt);
    TestInferBinaryCompare(kFnOpGe);
}

TEST_F(ExprNodeTest, InferBinaryLogicalTypeTest) {
    TestInferBinaryLogical(kFnOpAnd);
    TestInferBinaryLogical(kFnOpOr);
    TestInferBinaryLogical(kFnOpXor);
}

TEST_F(ExprNodeTest, InferNotTypeTest) {
    auto do_build = [](NodeManager* nm, ExprNode* expr) {
        return nm->MakeUnaryExprNode(expr, kFnOpNot);
    };
    CheckInfer<bool, bool>(do_build);
    CheckInfer<bool, int16_t>(do_build);
    CheckInfer<bool, int32_t>(do_build);
    CheckInfer<bool, int64_t>(do_build);
    CheckInfer<bool, float>(do_build);
    CheckInfer<bool, double>(do_build);
}

TEST_F(ExprNodeTest, InferUnaryMinusTypeTest) {
    auto do_build = [](NodeManager* nm, ExprNode* expr) {
        return nm->MakeUnaryExprNode(expr, kFnOpMinus);
    };
    CheckInfer<int16_t, int16_t>(do_build);
    CheckInfer<int32_t, int32_t>(do_build);
    CheckInfer<int64_t, int64_t>(do_build);
    CheckInfer<float, float>(do_build);
    CheckInfer<double, double>(do_build);
}

}  // namespace node
}  // namespace fesql

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
