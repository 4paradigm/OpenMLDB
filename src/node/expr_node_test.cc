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
#include <utility>
#include "node/expr_node.h"
#include "node/sql_node.h"
#include "gtest/gtest.h"
#include "node/node_manager.h"
#include "udf/literal_traits.h"

namespace fesql {
namespace node {

class ExprNodeTest : public ::testing::Test {
 public:
    SqlNodeTest() { node_manager_ = new NodeManager(); }

    ~SqlNodeTest() { delete node_manager_; }

 protected:
    NodeManager *node_manager_;
};


template <typename Ret, typename... T>
void CheckInfer(const std::function<ExprNode*(NodeManager*, typename std::pair<T, ExprNode*>::second_type...)>& build_expr) {
    NodeManager nm;
    std::vector<node::TypeNode*> arg_types = { DataTypeTrait<T>::to_type_node(&nm)... };
    std::vector<int> arg_nullable = { IsNullableTrait<T>::value... };
    std::vector<ExprNode*> args;
    for (size_t i = 0; i < sizeof...(T); ++i) {
        auto expr_id = nm->MakeExprIdNode("arg_" + std::to_string(i), ExprIdNode::GetNewId());
        expr_id->SetOutputType(arg_types[i]);
        expr_id->SetNullable(arg_nullable[i]);
        args.push_back(expr_id);
    }

    ExprNode* expr = DoBuildExpr(build_expr, &nm, args, std::index_sequence_for<Args...>());
    
    ExprAnalysisContext ctx(&nm, nullptr);
    auto status = expr->InferAttr(ctx);
    LOG(INFO) << "Infer expr status: " << status.msg;
    ASSERT_TRUE(status.isOK());

    ASSERT_EQ(IsNullableTrait<Ret>::value, expr->nullable());
    ASSERT_TRUE(TypeEquals(DataTypeTrait<Ret>::to_type_node(&nm), expr->GetOutputType()));
}

template <typename Ret, typename... T>
void CheckInferError(const std::function<ExprNode*(NodeManager*, typename std::pair<T, ExprNode*>::second_type...)>& build_expr) {
    NodeManager nm;
    std::vector<node::TypeNode*> arg_types = { DataTypeTrait<T>::to_type_node(&nm)... };
    std::vector<int> arg_nullable = { IsNullableTrait<T>::value... };
    std::vector<ExprNode*> args;
    for (size_t i = 0; i < sizeof...(T); ++i) {
        auto expr_id = nm->MakeExprIdNode("arg_" + std::to_string(i), ExprIdNode::GetNewId());
        expr_id->SetOutputType(arg_types[i]);
        expr_id->SetNullable(arg_nullable[i]);
        args.push_back(expr_id);
    }

    ExprNode* expr = DoBuildExpr(build_expr, &nm, args, std::index_sequence_for<Args...>());
    
    ExprAnalysisContext ctx(&nm, nullptr);
    auto status = expr->InferAttr(ctx);
    LOG(INFO) << "Infer expr status: " << status.msg;
    ASSERT_TRUE(!status.isOK());
}


TEST_F(ExprNodeTest, CondExprNodeTest) {
    auto do_build = [](NodeManager* nm, ExprNode* cond, ExprNode* left, ExprNode* right) {
        return nm->MakeCondExpr(cond, left, right);
    };

    CheckInfer<float, bool, float, float>(do_build);
    CheckInfer<Nullable<float>, bool, Nullable<float>, float>(do_build);
    CheckInfer<Nullable<float>, bool, float, Nullable<float>>(do_build);

    CheckInferError<float, int32_t, float, float>(do_build);
    CheckInferError<float, bool, double, float>(do_build);
}

}  // namespace node
}  // namespace fesql

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
