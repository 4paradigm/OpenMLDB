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

#include "node/expr_node.h"
#include <utility>
#include <vector>
#include "base/type.h"
#include "gtest/gtest.h"
#include "node/node_manager.h"
#include "node/sql_node.h"
#include "udf/default_udf_library.h"
#include "udf/literal_traits.h"

namespace hybridse {
namespace node {

using openmldb::base::Timestamp;
using openmldb::base::Date;
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
        auto expr_id = nm.MakeExprIdNode("arg_" + std::to_string(i));
        expr_id->SetOutputType(arg_types[i]);
        expr_id->SetNullable(arg_nullable[i]);
        args.push_back(expr_id);
    }

    ExprNode* expr =
        DoBuildExpr(build_expr, &nm, args, std::index_sequence_for<T...>());

    auto library = udf::DefaultUdfLibrary::get();
    ExprAnalysisContext ctx(&nm, library, nullptr, nullptr);
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
        auto expr_id = nm.MakeExprIdNode("arg_" + std::to_string(i));
        expr_id->SetOutputType(arg_types[i]);
        expr_id->SetNullable(arg_nullable[i]);
        args.push_back(expr_id);
    }

    ExprNode* expr =
        DoBuildExpr(build_expr, &nm, args, std::index_sequence_for<T...>());

    auto library = udf::DefaultUdfLibrary::get();
    ExprAnalysisContext ctx(&nm, library, nullptr, nullptr);
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
    CheckInfer<double, bool, double, float>(do_build);
    CheckInfer<double, bool, int64_t, float>(do_build);
    CheckInfer<codec::StringRef, bool, int64_t, codec::StringRef>(do_build);

    CheckInferError<int32_t, float, float>(do_build);
}
template <typename RET, typename LHS, typename RHS>
void CheckBinaryOpInfer(FnOperator op) {
    auto do_build = [op](NodeManager* nm, ExprNode* left, ExprNode* right) {
        return nm->MakeBinaryExprNode(left, right, op);
    };
    CheckInfer<RET, LHS, RHS>(do_build);
}
template <typename LHS, typename RHS>
void CheckBinaryOpInferError(FnOperator op) {
    auto do_build = [op](NodeManager* nm, ExprNode* left, ExprNode* right) {
        return nm->MakeBinaryExprNode(left, right, op);
    };
    CheckInferError<LHS, RHS>(do_build);
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
    CheckInfer<bool, bool, codec::StringRef>(do_build);

    CheckInfer<bool, int16_t, int16_t>(do_build);
    CheckInfer<bool, int16_t, int32_t>(do_build);
    CheckInfer<bool, int16_t, int64_t>(do_build);
    CheckInfer<bool, int16_t, float>(do_build);
    CheckInfer<bool, int16_t, double>(do_build);
    CheckInfer<bool, int16_t, codec::StringRef>(do_build);

    CheckInfer<bool, int32_t, int16_t>(do_build);
    CheckInfer<bool, int32_t, int32_t>(do_build);
    CheckInfer<bool, int32_t, int64_t>(do_build);
    CheckInfer<bool, int32_t, float>(do_build);
    CheckInfer<bool, int32_t, double>(do_build);
    CheckInfer<bool, int32_t, codec::StringRef>(do_build);

    CheckInfer<bool, int64_t, int16_t>(do_build);
    CheckInfer<bool, int64_t, int32_t>(do_build);
    CheckInfer<bool, int64_t, int64_t>(do_build);
    CheckInfer<bool, int64_t, float>(do_build);
    CheckInfer<bool, int64_t, double>(do_build);
    CheckInfer<bool, int64_t, codec::StringRef>(do_build);

    CheckInfer<bool, float, int16_t>(do_build);
    CheckInfer<bool, float, int32_t>(do_build);
    CheckInfer<bool, float, int64_t>(do_build);
    CheckInfer<bool, float, float>(do_build);
    CheckInfer<bool, float, double>(do_build);
    CheckInfer<bool, float, codec::StringRef>(do_build);

    CheckInfer<bool, double, int16_t>(do_build);
    CheckInfer<bool, double, int32_t>(do_build);
    CheckInfer<bool, double, int64_t>(do_build);
    CheckInfer<bool, double, float>(do_build);
    CheckInfer<bool, double, double>(do_build);
    CheckInfer<bool, double, codec::StringRef>(do_build);

    CheckInferError<Timestamp, bool>(do_build);
    CheckInferError<Timestamp, int16_t>(do_build);
    CheckInferError<Timestamp, int32_t>(do_build);
    CheckInferError<Timestamp, int64_t>(do_build);
    CheckInferError<Timestamp, float>(do_build);
    CheckInferError<Timestamp, double>(do_build);
    CheckInferError<Timestamp, Date>(do_build);
    CheckInferError<Date, bool>(do_build);
    CheckInferError<Date, int16_t>(do_build);
    CheckInferError<Date, int32_t>(do_build);
    CheckInferError<Date, int64_t>(do_build);
    CheckInferError<Date, float>(do_build);
    CheckInferError<Date, double>(do_build);
    CheckInferError<Date, Timestamp>(do_build);
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
TEST_F(ExprNodeTest, AddTypeInferTest_1) {
    CheckBinaryOpInfer<bool, bool, bool>(kFnOpAdd);
}
TEST_F(ExprNodeTest, AddTypeInferTest_2) {
    CheckBinaryOpInfer<int16_t, int16_t, bool>(kFnOpAdd);
    CheckBinaryOpInfer<int16_t, bool, int16_t>(kFnOpAdd);
}
TEST_F(ExprNodeTest, AddTypeInferTest_3) {
    CheckBinaryOpInfer<int32_t, int32_t, bool>(kFnOpAdd);
    CheckBinaryOpInfer<int32_t, bool, int32_t>(kFnOpAdd);
}
TEST_F(ExprNodeTest, AddTypeInferTest_4) {
    CheckBinaryOpInfer<int64_t, int64_t, bool>(kFnOpAdd);
    CheckBinaryOpInfer<int64_t, bool, int64_t>(kFnOpAdd);
}
TEST_F(ExprNodeTest, AddTypeInferTest_5) {
    CheckBinaryOpInfer<float, float, bool>(kFnOpAdd);
    CheckBinaryOpInfer<float, bool, float>(kFnOpAdd);
}
TEST_F(ExprNodeTest, AddTypeInferTest_6) {
    CheckBinaryOpInfer<double, double, bool>(kFnOpAdd);
    CheckBinaryOpInfer<double, bool, double>(kFnOpAdd);
}
TEST_F(ExprNodeTest, AddTypeInferTest_7) {
    CheckBinaryOpInfer<int16_t, bool, int16_t>(kFnOpAdd);
}
TEST_F(ExprNodeTest, AddTypeInferTest_8) {
    CheckBinaryOpInfer<int16_t, int16_t, int16_t>(kFnOpAdd);
}
TEST_F(ExprNodeTest, AddTypeInferTest_9) {
    CheckBinaryOpInfer<int32_t, int16_t, int32_t>(kFnOpAdd);
    CheckBinaryOpInfer<int32_t, int32_t, int16_t>(kFnOpAdd);
}
TEST_F(ExprNodeTest, AddTypeInferTest_10) {
    CheckBinaryOpInfer<int64_t, int16_t, int64_t>(kFnOpAdd);
    CheckBinaryOpInfer<int64_t, int64_t, int16_t>(kFnOpAdd);
}
TEST_F(ExprNodeTest, AddTypeInferTest_11) {
    CheckBinaryOpInfer<float, int16_t, float>(kFnOpAdd);
    CheckBinaryOpInfer<float, float, int16_t>(kFnOpAdd);
}
TEST_F(ExprNodeTest, AddTypeInferTest_12) {
    CheckBinaryOpInfer<double, int16_t, double>(kFnOpAdd);
    CheckBinaryOpInfer<double, double, int16_t>(kFnOpAdd);
}
TEST_F(ExprNodeTest, AddTypeInferTest_13) {
    CheckBinaryOpInfer<int32_t, int32_t, int32_t>(kFnOpAdd);
}
TEST_F(ExprNodeTest, AddTypeInferTest_14) {
    CheckBinaryOpInfer<int64_t, int32_t, int64_t>(kFnOpAdd);
    CheckBinaryOpInfer<int64_t, int64_t, int32_t>(kFnOpAdd);
}
TEST_F(ExprNodeTest, AddTypeInferTest_15) {
    CheckBinaryOpInfer<float, int32_t, float>(kFnOpAdd);
    CheckBinaryOpInfer<float, float, int32_t>(kFnOpAdd);
}
TEST_F(ExprNodeTest, AddTypeInferTest_16) {
    CheckBinaryOpInfer<double, int32_t, double>(kFnOpAdd);
    CheckBinaryOpInfer<double, double, int32_t>(kFnOpAdd);
}
TEST_F(ExprNodeTest, AddTypeInferTest_17) {
    CheckBinaryOpInfer<int64_t, int64_t, int64_t>(kFnOpAdd);
}
TEST_F(ExprNodeTest, AddTypeInferTest_18) {
    CheckBinaryOpInfer<float, int64_t, float>(kFnOpAdd);
    CheckBinaryOpInfer<float, float, int64_t>(kFnOpAdd);
}
TEST_F(ExprNodeTest, AddTypeInferTest_19) {
    CheckBinaryOpInfer<double, int64_t, double>(kFnOpAdd);
    CheckBinaryOpInfer<double, double, int64_t>(kFnOpAdd);
}
TEST_F(ExprNodeTest, AddTypeInferTest_20) {
    CheckBinaryOpInfer<float, float, float>(kFnOpAdd);
}
TEST_F(ExprNodeTest, AddTypeInferTest_21) {
    CheckBinaryOpInfer<double, float, double>(kFnOpAdd);
    CheckBinaryOpInfer<double, double, float>(kFnOpAdd);
}
TEST_F(ExprNodeTest, AddTypeInferTest_22) {
    CheckBinaryOpInfer<double, double, double>(kFnOpAdd);
}
TEST_F(ExprNodeTest, AddTypeInferTest_23) {
    CheckBinaryOpInfer<Timestamp, Timestamp, bool>(kFnOpAdd);
    CheckBinaryOpInfer<Timestamp, bool, Timestamp>(kFnOpAdd);
}
TEST_F(ExprNodeTest, AddTypeInferTest_24) {
    CheckBinaryOpInfer<Timestamp, Timestamp, int16_t>(kFnOpAdd);
    CheckBinaryOpInfer<Timestamp, int16_t, Timestamp>(kFnOpAdd);
}
TEST_F(ExprNodeTest, AddTypeInferTest_25) {
    CheckBinaryOpInfer<Timestamp, Timestamp, int32_t>(kFnOpAdd);
    CheckBinaryOpInfer<Timestamp, int32_t, Timestamp>(kFnOpAdd);
}
TEST_F(ExprNodeTest, AddTypeInferTest_26) {
    CheckBinaryOpInfer<Timestamp, Timestamp, int64_t>(kFnOpAdd);
    CheckBinaryOpInfer<Timestamp, int64_t, Timestamp>(kFnOpAdd);
}

TEST_F(ExprNodeTest, SubTypeInferTest_1) {
    CheckBinaryOpInfer<bool, bool, bool>(kFnOpMinus);
}
TEST_F(ExprNodeTest, SubTypeInferTest_2) {
    CheckBinaryOpInfer<int16_t, int16_t, bool>(kFnOpMinus);
    CheckBinaryOpInfer<int16_t, bool, int16_t>(kFnOpMinus);
}
TEST_F(ExprNodeTest, SubTypeInferTest_3) {
    CheckBinaryOpInfer<int32_t, int32_t, bool>(kFnOpMinus);
    CheckBinaryOpInfer<int32_t, bool, int32_t>(kFnOpMinus);
}
TEST_F(ExprNodeTest, SubTypeInferTest_4) {
    CheckBinaryOpInfer<int64_t, int64_t, bool>(kFnOpMinus);
    CheckBinaryOpInfer<int64_t, bool, int64_t>(kFnOpMinus);
}
TEST_F(ExprNodeTest, SubTypeInferTest_5) {
    CheckBinaryOpInfer<float, float, bool>(kFnOpMinus);
    CheckBinaryOpInfer<float, bool, float>(kFnOpMinus);
}
TEST_F(ExprNodeTest, SubTypeInferTest_6) {
    CheckBinaryOpInfer<double, double, bool>(kFnOpMinus);
    CheckBinaryOpInfer<double, bool, double>(kFnOpMinus);
}
TEST_F(ExprNodeTest, SubTypeInferTest_7) {
    CheckBinaryOpInfer<int16_t, bool, int16_t>(kFnOpMinus);
}
TEST_F(ExprNodeTest, SubTypeInferTest_8) {
    CheckBinaryOpInfer<int16_t, int16_t, int16_t>(kFnOpMinus);
}
TEST_F(ExprNodeTest, SubTypeInferTest_9) {
    CheckBinaryOpInfer<int32_t, int16_t, int32_t>(kFnOpMinus);
    CheckBinaryOpInfer<int32_t, int32_t, int16_t>(kFnOpMinus);
}
TEST_F(ExprNodeTest, SubTypeInferTest_10) {
    CheckBinaryOpInfer<int64_t, int16_t, int64_t>(kFnOpMinus);
    CheckBinaryOpInfer<int64_t, int64_t, int16_t>(kFnOpMinus);
}
TEST_F(ExprNodeTest, SubTypeInferTest_11) {
    CheckBinaryOpInfer<float, int16_t, float>(kFnOpMinus);
    CheckBinaryOpInfer<float, float, int16_t>(kFnOpMinus);
}
TEST_F(ExprNodeTest, SubTypeInferTest_12) {
    CheckBinaryOpInfer<double, int16_t, double>(kFnOpMinus);
    CheckBinaryOpInfer<double, double, int16_t>(kFnOpMinus);
}
TEST_F(ExprNodeTest, SubTypeInferTest_13) {
    CheckBinaryOpInfer<int32_t, int32_t, int32_t>(kFnOpMinus);
}
TEST_F(ExprNodeTest, SubTypeInferTest_14) {
    CheckBinaryOpInfer<int64_t, int32_t, int64_t>(kFnOpMinus);
    CheckBinaryOpInfer<int64_t, int64_t, int32_t>(kFnOpMinus);
}
TEST_F(ExprNodeTest, SubTypeInferTest_15) {
    CheckBinaryOpInfer<float, int32_t, float>(kFnOpMinus);
    CheckBinaryOpInfer<float, float, int32_t>(kFnOpMinus);
}
TEST_F(ExprNodeTest, SubTypeInferTest_16) {
    CheckBinaryOpInfer<double, int32_t, double>(kFnOpMinus);
    CheckBinaryOpInfer<double, double, int32_t>(kFnOpMinus);
}
TEST_F(ExprNodeTest, SubTypeInferTest_17) {
    CheckBinaryOpInfer<int64_t, int64_t, int64_t>(kFnOpMinus);
}
TEST_F(ExprNodeTest, SubTypeInferTest_18) {
    CheckBinaryOpInfer<float, int64_t, float>(kFnOpMinus);
    CheckBinaryOpInfer<float, float, int64_t>(kFnOpMinus);
}
TEST_F(ExprNodeTest, SubTypeInferTest_19) {
    CheckBinaryOpInfer<double, int64_t, double>(kFnOpMinus);
    CheckBinaryOpInfer<double, double, int64_t>(kFnOpMinus);
}
TEST_F(ExprNodeTest, SubTypeInferTest_20) {
    CheckBinaryOpInfer<float, float, float>(kFnOpMinus);
}
TEST_F(ExprNodeTest, SubTypeInferTest_21) {
    CheckBinaryOpInfer<double, float, double>(kFnOpMinus);
    CheckBinaryOpInfer<double, double, float>(kFnOpMinus);
}
TEST_F(ExprNodeTest, SubTypeInferTest_22) {
    CheckBinaryOpInfer<double, double, double>(kFnOpMinus);
}
TEST_F(ExprNodeTest, SubTypeInferTest_23) {
    CheckBinaryOpInfer<Timestamp, Timestamp, bool>(kFnOpMinus);
    CheckBinaryOpInferError<bool, Timestamp>(kFnOpMinus);
}
TEST_F(ExprNodeTest, SubTypeInferTest_24) {
    CheckBinaryOpInfer<Timestamp, Timestamp, int16_t>(kFnOpMinus);
    CheckBinaryOpInferError<int16_t, Timestamp>(kFnOpMinus);
}
TEST_F(ExprNodeTest, SubTypeInferTest_25) {
    CheckBinaryOpInfer<Timestamp, Timestamp, int32_t>(kFnOpMinus);
    CheckBinaryOpInferError<int32_t, Timestamp>(kFnOpMinus);
}
TEST_F(ExprNodeTest, SubTypeInferTest_26) {
    CheckBinaryOpInfer<Timestamp, Timestamp, int64_t>(kFnOpMinus);
    CheckBinaryOpInferError<int64_t, Timestamp>(kFnOpMinus);
}
TEST_F(ExprNodeTest, MultiTypeInferTest_1) {
    CheckBinaryOpInfer<bool, bool, bool>(kFnOpMulti);
}
TEST_F(ExprNodeTest, MultiTypeInferTest_2) {
    CheckBinaryOpInfer<int16_t, int16_t, bool>(kFnOpMulti);
    CheckBinaryOpInfer<int16_t, bool, int16_t>(kFnOpMulti);
}
TEST_F(ExprNodeTest, MultiTypeInferTest_3) {
    CheckBinaryOpInfer<int32_t, int32_t, bool>(kFnOpMulti);
    CheckBinaryOpInfer<int32_t, bool, int32_t>(kFnOpMulti);
}
TEST_F(ExprNodeTest, MultiTypeInferTest_4) {
    CheckBinaryOpInfer<int64_t, int64_t, bool>(kFnOpMulti);
    CheckBinaryOpInfer<int64_t, bool, int64_t>(kFnOpMulti);
}
TEST_F(ExprNodeTest, MultiTypeInferTest_5) {
    CheckBinaryOpInfer<float, float, bool>(kFnOpMulti);
    CheckBinaryOpInfer<float, bool, float>(kFnOpMulti);
}
TEST_F(ExprNodeTest, MultiTypeInferTest_6) {
    CheckBinaryOpInfer<double, double, bool>(kFnOpMulti);
    CheckBinaryOpInfer<double, bool, double>(kFnOpMulti);
}
TEST_F(ExprNodeTest, MultiTypeInferTest_7) {
    CheckBinaryOpInfer<int16_t, bool, int16_t>(kFnOpMulti);
}
TEST_F(ExprNodeTest, MultiTypeInferTest_8) {
    CheckBinaryOpInfer<int16_t, int16_t, int16_t>(kFnOpMulti);
}
TEST_F(ExprNodeTest, MultiTypeInferTest_9) {
    CheckBinaryOpInfer<int32_t, int16_t, int32_t>(kFnOpMulti);
    CheckBinaryOpInfer<int32_t, int32_t, int16_t>(kFnOpMulti);
}
TEST_F(ExprNodeTest, MultiTypeInferTest_10) {
    CheckBinaryOpInfer<int64_t, int16_t, int64_t>(kFnOpMulti);
    CheckBinaryOpInfer<int64_t, int64_t, int16_t>(kFnOpMulti);
}
TEST_F(ExprNodeTest, MultiTypeInferTest_11) {
    CheckBinaryOpInfer<float, int16_t, float>(kFnOpMulti);
    CheckBinaryOpInfer<float, float, int16_t>(kFnOpMulti);
}
TEST_F(ExprNodeTest, MultiTypeInferTest_12) {
    CheckBinaryOpInfer<double, int16_t, double>(kFnOpMulti);
    CheckBinaryOpInfer<double, double, int16_t>(kFnOpMulti);
}
TEST_F(ExprNodeTest, MultiTypeInferTest_13) {
    CheckBinaryOpInfer<int32_t, int32_t, int32_t>(kFnOpMulti);
}
TEST_F(ExprNodeTest, MultiTypeInferTest_14) {
    CheckBinaryOpInfer<int64_t, int32_t, int64_t>(kFnOpMulti);
    CheckBinaryOpInfer<int64_t, int64_t, int32_t>(kFnOpMulti);
}
TEST_F(ExprNodeTest, MultiTypeInferTest_15) {
    CheckBinaryOpInfer<float, int32_t, float>(kFnOpMulti);
    CheckBinaryOpInfer<float, float, int32_t>(kFnOpMulti);
}
TEST_F(ExprNodeTest, MultiTypeInferTest_16) {
    CheckBinaryOpInfer<double, int32_t, double>(kFnOpMulti);
    CheckBinaryOpInfer<double, double, int32_t>(kFnOpMulti);
}
TEST_F(ExprNodeTest, MultiTypeInferTest_17) {
    CheckBinaryOpInfer<int64_t, int64_t, int64_t>(kFnOpMulti);
}
TEST_F(ExprNodeTest, MultiTypeInferTest_18) {
    CheckBinaryOpInfer<float, int64_t, float>(kFnOpMulti);
    CheckBinaryOpInfer<float, float, int64_t>(kFnOpMulti);
}
TEST_F(ExprNodeTest, MultiTypeInferTest_19) {
    CheckBinaryOpInfer<double, int64_t, double>(kFnOpMulti);
    CheckBinaryOpInfer<double, double, int64_t>(kFnOpMulti);
}
TEST_F(ExprNodeTest, MultiTypeInferTest_20) {
    CheckBinaryOpInfer<float, float, float>(kFnOpMulti);
}
TEST_F(ExprNodeTest, MultiTypeInferTest_21) {
    CheckBinaryOpInfer<double, float, double>(kFnOpMulti);
    CheckBinaryOpInfer<double, double, float>(kFnOpMulti);
}
TEST_F(ExprNodeTest, MultiTypeInferTest_22) {
    CheckBinaryOpInfer<double, double, double>(kFnOpMulti);
}
TEST_F(ExprNodeTest, MultiTypeInferTest_23) {
    CheckBinaryOpInferError<Timestamp, bool>(kFnOpMulti);
    CheckBinaryOpInferError<bool, Timestamp>(kFnOpMulti);
}
TEST_F(ExprNodeTest, MultiTypeInferTest_24) {
    CheckBinaryOpInferError<Timestamp, int16_t>(kFnOpMulti);
    CheckBinaryOpInferError<int16_t, Timestamp>(kFnOpMulti);
}
TEST_F(ExprNodeTest, MultiTypeInferTest_25) {
    CheckBinaryOpInferError<Timestamp, int32_t>(kFnOpMulti);
    CheckBinaryOpInferError<int32_t, Timestamp>(kFnOpMulti);
}
TEST_F(ExprNodeTest, MultiTypeInferTest_26) {
    CheckBinaryOpInferError<Timestamp, int64_t>(kFnOpMulti);
    CheckBinaryOpInferError<int64_t, Timestamp>(kFnOpMulti);
}

TEST_F(ExprNodeTest, FDivTypeInferTest_1) {
    CheckBinaryOpInfer<double, bool, bool>(kFnOpFDiv);
}
TEST_F(ExprNodeTest, FDivTypeInferTest_2) {
    CheckBinaryOpInfer<double, int16_t, bool>(kFnOpFDiv);
    CheckBinaryOpInfer<double, bool, int16_t>(kFnOpFDiv);
}
TEST_F(ExprNodeTest, FDivTypeInferTest_3) {
    CheckBinaryOpInfer<double, int32_t, bool>(kFnOpFDiv);
    CheckBinaryOpInfer<double, bool, int32_t>(kFnOpFDiv);
}
TEST_F(ExprNodeTest, FDivTypeInferTest_4) {
    CheckBinaryOpInfer<double, int64_t, bool>(kFnOpFDiv);
    CheckBinaryOpInfer<double, bool, int64_t>(kFnOpFDiv);
}
TEST_F(ExprNodeTest, FDivTypeInferTest_5) {
    CheckBinaryOpInfer<double, float, bool>(kFnOpFDiv);
    CheckBinaryOpInfer<double, bool, float>(kFnOpFDiv);
}
TEST_F(ExprNodeTest, FDivTypeInferTest_6) {
    CheckBinaryOpInfer<double, double, bool>(kFnOpFDiv);
    CheckBinaryOpInfer<double, bool, double>(kFnOpFDiv);
}
TEST_F(ExprNodeTest, FDivTypeInferTest_7) {
    CheckBinaryOpInfer<double, bool, int16_t>(kFnOpFDiv);
}
TEST_F(ExprNodeTest, FDivTypeInferTest_8) {
    CheckBinaryOpInfer<double, int16_t, int16_t>(kFnOpFDiv);
}
TEST_F(ExprNodeTest, FDivTypeInferTest_9) {
    CheckBinaryOpInfer<double, int16_t, int32_t>(kFnOpFDiv);
    CheckBinaryOpInfer<double, int32_t, int16_t>(kFnOpFDiv);
}
TEST_F(ExprNodeTest, FDivTypeInferTest_10) {
    CheckBinaryOpInfer<double, int16_t, int64_t>(kFnOpFDiv);
    CheckBinaryOpInfer<double, int64_t, int16_t>(kFnOpFDiv);
}
TEST_F(ExprNodeTest, FDivTypeInferTest_11) {
    CheckBinaryOpInfer<double, int16_t, float>(kFnOpFDiv);
    CheckBinaryOpInfer<double, float, int16_t>(kFnOpFDiv);
}
TEST_F(ExprNodeTest, FDivTypeInferTest_12) {
    CheckBinaryOpInfer<double, int16_t, double>(kFnOpFDiv);
    CheckBinaryOpInfer<double, double, int16_t>(kFnOpFDiv);
}
TEST_F(ExprNodeTest, FDivTypeInferTest_13) {
    CheckBinaryOpInfer<double, int32_t, int32_t>(kFnOpFDiv);
}
TEST_F(ExprNodeTest, FDivTypeInferTest_14) {
    CheckBinaryOpInfer<double, int32_t, int64_t>(kFnOpFDiv);
    CheckBinaryOpInfer<double, int64_t, int32_t>(kFnOpFDiv);
}
TEST_F(ExprNodeTest, FDivTypeInferTest_15) {
    CheckBinaryOpInfer<double, int32_t, float>(kFnOpFDiv);
    CheckBinaryOpInfer<double, float, int32_t>(kFnOpFDiv);
}
TEST_F(ExprNodeTest, FDivTypeInferTest_16) {
    CheckBinaryOpInfer<double, int32_t, double>(kFnOpFDiv);
    CheckBinaryOpInfer<double, double, int32_t>(kFnOpFDiv);
}
TEST_F(ExprNodeTest, FDivTypeInferTest_17) {
    CheckBinaryOpInfer<double, int64_t, int64_t>(kFnOpFDiv);
}
TEST_F(ExprNodeTest, FDivTypeInferTest_18) {
    CheckBinaryOpInfer<double, int64_t, float>(kFnOpFDiv);
    CheckBinaryOpInfer<double, float, int64_t>(kFnOpFDiv);
}
TEST_F(ExprNodeTest, FDivTypeInferTest_19) {
    CheckBinaryOpInfer<double, int64_t, double>(kFnOpFDiv);
    CheckBinaryOpInfer<double, double, int64_t>(kFnOpFDiv);
}
TEST_F(ExprNodeTest, FDivTypeInferTest_20) {
    CheckBinaryOpInfer<double, float, float>(kFnOpFDiv);
}
TEST_F(ExprNodeTest, FDivTypeInferTest_21) {
    CheckBinaryOpInfer<double, float, double>(kFnOpFDiv);
    CheckBinaryOpInfer<double, double, float>(kFnOpFDiv);
}
TEST_F(ExprNodeTest, FDivTypeInferTest_22) {
    CheckBinaryOpInfer<double, double, double>(kFnOpFDiv);
}
TEST_F(ExprNodeTest, FDivTypeInferTest_23) {
    CheckBinaryOpInfer<double, Timestamp, bool>(kFnOpFDiv);
    CheckBinaryOpInferError<bool, Timestamp>(kFnOpFDiv);
}
TEST_F(ExprNodeTest, FDivTypeInferTest_24) {
    CheckBinaryOpInfer<double, Timestamp, int16_t>(kFnOpFDiv);
    CheckBinaryOpInferError<int16_t, Timestamp>(kFnOpFDiv);
}
TEST_F(ExprNodeTest, FDivTypeInferTest_25) {
    CheckBinaryOpInfer<double, Timestamp, int32_t>(kFnOpFDiv);
    CheckBinaryOpInferError<int32_t, Timestamp>(kFnOpFDiv);
}
TEST_F(ExprNodeTest, FDivTypeInferTest_26) {
    CheckBinaryOpInfer<double, Timestamp, int64_t>(kFnOpFDiv);
    CheckBinaryOpInferError<int64_t, Timestamp>(kFnOpFDiv);
}
TEST_F(ExprNodeTest, FDivTypeInferTest_27) {
    CheckBinaryOpInfer<double, Timestamp, float>(kFnOpFDiv);
    CheckBinaryOpInferError<float, Timestamp>(kFnOpFDiv);
}
TEST_F(ExprNodeTest, FDivTypeInferTest_28) {
    CheckBinaryOpInfer<double, Timestamp, double>(kFnOpFDiv);
    CheckBinaryOpInferError<double, Timestamp>(kFnOpFDiv);
}

TEST_F(ExprNodeTest, DIVTypeInferTest_1) {
    CheckBinaryOpInfer<bool, bool, bool>(kFnOpDiv);
}
TEST_F(ExprNodeTest, DIVTypeInferTest_2) {
    CheckBinaryOpInfer<int16_t, int16_t, bool>(kFnOpDiv);
    CheckBinaryOpInfer<int16_t, bool, int16_t>(kFnOpDiv);
}
TEST_F(ExprNodeTest, DIVTypeInferTest_3) {
    CheckBinaryOpInfer<int32_t, int32_t, bool>(kFnOpDiv);
    CheckBinaryOpInfer<int32_t, bool, int32_t>(kFnOpDiv);
}
TEST_F(ExprNodeTest, DIVTypeInferTest_4) {
    CheckBinaryOpInfer<int64_t, int64_t, bool>(kFnOpDiv);
    CheckBinaryOpInfer<int64_t, bool, int64_t>(kFnOpDiv);
}
TEST_F(ExprNodeTest, DIVTypeInferTest_5) {
    CheckBinaryOpInferError<float, bool>(kFnOpDiv);
    CheckBinaryOpInferError<bool, float>(kFnOpDiv);
}
TEST_F(ExprNodeTest, DIVTypeInferTest_6) {
    CheckBinaryOpInferError<double, bool>(kFnOpDiv);
    CheckBinaryOpInferError<bool, double>(kFnOpDiv);
}
TEST_F(ExprNodeTest, DIVTypeInferTest_7) {
    CheckBinaryOpInfer<int16_t, bool, int16_t>(kFnOpDiv);
}
TEST_F(ExprNodeTest, DIVTypeInferTest_8) {
    CheckBinaryOpInfer<int16_t, int16_t, int16_t>(kFnOpDiv);
}
TEST_F(ExprNodeTest, DIVTypeInferTest_9) {
    CheckBinaryOpInfer<int32_t, int16_t, int32_t>(kFnOpDiv);
    CheckBinaryOpInfer<int32_t, int32_t, int16_t>(kFnOpDiv);
}
TEST_F(ExprNodeTest, DIVTypeInferTest_10) {
    CheckBinaryOpInfer<int64_t, int16_t, int64_t>(kFnOpDiv);
    CheckBinaryOpInfer<int64_t, int64_t, int16_t>(kFnOpDiv);
}
TEST_F(ExprNodeTest, DIVTypeInferTest_11) {
    CheckBinaryOpInferError<int16_t, float>(kFnOpDiv);
    CheckBinaryOpInferError<float, int16_t>(kFnOpDiv);
}
TEST_F(ExprNodeTest, DIVTypeInferTest_12) {
    CheckBinaryOpInferError<int16_t, double>(kFnOpDiv);
    CheckBinaryOpInferError<double, int16_t>(kFnOpDiv);
}
TEST_F(ExprNodeTest, DIVTypeInferTest_13) {
    CheckBinaryOpInfer<int32_t, int32_t, int32_t>(kFnOpDiv);
}
TEST_F(ExprNodeTest, DIVTypeInferTest_14) {
    CheckBinaryOpInfer<int64_t, int32_t, int64_t>(kFnOpDiv);
    CheckBinaryOpInfer<int64_t, int64_t, int32_t>(kFnOpDiv);
}
TEST_F(ExprNodeTest, DIVTypeInferTest_15) {
    CheckBinaryOpInferError<int32_t, float>(kFnOpDiv);
    CheckBinaryOpInferError<float, int32_t>(kFnOpDiv);
}
TEST_F(ExprNodeTest, DIVTypeInferTest_16) {
    CheckBinaryOpInferError<int32_t, double>(kFnOpDiv);
    CheckBinaryOpInferError<double, int32_t>(kFnOpDiv);
}
TEST_F(ExprNodeTest, DIVTypeInferTest_17) {
    CheckBinaryOpInfer<int64_t, int64_t, int64_t>(kFnOpDiv);
}
TEST_F(ExprNodeTest, DIVTypeInferTest_18) {
    CheckBinaryOpInferError<int64_t, float>(kFnOpDiv);
    CheckBinaryOpInferError<float, int64_t>(kFnOpDiv);
}
TEST_F(ExprNodeTest, DIVTypeInferTest_19) {
    CheckBinaryOpInferError<int64_t, double>(kFnOpDiv);
    CheckBinaryOpInferError<double, int64_t>(kFnOpDiv);
}
TEST_F(ExprNodeTest, DIVTypeInferTest_20) {
    CheckBinaryOpInferError<float, float>(kFnOpDiv);
}
TEST_F(ExprNodeTest, DIVTypeInferTest_21) {
    CheckBinaryOpInferError<float, double>(kFnOpDiv);
    CheckBinaryOpInferError<double, float>(kFnOpDiv);
}
TEST_F(ExprNodeTest, DIVTypeInferTest_22) {
    CheckBinaryOpInferError<double, double>(kFnOpDiv);
}
TEST_F(ExprNodeTest, DIVTypeInferTest_23) {
    CheckBinaryOpInferError<Timestamp, bool>(kFnOpDiv);
    CheckBinaryOpInferError<bool, Timestamp>(kFnOpDiv);
}
TEST_F(ExprNodeTest, DIVTypeInferTest_24) {
    CheckBinaryOpInferError<Timestamp, int16_t>(kFnOpDiv);
    CheckBinaryOpInferError<int16_t, Timestamp>(kFnOpDiv);
}
TEST_F(ExprNodeTest, DIVTypeInferTest_25) {
    CheckBinaryOpInferError<Timestamp, int32_t>(kFnOpDiv);
    CheckBinaryOpInferError<int32_t, Timestamp>(kFnOpDiv);
}
TEST_F(ExprNodeTest, DIVTypeInferTest_26) {
    CheckBinaryOpInferError<Timestamp, int64_t>(kFnOpDiv);
    CheckBinaryOpInferError<int64_t, Timestamp>(kFnOpDiv);
}

TEST_F(ExprNodeTest, ModTypeInferTest_1) {
    CheckBinaryOpInfer<bool, bool, bool>(kFnOpMod);
}
TEST_F(ExprNodeTest, ModTypeInferTest_2) {
    CheckBinaryOpInfer<int16_t, int16_t, bool>(kFnOpMod);
    CheckBinaryOpInfer<int16_t, bool, int16_t>(kFnOpMod);
}
TEST_F(ExprNodeTest, ModTypeInferTest_3) {
    CheckBinaryOpInfer<int32_t, int32_t, bool>(kFnOpMod);
    CheckBinaryOpInfer<int32_t, bool, int32_t>(kFnOpMod);
}
TEST_F(ExprNodeTest, ModTypeInferTest_4) {
    CheckBinaryOpInfer<int64_t, int64_t, bool>(kFnOpMod);
    CheckBinaryOpInfer<int64_t, bool, int64_t>(kFnOpMod);
}
TEST_F(ExprNodeTest, ModTypeInferTest_5) {
    CheckBinaryOpInfer<float, float, bool>(kFnOpMod);
    CheckBinaryOpInfer<float, bool, float>(kFnOpMod);
}
TEST_F(ExprNodeTest, ModTypeInferTest_6) {
    CheckBinaryOpInfer<double, double, bool>(kFnOpMod);
    CheckBinaryOpInfer<double, bool, double>(kFnOpMod);
}
TEST_F(ExprNodeTest, ModTypeInferTest_7) {
    CheckBinaryOpInfer<int16_t, bool, int16_t>(kFnOpMod);
}
TEST_F(ExprNodeTest, ModTypeInferTest_8) {
    CheckBinaryOpInfer<int16_t, int16_t, int16_t>(kFnOpMod);
}
TEST_F(ExprNodeTest, ModTypeInferTest_9) {
    CheckBinaryOpInfer<int32_t, int16_t, int32_t>(kFnOpMod);
    CheckBinaryOpInfer<int32_t, int32_t, int16_t>(kFnOpMod);
}
TEST_F(ExprNodeTest, ModTypeInferTest_10) {
    CheckBinaryOpInfer<int64_t, int16_t, int64_t>(kFnOpMod);
    CheckBinaryOpInfer<int64_t, int64_t, int16_t>(kFnOpMod);
}
TEST_F(ExprNodeTest, ModTypeInferTest_11) {
    CheckBinaryOpInfer<float, int16_t, float>(kFnOpMod);
    CheckBinaryOpInfer<float, float, int16_t>(kFnOpMod);
}
TEST_F(ExprNodeTest, ModTypeInferTest_12) {
    CheckBinaryOpInfer<double, int16_t, double>(kFnOpMod);
    CheckBinaryOpInfer<double, double, int16_t>(kFnOpMod);
}
TEST_F(ExprNodeTest, ModTypeInferTest_13) {
    CheckBinaryOpInfer<int32_t, int32_t, int32_t>(kFnOpMod);
}
TEST_F(ExprNodeTest, ModTypeInferTest_14) {
    CheckBinaryOpInfer<int64_t, int32_t, int64_t>(kFnOpMod);
    CheckBinaryOpInfer<int64_t, int64_t, int32_t>(kFnOpMod);
}
TEST_F(ExprNodeTest, ModTypeInferTest_15) {
    CheckBinaryOpInfer<float, int32_t, float>(kFnOpMod);
    CheckBinaryOpInfer<float, float, int32_t>(kFnOpMod);
}
TEST_F(ExprNodeTest, ModTypeInferTest_16) {
    CheckBinaryOpInfer<double, int32_t, double>(kFnOpMod);
    CheckBinaryOpInfer<double, double, int32_t>(kFnOpMod);
}
TEST_F(ExprNodeTest, ModTypeInferTest_17) {
    CheckBinaryOpInfer<int64_t, int64_t, int64_t>(kFnOpMod);
}
TEST_F(ExprNodeTest, ModTypeInferTest_18) {
    CheckBinaryOpInfer<float, int64_t, float>(kFnOpMod);
    CheckBinaryOpInfer<float, float, int64_t>(kFnOpMod);
}
TEST_F(ExprNodeTest, ModTypeInferTest_19) {
    CheckBinaryOpInfer<double, int64_t, double>(kFnOpMod);
    CheckBinaryOpInfer<double, double, int64_t>(kFnOpMod);
}
TEST_F(ExprNodeTest, ModTypeInferTest_20) {
    CheckBinaryOpInfer<float, float, float>(kFnOpMod);
}
TEST_F(ExprNodeTest, ModTypeInferTest_21) {
    CheckBinaryOpInfer<double, float, double>(kFnOpMod);
    CheckBinaryOpInfer<double, double, float>(kFnOpMod);
}
TEST_F(ExprNodeTest, ModTypeInferTest_22) {
    CheckBinaryOpInfer<double, double, double>(kFnOpMod);
}
TEST_F(ExprNodeTest, ModTypeInferTest_23) {
    CheckBinaryOpInferError<Timestamp, bool>(kFnOpMod);
    CheckBinaryOpInferError<bool, Timestamp>(kFnOpMod);
}
TEST_F(ExprNodeTest, ModTypeInferTest_24) {
    CheckBinaryOpInferError<Timestamp, int16_t>(kFnOpMod);
    CheckBinaryOpInferError<int16_t, Timestamp>(kFnOpMod);
}
TEST_F(ExprNodeTest, ModTypeInferTest_25) {
    CheckBinaryOpInferError<Timestamp, int32_t>(kFnOpMod);
    CheckBinaryOpInferError<int32_t, Timestamp>(kFnOpMod);
}
TEST_F(ExprNodeTest, ModTypeInferTest_26) {
    CheckBinaryOpInferError<Timestamp, int64_t>(kFnOpMod);
    CheckBinaryOpInferError<int64_t, Timestamp>(kFnOpMod);
}

TEST_F(ExprNodeTest, InferBinaryCompareTypeTest) {
    TestInferBinaryCompare(kFnOpEq);
    TestInferBinaryCompare(kFnOpNeq);
    TestInferBinaryCompare(kFnOpLt);
    TestInferBinaryCompare(kFnOpLe);
    TestInferBinaryCompare(kFnOpGt);
    TestInferBinaryCompare(kFnOpGe);
}

TEST_F(ExprNodeTest, InferBinaryCompareTypeTest1) {
    auto do_build = [](NodeManager* nm, ExprNode* left, ExprNode* right) {
        return nm->MakeBinaryExprNode(left, right, kFnOpEq);
    };
    CheckInfer<bool, bool, bool>(do_build);
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
}  // namespace hybridse

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
