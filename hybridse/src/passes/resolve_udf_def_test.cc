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

#include "passes/resolve_udf_def.h"
#include "gtest/gtest.h"
#include "plan/plan_api.h"

namespace hybridse {
namespace passes {

class ResolveUdfDefTest : public ::testing::Test {};

TEST_F(ResolveUdfDefTest, TestResolve) {
    Status status;
    node::NodeManager nm;
    //    const std::string udf1 =
    //        "%%fun\n"
    //        "def test(x:i32, y:i32):i32\n"
    //        "    return x+y\n"
    //        "end\n";
    node::FnNodeList *params = nm.MakeFnListNode();
    params->AddChild(nm.MakeFnParaNode("x", nm.MakeTypeNode(node::kInt32)));
    params->AddChild(nm.MakeFnParaNode("y", nm.MakeTypeNode(node::kInt32)));

    node::FnNodeFnDef *fn_def = dynamic_cast<node::FnNodeFnDef *>(
        nm.MakeFnDefNode(nm.MakeFnHeaderNode("test", params, nm.MakeTypeNode(node::kInt32)),
                         nm.MakeFnListNode(nm.MakeReturnStmtNode(nm.MakeBinaryExprNode(
                             nm.MakeUnresolvedExprId("x"), nm.MakeUnresolvedExprId("y"), node::kFnOpAdd)))));
    auto def_plan = nm.MakeFuncPlanNode(fn_def);

    ResolveUdfDef resolver;
    status = resolver.Visit(def_plan->fn_def_);
    ASSERT_TRUE(status.isOK());
}

TEST_F(ResolveUdfDefTest, TestResolveFailed) {
    Status status;
    node::NodeManager nm;
    //    const std::string udf1 =
    //        "%%fun\n"
    //        "def test(x:i32, y:i32):i32\n"
    //        "    return x+z\n"
    //        "end\n";

    node::FnNodeList *params = nm.MakeFnListNode();
    params->AddChild(nm.MakeFnParaNode("x", nm.MakeTypeNode(node::kInt32)));
    params->AddChild(nm.MakeFnParaNode("y", nm.MakeTypeNode(node::kInt32)));

    node::FnNodeFnDef *fn_def = dynamic_cast<node::FnNodeFnDef *>(
        nm.MakeFnDefNode(nm.MakeFnHeaderNode("test", params, nm.MakeTypeNode(node::kInt32)),
                         nm.MakeFnListNode(nm.MakeReturnStmtNode(nm.MakeBinaryExprNode(
                             nm.MakeUnresolvedExprId("x"), nm.MakeUnresolvedExprId("z"), node::kFnOpAdd)))));

    auto def_plan = nm.MakeFuncPlanNode(fn_def);
    ASSERT_TRUE(def_plan != nullptr);

    ResolveUdfDef resolver;
    status = resolver.Visit(def_plan->fn_def_);
    ASSERT_TRUE(!status.isOK());
}

}  // namespace passes
}  // namespace hybridse

int main(int argc, char **argv) {
    ::testing::GTEST_FLAG(color) = "yes";
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
