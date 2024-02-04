/**
 * Copyright (c) 2024 OpenMLDB authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "codegen/insert_row_builder.h"

#include <string>

#include "gtest/gtest.h"
#include "node/sql_node.h"
#include "plan/plan_api.h"
#include "vm/sql_ctx.h"
#include "vm/engine.h"

namespace hybridse {
namespace codegen {

class InsertRowBuilderTest : public ::testing::Test {};

TEST_F(InsertRowBuilderTest, encode) {
    std::string sql = "insert into t1 values (1, map (1, '12'))";
    vm::SqlContext ctx;
    ctx.sql = sql;
    auto s = plan::PlanAPI::CreatePlanTreeFromScript(&ctx);
    ASSERT_TRUE(s.isOK()) << s;

    auto* exprlist = dynamic_cast<node::InsertPlanNode*>(ctx.logical_plan.front())->GetInsertNode()->values_[0];

    codec::Schema sc;
    {
        auto col1 = sc.Add();
        col1->mutable_schema()->set_base_type(type::kInt32);
        col1->set_type(type::kInt32);
    }

    {
        auto col = sc.Add();
        auto map_ty = col->mutable_schema()->mutable_map_type();
        map_ty->mutable_key_type()->set_base_type(type::kInt32);
        map_ty->mutable_value_type()->set_base_type(type::kVarchar);
    }

    auto jit = std::shared_ptr<vm::HybridSeJitWrapper>(vm::HybridSeJitWrapper::Create());
    ASSERT_TRUE(jit->Init());
    ASSERT_TRUE(vm::HybridSeJitWrapper::InitJitSymbols(jit.get()));

    InsertRowBuilder builder(jit, &sc);

    auto as = builder.ComputeRow(dynamic_cast<node::ExprListNode*>(exprlist));
    ASSERT_TRUE(as.ok()) << as.status();

    ASSERT_TRUE(as.value() != nullptr);
}
}  // namespace codegen
}  // namespace hybridse
//
int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    ::hybridse::vm::Engine::InitializeGlobalLLVM();
    return RUN_ALL_TESTS();
}
