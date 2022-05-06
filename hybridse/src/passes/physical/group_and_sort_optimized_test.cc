/*
 * Copyright 2022 4Paradigm
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

#include "passes/physical/group_and_sort_optimized.h"

#include <memory>
#include <vector>

#include "gtest/gtest.h"
#include "llvm/IR/LLVMContext.h"
#include "plan/plan_api.h"
#include "testing/test_base.h"
#include "udf/default_udf_library.h"
#include "vm/transform.h"

namespace hybridse {
namespace passes {

struct GASTestCase {
    absl::string_view sql;
    absl::string_view physical_tree_str;
};

std::ostream& operator<<(std::ostream& os, const GASTestCase& obj) {
    return os << "{sql: '" << obj.sql << "', tree: '" << obj.physical_tree_str << "'}";
}

class GroupAndSortOptOnCombinedIndexTest : public ::testing::TestWithParam<GASTestCase> {
 protected:
    void SetUp() override {
        hybridse::type::Database db;
        db.set_name("db");

        hybridse::type::TableDef table_def;
        table_def.set_name("t1");
        table_def.set_catalog("db");
        {
            auto* c1 = table_def.add_columns();
            c1->set_type(::hybridse::type::kVarchar);
            c1->set_name("a");

            auto* c2 = table_def.add_columns();
            c2->set_type(::hybridse::type::kInt32);
            c2->set_name("b");

            auto* c3 = table_def.add_columns();
            c3->set_type(::hybridse::type::kVarchar);
            c3->set_name("c");

            auto* index = table_def.add_indexes();
            index->set_name("idx");
            index->add_first_keys("a");
            index->add_first_keys("b");
            index->add_first_keys("c");
        }
        vm::AddTable(db, table_def);

        catalog_ = vm::BuildSimpleCatalog(db);
    }

 protected:
    node::NodeManager manager_;
    std::shared_ptr<vm::SimpleCatalog> catalog_;
};

absl::string_view optimized_plan = R"sql(FILTER_BY(condition=, left_keys=(), right_keys=(), index_keys=(aaa,12,ccc))
  DATA_PROVIDER(type=Partition, table=t1, index=idx))sql";
absl::string_view unoptimized_plan =
    R"sql(FILTER_BY(condition=12 = b AND ccc = c, left_keys=(), right_keys=(), index_keys=)
  DATA_PROVIDER(table=t1))sql";
static const std::vector<GASTestCase> cases = {
    {"select * from t1 where a = 'aaa' and b = 12 and c = 'ccc';", optimized_plan},
    {"select * from t1 where c = 'ccc' and b = 12 and a = 'aaa';", optimized_plan},
    {"select * from t1 where b = 12 and c = 'ccc' and a = 'aaa';", optimized_plan},
    {"select * from t1 where b = 12 and c = 'ccc';", unoptimized_plan},
};

INSTANTIATE_TEST_SUITE_P(CombinedIndex, GroupAndSortOptOnCombinedIndexTest, testing::ValuesIn(cases));

TEST_P(GroupAndSortOptOnCombinedIndexTest, OptimizeOutOfOrderFilter) {
    auto& cs = GetParam();
    ::hybridse::node::PlanNodeList plan_trees;
    ::hybridse::base::Status base_status;
    ASSERT_TRUE(plan::PlanAPI::CreatePlanTreeFromScript(std::string(cs.sql), plan_trees, &manager_, base_status))
        << base_status;

    auto ctx = llvm::make_unique<llvm::LLVMContext>();
    auto m = llvm::make_unique<llvm::Module>("test_op_generator", *ctx);
    auto lib = ::hybridse::udf::DefaultUdfLibrary::get();
    const codec::Schema empty_schema;

    vm::BatchModeTransformer tf(&manager_, "db", catalog_, &empty_schema, m.get(), lib);
    tf.AddDefaultPasses();

    PhysicalOpNode* physical_plan = nullptr;
    base::Status status = tf.TransformPhysicalPlan(plan_trees, &physical_plan);
    EXPECT_EQ(cs.physical_tree_str, physical_plan->GetTreeString());
}

}  // namespace passes
}  // namespace hybridse

int main(int argc, char** argv) {
    ::testing::GTEST_FLAG(color) = "yes";
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
