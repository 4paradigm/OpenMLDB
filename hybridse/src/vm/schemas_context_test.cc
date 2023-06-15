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

#include "vm/schemas_context.h"
#include <memory>
#include "case/sql_case.h"
#include "glog/logging.h"
#include "gtest/gtest.h"
#include "passes/physical/physical_pass.h"
#include "vm/engine.h"
#include "vm/simple_catalog.h"
#include "vm/sql_compiler.h"
#include "testing/test_base.h"
#include "yaml-cpp/yaml.h"

namespace hybridse {
namespace vm {
class SchemasContextTest : public ::testing::Test {
 public:
    SchemasContextTest()
        : catalog(new vm::SimpleCatalog()),
          plan_ctx(&nm, &lib, "db", catalog, nullptr, false) {}
    node::NodeManager nm;
    udf::UdfLibrary lib;
    std::shared_ptr<vm::SimpleCatalog> catalog;
    vm::PhysicalPlanContext plan_ctx;
};

struct ColumnNameResolveResult {
    int schema_idx = -1;
    int col_idx = -1;
    int path_idx = -9999;
    int source_col_idx = -1;
    std::string source_name = "";
    bool is_error = false;
};

class SchemasContextResolveTest : public ::testing::TestWithParam<SqlCase> {
 public:
    SchemasContextResolveTest() {}
};

void CheckColumnResolveCase(const std::string& db_name,
                            const std::string& relation_name,
                            const std::string& column_name,
                            PhysicalOpNode* node,
                            const ColumnNameResolveResult& expect) {
    Status status;
    const auto schemas_context = node->schemas_ctx();

    // check simple resolve
    size_t schema_idx;
    size_t col_idx;
    status = schemas_context->ResolveColumnIndexByName(db_name,
        relation_name, column_name, &schema_idx, &col_idx);
    if (expect.is_error) {
        LOG(INFO) << status;
        ASSERT_FALSE(status.isOK());
        return;
    }
    ASSERT_TRUE(status.isOK()) << status;

    if (expect.schema_idx >= 0) {
        ASSERT_EQ(static_cast<size_t>(expect.schema_idx), schema_idx);
    }
    if (expect.col_idx >= 0) {
        ASSERT_EQ(static_cast<size_t>(expect.col_idx), col_idx);
    }

    // check detailed resolve
    size_t column_id;
    int child_path_idx;
    size_t child_column_id;
    size_t source_column_id;
    const PhysicalOpNode* source_node = nullptr;
    status = schemas_context->ResolveColumnID(db_name,
        relation_name, column_name, &column_id, &child_path_idx,
        &child_column_id, &source_column_id, &source_node);
    ASSERT_TRUE(status.isOK()) << status;
    ASSERT_EQ(
        schemas_context->GetSchemaSource(schema_idx)->GetColumnID(col_idx),
        column_id);

    if (expect.path_idx >= -1) {
        ASSERT_EQ(expect.path_idx, child_path_idx);
    }
    if (expect.source_col_idx >= 0) {
        ASSERT_TRUE(source_node != nullptr);
        size_t source_schema_idx;
        size_t source_col_idx;
        status = source_node->schemas_ctx()->ResolveColumnIndexByID(
            source_column_id, &source_schema_idx, &source_col_idx);
        ASSERT_EQ(static_cast<size_t>(expect.source_col_idx), source_col_idx);
    }
    if (!expect.source_name.empty()) {
        ASSERT_TRUE(source_node != nullptr);
        ASSERT_EQ(expect.source_name, source_node->schemas_ctx()->GetName());
    }
}

void CheckColumnResolveCase(const YAML::Node& resolve_case,
                            PhysicalOpNode* node) {
    ColumnNameResolveResult expect;
    std::string db_name = "";
    if (resolve_case["db"]) {
        db_name = resolve_case["db"].as<std::string>();
    }
    std::string relation_name = "";
    if (resolve_case["relation_name"]) {
        relation_name = resolve_case["relation_name"].as<std::string>();
    }
    std::string column_name = resolve_case["column_name"].as<std::string>();
    if (resolve_case["is_error"]) {
        if (boost::lexical_cast<int32_t>(
                resolve_case["is_error"].as<std::string>() == "true")) {
            expect.is_error = true;
        }
    }
    if (resolve_case["schema_idx"]) {
        expect.schema_idx = boost::lexical_cast<int32_t>(
            resolve_case["schema_idx"].as<std::string>());
    }
    if (resolve_case["col_idx"]) {
        expect.col_idx = boost::lexical_cast<int32_t>(
            resolve_case["col_idx"].as<std::string>());
    }
    if (resolve_case["path_idx"]) {
        expect.path_idx = boost::lexical_cast<int32_t>(
            resolve_case["path_idx"].as<std::string>());
    }
    if (resolve_case["source_col_idx"]) {
        expect.source_col_idx = boost::lexical_cast<int32_t>(
            resolve_case["source_col_idx"].as<std::string>());
    }
    if (resolve_case["source_name"]) {
        expect.source_name = resolve_case["source_name"].as<std::string>();
    }
    CheckColumnResolveCase(db_name, relation_name, column_name, node, expect);
}

void CheckColumnResolveCases(const SqlCase& sql_case, PhysicalOpNode* node) {
    LOG(INFO) << "Physical plan:\n" << node->SchemaToString("");

    auto& raw_node = sql_case.raw_node();
    const auto& expect = raw_node["expect"];
    if (!expect) {
        return;
    }
    const auto& resolve_cases = expect["resolve_column"];
    if (!resolve_cases) {
        return;
    }
    for (size_t i = 0; i < resolve_cases.size(); i++) {
        CheckColumnResolveCase(resolve_cases[i], node);
    }
}

PhysicalOpNode* GetTestSqlPlan(SqlCase& sql_case,  // NOLINT
                               RunSession* session) {
    std::map<size_t, std::string> idx_to_table_dict;
    auto catalog = std::make_shared<SimpleCatalog>();
    EngineOptions options;
    options.SetPlanOnly(true);
    auto engine = std::make_shared<vm::Engine>(catalog, options);
    InitSimpleCataLogFromSqlCase(sql_case, catalog);

    // Compile sql plan
    base::Status status;
    bool ok = engine->Get(sql_case.sql_str(), sql_case.db(), *session, status);
    if (!ok) {
        LOG(WARNING) << status;
        return nullptr;
    }
    return std::dynamic_pointer_cast<SqlCompileInfo>(session->GetCompileInfo())
        ->get_sql_context()
        .physical_plan;
}

INSTANTIATE_TEST_SUITE_P(
    ResolveNameTest, SchemasContextResolveTest,
    testing::ValuesIn(
        sqlcase::InitCases("cases/plan/schemas_context/resolve_column_name.yaml")));

TEST_P(SchemasContextResolveTest, test_request_column_resolve) {
    SqlCase sql_case = GetParam();
    LOG(INFO) << "Test resolve request mode sql root of: "
              << sql_case.sql_str();

    RequestRunSession session;
    auto plan = GetTestSqlPlan(sql_case, &session);
    CheckColumnResolveCases(sql_case, plan);
}

TEST_P(SchemasContextResolveTest, test_batch_column_resolve) {
    SqlCase sql_case = GetParam();
    LOG(INFO) << "Test resolve request mode sql root of: "
              << sql_case.sql_str();

    BatchRunSession session;
    auto plan = GetTestSqlPlan(sql_case, &session);
    CheckColumnResolveCases(sql_case, plan);
}

TEST_F(SchemasContextTest, NewSchemasContextTest) {
    type::TableDef t1;
    BuildTableDef(t1);
    t1.set_name("t1");
    t1.set_catalog("db1");

    type::TableDef t2;
    BuildTableDef(t2);
    t2.set_name("t2");
    t2.set_catalog("db2");

    auto init_source = [](SchemaSource* source, const type::TableDef& table,
                          size_t offset) {
        source->SetSourceDBAndTableName(table.catalog(), table.name());
        source->SetSchema(&table.columns());
        for (int i = 0; i < table.columns_size(); ++i) {
            source->SetColumnID(i, offset);
            offset += 1;
        }
    };

    vm::SchemasContext schemas_context;
    schemas_context.SetDefaultDBName("db1");
    auto source1 = schemas_context.AddSource();
    init_source(source1, t1, 0);
    auto source2 = schemas_context.AddSource();
    init_source(source2, t2, t1.columns_size());
    schemas_context.Build();

    size_t column_id;
    Status status;
    status = schemas_context.ResolveColumnID("db1", "t1", "col1", &column_id);
    ASSERT_TRUE(status.isOK()) << status;
    ASSERT_EQ(1u, column_id);

    status = schemas_context.ResolveColumnID("db2", "t2", "col1", &column_id);
    ASSERT_TRUE(status.isOK()) << status;
    ASSERT_EQ(8u, column_id);
}


TEST_F(SchemasContextTest, DifferentDBNameAndSameTableName) {
    type::TableDef t1;
    BuildTableDef(t1);
    t1.set_name("tx");
    t1.set_catalog("db1");

    type::TableDef t2;
    BuildTableDef(t2);
    t2.set_name("tx");
    t2.set_catalog("db2");

    auto init_source = [](SchemaSource* source, const type::TableDef& table,
                          size_t offset) {
      source->SetSourceDBAndTableName(table.catalog(), table.name());
      source->SetSchema(&table.columns());
      for (int i = 0; i < table.columns_size(); ++i) {
          source->SetColumnID(i, offset);
          offset += 1;
      }
    };

    vm::SchemasContext schemas_context;
    schemas_context.SetDefaultDBName("db1");
    auto source1 = schemas_context.AddSource();
    init_source(source1, t1, 0);
    auto source2 = schemas_context.AddSource();
    init_source(source2, t2, t1.columns_size());
    schemas_context.Build();

    size_t column_id;
    Status status;
    status = schemas_context.ResolveColumnID("db1", "tx", "col1", &column_id);
    ASSERT_TRUE(status.isOK()) << status;
    ASSERT_EQ(1u, column_id);

    status = schemas_context.ResolveColumnID("db2", "tx", "col1", &column_id);
    ASSERT_TRUE(status.isOK()) << status;
    ASSERT_EQ(8u, column_id);
}
}  // namespace vm
}  // namespace hybridse

int main(int argc, char** argv) {
    ::testing::GTEST_FLAG(color) = "yes";
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
