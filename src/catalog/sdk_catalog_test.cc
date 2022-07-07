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

#include "catalog/sdk_catalog.h"

#include "base/fe_status.h"
#include "codec/fe_row_codec.h"
#include "gtest/gtest.h"
#include "proto/fe_common.pb.h"
#include "vm/engine.h"

namespace openmldb {
namespace catalog {

typedef ::google::protobuf::RepeatedPtrField<::openmldb::common::ColumnDesc> PBSchema;
typedef ::google::protobuf::RepeatedPtrField<::openmldb::common::ColumnKey> PBIndex;

class SDKCatalogTest : public ::testing::Test {};

struct TestArgs {
    ::openmldb::nameserver::TableInfo meta;
};

TestArgs* PrepareTable(const std::string& tname, const std::string& db) {
    TestArgs* args = new TestArgs();
    args->meta.set_name(tname);
    args->meta.set_format_version(1);
    args->meta.set_db(db);
    PBSchema* schema = args->meta.mutable_column_desc();
    auto col1 = schema->Add();
    col1->set_name("col1");
    col1->set_data_type(::openmldb::type::kVarchar);
    auto col2 = schema->Add();
    col2->set_name("col2");
    col2->set_data_type(::openmldb::type::kBigInt);
    PBIndex* index = args->meta.mutable_column_key();
    auto key1 = index->Add();
    key1->set_index_name("index0");
    key1->add_col_name("col1");
    key1->set_ts_name("col2");
    return args;
}

TEST_F(SDKCatalogTest, SdkSmokeTest) {
    TestArgs* args = PrepareTable("t1", "db1");
    std::vector<::openmldb::nameserver::TableInfo> tables;
    tables.push_back(args->meta);
    auto client_manager = std::make_shared<ClientManager>();
    std::shared_ptr<SDKCatalog> catalog(new SDKCatalog(client_manager));
    Procedures procedures;
    ASSERT_TRUE(catalog->Init(tables, procedures));
    ::hybridse::vm::EngineOptions options;
    options.SetCompileOnly(true);
    ::hybridse::vm::Engine engine(catalog, options);
    std::string sql = "select col1, col2 + 1 from t1;";
    ::hybridse::vm::BatchRunSession session;
    ::hybridse::base::Status status;
    ASSERT_TRUE(engine.Get(sql, "db1", session, status));
    /* std::stringstream ss;
    session.GetCompileInfo()->DumpPhysicalPlan(ss, "\t");
    std::cout << ss.str() << std::endl;*/
}

TEST_F(SDKCatalogTest, SdkWindowSmokeTest) {
    TestArgs* args = PrepareTable("t1", "db1");
    std::vector<::openmldb::nameserver::TableInfo> tables;
    tables.push_back(args->meta);
    auto client_manager = std::make_shared<ClientManager>();
    std::shared_ptr<SDKCatalog> catalog(new SDKCatalog(client_manager));
    Procedures procedures;
    ASSERT_TRUE(catalog->Init(tables, procedures));
    ::hybridse::vm::EngineOptions options;
    options.SetCompileOnly(true);
    ::hybridse::vm::Engine engine(catalog, options);
    std::string sql =
        "select sum(col2) over w1, t1.col1, t1.col2 from t1 window w1 "
        "as(partition by t1.col1 order by t1.col2 ROWS BETWEEN 3 PRECEDING AND "
        "CURRENT ROW);";
    ::hybridse::vm::BatchRunSession session;
    ::hybridse::base::Status status;
    ASSERT_TRUE(engine.Get(sql, "db1", session, status));
    /*std::stringstream ss;
    session.GetCompileInfo()->DumpPhysicalPlan(ss, "\t");
    std::cout << ss.str() << std::endl;*/
}

TEST_F(SDKCatalogTest, SdkLastjoinSmokeTest) {
    TestArgs* args = PrepareTable("t1", "db1");
    TestArgs* args2 = PrepareTable("t2", "db1");
    std::vector<::openmldb::nameserver::TableInfo> tables;
    tables.push_back(args->meta);
    tables.push_back(args2->meta);
    auto client_manager = std::make_shared<ClientManager>();
    std::shared_ptr<SDKCatalog> catalog(new SDKCatalog(client_manager));
    Procedures procedures;
    ASSERT_TRUE(catalog->Init(tables, procedures));
    ::hybridse::vm::EngineOptions options;
    options.SetCompileOnly(true);
    ::hybridse::vm::Engine engine(catalog, options);
    std::string sql =
        "select t1.col1 as c1, t1.col2 as c2 , t2.col1 as c3, t2.col2 as c4 "
        "from t1 last join t2 order by t2.col2 "
        "on t1.col1 = t2.col1 and t1.col2 > t2.col2;";
    ::hybridse::vm::BatchRunSession session;
    ::hybridse::base::Status status;
    ASSERT_TRUE(engine.Get(sql, "db1", session, status));
    /*std::stringstream ss;
    session.GetCompileInfo()->DumpPhysicalPlan(ss, "\t");
    std::cout << ss.str() << std::endl;*/
}

}  // namespace catalog
}  // namespace openmldb

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    ::hybridse::vm::Engine::InitializeGlobalLLVM();
    return RUN_ALL_TESTS();
}
