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

#include "apiserver/api_server_impl.h"
#include "brpc/channel.h"
#include "memory"
#include "brpc/restful.h"
#include "brpc/server.h"
#include "butil/logging.h"
#include "gflags/gflags.h"
#include "gtest/gtest.h"
#include "json2pb/rapidjson.h"
#include "sdk/mini_cluster.h"


namespace openmldb::apiserver {

class APIServerTestEnv : public testing::Environment {
 public:
    static APIServerTestEnv* Instance() {
        static auto* instance = new APIServerTestEnv;
        return instance;
    }
    void SetUp() override {
        std::cout << "Environment SetUp!" << std::endl;
        ::hybridse::vm::Engine::InitializeGlobalLLVM();
        FLAGS_zk_session_timeout = 100000;

        mc = std::make_shared<sdk::MiniCluster>(6181);
        ASSERT_TRUE(mc->SetUp()) << "Fail to set up mini cluster";

        sdk::ClusterOptions cluster_options;
        cluster_options.zk_cluster = mc->GetZkCluster();
        cluster_options.zk_path = mc->GetZkPath();
        // Owned by queue_svc
        cluster_sdk = new ::openmldb::sdk::ClusterSDK(cluster_options);
        ASSERT_TRUE(cluster_sdk->Init()) << "Fail to connect to db";
        queue_svc = std::make_shared<APIServerImpl>();
        ASSERT_TRUE(queue_svc->Init(cluster_sdk));

        sdk::SQLRouterOptions sql_opt;
        sql_opt.session_timeout = 30000;
        sql_opt.zk_cluster = mc->GetZkCluster();
        sql_opt.zk_path = mc->GetZkPath();
        // sql_opt.enable_debug = true;
        cluster_remote = sdk::NewClusterSQLRouter(sql_opt);

        // Http server set up
        ASSERT_TRUE(server.AddService(queue_svc.get(), brpc::SERVER_DOESNT_OWN_SERVICE, "/* => Process") == 0)
            << "Fail to add queue_svc";

        // Start the server.
        int api_server_port = 8010;
        brpc::ServerOptions server_options;
        // options.idle_timeout_sec = FLAGS_idle_timeout_s;
        ASSERT_TRUE(server.Start(api_server_port, &server_options) == 0) << "Fail to start HttpServer";

        hybridse::sdk::Status status;
        ASSERT_TRUE(cluster_remote != nullptr);
        cluster_remote->ExecuteSQL("SET @@execute_mode='online';", &status);

        db = "api_server_test";
        cluster_remote->DropDB(db, &status);
        cluster_remote->CreateDB(db, &status);
        std::vector<std::string> dbs;
        ASSERT_TRUE(cluster_remote->ShowDB(&dbs, &status));
        ASSERT_TRUE(std::find(dbs.begin(), dbs.end(), db) != dbs.end());

        brpc::ChannelOptions options;
        options.protocol = "http";
        options.timeout_ms = 2000 /*milliseconds*/;
        options.max_retry = 3;
        ASSERT_TRUE(http_channel.Init("http://127.0.0.1:", api_server_port, &options) == 0)
            << "Fail to initialize http channel";
    }

    void TearDown() override {
        std::cout << "Environment TearDown!" << std::endl;
        hybridse::sdk::Status status;
        cluster_remote->DropDB(db, &status);
        server.Stop(0);
        server.Join();
        mc->Close();
    }

    std::string db;
    ::openmldb::sdk::DBSDK* cluster_sdk = nullptr;
    std::shared_ptr<sdk::MiniCluster> mc;
    std::shared_ptr<APIServerImpl> queue_svc;
    brpc::Server server;
    brpc::Channel http_channel;
    std::shared_ptr<sdk::SQLRouter> cluster_remote;

 private:
    APIServerTestEnv() = default;
    GTEST_DISALLOW_COPY_AND_ASSIGN_(APIServerTestEnv);
};

class APIServerTest : public ::testing::Test {
 public:
    APIServerTest() = default;
    ~APIServerTest() override {}
};

TEST_F(APIServerTest, jsonFormat) {
    butil::rapidjson::Document document;

    // Check the format of put request
    if (document
            .Parse(R"({
    "value": [
        ["value1", 111, 1.4, "2021-04-27", 1620471840256, true, null]
    ]
    })")
            .HasParseError()) {
        ASSERT_TRUE(false) << "json parse failed with code " << document.GetParseError();
    }

    hybridse::sdk::Status status;
    const auto& value = document["value"];
    ASSERT_TRUE(value.IsArray());
    ASSERT_EQ(1, value.Size());
    const auto& arr = value[0];
    ASSERT_EQ(7, arr.Size());
    ASSERT_EQ(butil::rapidjson::kStringType, arr[0].GetType());
    ASSERT_EQ(butil::rapidjson::kNumberType, arr[1].GetType());
    ASSERT_EQ(butil::rapidjson::kNumberType, arr[2].GetType());
    ASSERT_EQ(butil::rapidjson::kStringType, arr[3].GetType());
    ASSERT_EQ(butil::rapidjson::kNumberType, arr[4].GetType());
    ASSERT_EQ(butil::rapidjson::kTrueType, arr[5].GetType());
    ASSERT_EQ(butil::rapidjson::kNullType, arr[6].GetType());
}

TEST_F(APIServerTest, invalidPut) {
    const auto env = APIServerTestEnv::Instance();
    brpc::Controller cntl;
    cntl.http_request().set_method(brpc::HTTP_METHOD_PUT);
    PutResp resp;

    // Empty body
    // NOTE: host:port is defined in SetUp, so the host:port here won't work. Only the path works.
    cntl.http_request().uri() = "http://whaterver/dbs/a/tables/b";
    env->http_channel.CallMethod(NULL, &cntl, NULL, NULL, NULL);
    ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
    {
        JsonReader reader(cntl.response_attachment().to_string().c_str());
        reader >> resp;
    }
    ASSERT_EQ(-1, resp.code);
    LOG(INFO) << resp.msg;

    // Invalid db
    cntl.Reset();
    cntl.http_request().set_method(brpc::HTTP_METHOD_PUT);
    cntl.http_request().uri() = "http://whaterver/dbs/a/tables/b";
    cntl.request_attachment().append(R"({"value":[[-1]]})");
    env->http_channel.CallMethod(NULL, &cntl, NULL, NULL, NULL);
    ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
    {
        JsonReader reader(cntl.response_attachment().to_string().c_str());
        reader >> resp;
    }
    ASSERT_EQ(-1, resp.code);
    LOG(INFO) << resp.msg;

    // Invalid table
    cntl.Reset();
    cntl.http_request().set_method(brpc::HTTP_METHOD_PUT);
    cntl.http_request().uri() = "http://whaterver/dbs/" + env->db + "/tables/b";
    cntl.request_attachment().append(R"({"value":[[-1]]})");
    env->http_channel.CallMethod(NULL, &cntl, NULL, NULL, NULL);
    ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
    {
        JsonReader reader(cntl.response_attachment().to_string().c_str());
        reader >> resp;
    }
    ASSERT_EQ(-1, resp.code);
    LOG(INFO) << resp.msg;

    // Invalid pattern
    cntl.Reset();
    cntl.http_request().set_method(brpc::HTTP_METHOD_PUT);
    cntl.http_request().uri() = "http://whaterver/db11s/" + env->db + "/tables/b";
    env->http_channel.CallMethod(NULL, &cntl, NULL, NULL, NULL);
    ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
    {
        JsonReader reader(cntl.response_attachment().to_string().c_str());
        reader >> resp;
    }
    ASSERT_EQ(-1, resp.code);
    LOG(INFO) << resp.msg;
}

TEST_F(APIServerTest, validPut) {
    const auto env = APIServerTestEnv::Instance();

    // create table
    std::string table = "put";
    std::string ddl = "create table if not exists " + table +
                      "(field1 string, "
                      "field2 timestamp, "
                      "field3 double, "
                      "field4 date, "
                      "field5 bigint, "
                      "field6 bool,"
                      "field7 string,"
                      "field8 bigint,"
                      "index(key=field1, ts=field2));";
    hybridse::sdk::Status status;
    ASSERT_TRUE(env->cluster_remote->ExecuteDDL(env->db, ddl, &status)) << status.msg;
    ASSERT_TRUE(env->cluster_sdk->Refresh());

    int insert_cnt = 10;
    for (int i = 0; i < insert_cnt; i++) {
        std::string key = "k" + std::to_string(i);

        brpc::Controller cntl;
        cntl.http_request().set_method(brpc::HTTP_METHOD_PUT);
        cntl.http_request().uri() = "http://127.0.0.1:8010/dbs/" + env->db + "/tables/" + table;
        cntl.request_attachment().append("{\"value\": [[\"" + key +
                                         "\", 111, 1.4,  \"2021-04-27\", 1620471840256, true, \"more str\", null]]}");
        env->http_channel.CallMethod(NULL, &cntl, NULL, NULL, NULL);
        ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
        PutResp resp;
        JsonReader reader(cntl.response_attachment().to_string().c_str());
        reader >> resp;
        ASSERT_EQ(0, resp.code) << resp.msg;
        ASSERT_STREQ("ok", resp.msg.c_str());
    }

    // Check data
    std::string select_all = "select * from " + table + ";";
    auto rs = env->cluster_remote->ExecuteSQL(env->db, select_all, &status);
    ASSERT_TRUE(rs) << "fail to execute sql";
    ASSERT_EQ(insert_cnt, rs->Size());

    if (rs->Next()) {
        // just peek one
        LOG(INFO) << rs->GetRowString();
        int64_t res;
        ASSERT_TRUE(rs->GetTime(1, &res));
        ASSERT_EQ(111, res);

        ASSERT_TRUE(rs->GetInt64(4, &res));
        ASSERT_EQ(1620471840256, res);
    }
    ASSERT_TRUE(env->cluster_remote->ExecuteDDL(env->db, "drop table " + table + ";", &status)) << status.msg;
}

TEST_F(APIServerTest, putCase1) {
    const auto env = APIServerTestEnv::Instance();

    // create table
    std::string table = "put";
    std::string ddl = "create table if not exists " + table +
                      "(c1 string, "
                      "c3 int, "
                      "c7 timestamp, "
                      "index(key=(c1), ts=c7));";
    hybridse::sdk::Status status;
    ASSERT_TRUE(env->cluster_remote->ExecuteDDL(env->db, ddl, &status)) << status.msg;
    ASSERT_TRUE(env->cluster_sdk->Refresh());

    {
        brpc::Controller cntl;
        cntl.http_request().set_method(brpc::HTTP_METHOD_PUT);
        cntl.http_request().uri() = "http://127.0.0.1:8010/dbs/" + env->db + "/tables/" + table;
        cntl.request_attachment().append(R"({
        "value": [
            ["", 111, 1620471840256]
        ]
        })");
        env->http_channel.CallMethod(NULL, &cntl, NULL, NULL, NULL);
        ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
        LOG(INFO) << cntl.response_attachment().to_string();
        PutResp resp;
        JsonReader reader(cntl.response_attachment().to_string().c_str());
        reader >> resp;
        ASSERT_EQ(0, resp.code) << resp.msg;
        ASSERT_STREQ("ok", resp.msg.c_str());
    }
    {
        brpc::Controller cntl;
        cntl.http_request().set_method(brpc::HTTP_METHOD_PUT);
        cntl.http_request().uri() = "http://127.0.0.1:8010/dbs/" + env->db + "/tables/" + table;
        cntl.request_attachment().append(R"({
        "value": [
            ["drop table test1;", 111, 1620471840256]
        ]
        })");
        env->http_channel.CallMethod(NULL, &cntl, NULL, NULL, NULL);
        ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
        LOG(INFO) << cntl.response_attachment().to_string();
        PutResp resp;
        JsonReader reader(cntl.response_attachment().to_string().c_str());
        reader >> resp;
        ASSERT_EQ(0, resp.code) << resp.msg;
        ASSERT_STREQ("ok", resp.msg.c_str());
    }
    {
        brpc::Controller cntl;
        cntl.http_request().set_method(brpc::HTTP_METHOD_PUT);
        cntl.http_request().uri() = "http://127.0.0.1:8010/dbs/" + env->db + "/tables/" + table;
        // Invalid timestamp
        cntl.request_attachment().append(R"({
        "value": [
            ["drop table test1;", 111, "2020-05-01"]
        ]
        })");
        env->http_channel.CallMethod(NULL, &cntl, NULL, NULL, NULL);
        ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
        LOG(INFO) << cntl.response_attachment().to_string();
        PutResp resp;
        JsonReader reader(cntl.response_attachment().to_string().c_str());
        reader >> resp;
        ASSERT_EQ(-1, resp.code);
    }

    {
        brpc::Controller cntl;
        cntl.http_request().set_method(brpc::HTTP_METHOD_PUT);
        cntl.http_request().uri() = "http://127.0.0.1:8010/dbs/" + env->db + "/tables/" + table;
        cntl.request_attachment().append(R"({
        "value": [
            ["中文", 111, 1620471840256]
        ]
        })");
        env->http_channel.CallMethod(NULL, &cntl, NULL, NULL, NULL);
        ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
        LOG(INFO) << cntl.response_attachment().to_string();
        PutResp resp;
        JsonReader reader(cntl.response_attachment().to_string().c_str());
        reader >> resp;
        ASSERT_EQ(0, resp.code) << resp.msg;
        ASSERT_STREQ("ok", resp.msg.c_str());
    }

    {
        brpc::Controller cntl;
        cntl.http_request().set_method(brpc::HTTP_METHOD_PUT);
        cntl.http_request().uri() = "http://127.0.0.1:8010/dbs/" + env->db + "/tables/" + table;
        cntl.request_attachment().append(R"({
        "value": [
            [null, 111, 1620471840256]
        ]
        })");
        env->http_channel.CallMethod(NULL, &cntl, NULL, NULL, NULL);
        ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
        LOG(INFO) << cntl.response_attachment().to_string();
        PutResp resp;
        JsonReader reader(cntl.response_attachment().to_string().c_str());
        reader >> resp;
        ASSERT_EQ(0, resp.code) << resp.msg;
        ASSERT_STREQ("ok", resp.msg.c_str());
    }

    ASSERT_TRUE(env->cluster_remote->ExecuteDDL(env->db, "drop table " + table + ";", &status)) << status.msg;
}

TEST_F(APIServerTest, procedure) {
    const auto env = APIServerTestEnv::Instance();

    // create table
    std::string ddl =
        "create table trans(c1 string,\n"
        "                   c3 int,\n"
        "                   c4 bigint,\n"
        "                   c5 float,\n"
        "                   c6 double,\n"
        "                   c7 timestamp,\n"
        "                   c8 date,\n"
        "                   index(key=c1, ts=c7));";
    hybridse::sdk::Status status;
    env->cluster_remote->ExecuteDDL(env->db, "drop table trans;", &status);
    ASSERT_TRUE(env->cluster_sdk->Refresh());
    ASSERT_TRUE(env->cluster_remote->ExecuteDDL(env->db, ddl, &status)) << "fail to create table";

    ASSERT_TRUE(env->cluster_sdk->Refresh());
    // insert
    std::string insert_sql = "insert into trans values(\"bb\",24,34,1.5,2.5,1590738994000,\"2020-05-05\");";
    ASSERT_TRUE(env->cluster_remote->ExecuteInsert(env->db, insert_sql, &status));
    // create procedure
    std::string sp_name = "sp";
    std::string sql =
        "SELECT c1, c3, sum(c4) OVER w1 as w1_c4_sum FROM trans WINDOW w1 AS"
        " (PARTITION BY trans.c1 ORDER BY trans.c7 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW);";
    std::string sp_ddl =
        "create procedure " + sp_name +
        " (const c1 string, const c3 int, c4 bigint, c5 float, c6 double, const c7 timestamp, c8 date" + ")" +
        " begin " + sql + " end;";
    ASSERT_TRUE(env->cluster_remote->ExecuteDDL(env->db, sp_ddl, &status)) << "fail to create procedure";
    ASSERT_TRUE(env->cluster_sdk->Refresh());

    // show procedure
    brpc::Controller show_cntl;  // default is GET
    show_cntl.http_request().uri() = "http://127.0.0.1:8010/dbs/" + env->db + "/procedures/" + sp_name;
    env->http_channel.CallMethod(NULL, &show_cntl, NULL, NULL, NULL);
    ASSERT_FALSE(show_cntl.Failed()) << show_cntl.ErrorText();
    LOG(INFO) << "get sp resp: " << show_cntl.response_attachment();

    butil::rapidjson::Document document;
    if (document.Parse(show_cntl.response_attachment().to_string().c_str()).HasParseError()) {
        ASSERT_TRUE(false) << "response parse failed with code " << document.GetParseError()
                           << ", raw resp: " << show_cntl.response_attachment().to_string();
    }
    ASSERT_EQ(0, document["code"].GetInt());
    ASSERT_STREQ("ok", document["msg"].GetString());
    ASSERT_EQ(7, document["data"]["input_schema"].Size());
    ASSERT_EQ(3, document["data"]["input_common_cols"].Size());
    ASSERT_EQ(3, document["data"]["output_schema"].Size());
    ASSERT_EQ(2, document["data"]["output_common_cols"].Size());

    // call procedure, need schema
    {
        brpc::Controller cntl;
        cntl.http_request().set_method(brpc::HTTP_METHOD_POST);
        cntl.http_request().uri() = "http://127.0.0.1:8010/dbs/" + env->db + "/procedures/" + sp_name;
        cntl.request_attachment().append(R"({
        "common_cols":["bb", 23, 1590738994000],
        "input": [[123, 5.1, 6.1, "2021-08-01"],[234, 5.2, 6.2, "2021-08-02"]],
        "need_schema": true
    })");
        env->http_channel.CallMethod(NULL, &cntl, NULL, NULL, NULL);
        ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();

        LOG(INFO) << "exec procedure resp:\n" << cntl.response_attachment().to_string();

        // check resp data
        if (document.Parse(cntl.response_attachment().to_string().c_str()).HasParseError()) {
            ASSERT_TRUE(false) << "response parse failed with code " << document.GetParseError()
                               << ", raw resp: " << cntl.response_attachment().to_string();
        }
        ASSERT_EQ(0, document["code"].GetInt());
        ASSERT_STREQ("ok", document["msg"].GetString());
        ASSERT_EQ(2, document["data"]["data"].Size());
        ASSERT_EQ(2, document["data"]["common_cols_data"].Size());
    }

    // call procedure, without schema
    {
        brpc::Controller cntl;
        cntl.http_request().set_method(brpc::HTTP_METHOD_POST);
        cntl.http_request().uri() = "http://127.0.0.1:8010/dbs/" + env->db + "/procedures/" + sp_name;
        cntl.request_attachment().append(R"({
        "common_cols":["bb", 23, 1590738994000],
        "input": [[123, 5.1, 6.1, "2021-08-01"],[234, 5.2, 6.2, "2021-08-02"]],
        "need_schema": false
    })");
        env->http_channel.CallMethod(NULL, &cntl, NULL, NULL, NULL);
        ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();

        LOG(INFO) << "exec procedure resp:\n" << cntl.response_attachment().to_string();

        // check resp data
        if (document.Parse(cntl.response_attachment().to_string().c_str()).HasParseError()) {
            ASSERT_TRUE(false) << "response parse failed with code " << document.GetParseError()
                               << ", raw resp: " << cntl.response_attachment().to_string();
        }
        ASSERT_EQ(0, document["code"].GetInt());
        ASSERT_STREQ("ok", document["msg"].GetString());
        ASSERT_TRUE(document["data"].FindMember("schema") == document["data"].MemberEnd());
        ASSERT_EQ(2, document["data"]["data"].Size());
        ASSERT_EQ(2, document["data"]["common_cols_data"].Size());
    }

    // drop procedure and table
    std::string drop_sp_sql = "drop procedure " + sp_name + ";";
    ASSERT_TRUE(env->cluster_remote->ExecuteDDL(env->db, drop_sp_sql, &status));
    ASSERT_TRUE(env->cluster_remote->ExecuteDDL(env->db, "drop table trans;", &status));
}

TEST_F(APIServerTest, no_common) {
    const auto env = APIServerTestEnv::Instance();

    // create table
    std::string ddl =
        "create table trans(c1 string,\n"
        "                   c3 int,\n"
        "                   c4 bigint,\n"
        "                   c5 float,\n"
        "                   c6 double,\n"
        "                   c7 timestamp,\n"
        "                   c8 date,\n"
        "                   index(key=c1, ts=c7));";
    hybridse::sdk::Status status;
    env->cluster_remote->ExecuteDDL(env->db, "drop table trans;", &status);
    ASSERT_TRUE(env->cluster_sdk->Refresh());
    ASSERT_TRUE(env->cluster_remote->ExecuteDDL(env->db, ddl, &status)) << "fail to create table";

    ASSERT_TRUE(env->cluster_sdk->Refresh());
    // insert
    std::string insert_sql = "insert into trans values(\"bb\",24,34,1.5,2.5,1590738994000,\"2020-05-05\");";
    ASSERT_TRUE(env->cluster_remote->ExecuteInsert(env->db, insert_sql, &status));
    // create procedure
    std::string sp_name = "sp";
    std::string sql =
        "SELECT c1, c3, sum(c4) OVER w1 as w1_c4_sum FROM trans WINDOW w1 AS"
        " (PARTITION BY trans.c1 ORDER BY trans.c7 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW);";
    std::string sp_ddl =
        "create procedure " + sp_name +
        " (c1 string, c3 int, c4 bigint, c5 float, c6 double, c7 timestamp, c8 date" + ")" +
        " begin " + sql + " end;";
    ASSERT_TRUE(env->cluster_remote->ExecuteDDL(env->db, sp_ddl, &status)) << "fail to create procedure";
    ASSERT_TRUE(env->cluster_sdk->Refresh());

    // show procedure
    brpc::Controller show_cntl;  // default is GET
    show_cntl.http_request().uri() = "http://127.0.0.1:8010/dbs/" + env->db + "/procedures/" + sp_name;
    env->http_channel.CallMethod(NULL, &show_cntl, NULL, NULL, NULL);
    ASSERT_FALSE(show_cntl.Failed()) << show_cntl.ErrorText();
    LOG(INFO) << "get sp resp: " << show_cntl.response_attachment();

    butil::rapidjson::Document document;
    if (document.Parse(show_cntl.response_attachment().to_string().c_str()).HasParseError()) {
        ASSERT_TRUE(false) << "response parse failed with code " << document.GetParseError()
                           << ", raw resp: " << show_cntl.response_attachment().to_string();
    }
    ASSERT_EQ(0, document["code"].GetInt());
    ASSERT_STREQ("ok", document["msg"].GetString());
    ASSERT_EQ(7, document["data"]["input_schema"].Size());
    ASSERT_EQ(3, document["data"]["output_schema"].Size());
    ASSERT_EQ(0, document["data"]["output_common_cols"].Size());

    // call procedure, without schema
    {
        brpc::Controller cntl;
        cntl.http_request().set_method(brpc::HTTP_METHOD_POST);
        cntl.http_request().uri() = "http://127.0.0.1:8010/dbs/" + env->db + "/deployments/" + sp_name;
        cntl.request_attachment().append(R"({
        "input": [["bb", 23, 123, 5.1, 6.1, 1590738994000, "2021-08-01"],
                  ["bb", 23, 234, 5.2, 6.2, 1590738994000, "2021-08-02"]],
        "need_schema": false
    })");
        env->http_channel.CallMethod(NULL, &cntl, NULL, NULL, NULL);
        ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();

        LOG(INFO) << "exec procedure resp:\n" << cntl.response_attachment().to_string();

        // check resp data
        if (document.Parse(cntl.response_attachment().to_string().c_str()).HasParseError()) {
            ASSERT_TRUE(false) << "response parse failed with code " << document.GetParseError()
                               << ", raw resp: " << cntl.response_attachment().to_string();
        }
        ASSERT_EQ(0, document["code"].GetInt());
        ASSERT_STREQ("ok", document["msg"].GetString());
        ASSERT_TRUE(document["data"].FindMember("schema") == document["data"].MemberEnd());
        ASSERT_EQ(2, document["data"]["data"].Size());
        ASSERT_EQ(0, document["data"]["common_cols_data"].Size());
    }

    // drop procedure and table
    std::string drop_sp_sql = "drop procedure " + sp_name + ";";
    ASSERT_TRUE(env->cluster_remote->ExecuteDDL(env->db, drop_sp_sql, &status));
    ASSERT_TRUE(env->cluster_remote->ExecuteDDL(env->db, "drop table trans;", &status));
}

TEST_F(APIServerTest, no_common_not_first_string) {
    const auto env = APIServerTestEnv::Instance();

    // create table
    std::string ddl =
        "create table trans1(id int,\n"
        "                   c1 string,\n"
        "                   c3 int,\n"
        "                   c4 bigint,\n"
        "                   c5 float,\n"
        "                   c6 double,\n"
        "                   c7 timestamp,\n"
        "                   c8 date,\n"
        "                   index(key=c1, ts=c7));";
    hybridse::sdk::Status status;
    env->cluster_remote->ExecuteDDL(env->db, "drop table trans1;", &status);
    ASSERT_TRUE(env->cluster_sdk->Refresh());
    ASSERT_TRUE(env->cluster_remote->ExecuteDDL(env->db, ddl, &status)) << "fail to create table";

    ASSERT_TRUE(env->cluster_sdk->Refresh());
    // insert
    std::string insert_sql = "insert into trans1 values(11,\"bb\",24,34,1.5,2.5,1590738994000,\"2020-05-05\");";
    ASSERT_TRUE(env->cluster_remote->ExecuteInsert(env->db, insert_sql, &status));
    // create procedure
    std::string sp_name = "sp1";
    std::string sql = "SELECT id, c1, sum(c4) OVER (w1) AS w1_c4_sum FROM trans1 "
        "WINDOW w1 AS (PARTITION BY trans1.c1 ORDER BY trans1.c7 "
        "ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING);";

    std::string sp_ddl =
        "create procedure " + sp_name +
        " (id int, c1 string, c3 int, c4 bigint, c5 float, c6 double, c7 timestamp, c8 date" + ")" +
        " begin " + sql + " end;";
    ASSERT_TRUE(env->cluster_remote->ExecuteDDL(env->db, sp_ddl, &status)) << "fail to create procedure";
    ASSERT_TRUE(env->cluster_sdk->Refresh());

    // show procedure
    brpc::Controller show_cntl;  // default is GET
    show_cntl.http_request().uri() = "http://127.0.0.1:8010/dbs/" + env->db + "/procedures/" + sp_name;
    env->http_channel.CallMethod(NULL, &show_cntl, NULL, NULL, NULL);
    ASSERT_FALSE(show_cntl.Failed()) << show_cntl.ErrorText();
    LOG(INFO) << "get sp resp: " << show_cntl.response_attachment();

    butil::rapidjson::Document document;
    if (document.Parse(show_cntl.response_attachment().to_string().c_str()).HasParseError()) {
        ASSERT_TRUE(false) << "response parse failed with code " << document.GetParseError()
                           << ", raw resp: " << show_cntl.response_attachment().to_string();
    }
    ASSERT_EQ(0, document["code"].GetInt());
    ASSERT_STREQ("ok", document["msg"].GetString());
    ASSERT_EQ(8, document["data"]["input_schema"].Size());
    ASSERT_EQ(3, document["data"]["output_schema"].Size());
    ASSERT_EQ(0, document["data"]["output_common_cols"].Size());

    // call procedure, without schema
    {
        brpc::Controller cntl;
        cntl.http_request().set_method(brpc::HTTP_METHOD_POST);
        cntl.http_request().uri() = "http://127.0.0.1:8010/dbs/" + env->db + "/procedures/" + sp_name;
        cntl.request_attachment().append(R"({
        "input": [[11, "bb", 23, 123, 5.1, 6.1, 1590738994000, "2021-08-01"],
                  [11, "bb", 23, 234, 5.2, 6.2, 1590738994000, "2021-08-02"]],
        "need_schema": false
    })");
        env->http_channel.CallMethod(NULL, &cntl, NULL, NULL, NULL);
        ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();

        LOG(INFO) << "exec procedure resp:\n" << cntl.response_attachment().to_string();

        // check resp data
        if (document.Parse(cntl.response_attachment().to_string().c_str()).HasParseError()) {
            ASSERT_TRUE(false) << "response parse failed with code " << document.GetParseError()
                               << ", raw resp: " << cntl.response_attachment().to_string();
        }
        ASSERT_EQ(0, document["code"].GetInt());
        ASSERT_STREQ("ok", document["msg"].GetString());
        ASSERT_TRUE(document["data"].FindMember("schema") == document["data"].MemberEnd());
        ASSERT_EQ(2, document["data"]["data"].Size());
        ASSERT_EQ(0, document["data"]["common_cols_data"].Size());
    }

    // drop procedure and table
    std::string drop_sp_sql = "drop procedure " + sp_name + ";";
    ASSERT_TRUE(env->cluster_remote->ExecuteDDL(env->db, drop_sp_sql, &status));
    ASSERT_TRUE(env->cluster_remote->ExecuteDDL(env->db, "drop table trans1;", &status));
}

TEST_F(APIServerTest, getDBs) {
    const auto env = APIServerTestEnv::Instance();
    {
        hybridse::sdk::Status status;
        env->cluster_remote->DropDB("api_server_test", &status);
        brpc::Controller show_cntl;  // default is GET
        show_cntl.http_request().uri() = "http://127.0.0.1:8010/dbs";
        env->http_channel.CallMethod(NULL, &show_cntl, NULL, NULL, NULL);
        butil::rapidjson::Document document;
        if (document.Parse(show_cntl.response_attachment().to_string().c_str()).HasParseError()) {
            ASSERT_TRUE(false) << "response parse failed with code " << document.GetParseError()
                               << ", raw resp: " << show_cntl.response_attachment().to_string();
        }
        ASSERT_FALSE(show_cntl.Failed()) << show_cntl.ErrorText();
        ASSERT_TRUE(document.HasMember("msg"));
        ASSERT_STREQ("ok", document["msg"].GetString());
        ASSERT_TRUE(document.HasMember("dbs"));
        ASSERT_TRUE(document["dbs"].IsArray());
        ASSERT_EQ(document["dbs"].Size(), 0);
    }
    {
        hybridse::sdk::Status status;
        std::vector<std::string> db_names = {"api_server_test", "monkey", "shark", "zebra"};
        for (size_t i = 0; i < db_names.size(); i++) {
            env->cluster_remote->DropDB(db_names[i], &status);
            env->cluster_remote->CreateDB(db_names[i], &status);
        }
        brpc::Controller show_cntl;
        show_cntl.http_request().uri() = "http://127.0.0.1:8010/dbs";
        env->http_channel.CallMethod(NULL, &show_cntl, NULL, NULL, NULL);
        ASSERT_FALSE(show_cntl.Failed()) << show_cntl.ErrorText();
        butil::rapidjson::Document document;
        if (document.Parse(show_cntl.response_attachment().to_string().c_str()).HasParseError()) {
            ASSERT_TRUE(false) << "response parse failed with code " << document.GetParseError()
                               << ", raw resp: " << show_cntl.response_attachment().to_string();
        }
        ASSERT_TRUE(document.HasMember("msg"));
        ASSERT_STREQ("ok", document["msg"].GetString());
        ASSERT_TRUE(document.HasMember("dbs"));
        ASSERT_TRUE(document["dbs"].IsArray());
        ASSERT_EQ(document["dbs"].Size(), db_names.size());
        std::vector<std::string> result;
        for (size_t i = 0; i < document["dbs"].Size(); i++) {
            result.push_back(document["dbs"][i].GetString());
        }
        sort(result.begin(), result.end());
        for (size_t i = 0; i < document["dbs"].Size(); i++) {
            ASSERT_EQ(result[i], db_names[i]);
        }
    }
}

TEST_F(APIServerTest, getTables) {
    const auto env = APIServerTestEnv::Instance();
    {
        brpc::Controller show_cntl;  // default is GET
        show_cntl.http_request().uri() = "http://127.0.0.1:8010/dbs/api_server_test/tables";
        env->http_channel.CallMethod(NULL, &show_cntl, NULL, NULL, NULL);
        ASSERT_FALSE(show_cntl.Failed()) << show_cntl.ErrorText();
        butil::rapidjson::Document document;
        if (document.Parse(show_cntl.response_attachment().to_string().c_str()).HasParseError()) {
            ASSERT_TRUE(false) << "response parse failed with code " << document.GetParseError()
                               << ", raw resp: " << show_cntl.response_attachment().to_string();
        }
        ASSERT_TRUE(document.HasMember("msg"));
        ASSERT_STREQ("ok", document["msg"].GetString());
        ASSERT_TRUE(document.HasMember("tables"));
        ASSERT_TRUE(document["tables"].IsArray());
        ASSERT_EQ(0, document["tables"].Size());
    }
    std::vector<std::string> tables = {"apple", "banana", "pear"};
    hybridse::sdk::Status status;
    for (auto table : tables) {
        std::string ddl = "create table " + table +
                          "(c1 string,\n"
                          "                   c3 int,\n"
                          "                   c4 bigint,\n"
                          "                   c5 float,\n"
                          "                   c6 double,\n"
                          "                   c7 timestamp,\n"
                          "                   c8 date,\n"
                          "                   index(key=c1, ts=c7));";
        ASSERT_TRUE(env->cluster_remote->ExecuteDDL(env->db, ddl, &status)) << "fail to create table";
        ASSERT_TRUE(env->cluster_sdk->Refresh());
    }
    {
        brpc::Controller show_cntl;  // default is GET
        show_cntl.http_request().uri() = "http://127.0.0.1:8010/dbs/api_server_test/tables";
        env->http_channel.CallMethod(NULL, &show_cntl, NULL, NULL, NULL);
        ASSERT_FALSE(show_cntl.Failed()) << show_cntl.ErrorText();
        butil::rapidjson::Document document;
        if (document.Parse(show_cntl.response_attachment().to_string().c_str()).HasParseError()) {
            ASSERT_TRUE(false) << "response parse failed with code " << document.GetParseError()
                               << ", raw resp: " << show_cntl.response_attachment().to_string();
        }
        ASSERT_TRUE(document.HasMember("msg"));
        ASSERT_STREQ("ok", document["msg"].GetString());
        ASSERT_TRUE(document.HasMember("tables"));
        ASSERT_TRUE(document["tables"].IsArray());
        ASSERT_EQ(tables.size(), document["tables"].Size());
        std::vector<std::string> result;
        for (size_t i = 0; i < document["tables"].Size(); i++) {
            ASSERT_TRUE(document["tables"][i].HasMember("name"));
            result.push_back(document["tables"][i]["name"].GetString());
        }
        sort(result.begin(), result.end());
        for (size_t i = 0; i < document["tables"].Size(); i++) {
            ASSERT_EQ(result[i], tables[i]);
        }
    }
    {
        brpc::Controller show_cntl;  // default is GET
        show_cntl.http_request().uri() = "http://127.0.0.1:8010/dbs/db_not_exist/tables";
        env->http_channel.CallMethod(NULL, &show_cntl, NULL, NULL, NULL);
        ASSERT_FALSE(show_cntl.Failed()) << show_cntl.ErrorText();
        butil::rapidjson::Document document;
        if (document.Parse(show_cntl.response_attachment().to_string().c_str()).HasParseError()) {
            ASSERT_TRUE(false) << "response parse failed with code " << document.GetParseError()
                               << ", raw resp: " << show_cntl.response_attachment().to_string();
        }
        ASSERT_TRUE(document.HasMember("msg"));
        ASSERT_STREQ("DB not found", document["msg"].GetString());
    }
    for (auto table : tables) {
        brpc::Controller show_cntl;  // default is GET
        show_cntl.http_request().uri() = "http://127.0.0.1:8010/dbs/api_server_test/tables/" + table;
        env->http_channel.CallMethod(NULL, &show_cntl, NULL, NULL, NULL);
        ASSERT_FALSE(show_cntl.Failed()) << show_cntl.ErrorText();
        butil::rapidjson::Document document;
        if (document.Parse(show_cntl.response_attachment().to_string().c_str()).HasParseError()) {
            ASSERT_TRUE(false) << "response parse failed with code " << document.GetParseError()
                               << ", raw resp: " << show_cntl.response_attachment().to_string();
        }
        ASSERT_TRUE(document.HasMember("msg"));
        ASSERT_STREQ("ok", document["msg"].GetString());
        ASSERT_TRUE(document.HasMember("table"));
        ASSERT_TRUE(document["table"].HasMember("name"));
        ASSERT_EQ(table, std::string(document["table"]["name"].GetString()));
    }
    {
        brpc::Controller show_cntl;  // default is GET
        show_cntl.http_request().uri() = "http://127.0.0.1:8010/dbs/api_server_test/tables/not_exist";
        env->http_channel.CallMethod(NULL, &show_cntl, NULL, NULL, NULL);
        ASSERT_FALSE(show_cntl.Failed()) << show_cntl.ErrorText();
        butil::rapidjson::Document document;
        if (document.Parse(show_cntl.response_attachment().to_string().c_str()).HasParseError()) {
            ASSERT_TRUE(false) << "response parse failed with code " << document.GetParseError()
                               << ", raw resp: " << show_cntl.response_attachment().to_string();
        }
        ASSERT_TRUE(document.HasMember("msg"));
        ASSERT_STREQ("Table not found", document["msg"].GetString());
    }
    {
        brpc::Controller show_cntl;  // default is GET
        show_cntl.http_request().uri() = "http://127.0.0.1:8010/dbs/db_not_exist/tables/apple";
        env->http_channel.CallMethod(NULL, &show_cntl, NULL, NULL, NULL);
        ASSERT_FALSE(show_cntl.Failed()) << show_cntl.ErrorText();
        butil::rapidjson::Document document;
        if (document.Parse(show_cntl.response_attachment().to_string().c_str()).HasParseError()) {
            ASSERT_TRUE(false) << "response parse failed with code " << document.GetParseError()
                               << ", raw resp: " << show_cntl.response_attachment().to_string();
        }
        ASSERT_TRUE(document.HasMember("msg"));
        ASSERT_STREQ("DB not found", document["msg"].GetString());
    }
    for (auto table : tables) {
        env->cluster_remote->ExecuteDDL(env->db, "drop table " + table + ";", &status);
        ASSERT_TRUE(env->cluster_sdk->Refresh());
    }
}

}  // namespace openmldb::apiserver

int main(int argc, char* argv[]) {
    testing::AddGlobalTestEnvironment(openmldb::apiserver::APIServerTestEnv::Instance());
    ::testing::InitGoogleTest(&argc, argv);
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    return RUN_ALL_TESTS();
}
