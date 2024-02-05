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

#include <memory>
#include <random>

#include "apiserver/api_server_impl.h"
#include "brpc/channel.h"
#include "brpc/restful.h"
#include "brpc/server.h"
#include "butil/logging.h"
#include "gflags/gflags.h"
#include "gtest/gtest.h"
#include "rapidjson/error/en.h"
#include "rapidjson/rapidjson.h"
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

        auto cluster_options = std::make_shared<sdk::SQLRouterOptions>();;
        cluster_options->zk_cluster = mc->GetZkCluster();
        cluster_options->zk_path = mc->GetZkPath();
        // Owned by queue_svc
        cluster_sdk = new ::openmldb::sdk::ClusterSDK(cluster_options);
        ASSERT_TRUE(cluster_sdk->Init()) << "Fail to connect to db";
        queue_svc = std::make_shared<APIServerImpl>("127.0.0.1:8010");  // fake endpoint for metrics
        ASSERT_TRUE(queue_svc->Init(cluster_sdk));

        sdk::SQLRouterOptions sql_opt;
        sql_opt.zk_session_timeout = 30000;
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
        cluster_remote->ExecuteSQL("drop database " + db, &status);
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
    // test raw document
    rapidjson::Document document;

    // Check the format of put request
    if (document
            .Parse(R"({
    "value": [
        ["value1", 111, 1.4, "2021-04-27", 1620471840256, true, null]
    ]
    })")
            .HasParseError()) {
        ASSERT_TRUE(false) << "json parse failed: " << rapidjson::GetParseError_En(document.GetParseError());
    }

    hybridse::sdk::Status status;
    const auto& value = document["value"];
    ASSERT_TRUE(value.IsArray());
    ASSERT_EQ(1, value.Size());
    const auto& arr = value[0];
    ASSERT_EQ(7, arr.Size());
    ASSERT_EQ(rapidjson::kStringType, arr[0].GetType());
    ASSERT_EQ(rapidjson::kNumberType, arr[1].GetType());
    ASSERT_EQ(rapidjson::kNumberType, arr[2].GetType());
    ASSERT_EQ(rapidjson::kStringType, arr[3].GetType());
    ASSERT_EQ(rapidjson::kNumberType, arr[4].GetType());
    ASSERT_EQ(rapidjson::kTrueType, arr[5].GetType());
    ASSERT_EQ(rapidjson::kNullType, arr[6].GetType());

    // raw document with default flags can't parse unquoted nan&inf
    ASSERT_TRUE(document.Parse("[NaN,Infinity]").HasParseError());
    ASSERT_EQ(rapidjson::kParseErrorValueInvalid, document.GetParseError()) << document.GetParseError();

    // test json reader
    // can read inf number to inf
    {
        JsonReader reader("1.797693134862316e308");
        ASSERT_TRUE(reader);
        double d_res = -1.0;
        reader >> d_res;
        ASSERT_EQ(0x7ff0000000000000, *reinterpret_cast<uint64_t*>(&d_res))
            << std::hex << std::setprecision(16) << *reinterpret_cast<uint64_t*>(&d_res);
        ASSERT_TRUE(std::isinf(d_res));
    }

    // read unquoted inf&nan, legal words
    {
        JsonReader reader("[NaN, Inf, -Inf, Infinity, -Infinity]");
        ASSERT_TRUE(reader);
        double d_res = -1.0;
        reader.StartArray();
        reader >> d_res;
        ASSERT_TRUE(std::isnan(d_res));
        // nan hex
        reader >> d_res;
        ASSERT_TRUE(std::isinf(d_res));
        reader >> d_res;
        ASSERT_TRUE(std::isinf(d_res));
        reader >> d_res;
        ASSERT_TRUE(std::isinf(d_res));
        reader >> d_res;
        ASSERT_TRUE(std::isinf(d_res));
    }
    {
        // float nan inf
        // IEEE 754 arithmetic allows cast nan/inf to float, so GetFloat is fine
        JsonReader reader("[NaN, Infinity, -Infinity]");
        ASSERT_TRUE(reader);
        float f_res = -1.0;
        reader.StartArray();
        reader >> f_res;
        ASSERT_TRUE(std::isnan(f_res));
        reader >> f_res;
        ASSERT_TRUE(std::isinf(f_res));
        reader >> f_res;
        ASSERT_TRUE(std::isinf(f_res));
        // raw way for put and procedure(common cols)
        f_res = -1.0;
        rapidjson::Document document;
        document.Parse<rapidjson::kParseNanAndInfFlag>("[NaN, Infinity, -Infinity]");
        document.StartArray();
        f_res = document[0].GetFloat();
        ASSERT_TRUE(std::isnan(f_res));
        f_res = document[1].GetFloat();
        ASSERT_TRUE(std::isinf(f_res));
        f_res = document[2].GetFloat();
        ASSERT_TRUE(std::isinf(f_res));
    }
    {  // illegal words
        JsonReader reader("nan");
        ASSERT_FALSE(reader);
    }
    {  // illegal words
        JsonReader reader("+Inf");
        ASSERT_FALSE(reader);
    }
    {  // string, not double
        JsonReader reader("\"NaN\"");
        ASSERT_TRUE(reader);
        double d = -1.0;
        reader >> d;
        ASSERT_FALSE(reader);      // get double failed
        ASSERT_FLOAT_EQ(d, -1.0);  // won't change
    }

    // test json writer
    JsonWriter writer;
    // about double nan, inf
    double nan = std::numeric_limits<double>::quiet_NaN();
    double inf = std::numeric_limits<double>::infinity();
    writer.StartArray();
    writer << nan;
    writer << inf;
    double ninf = -inf;
    writer << ninf;
    writer.EndArray();
    ASSERT_STREQ("[NaN,Infinity,-Infinity]", writer.GetString());
}

TEST_F(APIServerTest, query) {
    const auto env = APIServerTestEnv::Instance();

    std::string ddl = "create table demo (c1 int, c2 string);";
    hybridse::sdk::Status status;
    ASSERT_TRUE(env->cluster_remote->ExecuteDDL(env->db, ddl, &status)) << "fail to create table";

    std::string insert_sql = "insert into demo values (1, \"bb\");";
    ASSERT_TRUE(env->cluster_sdk->Refresh());
    ASSERT_TRUE(env->cluster_remote->ExecuteInsert(env->db, insert_sql, &status));

    {
        brpc::Controller cntl;
        cntl.http_request().set_method(brpc::HTTP_METHOD_POST);
        cntl.http_request().uri() = "http://127.0.0.1:8010/dbs/" + env->db;
        cntl.request_attachment().append(R"({
            "sql": "select c1, c2 from demo;", "mode": "online"
        })");
        env->http_channel.CallMethod(NULL, &cntl, NULL, NULL, NULL);
        ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();

        LOG(INFO) << "exec query resp:\n" << cntl.response_attachment().to_string();

        rapidjson::Document document;
        if (document.Parse(cntl.response_attachment().to_string().c_str()).HasParseError()) {
            ASSERT_TRUE(false) << "response parse failed with code " << document.GetParseError()
                               << ", raw resp: " << cntl.response_attachment().to_string();
        }

        /*
        {
            "code": 0,
            "msg": "ok",
            "data": {
                "schema": ["Int32", "String"],
                "data": [[1, "bb"]]
            }
        }
        */
        ASSERT_EQ(0, document["code"].GetInt());
        ASSERT_STREQ("ok", document["msg"].GetString());
        ASSERT_EQ(2, document["data"]["schema"].Size());
        ASSERT_STREQ("Int32", document["data"]["schema"][0].GetString());
        ASSERT_STREQ("String", document["data"]["schema"][1].GetString());
        ASSERT_EQ(1, document["data"]["data"].Size());
        ASSERT_EQ(2, document["data"]["data"][0].Size());
        ASSERT_EQ(1, document["data"]["data"][0][0].GetInt());
        ASSERT_STREQ("bb", document["data"]["data"][0][1].GetString());
    }

    ASSERT_TRUE(env->cluster_remote->ExecuteDDL(env->db, "drop table demo;", &status));
}

TEST_F(APIServerTest, parameterizedQuery) {
    const auto env = APIServerTestEnv::Instance();

    std::string ddl = "create table demo (c1 int, c2 string);";
    hybridse::sdk::Status status;
    ASSERT_TRUE(env->cluster_remote->ExecuteDDL(env->db, ddl, &status)) << "fail to create table";

    std::string insert_sql = "insert into demo values (1, \"bb\");";
    ASSERT_TRUE(env->cluster_sdk->Refresh());
    ASSERT_TRUE(env->cluster_remote->ExecuteInsert(env->db, insert_sql, &status));
    insert_sql = "insert into demo values (2, \"bb\");";
    ASSERT_TRUE(env->cluster_sdk->Refresh());
    ASSERT_TRUE(env->cluster_remote->ExecuteInsert(env->db, insert_sql, &status));

    {
        brpc::Controller cntl;
        cntl.http_request().set_method(brpc::HTTP_METHOD_POST);
        cntl.http_request().uri() = "http://127.0.0.1:8010/dbs/" + env->db;
        cntl.request_attachment().append(R"({
            "sql": "select c1, c2 from demo where c2 = ?;",
            "mode": "online", 
            "input": {
                "schema": ["STRING"],
                "data": ["bb"]
            }
        })");
        env->http_channel.CallMethod(NULL, &cntl, NULL, NULL, NULL);
        ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();

        LOG(INFO) << "exec query resp:\n" << cntl.response_attachment().to_string();

        rapidjson::Document document;
        if (document.Parse(cntl.response_attachment().to_string().c_str()).HasParseError()) {
            ASSERT_TRUE(false) << "response parse failed with code " << document.GetParseError()
                               << ", raw resp: " << cntl.response_attachment().to_string();
        }
        /*
        {
            "code": 0,
            "msg": "ok",
            "data": {
                "schema": ["Int32", "String"],
                "data": [[1, "bb"], [2, "bb"]]
            }
        }
        */
        ASSERT_EQ(0, document["code"].GetInt());
        ASSERT_STREQ("ok", document["msg"].GetString());
        ASSERT_EQ(2, document["data"]["schema"].Size());
        ASSERT_STREQ("Int32", document["data"]["schema"][0].GetString());
        ASSERT_STREQ("String", document["data"]["schema"][1].GetString());
        ASSERT_EQ(2, document["data"]["data"].Size());
        ASSERT_EQ(2, document["data"]["data"][0].Size());
        ASSERT_EQ(1, document["data"]["data"][0][0].GetInt());
        ASSERT_STREQ("bb", document["data"]["data"][0][1].GetString());
        ASSERT_EQ(2, document["data"]["data"][1].Size());
        ASSERT_EQ(2, document["data"]["data"][1][0].GetInt());
        ASSERT_STREQ("bb", document["data"]["data"][1][1].GetString());
    }
    {
        brpc::Controller cntl;
        cntl.http_request().set_method(brpc::HTTP_METHOD_POST);
        cntl.http_request().uri() = "http://127.0.0.1:8010/dbs/" + env->db;
        cntl.request_attachment().append(R"({
            "sql": "select c1, c2 from demo where c2 = ? and c1 = ?;",
            "mode": "online",
            "input": {
                "schema": ["STRING", "INT"],
                "data": ["bb", 1]
            }
        })");
        env->http_channel.CallMethod(NULL, &cntl, NULL, NULL, NULL);
        ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();

        LOG(INFO) << "exec query resp:\n" << cntl.response_attachment().to_string();

        rapidjson::Document document;
        if (document.Parse(cntl.response_attachment().to_string().c_str()).HasParseError()) {
            ASSERT_TRUE(false) << "response parse failed with code " << document.GetParseError()
                               << ", raw resp: " << cntl.response_attachment().to_string();
        }
        /*
        {
            "code": 0,
            "msg": "ok",
            "data": {
                "schema": ["Int32", "String"],
                "data": [[1, "bb"]]
            }
        }
        */
        ASSERT_EQ(0, document["code"].GetInt());
        ASSERT_STREQ("ok", document["msg"].GetString());
        ASSERT_EQ(2, document["data"]["schema"].Size());
        ASSERT_STREQ("Int32", document["data"]["schema"][0].GetString());
        ASSERT_STREQ("String", document["data"]["schema"][1].GetString());
        ASSERT_EQ(1, document["data"]["data"].Size());
        ASSERT_EQ(2, document["data"]["data"][0].Size());
        ASSERT_EQ(1, document["data"]["data"][0][0].GetInt());
        ASSERT_STREQ("bb", document["data"]["data"][0][1].GetString());
    }

    ASSERT_TRUE(env->cluster_remote->ExecuteDDL(env->db, "drop table demo;", &status));
}

TEST_F(APIServerTest, invalidPut) {
    const auto env = APIServerTestEnv::Instance();
    brpc::Controller cntl;
    cntl.http_request().set_method(brpc::HTTP_METHOD_PUT);
    GeneralResp resp;

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

    // invalid date 2021-0 4-27
    {
        brpc::Controller cntl;
        cntl.http_request().set_method(brpc::HTTP_METHOD_PUT);
        cntl.http_request().uri() = "http://127.0.0.1:8010/dbs/" + env->db + "/tables/" + table;
        cntl.request_attachment().append(
            "{\"value\": [[\"foo\", 111, 1.4,  \"2021-0 4-27\", 1620471840256, true, \"more str\", null]]}");
        env->http_channel.CallMethod(NULL, &cntl, NULL, NULL, NULL);
        ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
        GeneralResp resp;
        JsonReader reader(cntl.response_attachment().to_string().c_str());
        reader >> resp;
        ASSERT_EQ(-1, resp.code) << resp.msg;
        ASSERT_STREQ("convertion failed for col field4", resp.msg.c_str());
    }

    // valid data
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
        GeneralResp resp;
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
        GeneralResp resp;
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
        GeneralResp resp;
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
        GeneralResp resp;
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
        GeneralResp resp;
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
        GeneralResp resp;
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

    rapidjson::Document document;
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

    // invalid date
    {
        brpc::Controller cntl;
        cntl.http_request().set_method(brpc::HTTP_METHOD_POST);
        cntl.http_request().uri() = "http://127.0.0.1:8010/dbs/" + env->db + "/procedures/" + sp_name;
        cntl.request_attachment().append(R"({
        "common_cols":["bb", 23, 1590738994000],
        "input": [[123, 5.1, 6.1, "20 21-08-01"],[234, 5.2, 6.2, "2021-08-0 2"]],
    })");
        env->http_channel.CallMethod(NULL, &cntl, NULL, NULL, NULL);
        ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();

        LOG(INFO) << "exec procedure resp:\n" << cntl.response_attachment().to_string();

        // check resp data
        if (document.Parse(cntl.response_attachment().to_string().c_str()).HasParseError()) {
            ASSERT_TRUE(false) << "response parse failed with code " << document.GetParseError()
                               << ", raw resp: " << cntl.response_attachment().to_string();
        }
        ASSERT_EQ(-1, document["code"].GetInt());
        ASSERT_STREQ("Request body json parse failed", document["msg"].GetString());
    }

    // drop procedure and table
    std::string drop_sp_sql = "drop procedure " + sp_name + ";";
    ASSERT_TRUE(env->cluster_remote->ExecuteDDL(env->db, drop_sp_sql, &status));
    ASSERT_TRUE(env->cluster_remote->ExecuteDDL(env->db, "drop table trans;", &status));
}

TEST_F(APIServerTest, testResultType) {
    // check about json write
    const auto env = APIServerTestEnv::Instance();
    // don't need table data
    std::string ddl =
        "create table if not exists multi_type(c1 bool, c2 smallint, c3 int, c4 bigint, c5 float, c6 double, c7 "
        "string, c8 date, c9 timestamp, c10_str string);";
    hybridse::sdk::Status status;

    env->cluster_remote->ExecuteSQL(env->db, ddl, &status);
    ASSERT_TRUE(status.IsOK()) << status.msg;
    ASSERT_TRUE(env->cluster_sdk->Refresh());
    std::string deploy = "deploy d1 select c1, c2, c3, c4, c5, c6, c7, c8, c9, c10_str from multi_type;";
    env->cluster_remote->ExecuteSQL(env->db, deploy, &status);
    ASSERT_TRUE(status.IsOK()) << status.msg;
    ASSERT_TRUE(env->cluster_sdk->Refresh());

    {
        brpc::Controller cntl;
        cntl.http_request().set_method(brpc::HTTP_METHOD_POST);
        cntl.http_request().uri() = "http://127.0.0.1:8010/dbs/" + env->db + "/deployments/" + "d1";
        cntl.request_attachment().append(R"({
        "input": 
          [
            [ true,32767,65535,9223372036854775807,1.1,2.2,"abc","2021-12-01",11111111111111,"def" ],
            [ false,32767,65535,null,1.1,2.2,"abc",null,null,"def" ]
          ]
    })");  // null must be lower case
        env->http_channel.CallMethod(NULL, &cntl, NULL, NULL, NULL);
        ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();

        LOG(INFO) << "exec deployment resp:\n" << cntl.response_attachment().to_string();
        rapidjson::Document document;
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

        // check data.data 2 results
        auto& data = document["data"]["data"];
        ASSERT_TRUE(data[0].IsArray());
        ASSERT_TRUE(data[0][0].GetBool());
        ASSERT_EQ(data[0][2].GetInt(), 65535);
        ASSERT_STREQ(data[0][7].GetString(), "2021-12-1");

        ASSERT_TRUE(data[1].IsArray());
        ASSERT_FALSE(data[1][0].GetBool());
        ASSERT_TRUE(data[1][3].IsNull());
    }

    env->cluster_remote->ExecuteSQL(env->db, "drop deployment d1", &status);
    ASSERT_TRUE(status.IsOK()) << status.msg;
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
    std::string sp_ddl = "create procedure " + sp_name +
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

    rapidjson::Document document;
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
    std::string sql =
        "SELECT id, c1, sum(c4) OVER (w1) AS w1_c4_sum FROM trans1 "
        "WINDOW w1 AS (PARTITION BY trans1.c1 ORDER BY trans1.c7 "
        "ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING);";

    std::string sp_ddl = "create procedure " + sp_name +
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

    rapidjson::Document document;
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
    std::default_random_engine e;
    std::string db_name = "" + std::to_string(e() % 100000);  // to avoid use exists db, e.g. api_server_test
    LOG(INFO) << "test on db " << db_name;

    std::set<std::string> test_dbs = {db_name, "monkey", "shark", "zebra"};
    // cluster may have some dbs
    std::set<std::string> exists_db_set;
    {
        brpc::Controller show_cntl;  // default is GET
        show_cntl.http_request().uri() = "http://127.0.0.1:8010/dbs";
        env->http_channel.CallMethod(NULL, &show_cntl, NULL, NULL, NULL);
        rapidjson::Document document;
        if (document.Parse(show_cntl.response_attachment().to_string().c_str()).HasParseError()) {
            ASSERT_TRUE(false) << "response parse failed with code " << document.GetParseError()
                               << ", raw resp: " << show_cntl.response_attachment().to_string();
        }
        ASSERT_FALSE(show_cntl.Failed()) << show_cntl.ErrorText();
        ASSERT_TRUE(document.HasMember("msg"));
        ASSERT_STREQ("ok", document["msg"].GetString());
        ASSERT_TRUE(document.HasMember("dbs"));
        auto& exists_dbs = document["dbs"];
        ASSERT_TRUE(exists_dbs.IsArray());
        for (decltype(exists_dbs.Size()) i = 0; i < exists_dbs.Size(); ++i) {
            auto db = exists_dbs[i].GetString();
            if (test_dbs.find(db) != test_dbs.end()) {
                FAIL() << "can't have test db " << db;
            }
            exists_db_set.emplace(db);
        }
    }
    {
        hybridse::sdk::Status status;
        for (auto& db : test_dbs) {
            // empty db can be droped
            env->cluster_remote->DropDB(db, &status);
            ASSERT_TRUE(env->cluster_remote->CreateDB(db, &status));
        }
        env->queue_svc->Refresh();

        brpc::Controller show_cntl;
        show_cntl.http_request().uri() = "http://127.0.0.1:8010/dbs";
        env->http_channel.CallMethod(NULL, &show_cntl, NULL, NULL, NULL);
        ASSERT_FALSE(show_cntl.Failed()) << show_cntl.ErrorText();
        rapidjson::Document document;
        if (document.Parse(show_cntl.response_attachment().to_string().c_str()).HasParseError()) {
            ASSERT_TRUE(false) << "response parse failed with code " << document.GetParseError()
                               << ", raw resp: " << show_cntl.response_attachment().to_string();
        }
        ASSERT_TRUE(document.HasMember("msg"));
        ASSERT_STREQ("ok", document["msg"].GetString());
        ASSERT_TRUE(document.HasMember("dbs"));
        ASSERT_TRUE(document["dbs"].IsArray());
        ASSERT_EQ(document["dbs"].Size(), test_dbs.size() + exists_db_set.size());
        std::set<std::string> result;
        for (size_t i = 0; i < document["dbs"].Size(); i++) {
            result.emplace(document["dbs"][i].GetString());
        }

        test_dbs.merge(exists_db_set);
        ASSERT_EQ(result, test_dbs);
    }
}

TEST_F(APIServerTest, getTables) {
    const auto env = APIServerTestEnv::Instance();
    std::default_random_engine e;
    std::string db_name = "" + std::to_string(e() % 100000);  // to avoid use db which has tables
    LOG(INFO) << "test on db " << db_name;
    // setup
    {
        hybridse::sdk::Status status;
        env->cluster_remote->CreateDB(db_name, &status);
        env->queue_svc->Refresh();
        brpc::Controller show_cntl;  // default is GET
        show_cntl.http_request().uri() = "http://127.0.0.1:8010/dbs/" + db_name + "/tables";
        env->http_channel.CallMethod(NULL, &show_cntl, NULL, NULL, NULL);
        ASSERT_FALSE(show_cntl.Failed()) << show_cntl.ErrorText();
        rapidjson::Document document;
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
        ASSERT_TRUE(env->cluster_remote->ExecuteDDL(db_name, ddl, &status)) << "fail to create table";
        ASSERT_TRUE(env->cluster_sdk->Refresh());
    }
    {
        brpc::Controller show_cntl;  // default is GET
        show_cntl.http_request().uri() = "http://127.0.0.1:8010/dbs/" + db_name + "/tables";
        env->http_channel.CallMethod(NULL, &show_cntl, NULL, NULL, NULL);
        ASSERT_FALSE(show_cntl.Failed()) << show_cntl.ErrorText();
        rapidjson::Document document;
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
        rapidjson::Document document;
        if (document.Parse(show_cntl.response_attachment().to_string().c_str()).HasParseError()) {
            ASSERT_TRUE(false) << "response parse failed with code " << document.GetParseError()
                               << ", raw resp: " << show_cntl.response_attachment().to_string();
        }
        ASSERT_TRUE(document.HasMember("msg"));
        ASSERT_STREQ("DB not found", document["msg"].GetString());
    }
    for (auto table : tables) {
        brpc::Controller show_cntl;  // default is GET
        show_cntl.http_request().uri() = "http://127.0.0.1:8010/dbs/" + db_name + "/tables/" + table;
        env->http_channel.CallMethod(NULL, &show_cntl, NULL, NULL, NULL);
        ASSERT_FALSE(show_cntl.Failed()) << show_cntl.ErrorText();
        rapidjson::Document document;
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
        show_cntl.http_request().uri() = "http://127.0.0.1:8010/dbs/" + db_name + "/tables/not_exist";
        env->http_channel.CallMethod(NULL, &show_cntl, NULL, NULL, NULL);
        ASSERT_FALSE(show_cntl.Failed()) << show_cntl.ErrorText();
        rapidjson::Document document;
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
        rapidjson::Document document;
        if (document.Parse(show_cntl.response_attachment().to_string().c_str()).HasParseError()) {
            ASSERT_TRUE(false) << "response parse failed with code " << document.GetParseError()
                               << ", raw resp: " << show_cntl.response_attachment().to_string();
        }
        ASSERT_TRUE(document.HasMember("msg"));
        ASSERT_STREQ("DB not found", document["msg"].GetString());
    }
    for (auto table : tables) {
        env->cluster_remote->ExecuteDDL(db_name, "drop table " + table + ";", &status);
        ASSERT_TRUE(env->cluster_sdk->Refresh());
    }
    env->cluster_remote->DropDB(db_name, &status);
}

TEST_F(APIServerTest, jsonInput) {
    const auto env = APIServerTestEnv::Instance();

    std::string table = "json_test";
    // create table, no c2
    std::string ddl = "create table " + table +
                      "(c1 string,\n"
                      "c3 int,\n"
                      "c4 bigint,\n"
                      "c5 float,\n"
                      "c6 double,\n"
                      "c7 timestamp,\n"
                      "c8 date,\n"
                      "index(key=c1, ts=c7));";
    hybridse::sdk::Status status;
    env->cluster_remote->ExecuteDDL(env->db, "drop table " + table, &status);
    ASSERT_TRUE(env->cluster_sdk->Refresh());
    ASSERT_TRUE(env->cluster_remote->ExecuteDDL(env->db, ddl, &status)) << "fail to create table";

    ASSERT_TRUE(env->cluster_sdk->Refresh());
    // insert
    std::string insert_sql = "insert into " + table + " values(\"bb\",24,34,1.5,2.5,1590738994000,\"2020-05-05\");";
    ASSERT_TRUE(env->cluster_remote->ExecuteInsert(env->db, insert_sql, &status));
    // create procedure
    std::string sp_name = "json_deploy";
    std::string sql = "SELECT c1, c3, sum(c4) OVER w1 as w1_c4_sum FROM " + table +
                      " WINDOW w1 AS"
                      " (PARTITION BY c1 ORDER BY c7 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW);";
    std::string sp_ddl = "create procedure " + sp_name +
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

    // call sp in deployment api with json style input(won't check if it's a sp or deployment), so it'll have field
    // `common_cols_data`
    {
        brpc::Controller cntl;
        cntl.http_request().set_method(brpc::HTTP_METHOD_POST);
        cntl.http_request().uri() = "http://127.0.0.1:8010/dbs/" + env->db + "/deployments/" + sp_name;
        cntl.request_attachment().append(R"({
        "input": [{"c1":"bb", "c3":23, "c4":123, "c5":5.1, "c6":6.1, "c7":1590738994000, "c8":"2021-08-01"},
                  {"c1":"bb", "c3":23, "c4":234, "c5":5.2, "c6":6.2, "c7":1590738994000, "c8":"2021-08-02"}]
    })");
        env->http_channel.CallMethod(NULL, &cntl, NULL, NULL, NULL);
        ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();

        LOG(INFO) << "exec deployment resp:\n" << cntl.response_attachment().to_string();
        rapidjson::Document document;
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

        // check data.data 2 results
        auto& data = document["data"]["data"];
        ASSERT_TRUE(data[0].IsObject());
        ASSERT_STREQ(data[0].FindMember("c1")->value.GetString(), "bb");
        ASSERT_EQ(data[0].FindMember("c3")->value.GetInt(), 23);
        ASSERT_EQ(data[0].FindMember("w1_c4_sum")->value.GetInt64(), 157);  // 34 + 123

        ASSERT_TRUE(data[1].IsObject());
        ASSERT_STREQ(data[1].FindMember("c1")->value.GetString(), "bb");
        ASSERT_EQ(data[1].FindMember("c3")->value.GetInt(), 23);
        ASSERT_EQ(data[1].FindMember("w1_c4_sum")->value.GetInt64(), 268);  // 34 + 234
    }

    // drop procedure and table
    std::string drop_sp_sql = "drop procedure " + sp_name + ";";
    ASSERT_TRUE(env->cluster_remote->ExecuteDDL(env->db, drop_sp_sql, &status));
    ASSERT_TRUE(env->cluster_remote->ExecuteDDL(env->db, "drop table " + table, &status));
}
}  // namespace openmldb::apiserver

int main(int argc, char* argv[]) {
    testing::AddGlobalTestEnvironment(openmldb::apiserver::APIServerTestEnv::Instance());
    ::testing::InitGoogleTest(&argc, argv);
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    ::openmldb::base::SetupGlog(true);
    return RUN_ALL_TESTS();
}
