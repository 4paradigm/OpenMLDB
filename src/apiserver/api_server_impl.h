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

#ifndef SRC_APISERVER_API_SERVER_IMPL_H_
#define SRC_APISERVER_API_SERVER_IMPL_H_

#include <memory>
#include <string>
#include <utility>
#include <vector>
#include <algorithm>

#include "apiserver/interface_provider.h"
#include "apiserver/json_helper.h"
#include "json2pb/rapidjson.h"  // rapidjson's DOM-style API
#include "proto/api_server.pb.h"
#include "sdk/sql_cluster_router.h"

namespace openmldb {
namespace apiserver {

using butil::rapidjson::Document;
using butil::rapidjson::StringBuffer;
using butil::rapidjson::Writer;

// APIServer is a service for brpc::Server. The entire implement is `StartAPIServer()` in src/cmd/openmldb.cc
// Every request is handled by `Process()`, we will choose the right method of the request by `InterfaceProvider`.
// InterfaceProvider's url parser supports to parse urls like "/a/:arg1/b/:arg2/:arg3", but doesn't support wildcards.
// Methods should be registered in `InterfaceProvider` in the init phase.
// Both input and output are json data. We use rapidjson to handle it.
class APIServerImpl : public APIServer {
 public:
    APIServerImpl() = default;
    ~APIServerImpl() override;
    bool Init(const sdk::ClusterOptions& options);
    bool Init(::openmldb::sdk::ClusterSDK* cluster);
    void Process(google::protobuf::RpcController* cntl_base, const HttpRequest*, HttpResponse*,
                 google::protobuf::Closure* done) override;
    static std::string InnerTypeTransform(const std::string& s);

    void Refresh(google::protobuf::RpcController* cntl_base, const HttpRequest*, HttpResponse*,
                 google::protobuf::Closure* done) override;

 private:
    void RegisterPut();
    void RegisterExecSP();
    void RegisterGetSP();
    void RegisterGetDB();
    void RegisterGetTable();

    static bool Json2SQLRequestRow(const butil::rapidjson::Value& non_common_cols_v,
                                   const butil::rapidjson::Value& common_cols_v,
                                   std::shared_ptr<openmldb::sdk::SQLRequestRow> row);
    template <typename T>
    static bool AppendJsonValue(const butil::rapidjson::Value& v, hybridse::sdk::DataType type, bool is_not_null,
                                T row);

 private:
    std::shared_ptr<sdk::SQLRouter> sql_router_;
    InterfaceProvider provider_;
    // cluster_sdk_ is not owned by this class.
    ::openmldb::sdk::ClusterSDK* cluster_sdk_;
};

struct PutResp {
    PutResp() = default;
    int code = 0;
    std::string msg = "ok";
};

template <typename Archiver>
Archiver& operator&(Archiver& ar, PutResp& s) {  // NOLINT
    ar.StartObject();
    ar.Member("code") & s.code;
    ar.Member("msg") & s.msg;
    return ar.EndObject();
}

struct ExecSPResp {
    ExecSPResp() = default;
    int code = 0;
    std::string msg = "ok";
    std::shared_ptr<hybridse::sdk::ProcedureInfo> sp_info;
    bool need_schema = false;
    std::shared_ptr<hybridse::sdk::ResultSet> rs;
};

void WriteSchema(JsonWriter& ar, const std::string& name, const hybridse::sdk::Schema& schema,  // NOLINT
                 bool only_const);

void WriteValue(JsonWriter& ar, std::shared_ptr<hybridse::sdk::ResultSet> rs, int i);  // NOLINT

// ExecSPResp reading is unsupported now, cuz we decode ResultSet with Schema here, it's irreversible
JsonWriter& operator&(JsonWriter& ar, ExecSPResp& s);  // NOLINT

struct GetSPResp {
    GetSPResp() = default;
    int code = 0;
    std::string msg = "ok";
    std::shared_ptr<hybridse::sdk::ProcedureInfo> sp_info;
};

JsonWriter& operator&(JsonWriter& ar, std::shared_ptr<hybridse::sdk::ProcedureInfo> sp_info);  // NOLINT

// ExecSPResp reading is unsupported now, cuz we decode sp_info here, it's irreversible
JsonWriter& operator&(JsonWriter& ar, GetSPResp& s);  // NOLINT

JsonWriter& operator&(JsonWriter& ar,  // NOLINT
                      const ::google::protobuf::RepeatedPtrField<::openmldb::common::ColumnDesc>& column_desc);

JsonWriter& operator&(JsonWriter& ar,  // NOLINT
                      const ::google::protobuf::RepeatedPtrField<::openmldb::common::ColumnKey>& column_key);

JsonWriter& operator&(JsonWriter& ar, std::shared_ptr<::openmldb::nameserver::TableInfo> info);  // NOLINT

}  // namespace apiserver
}  // namespace openmldb

#endif  // SRC_APISERVER_API_SERVER_IMPL_H_
