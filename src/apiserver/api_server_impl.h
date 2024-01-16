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

#include <algorithm>
#include <charconv>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "apiserver/interface_provider.h"
#include "apiserver/json_helper.h"
#include "bvar/bvar.h"
#include "bvar/multi_dimension.h"  // latency recorder
#include "proto/api_server.pb.h"
#include "rapidjson/document.h"  // raw rapidjson 1.1.0, not in butil
#include "sdk/sql_cluster_router.h"
#include "sdk/sql_request_row.h"

namespace openmldb {
namespace apiserver {

using rapidjson::Document;
using rapidjson::Value;

// APIServer is a service for brpc::Server. The entire implement is `StartAPIServer()` in src/cmd/openmldb.cc
// Every request is handled by `Process()`, we will choose the right method of the request by `InterfaceProvider`.
// InterfaceProvider's url parser supports to parse urls like "/a/:arg1/b/:arg2/:arg3", but doesn't support wildcards.
// Methods should be registered in `InterfaceProvider` in the init phase.
// Both input and output are json data. We use rapidjson to handle it.
class APIServerImpl : public APIServer {
 public:
    explicit APIServerImpl(const std::string& endpoint);
    ~APIServerImpl() override;
    bool Init(const std::shared_ptr<::openmldb::sdk::SQLRouterOptions>& options);
    bool Init(::openmldb::sdk::DBSDK* cluster);
    void Process(google::protobuf::RpcController* cntl_base, const HttpRequest*, HttpResponse*,
                 google::protobuf::Closure* done) override;
    static std::string InnerTypeTransform(const std::string& s);

    void Refresh();

 private:
    void RegisterQuery();
    void RegisterPut();
    void RegisterExecSP();
    void RegisterExecDeployment();
    void RegisterGetSP();
    void RegisterGetDeployment();
    void RegisterGetDB();
    void RegisterGetTable();
    void RegisterRefresh();

    void ExecuteProcedure(bool has_common_col, const InterfaceProvider::Params& param, const butil::IOBuf& req_body,
                          JsonWriter& writer);  // NOLINT

    static absl::Status JsonArray2SQLRequestRow(const Value& non_common_cols_v,
                                                const Value& common_cols_v,
                                                std::shared_ptr<openmldb::sdk::SQLRequestRow> row);
    static absl::Status JsonMap2SQLRequestRow(const Value& non_common_cols_v,
                                              const Value& common_cols_v,
                                              std::shared_ptr<openmldb::sdk::SQLRequestRow> row);
    template <typename T>
    static bool AppendJsonValue(const Value& v, hybridse::sdk::DataType type, bool is_not_null,
                                T row);

    // may get segmentation fault when throw boost::bad_lexical_cast, so we use std::from_chars
    template <typename T>
    static bool FromString(const std::string& s, T& value) {  // NOLINT
        if (auto res = std::from_chars(s.data(), s.data() + s.size(), value); res.ec == std::errc()) {
            auto len = res.ptr - s.data();
            return len >= 0 ? (uint64_t)len == s.size() : false;
        } else {
            return false;
        }
    }

 private:
    bvar::MultiDimension<bvar::LatencyRecorder> md_recorder_;
    InterfaceProvider provider_;

    std::shared_ptr<sdk::SQLRouter> sql_router_;
    // cluster_sdk_ is not owned by this class.
    ::openmldb::sdk::DBSDK* cluster_sdk_ = nullptr;
};

struct QueryReq {
    std::string mode;
    int timeout = -1;  // only for offline jobs
    std::string sql;
    std::shared_ptr<openmldb::sdk::SQLRequestRow> parameter;
    bool write_nan_and_inf_null = false;
};

JsonReader& operator&(JsonReader& ar, QueryReq& s);  // NOLINT

JsonReader& operator&(JsonReader& ar, std::shared_ptr<openmldb::sdk::SQLRequestRow>& parameter);  // NOLINT

struct ExecSPResp {
    ExecSPResp() = default;
    int code = 0;
    std::string msg = "ok";
    std::shared_ptr<hybridse::sdk::ProcedureInfo> sp_info;
    bool need_schema = false;
    bool json_result = false;
    std::shared_ptr<hybridse::sdk::ResultSet> rs;
    bool write_nan_and_inf_null = false;
};

void WriteSchema(JsonWriter& ar, const std::string& name, const hybridse::sdk::Schema& schema,  // NOLINT
                 bool only_const);

void WriteValue(JsonWriter& ar, std::shared_ptr<hybridse::sdk::ResultSet> rs, int i, bool write_nan_and_inf_null);  // NOLINT

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

struct QueryResp {
    QueryResp() = default;
    int code = 0;
    std::string msg = "ok";
    std::shared_ptr<hybridse::sdk::ResultSet> rs;
    // option, won't write to result
    bool write_nan_and_inf_null = false;
};

JsonWriter& operator&(JsonWriter& ar, QueryResp& s);  // NOLINT

}  // namespace apiserver
}  // namespace openmldb

#endif  // SRC_APISERVER_API_SERVER_IMPL_H_
