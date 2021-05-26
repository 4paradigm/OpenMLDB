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

#ifndef SRC_APISERVER_API_SERVICE_IMPL_H_
#define SRC_APISERVER_API_SERVICE_IMPL_H_

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "apiserver/interface_provider.h"
#include "apiserver/json_helper.h"
#include "json2pb/rapidjson.h"  // rapidjson's DOM-style API
#include "proto/http.pb.h"
#include "sdk/sql_cluster_router.h"

namespace fedb {
namespace http {

using butil::rapidjson::Document;
using butil::rapidjson::StringBuffer;
using butil::rapidjson::Writer;

struct Column;
typedef std::vector<Column> SimpleSchema;

class APIServiceImpl : public APIService {
 public:
    APIServiceImpl() = default;
    ~APIServiceImpl() override;
    bool Init(const sdk::ClusterOptions& options);
    void Process(google::protobuf::RpcController* cntl_base, const HttpRequest*, HttpResponse*,
                 google::protobuf::Closure* done) override;

 private:
    void RegisterPut();
    void RegisterExecSP();
    void RegisterGetSP();

    static bool Json2SQLRequestRow(const butil::rapidjson::Value& non_common_cols_v,
                                   const butil::rapidjson::Value& common_cols_v,
                                   std::shared_ptr<fedb::sdk::SQLRequestRow> row);
    template <typename T>
    static bool AppendJsonValue(const butil::rapidjson::Value& v, hybridse::sdk::DataType type, bool is_not_null,
                                T row);

    static SimpleSchema TransToSimpleSchema(const hybridse::sdk::Schema* schema) {
        SimpleSchema ss;
        for (int i = 0; i < schema->GetColumnCnt(); ++i) {
            ss.emplace_back(schema->GetColumnName(i), schema->GetColumnType(i), schema->IsConstant(i),
                            schema->IsColumnNotNull(i));
        }
        return ss;
    }

 private:
    std::unique_ptr<sdk::SQLRouter> sql_router_;
    InterfaceProvider provider_;
};

struct Column {
    std::string name;
    hybridse::sdk::DataType type = hybridse::sdk::kTypeUnknow;
    bool is_constant = false;
    bool is_null = false;
    Column() = default;
    Column(std::string n, hybridse::sdk::DataType t) : name(std::move(n)), type(t) {}
    Column(std::string n, hybridse::sdk::DataType t, bool c, bool is_null)
        : name(std::move(n)), type(t), is_constant(c), is_null(is_null) {}
};

template <typename Archiver>
Archiver& operator&(Archiver& ar, Column& s) {  // NOLINT
    ar.StartObject();
    ar.Member("name") & s.name;
    ar.Member("type") & s.type;
    return ar.EndObject();
}

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

}  // namespace http
}  // namespace fedb

#endif  // SRC_APISERVER_API_SERVICE_IMPL_H_
