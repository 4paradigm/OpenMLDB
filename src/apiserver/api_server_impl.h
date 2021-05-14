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

#include <utility>

#include "interface_provider.h"
#include "json2pb/rapidjson.h"  // rapidjson's DOM-style API
#include "proto/http.pb.h"
#include "sdk/sql_cluster_router.h"

DECLARE_string(zk_cluster);
DECLARE_string(zk_root_path);

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
    static bool Json2SQLRequestRow(const butil::rapidjson::Value& input, const butil::rapidjson::Value& common_cols_v,
                                   std::shared_ptr<fedb::sdk::SQLRequestRow> row);
    static bool AppendJsonValue(const butil::rapidjson::Value& v, hybridse::sdk::DataType type,
                                std::shared_ptr<fedb::sdk::SQLRequestRow> row);
    void RegisterPut();
    void RegisterExecSP();
    void RegisterGetSP();

    static SimpleSchema TransToSimpleSchema(const hybridse::sdk::Schema* schema) {
        SimpleSchema ss;
        for (int i = 0; i < schema->GetColumnCnt(); ++i) {
            ss.emplace_back(schema->GetColumnName(i), schema->GetColumnType(i), schema->IsConstant(i),
                            schema->IsColumnNotNull(i));
        }
        return std::move(ss);
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
    Column(std::string n, hybridse::sdk::DataType t, bool c, bool null)
        : name(std::move(n)), type(t), is_constant(c), is_null(null) {}
};

template <typename Archiver>
Archiver& operator&(Archiver& ar, Column& s) {
    ar.StartObject();
    ar.Member(s.name.c_str()) & hybridse::sdk::DataTypeName(s.type);
    return ar.EndObject();
}

struct PutResp {
    PutResp() = default;
    int code = 0;
    std::string msg = "ok";
};

template <typename Archiver>
Archiver& operator&(Archiver& ar, PutResp& s) {
    ar.StartObject();
    ar.Member("code") & s.code;
    ar.Member("msg") & s.msg;
    return ar.EndObject();
}

struct ExecSPResp {
    ExecSPResp() = default;
    int code = 0;
    std::string msg = "ok";
    struct Data {
        SimpleSchema schema;
        std::shared_ptr<hybridse::sdk::ResultSet> rs;
    };
    Data data;
};

template <typename Archiver, typename Type>
void WriteArray(Archiver& ar, const std::string& name, std::vector<Type>& vec) {
    ar.Member(name.c_str());
    size_t count = vec.size();
    ar.StartArray();
    for (size_t i = 0; i < count; i++) {
        ar& vec[i];
    }
    ar.EndArray();
}

template <typename Archiver>
void WriteValue(Archiver& ar, std::shared_ptr<hybridse::sdk::ResultSet> rs, SimpleSchema& schema, int i) {
    if (rs->IsNULL(i)) {
        if (!schema[i].is_null) {
            LOG(ERROR) << "mismatch";
        }
        ar.SetNull();
        return;
    }
    switch (schema[i].type) {
        case hybridse::sdk::kTypeInt32: {
            auto v = rs->GetInt32Unsafe(i);
            ar& v;
            break;
        }
        case hybridse::sdk::kTypeInt64: {
            auto v = rs->GetInt64Unsafe(i);
            ar& v;
            break;
        }
        case hybridse::sdk::kTypeInt16: {
            auto v = static_cast<int>(rs->GetInt16Unsafe(i));
            ar& v;
            break;
        }
        case hybridse::sdk::kTypeFloat: {
            auto v = static_cast<double>(rs->GetFloatUnsafe(i));
            ar& v;
            break;
        }
        case hybridse::sdk::kTypeDouble: {
            auto v = rs->GetDoubleUnsafe(i);
            ar& v;
            break;
        }
        case hybridse::sdk::kTypeString: {
            auto v = rs->GetStringUnsafe(i);
            ar& v;
            break;
        }
        case hybridse::sdk::kTypeTimestamp: {
            auto v = rs->GetTimeUnsafe(i);
            ar& v;
            break;
        }
        case hybridse::sdk::kTypeDate: {
            auto v = rs->GetDateUnsafe(i);
            ar& v;
            break;
        }
        case hybridse::sdk::kTypeBool: {
            auto v = rs->GetBoolUnsafe(i);
            ar& v;
            break;
        }
        default: {
            LOG(ERROR) << "Invalid Column Type";
            ar & "err";
            break;
        }
    }
}

template <typename Archiver>
Archiver& operator&(Archiver& ar, ExecSPResp& s) {
    ar.StartObject();
    ar.Member("code") & s.code;
    ar.Member("msg") & s.msg;

    ar.Member("data");  // start data
    ar.StartObject();
    auto& schema = s.data.schema;
    // data-schema
    WriteArray(ar, "schema", schema);

    // data-data: non common cols data
    ar.Member("data");
    ar.StartArray();
    auto& rs = s.data.rs;
    rs->Reset();
    while (rs->Next()) {
        ar.StartArray();
        for (decltype(schema.size()) i = 0; i < schema.size(); i++) {
            if (schema[i].is_constant) {
                continue;
            }
            LOG(INFO) << "get value in " << schema[i].name;
            WriteValue(ar, rs, schema, i);
        }
        ar.EndArray();  // one row end
    }
    ar.EndArray();

    // data-common_cols_data
    ar.Member("common_cols_data");
    rs->Reset();
    if (rs->Next()) {
        ar.StartArray();
        for (decltype(schema.size()) i = 0; i < schema.size(); i++) {
            if (schema[i].is_constant) {
                LOG(INFO) << "get value in " << schema[i].name;
                WriteValue(ar, rs, schema, i);
            }
        }
        ar.EndArray();  // one row end
    }

    ar.EndObject();  // end data

    return ar.EndObject();
}

struct GetSPResp {
    GetSPResp() = default;
    int code = 0;
    std::string msg = "ok";
    struct Data {
        std::string name;
        std::string procedure;
        SimpleSchema input_schema;
        std::vector<std::string> input_common_cols;
        SimpleSchema output_schema;
        std::vector<std::string> output_common_cols;
        std::vector<std::string> tables;
    };
    Data data;
};

template <typename Archiver>
Archiver& operator&(Archiver& ar, GetSPResp& s) {
    ar.StartObject();
    ar.Member("code") & s.code;
    ar.Member("msg") & s.msg;

    ar.Member("data");  // start data
    ar.StartObject();

    ar.Member("name") & s.data.name;
    ar.Member("procedure") & s.data.procedure;

    // data-input_schema
    WriteArray(ar, "input_schema", s.data.input_schema);
    WriteArray(ar, "input_common_cols", s.data.input_common_cols);
    WriteArray(ar, "output_schema", s.data.output_schema);
    WriteArray(ar, "output_common_cols", s.data.output_common_cols);
    WriteArray(ar, "tables", s.data.tables);

    ar.EndObject();  // end data

    return ar.EndObject();
}

}  // namespace http
}  // namespace fedb

#endif  // SRC_APISERVER_API_SERVER_IMPL_H_