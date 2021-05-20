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

#include "apiserver/api_service_impl.h"

#include <memory>
#include <set>
#include <string>

#include "apiserver/interface_provider.h"
#include "apiserver/json_helper.h"
#include "brpc/server.h"

namespace fedb {
namespace http {

APIServiceImpl::~APIServiceImpl() = default;

bool APIServiceImpl::Init(const sdk::ClusterOptions& options) {
    // If cluster sdk is needed, use ptr, don't own it. SQLClusterRouter owns it.
    auto cluster_sdk = new ::fedb::sdk::ClusterSDK(options);
    bool ok = cluster_sdk->Init();
    if (!ok) {
        LOG(ERROR) << "Fail to connect to db";
        return false;
    }

    auto router = std::make_unique<::fedb::sdk::SQLClusterRouter>(cluster_sdk);
    if (!router->Init()) {
        LOG(ERROR) << "Fail to connect to db";
        return false;
    }
    sql_router_ = std::move(router);

    RegisterPut();
    RegisterExecSP();
    RegisterGetSP();

    return true;
}

void APIServiceImpl::Process(google::protobuf::RpcController* cntl_base, const HttpRequest*, HttpResponse*,
                             google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    auto* cntl = dynamic_cast<brpc::Controller*>(cntl_base);

    // The unresolved path has no slashes at the beginning(guaranteed by brpc), it's not good for url parsing
    auto unresolved_path = "/" + cntl->http_request().unresolved_path();
    auto method = cntl->http_request().method();
    DLOG(INFO) << "unresolved path: " << unresolved_path << ", method: " << HttpMethod2Str(method);
    const butil::IOBuf& req_body = cntl->request_attachment();

    JsonWriter writer;
    provider_.handle(unresolved_path, method, req_body, writer);

    cntl->response_attachment().append(writer.GetString());
}

bool APIServiceImpl::Json2SQLRequestRow(const butil::rapidjson::Value& non_common_cols_v,
                                        const butil::rapidjson::Value& common_cols_v,
                                        std::shared_ptr<fedb::sdk::SQLRequestRow> row) {
    auto sch = row->GetSchema();

    // scan all strings to init the total string length
    decltype(common_cols_v.Size()) str_len_sum = 0;
    decltype(common_cols_v.Size()) non_common_idx = 0, common_idx = 0;
    for (decltype(sch->GetColumnCnt()) i = 0; i < sch->GetColumnCnt(); ++i) {
        if (sch->GetColumnType(i) != hybridse::sdk::kTypeString) {
            continue;
        }
        if (sch->IsConstant(i)) {
            if (common_idx >= common_cols_v.Size()) {
                return false;
            }
            str_len_sum += common_cols_v[common_idx].GetStringLength();
            ++common_idx;
        } else {
            if (non_common_idx >= non_common_cols_v.Size()) {
                return false;
            }
            str_len_sum += non_common_cols_v[non_common_idx].GetStringLength();
            ++non_common_idx;
        }
    }
    row->Init(static_cast<int32_t>(str_len_sum));

    non_common_idx = 0, common_idx = 0;
    for (decltype(sch->GetColumnCnt()) i = 0; i < sch->GetColumnCnt(); ++i) {
        // TODO(hw): no need to append common cols
        if (sch->IsConstant(i)) {
            if (!AppendJsonValue(common_cols_v[common_idx], sch->GetColumnType(i), sch->IsColumnNotNull(i), row)) {
                return false;
            }
            ++common_idx;
        } else {
            if (!AppendJsonValue(non_common_cols_v[non_common_idx], sch->GetColumnType(i), sch->IsColumnNotNull(i),
                                 row)) {
                return false;
            }
            ++non_common_idx;
        }
    }
    return true;
}

template <typename T>
bool APIServiceImpl::AppendJsonValue(const butil::rapidjson::Value& v, hybridse::sdk::DataType type, bool is_not_null,
                                     T row) {
    // check if null
    if (v.IsNull()) {
        if (is_not_null) {
            return false;
        }
        return row->AppendNULL();
    }

    switch (type) {
        case hybridse::sdk::kTypeBool: {
            return row->AppendBool(v.GetBool());
        }
        case hybridse::sdk::kTypeInt16: {
            return row->AppendInt16(boost::lexical_cast<int16_t>(v.GetInt()));
        }
        case hybridse::sdk::kTypeInt32: {
            return row->AppendInt32(v.GetInt());
        }
        case hybridse::sdk::kTypeInt64: {
            return row->AppendInt64(v.GetInt64());
        }
        case hybridse::sdk::kTypeFloat: {
            return row->AppendFloat(boost::lexical_cast<float>(v.GetDouble()));
        }
        case hybridse::sdk::kTypeDouble: {
            return row->AppendDouble(v.GetDouble());
        }
        case hybridse::sdk::kTypeString: {
            return row->AppendString(v.GetString(), v.GetStringLength());
        }
        case hybridse::sdk::kTypeDate: {
            std::vector<std::string> parts;
            ::fedb::base::SplitString(v.GetString(), "-", parts);
            if (parts.size() != 3) {
                return false;
            }
            auto year = boost::lexical_cast<int32_t>(parts[0]);
            auto mon = boost::lexical_cast<int32_t>(parts[1]);
            auto day = boost::lexical_cast<int32_t>(parts[2]);
            return row->AppendDate(year, mon, day);
        }
        case hybridse::sdk::kTypeTimestamp: {
            return row->AppendTimestamp(v.GetInt64());
        }
        default:
            return false;
    }
}

void APIServiceImpl::RegisterPut() {
    provider_.put("/dbs/:db_name/tables/:table_name",
                  [this](const InterfaceProvider::Params& param, const butil::IOBuf& req_body, JsonWriter& writer) {
                      auto err = GeneralError();
                      auto db_it = param.find("db_name");
                      auto table_it = param.find("table_name");
                      if (db_it == param.end() || table_it == param.end()) {
                          writer << err.Set("Invalid path");
                          return;
                      }
                      auto db = db_it->second;
                      auto table = table_it->second;

                      // json2doc, then generate an insert sql
                      Document document;
                      if (document.Parse(req_body.to_string().c_str()).HasParseError()) {
                          writer << err.Set("Json parse failed");
                          return;
                      }

                      const auto& value = document["value"];
                      // value should be array, and multi put is not supported now
                      if (!value.IsArray() || value.Empty() || value.Size() > 1 || !value[0].IsArray()) {
                          writer << err.Set("Invalid value in body, only support to put one row");
                          return;
                      }
                      const auto& arr = value[0];
                      std::string holders;
                      for (decltype(arr.Size()) i = 0; i < arr.Size(); ++i) {
                          holders += ((i == 0) ? "?" : ",?");
                      }
                      hybridse::sdk::Status status;
                      std::string insert_placeholder = "insert into " + table + " values(" + holders + ");";
                      auto row = sql_router_->GetInsertRow(db, insert_placeholder, &status);
                      if (!row) {
                          writer << err.Set(status.msg);
                          return;
                      }
                      auto schema = row->GetSchema();
                      auto cnt = schema->GetColumnCnt();
                      if (cnt != static_cast<int>(arr.Size())) {
                          writer << err.Set("column size != schema size");
                          return;
                      }

                      // scan all strings , calc the sum, to init SQLInsertRow's string length
                      decltype(arr.Size()) str_len_sum = 0;
                      for (int i = 0; i < cnt; ++i) {
                          if (schema->GetColumnType(i) == hybridse::sdk::kTypeString) {
                              str_len_sum += arr[i].GetStringLength();
                          }
                      }
                      row->Init(static_cast<int>(str_len_sum));

                      for (int i = 0; i < cnt; ++i) {
                          if (!AppendJsonValue(arr[i], schema->GetColumnType(i), schema->IsColumnNotNull(i), row)) {
                              writer << err.Set("Translate to insert row failed");
                              return;
                          }
                      }

                      auto ok = sql_router_->ExecuteInsert(db, insert_placeholder, row, &status);
                      if (ok) {
                          PutResp resp;
                          writer << resp;
                      } else {
                          writer << err.Set(status.msg);
                      }
                  });
}

void APIServiceImpl::RegisterExecSP() {
    provider_.post("/dbs/:db_name/procedures/:sp_name", [this](const InterfaceProvider::Params& param,
                                                               const butil::IOBuf& req_body, JsonWriter& writer) {
        auto err = GeneralError();
        auto db_it = param.find("db_name");
        auto sp_it = param.find("sp_name");
        if (db_it == param.end() || sp_it == param.end()) {
            writer << err.Set("Invalid path");
            return;
        }
        auto db = db_it->second;
        auto sp = sp_it->second;

        Document document;
        if (document.Parse(req_body.to_string().c_str()).HasParseError()) {
            writer << err.Set("Json parse failed");
            return;
        }
        auto common_cols_v = document.FindMember("common_cols");
        if (common_cols_v == document.MemberEnd()) {
            writer << err.Set("No member common_cols");
            return;
        }
        auto input = document.FindMember("input");
        if (input == document.MemberEnd() || !input->value.IsArray()) {
            writer << err.Set("Invalid input");
            return;
        }
        const auto& rows = input->value;

        hybridse::sdk::Status status;
        // We need to use ShowProcedure to get input schema(should know which column is constant).
        // GetRequestRowByProcedure can't do that.
        auto sp_info = sql_router_->ShowProcedure(db, sp, &status);
        if (!sp_info) {
            writer << err.Set(status.msg);
            return;
        }

        const auto& schema_impl = dynamic_cast<const ::hybridse::sdk::SchemaImpl&>(sp_info->GetInputSchema());
        // Hard copy, and RequestRow needs shared schema
        auto input_schema = std::make_shared<::hybridse::sdk::SchemaImpl>(schema_impl.GetSchema());
        auto common_column_indices = std::make_shared<fedb::sdk::ColumnIndicesSet>(input_schema);
        for (int i = 0; i < input_schema->GetColumnCnt(); ++i) {
            if (input_schema->IsConstant(i)) {
                common_column_indices->AddCommonColumnIdx(i);
            }
        }
        auto row_batch = std::make_shared<sdk::SQLRequestRowBatch>(input_schema, common_column_indices);
        std::set<std::string> col_set;
        for (decltype(rows.Size()) i = 0; i < rows.Size(); ++i) {
            if (!rows[i].IsArray()) {
                writer << err.Set("Invalid input data row");
                return;
            }
            auto row = std::make_shared<sdk::SQLRequestRow>(input_schema, col_set);

            if (!Json2SQLRequestRow(rows[i], common_cols_v->value, row)) {
                writer << err.Set("Translate to request row failed");
                return;
            }
            row->Build();
            row_batch->AddRow(row);
        }

        auto rs = sql_router_->CallSQLBatchRequestProcedure(db, sp, row_batch, &status);
        if (!rs) {
            writer << err.Set(status.msg);
            return;
        }

        ExecSPResp resp;

        // use output schema to encode data
        auto& output_schema = sp_info->GetOutputSchema();
        auto output_simple_schema = TransToSimpleSchema(&output_schema);

        if (document.HasMember("need_schema") && document["need_schema"].IsBool() &&
            document["need_schema"].GetBool()) {
            resp.data.schema = output_simple_schema;
        }

        resp.data.rs = rs;
        writer << resp;
    });
}

void APIServiceImpl::RegisterGetSP() {
    provider_.get("/dbs/:db_name/procedures/:sp_name", [this](const InterfaceProvider::Params& param,
                                                              const butil::IOBuf& req_body, JsonWriter& writer) {
        auto err = GeneralError();
        auto db_it = param.find("db_name");
        auto sp_it = param.find("sp_name");
        if (db_it == param.end() || sp_it == param.end()) {
            writer << err.Set("Invalid path");
            return;
        }
        auto db = db_it->second;
        auto sp = sp_it->second;

        hybridse::sdk::Status status;
        auto sp_info = sql_router_->ShowProcedure(db, sp, &status);
        if (!sp_info) {
            writer << err.Set(status.msg);
            return;
        }

        GetSPResp resp;
        resp.data.name = sp_info->GetSpName();
        resp.data.procedure = sp_info->GetSql();
        auto& input_schema = sp_info->GetInputSchema();
        resp.data.input_schema = TransToSimpleSchema(&input_schema);
        std::for_each(resp.data.input_schema.begin(), resp.data.input_schema.end(), [&resp](const Column& c) {
            if (c.is_constant) {
                resp.data.input_common_cols.emplace_back(c.name);
            }
        });
        auto& output_schema = sp_info->GetOutputSchema();
        resp.data.output_schema = TransToSimpleSchema(&output_schema);
        std::for_each(resp.data.output_schema.begin(), resp.data.output_schema.end(), [&resp](const Column& c) {
            if (c.is_constant) {
                resp.data.output_common_cols.emplace_back(c.name);
            }
        });
        resp.data.tables = sp_info->GetTables();
        writer << resp;
    });
}
}  // namespace http
}  // namespace fedb
