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

#include <memory>
#include <set>
#include <string>

#include "apiserver/interface_provider.h"
#include "brpc/server.h"

namespace openmldb {
namespace apiserver {

APIServerImpl::~APIServerImpl() = default;

bool APIServerImpl::Init(const sdk::ClusterOptions& options) {
    // If cluster sdk is needed, use ptr, don't own it. SQLClusterRouter owns it.
    auto cluster_sdk = new ::openmldb::sdk::ClusterSDK(options);
    bool ok = cluster_sdk->Init();
    if (!ok) {
        LOG(ERROR) << "Fail to connect to db";
        return false;
    }
    return Init(cluster_sdk);
}

bool APIServerImpl::Init(::openmldb::sdk::DBSDK* cluster) {
    // If cluster sdk is needed, use ptr, don't own it. SQLClusterRouter owns it.
    cluster_sdk_ = cluster;
    auto router = std::make_shared<::openmldb::sdk::SQLClusterRouter>(cluster_sdk_);
    if (!router->Init()) {
        LOG(ERROR) << "Fail to connect to db";
        return false;
    }
    sql_router_ = std::move(router);
    RegisterPut();
    RegisterExecSP();
    RegisterExecDeployment();
    RegisterGetSP();
    RegisterGetDeployment();
    RegisterGetDB();
    RegisterGetTable();
    return true;
}

void APIServerImpl::Refresh(google::protobuf::RpcController* cntl_base, const HttpRequest*, HttpResponse*,
                            google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    if (sql_router_) {
        sql_router_->RefreshCatalog();
    }
}

void APIServerImpl::Process(google::protobuf::RpcController* cntl_base, const HttpRequest*, HttpResponse*,
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

bool APIServerImpl::Json2SQLRequestRow(const butil::rapidjson::Value& non_common_cols_v,
                                       const butil::rapidjson::Value& common_cols_v,
                                       std::shared_ptr<openmldb::sdk::SQLRequestRow> row) {
    auto sch = row->GetSchema();

    // scan all strings to init the total string length
    decltype(common_cols_v.Size()) str_len_sum = 0;
    decltype(common_cols_v.Size()) non_common_idx = 0, common_idx = 0;
    for (decltype(sch->GetColumnCnt()) i = 0; i < sch->GetColumnCnt(); ++i) {
        // if element is null, GetStringLength() will get 0
        if (sch->IsConstant(i)) {
            if (sch->GetColumnType(i) == hybridse::sdk::kTypeString) {
                str_len_sum += common_cols_v[common_idx].GetStringLength();
            }
            ++common_idx;
        } else {
            if (sch->GetColumnType(i) == hybridse::sdk::kTypeString) {
                str_len_sum += non_common_cols_v[non_common_idx].GetStringLength();
            }
            ++non_common_idx;
        }
    }
    row->Init(static_cast<int32_t>(str_len_sum));

    non_common_idx = 0, common_idx = 0;
    for (decltype(sch->GetColumnCnt()) i = 0; i < sch->GetColumnCnt(); ++i) {
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
bool APIServerImpl::AppendJsonValue(const butil::rapidjson::Value& v, hybridse::sdk::DataType type, bool is_not_null,
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
            if (!v.IsBool()) {
                return false;
            }
            return row->AppendBool(v.GetBool());
        }
        case hybridse::sdk::kTypeInt16: {
            if (!v.IsInt()) {
                return false;
            }
            return row->AppendInt16(boost::lexical_cast<int16_t>(v.GetInt()));
        }
        case hybridse::sdk::kTypeInt32: {
            if (!v.IsInt()) {
                return false;
            }
            return row->AppendInt32(v.GetInt());
        }
        case hybridse::sdk::kTypeInt64: {
            if (!v.IsInt64()) {
                return false;
            }
            return row->AppendInt64(v.GetInt64());
        }
        case hybridse::sdk::kTypeFloat: {
            if (!v.IsDouble()) {
                return false;
            }
            return row->AppendFloat(boost::lexical_cast<float>(v.GetDouble()));
        }
        case hybridse::sdk::kTypeDouble: {
            if (!v.IsDouble()) {
                return false;
            }
            return row->AppendDouble(v.GetDouble());
        }
        case hybridse::sdk::kTypeString: {
            if (!v.IsString()) {
                return false;
            }
            return row->AppendString(v.GetString(), v.GetStringLength());
        }
        case hybridse::sdk::kTypeDate: {
            if (!v.IsString()) {
                return false;
            }
            std::vector<std::string> parts;
            ::openmldb::base::SplitString(v.GetString(), "-", parts);
            if (parts.size() != 3) {
                return false;
            }
            auto year = boost::lexical_cast<int32_t>(parts[0]);
            auto mon = boost::lexical_cast<int32_t>(parts[1]);
            auto day = boost::lexical_cast<int32_t>(parts[2]);
            return row->AppendDate(year, mon, day);
        }
        case hybridse::sdk::kTypeTimestamp: {
            if (!v.IsInt64()) {
                return false;
            }
            return row->AppendTimestamp(v.GetInt64());
        }
        default:
            return false;
    }
}

void APIServerImpl::RegisterPut() {
    provider_.put("/dbs/:db_name/tables/:table_name", [this](const InterfaceProvider::Params& param,
                                                             const butil::IOBuf& req_body, JsonWriter& writer) {
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
            DLOG(INFO) << "rapidjson doc parse [" << req_body.to_string().c_str() << "] failed, code "
                       << document.GetParseError() << ", offset " << document.GetErrorOffset();
            writer << err.Set("Json parse failed, error code: " + std::to_string(document.GetParseError()));
            return;
        }

        const auto& value = document["value"];
        // value should be an array, and multi put is not supported now
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
            // if null, GetStringLength() will get 0
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

void APIServerImpl::RegisterExecDeployment() {
    provider_.post("/dbs/:db_name/deployments/:sp_name", std::bind(&APIServerImpl::ExecuteProcedure, this,
                false, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3));
}

void APIServerImpl::RegisterExecSP() {
    provider_.post("/dbs/:db_name/procedures/:sp_name", std::bind(&APIServerImpl::ExecuteProcedure, this,
                true, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3));
}

void APIServerImpl::ExecuteProcedure(bool has_common_col, const InterfaceProvider::Params& param,
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

    butil::rapidjson::Value common_cols_v;
    if (has_common_col) {
        auto common_cols = document.FindMember("common_cols");
        if (common_cols != document.MemberEnd()) {
            common_cols_v = common_cols->value;  // move
            if (!common_cols_v.IsArray()) {
                writer << err.Set("common_cols is not array");
                return;
            }
        } else {
            common_cols_v.SetArray();  // If there's no common cols, no need to add this field in request
        }
    } else {
        common_cols_v.SetArray();
    }

    auto input = document.FindMember("input");
    if (input == document.MemberEnd() || !input->value.IsArray() || input->value.Empty()) {
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
    auto common_column_indices = std::make_shared<openmldb::sdk::ColumnIndicesSet>(input_schema);
    decltype(common_cols_v.Size()) expected_common_size = 0;
    if (has_common_col) {
        for (int i = 0; i < input_schema->GetColumnCnt(); ++i) {
            if (input_schema->IsConstant(i)) {
                common_column_indices->AddCommonColumnIdx(i);
                ++expected_common_size;
            }
        }
        if (common_cols_v.Size() != expected_common_size) {
            writer << err.Set("Invalid common cols size");
            return;
        }
    }
    auto expected_input_size = input_schema->GetColumnCnt() - expected_common_size;

    // TODO(hw): SQLRequestRowBatch should add common & non-common cols directly
    auto row_batch = std::make_shared<sdk::SQLRequestRowBatch>(input_schema, common_column_indices);
    std::set<std::string> col_set;
    for (decltype(rows.Size()) i = 0; i < rows.Size(); ++i) {
        if (!rows[i].IsArray() || rows[i].Size() != expected_input_size) {
            writer << err.Set("Invalid input data row");
            return;
        }
        auto row = std::make_shared<sdk::SQLRequestRow>(input_schema, col_set);

        // sizes have been checked
        if (!Json2SQLRequestRow(rows[i], common_cols_v, row)) {
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
    // output schema in sp_info is needed for encoding data, so we need a bool in ExecSPResp to know whether to
    // print schema
    resp.sp_info = sp_info;
    if (document.HasMember("need_schema") && document["need_schema"].IsBool() &&
        document["need_schema"].GetBool()) {
        resp.need_schema = true;
    }
    resp.rs = rs;
    writer << resp;
}

void APIServerImpl::RegisterGetSP() {
    provider_.get("/dbs/:db_name/procedures/:sp_name",
                  [this](const InterfaceProvider::Params& param, const butil::IOBuf& req_body, JsonWriter& writer) {
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
                      if (sp_info->GetType() == ::hybridse::sdk::ProcedureType::kReqProcedure) {
                          GetSPResp resp;
                          resp.sp_info = sp_info;
                          writer << resp;
                      } else {
                          writer << err.Set("procedure not found");
                      }
                  });
}

void APIServerImpl::RegisterGetDeployment() {
    provider_.get("/dbs/:db_name/deployments/:dep_name",
                  [this](const InterfaceProvider::Params& param, const butil::IOBuf& req_body, JsonWriter& writer) {
                      auto err = GeneralError();
                      auto db_it = param.find("db_name");
                      auto sp_it = param.find("dep_name");
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
                      if (sp_info->GetType() == ::hybridse::sdk::ProcedureType::kReqDeployment) {
                          GetSPResp resp;
                          resp.sp_info = sp_info;
                          writer << resp;
                      } else {
                          writer << err.Set("deployment not found");
                      }
                  });
}

void APIServerImpl::RegisterGetDB() {
    provider_.get("/dbs",
                  [this](const InterfaceProvider::Params& param, const butil::IOBuf& req_body, JsonWriter& writer) {
                      auto err = GeneralError();
                      std::vector<std::string> dbs;
                      hybridse::sdk::Status status;
                      auto ok = sql_router_->ShowDB(&dbs, &status);
                      if (!ok) {
                          writer << err.Set(status.msg);
                          return;
                      }
                      writer.StartObject();
                      writer.Member("code") & 0;
                      writer.Member("msg") & std::string("ok");
                      writer.Member("dbs");
                      writer.StartArray();
                      for (auto db : dbs) {
                          writer& db;
                      }
                      writer.EndArray();
                      writer.EndObject();
                  });
}

void APIServerImpl::RegisterGetTable() {
    // show all of the tables
    provider_.get("/dbs/:db_name/tables",
                  [this](const InterfaceProvider::Params& param, const butil::IOBuf& req_body, JsonWriter& writer) {
                      auto err = GeneralError();
                      auto db_it = param.find("db_name");
                      if (db_it == param.end()) {
                          writer << err.Set("Invalid path");
                          return;
                      }
                      std::vector<std::string> dbs;
                      hybridse::sdk::Status status;
                      auto ok = sql_router_->ShowDB(&dbs, &status);
                      if (!ok) {
                          writer << err.Set(status.msg);
                          return;
                      }
                      auto db = db_it->second;
                      bool db_ok = std::find(dbs.begin(), dbs.end(), db) != dbs.end();
                      if (!db_ok) {
                          writer << err.Set("DB not found");
                          return;
                      }
                      auto tables = cluster_sdk_->GetTables(db);
                      writer.StartObject();
                      writer.Member("code") & 0;
                      writer.Member("msg") & std::string("ok");
                      writer.Member("tables");
                      writer.StartArray();
                      for (std::shared_ptr<::openmldb::nameserver::TableInfo> table : tables) {
                          writer << table;
                      }
                      writer.EndArray();
                      writer.EndObject();
                  });
    // show a certain table
    provider_.get("/dbs/:db_name/tables/:table_name",
                  [this](const InterfaceProvider::Params& param, const butil::IOBuf& req_body, JsonWriter& writer) {
                      auto err = GeneralError();
                      auto db_it = param.find("db_name");
                      auto table_it = param.find("table_name");
                      if (db_it == param.end() || table_it == param.end()) {
                          writer << err.Set("Invalid path");
                          return;
                      }
                      std::vector<std::string> dbs;
                      hybridse::sdk::Status status;
                      auto ok = sql_router_->ShowDB(&dbs, &status);
                      if (!ok) {
                          writer << err.Set(status.msg);
                          return;
                      }
                      auto db = db_it->second;
                      bool db_ok = std::find(dbs.begin(), dbs.end(), db) != dbs.end();
                      if (!db_ok) {
                          writer << err.Set("DB not found");
                          return;
                      }
                      auto table = table_it->second;
                      auto table_info = cluster_sdk_->GetTableInfo(db, table);
                      // if there is no such db or such table, table_info will be nullptr
                      if (table_info == nullptr) {
                          writer << err.Set("Table not found");
                          return;
                      } else {
                          writer.StartObject();
                          writer.Member("code") & 0;
                          writer.Member("msg") & std::string("ok");
                          writer.Member("table") & table_info;
                          writer.EndObject();
                      }
                  });
}

std::string APIServerImpl::InnerTypeTransform(const std::string& s) {
    std::string out = s;
    if (out.size() > 0 && out.at(0) == 'k') {
        out.erase(out.begin());
    }
    std::transform(out.begin(), out.end(), out.begin(), [](unsigned char c) { return std::tolower(c); });
    return out;
}

void WriteSchema(JsonWriter& ar, const std::string& name, const hybridse::sdk::Schema& schema,  // NOLINT
                 bool only_const) {
    ar.Member(name.c_str());
    ar.StartArray();
    for (decltype(schema.GetColumnCnt()) i = 0; i < schema.GetColumnCnt(); i++) {
        if (only_const) {
            if (!schema.IsConstant(i)) {
                continue;
            }
            // Only print name, no type
            ar& schema.GetColumnName(i);
        } else {
            ar.StartObject();
            ar.Member("name") & schema.GetColumnName(i);
            ar.Member("type") & DataTypeName(schema.GetColumnType(i));
            ar.EndObject();
        }
    }

    ar.EndArray();
}

void WriteValue(JsonWriter& ar, std::shared_ptr<hybridse::sdk::ResultSet> rs, int i) {  // NOLINT
    auto schema = rs->GetSchema();
    if (rs->IsNULL(i)) {
        if (schema->IsColumnNotNull(i)) {
            LOG(ERROR) << "Value in " << schema->GetColumnName(i) << " is null but it can't be null";
        }
        ar.SetNull();
        return;
    }
    switch (schema->GetColumnType(i)) {
        case hybridse::sdk::kTypeInt32: {
            int32_t value = 0;
            rs->GetInt32(i, &value);
            ar& value;
            break;
        }
        case hybridse::sdk::kTypeInt64: {
            int64_t value = 0;
            rs->GetInt64(i, &value);
            ar& value;
            break;
        }
        case hybridse::sdk::kTypeInt16: {
            int16_t value = 0;
            rs->GetInt16(i, &value);
            ar& static_cast<int>(value);
            break;
        }
        case hybridse::sdk::kTypeFloat: {
            float value = 0;
            rs->GetFloat(i, &value);
            ar& static_cast<double>(value);
            break;
        }
        case hybridse::sdk::kTypeDouble: {
            double value = 0;
            rs->GetDouble(i, &value);
            ar& value;
            break;
        }
        case hybridse::sdk::kTypeString: {
            std::string val;
            rs->GetString(i, &val);
            ar& val;
            break;
        }
        case hybridse::sdk::kTypeTimestamp: {
            int64_t ts = 0;
            rs->GetTime(i, &ts);
            ar& ts;
            break;
        }
        case hybridse::sdk::kTypeDate: {
            int32_t year = 0;
            int32_t month = 0;
            int32_t day = 0;
            std::stringstream ss;
            rs->GetDate(i, &year, &month, &day);
            ss << year << "-" << month << "-" << day;
            ar& ss.str();
            break;
        }
        case hybridse::sdk::kTypeBool: {
            bool value = false;
            rs->GetBool(i, &value);
            ar&(value ? "true" : "false");
            break;
        }
        default: {
            LOG(ERROR) << "Invalid Column Type";
            ar & "NA";
            break;
        }
    }
}

// ExecSPResp reading is unsupported now, cuz we decode ResultSet with Schema here, it's irreversible
JsonWriter& operator&(JsonWriter& ar, ExecSPResp& s) {  // NOLINT
    ar.StartObject();
    ar.Member("code") & s.code;
    ar.Member("msg") & s.msg;

    ar.Member("data");  // start data
    ar.StartObject();

    // data-schema
    auto& schema = s.sp_info->GetOutputSchema();
    if (s.need_schema) {
        WriteSchema(ar, "schema", schema, false);
    }

    // data-data: non common cols data
    ar.Member("data");
    ar.StartArray();
    auto& rs = s.rs;
    rs->Reset();
    while (rs->Next()) {
        ar.StartArray();
        for (decltype(schema.GetColumnCnt()) i = 0; i < schema.GetColumnCnt(); i++) {
            if (!schema.IsConstant(i)) {
                WriteValue(ar, rs, i);
            }
        }
        ar.EndArray();  // one row end
    }
    ar.EndArray();

    // data-common_cols_data : only Procedure will return common_cols_data
    if (s.sp_info->GetType() == hybridse::sdk::kReqProcedure) {
        ar.Member("common_cols_data");
        rs->Reset();
        if (rs->Next()) {
            ar.StartArray();
            for (decltype(schema.GetColumnCnt()) i = 0; i < schema.GetColumnCnt(); i++) {
                if (schema.IsConstant(i)) {
                    WriteValue(ar, rs, i);
                }
            }
            ar.EndArray();  // one row end
        }
    }

    ar.EndObject();  // end data

    return ar.EndObject();
}

JsonWriter& operator&(JsonWriter& ar, std::shared_ptr<hybridse::sdk::ProcedureInfo> sp_info) {  // NOLINT
    ar.StartObject();
    ar.Member("name") & sp_info->GetSpName();
    ar.Member("procedure") & sp_info->GetSql();

    WriteSchema(ar, "input_schema", sp_info->GetInputSchema(), false);
    WriteSchema(ar, "input_common_cols", sp_info->GetInputSchema(), true);
    WriteSchema(ar, "output_schema", sp_info->GetOutputSchema(), false);
    WriteSchema(ar, "output_common_cols", sp_info->GetOutputSchema(), true);

    // Write db names
    ar.Member("dbs");
    auto dbs = sp_info->GetDbs();
    ar.StartArray();
    for (auto& db : dbs) {
        ar& db;
    }
    ar.EndArray();

    // Write table names
    ar.Member("tables");
    auto tables = sp_info->GetTables();
    ar.StartArray();
    for (auto& table : tables) {
        ar& table;
    }
    ar.EndArray();

    return ar.EndObject();
}

// ExecSPResp reading is unsupported now, cuz we decode sp_info here, it's irreversible
JsonWriter& operator&(JsonWriter& ar, GetSPResp& s) {  // NOLINT
    ar.StartObject();
    ar.Member("code") & s.code;
    ar.Member("msg") & s.msg;
    ar.Member("data") & s.sp_info;
    return ar.EndObject();
}

JsonWriter& operator&(JsonWriter& ar,  // NOLINT
                      const ::google::protobuf::RepeatedPtrField<::openmldb::common::ColumnDesc>& column_desc) {
    ar.StartArray();
    for (auto column : column_desc) {
        ar.StartObject();
        if (column.has_name()) {
            ar.Member("name") & column.name();
        }
        if (column.has_data_type()) {
            ar.Member("data_type") & ::openmldb::type::DataType_Name(column.data_type());
        }
        if (column.has_not_null()) {
            ar.Member("not_null") & column.not_null();
        }
        if (column.has_is_constant()) {
            ar.Member("is_constant") & column.is_constant();
        }
        ar.EndObject();
    }
    return ar.EndArray();
}

JsonWriter& operator&(JsonWriter& ar,  // NOLINT
                      const ::google::protobuf::RepeatedPtrField<::openmldb::common::ColumnKey>& column_key) {
    ar.StartArray();
    for (auto key : column_key) {
        ar.StartObject();
        if (key.has_index_name()) {
            ar.Member("index_name") & key.index_name();
        }
        ar.Member("col_name");
        ar.StartArray();
        for (auto col : key.col_name()) {
            ar& col;
        }
        ar.EndArray();
        if (key.has_ts_name()) {
            ar.Member("ts_name") & key.ts_name();
        }
        if (key.has_flag()) {
            ar.Member("flag") & key.flag();
        }

        if (key.has_ttl()) {
            ar.Member("ttl");
            auto& ttl = key.ttl();
            ar.StartObject();
            if (ttl.has_ttl_type()) {
                switch (ttl.ttl_type()) {
                    case ::openmldb::type::TTLType::kAbsoluteTime:
                        ar.Member("ttl_type") & std::string("absolute");
                        break;
                    case ::openmldb::type::TTLType::kLatestTime:
                        ar.Member("ttl_type") & std::string("latest");
                        break;
                    case ::openmldb::type::TTLType::kAbsAndLat:
                        ar.Member("ttl_type") & std::string("absandlat");
                        break;
                    case ::openmldb::type::TTLType::kAbsOrLat:
                        ar.Member("ttl_type") & std::string("absorlat");
                        break;
                    default:
                        break;
                }
            }
            if (ttl.has_abs_ttl()) {
                ar.Member("abs_ttl") & ttl.abs_ttl();
            }
            if (ttl.has_lat_ttl()) {
                ar.Member("lat_ttl") & ttl.lat_ttl();
            }
            ar.EndObject();
        }
        ar.EndObject();
    }
    return ar.EndArray();
}

JsonWriter& operator&(JsonWriter& ar,  // NOLINT
                      const ::google::protobuf::RepeatedPtrField<::openmldb::common::VersionPair>& schema_versions) {
    ar.StartArray();
    for (auto version : schema_versions) {
        ar.StartObject();
        if (version.has_id()) {
            ar.Member("id") & version.id();
        }
        if (version.has_field_count()) {
            ar.Member("field_count") & version.field_count();
        }
        ar.EndObject();
    }
    return ar.EndArray();
}

JsonWriter& operator&(JsonWriter& ar, std::shared_ptr<::openmldb::nameserver::TableInfo> info) {  // NOLINT
    ar.StartObject();
    if (info->has_name()) {
        ar.Member("name") & info->name();
    }
    if (info->has_seg_cnt()) {
        ar.Member("seg_cnt") & info->seg_cnt();
    }
    ar.Member("table_partition_size") & info->table_partition_size();
    if (info->has_tid()) {
        ar.Member("tid") & info->tid();
    }
    if (info->has_partition_num()) {
        ar.Member("partition_num") & info->partition_num();
    }
    if (info->has_replica_num()) {
        ar.Member("replica_num") & info->replica_num();
    }
    if (info->has_compress_type()) {
        ar.Member("compress_type") &
            APIServerImpl::InnerTypeTransform(::openmldb::type::CompressType_Name(info->compress_type()));
    }
    if (info->has_key_entry_max_height()) {
        ar.Member("key_entry_max_height") & info->key_entry_max_height();
    }

    ar.Member("column_desc") & info->column_desc();

    ar.Member("column_key") & info->column_key();

    ar.Member("added_column_desc") & info->added_column_desc();

    if (info->has_format_version()) {
        ar.Member("format_version") & info->format_version();
    }
    if (info->has_db()) {
        ar.Member("db") & info->db();
    }
    ar.Member("partition_key");
    ar.StartArray();
    for (auto key : info->partition_key()) {
        ar& key;
    }
    ar.EndArray();

    ar.Member("schema_versions") & info->schema_versions();

    return ar.EndObject();
}

}  // namespace apiserver
}  // namespace openmldb
