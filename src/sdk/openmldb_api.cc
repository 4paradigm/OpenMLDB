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

#include "sdk/openmldb_api.h"

#include <any>
#include <string>
#include <vector>

#include "base/texttable.h"
#include "sdk/request_row.h"
#include "sdk/sql_cluster_router.h"


OpenmldbHandler::OpenmldbHandler(std::string _zk_cluster, std::string _zk_path) {
    cluster_ = new openmldb::sdk::SQLRouterOptions;
    cluster_->zk_cluster = _zk_cluster;
    cluster_->zk_path = _zk_path;
    status_ = new hybridse::sdk::Status;
    router_ = new openmldb::sdk::SQLClusterRouter(*cluster_);
    router_->Init();
}

OpenmldbHandler::OpenmldbHandler(std::string _host, uint32_t _port) {
    standalone_ = new openmldb::sdk::StandaloneOptions;
    standalone_->host = _host;
    standalone_->port = _port;
    status_ = new hybridse::sdk::Status;
    router_ = new openmldb::sdk::SQLClusterRouter(*standalone_);
    router_->Init();
}

OpenmldbHandler::~OpenmldbHandler() {
    if (cluster_ != nullptr) delete cluster_;
    if (standalone_ != nullptr) delete standalone_;
    delete status_;
    delete router_;
}

ParameterRow::ParameterRow(const OpenmldbHandler* handler) : handler_(handler) {
    parameter_types_ = std::make_shared<hybridse::sdk::ColumnTypes>();
}

std::shared_ptr<openmldb::sdk::SQLRequestRow> ParameterRow::get_parameter_row() const {
    sql_parameter_row_ = ::openmldb::sdk::SQLRequestRow::CreateSQLRequestRowFromColumnTypes(parameter_types_);
    sql_parameter_row_->Init(str_length_);
    for (size_t i = 0; i < record_.size(); ++i) {
        auto type = parameter_types_->GetColumnType(i);
        switch (type) {
            case ::hybridse::sdk::kTypeBool:
                sql_parameter_row_->AppendBool(std::any_cast<bool>(record_[i]));
                break;
            case ::hybridse::sdk::kTypeInt16:
                sql_parameter_row_->AppendInt16(std::any_cast<int16_t>(record_[i]));
                break;
            case ::hybridse::sdk::kTypeInt32:
                sql_parameter_row_->AppendInt32(std::any_cast<int32_t>(record_[i]));
                break;
            case ::hybridse::sdk::kTypeInt64:
                sql_parameter_row_->AppendInt64(std::any_cast<int64_t>(record_[i]));
                break;
            case ::hybridse::sdk::kTypeTimestamp:
                sql_parameter_row_->AppendTimestamp(std::any_cast<int64_t>(record_[i]));
                break;
            case ::hybridse::sdk::kTypeDate:
                sql_parameter_row_->AppendDate(std::any_cast<int32_t>(record_[i]),
                        std::any_cast<int32_t>(record_[i + 1]),
                        std::any_cast<int32_t>(record_[i + 2]));
                i = i + 2;
                break;
            case ::hybridse::sdk::kTypeFloat:
                sql_parameter_row_->AppendFloat(std::any_cast<float>(record_[i]));
                break;
            case ::hybridse::sdk::kTypeDouble:
                sql_parameter_row_->AppendDouble(std::any_cast<double>(record_[i]));
                break;
            case ::hybridse::sdk::kTypeString:
                sql_parameter_row_->AppendString(std::any_cast<std::string>(record_[i]));
                break;
            case ::hybridse::sdk::kTypeUnknow:
                sql_parameter_row_->AppendNULL();
                break;
            default:
                break;
        }
    }
    sql_parameter_row_->Build();
    return sql_parameter_row_;
}

ParameterRow& ParameterRow::operator<<(const bool& value) {
    parameter_types_->AddColumnType(::hybridse::sdk::kTypeBool);
    record_.push_back(value);
    return *this;
}

ParameterRow& ParameterRow::operator<<(const int16_t value) {
    parameter_types_->AddColumnType(::hybridse::sdk::kTypeInt16);
    record_.push_back(value);
    return *this;
}

ParameterRow& ParameterRow::operator<<(const int32_t value) {
    parameter_types_->AddColumnType(::hybridse::sdk::kTypeInt32);
    record_.push_back(value);
    return *this;
}

ParameterRow& ParameterRow::operator<<(const int64_t value) {
    parameter_types_->AddColumnType(::hybridse::sdk::kTypeInt64);
    record_.push_back(value);
    return *this;
}

ParameterRow& ParameterRow::operator<<(const TimeStamp value) {
    parameter_types_->AddColumnType(::hybridse::sdk::kTypeTimestamp);
    record_.push_back(value.get_Timestamp());
    return *this;
}

ParameterRow& ParameterRow::operator<<(const Date value) {
    parameter_types_->AddColumnType(::hybridse::sdk::kTypeDate);
    record_.push_back(value.get_year());
    record_.push_back(value.get_month());
    record_.push_back(value.get_day());
    return *this;
}

ParameterRow& ParameterRow::operator<<(const float value) {
    parameter_types_->AddColumnType(::hybridse::sdk::kTypeFloat);
    record_.push_back(value);
    return *this;
}

ParameterRow& ParameterRow::operator<<(const double value) {
    parameter_types_->AddColumnType(::hybridse::sdk::kTypeDouble);
    record_.push_back(value);
    return *this;
}

ParameterRow& ParameterRow::operator<<(const std::string&value) {
    parameter_types_->AddColumnType(::hybridse::sdk::kTypeString);
    str_length_ += value.length();
    record_.push_back(value);
    return *this;
}

ParameterRow& ParameterRow::operator<<(const char* value) {
    parameter_types_->AddColumnType(::hybridse::sdk::kTypeString);
    str_length_ += strlen(value);
    record_.push_back((std::string)value);
    return *this;
}

ParameterRow& ParameterRow::operator<<(const OpenmldbNull value) {
    parameter_types_->AddColumnType(::hybridse::sdk::kTypeUnknow);
    record_.push_back(value);
    return *this;
}

void ParameterRow::reset() {
    record_.clear();
    parameter_types_ = std::make_shared<hybridse::sdk::ColumnTypes>();
    str_length_ = 0;
}

RequestRow::RequestRow(OpenmldbHandler* handler, const std::string& db, const std::string& sql)
    : handler_(handler), db_(db), sql_(sql) {
    parameter_types_ = std::make_shared<hybridse::sdk::ColumnTypes>();
}

std::shared_ptr<openmldb::sdk::SQLRequestRow> RequestRow::get_request_row() const {
    sql_request_row_ = (handler_->get_router())->GetRequestRow(db_, sql_, handler_->get_status());
    sql_request_row_->Init(str_length_);
    for (size_t i = 0; i < record_.size(); ++i) {
        auto type = parameter_types_->GetColumnType(i);
        switch (type) {
            case ::hybridse::sdk::kTypeBool:
                sql_request_row_->AppendBool(std::any_cast<bool>(record_[i]));
                break;
            case ::hybridse::sdk::kTypeInt16:
                sql_request_row_->AppendInt16(std::any_cast<int16_t>(record_[i]));
                break;
            case ::hybridse::sdk::kTypeInt32:
                sql_request_row_->AppendInt32(std::any_cast<int32_t>(record_[i]));
                break;
            case ::hybridse::sdk::kTypeInt64:
                sql_request_row_->AppendInt64(std::any_cast<int64_t>(record_[i]));
                break;
            case ::hybridse::sdk::kTypeTimestamp:
                sql_request_row_->AppendTimestamp(std::any_cast<int64_t>(record_[i]));
                break;
            case ::hybridse::sdk::kTypeDate:
                sql_request_row_->AppendDate(std::any_cast<int32_t>(record_[i]), std::any_cast<int32_t>(record_[++i]),
                                            std::any_cast<int32_t>(record_[++i]));
                break;
            case ::hybridse::sdk::kTypeFloat:
                sql_request_row_->AppendFloat(std::any_cast<float>(record_[i]));
                break;
            case ::hybridse::sdk::kTypeDouble:
                sql_request_row_->AppendDouble(std::any_cast<double>(record_[i]));
                break;
            case ::hybridse::sdk::kTypeString:
                sql_request_row_->AppendString(std::any_cast<std::string>(record_[i]));
                break;
            case ::hybridse::sdk::kTypeUnknow:
                sql_request_row_->AppendNULL();
                break;
            default:
                break;
        }
    }
    sql_request_row_->Build();
    return sql_request_row_;
}

RequestRow& RequestRow::operator<<(const bool& value) {
    parameter_types_->AddColumnType(::hybridse::sdk::kTypeBool);
    record_.push_back(value);
    return *this;
}

RequestRow& RequestRow::operator<<(const int16_t value) {
    parameter_types_->AddColumnType(::hybridse::sdk::kTypeInt16);
    record_.push_back(value);
    return *this;
}

RequestRow& RequestRow::operator<<(const int32_t value) {
    parameter_types_->AddColumnType(::hybridse::sdk::kTypeInt32);
    record_.push_back(value);
    return *this;
}

RequestRow& RequestRow::operator<<(const int64_t value) {
    parameter_types_->AddColumnType(::hybridse::sdk::kTypeInt64);
    record_.push_back(value);
    return *this;
}

RequestRow& RequestRow::operator<<(const TimeStamp value) {
    parameter_types_->AddColumnType(::hybridse::sdk::kTypeTimestamp);
    record_.push_back(value.get_Timestamp());
    return *this;
}

RequestRow& RequestRow::operator<<(const Date value) {
    parameter_types_->AddColumnType(::hybridse::sdk::kTypeDate);
    record_.push_back(value.get_year());
    record_.push_back(value.get_month());
    record_.push_back(value.get_day());
    return *this;
}

RequestRow& RequestRow::operator<<(const float value) {
    parameter_types_->AddColumnType(::hybridse::sdk::kTypeFloat);
    record_.push_back(value);
    return *this;
}

RequestRow& RequestRow::operator<<(const double value) {
    parameter_types_->AddColumnType(::hybridse::sdk::kTypeDouble);
    record_.push_back(value);
    return *this;
}

RequestRow& RequestRow::operator<<(const std::string&value) {
    parameter_types_->AddColumnType(::hybridse::sdk::kTypeString);
    str_length_ += value.length();
    record_.push_back(value);
    return *this;
}

RequestRow& RequestRow::operator<<(const char* value) {
    parameter_types_->AddColumnType(::hybridse::sdk::kTypeString);
    str_length_ += strlen(value);
    record_.push_back((std::string)value);
    return *this;
}

RequestRow& RequestRow::operator<<(const OpenmldbNull value) {
    parameter_types_->AddColumnType(::hybridse::sdk::kTypeUnknow);
    record_.push_back(value);
    return *this;
}

void RequestRow::reset() {
    record_.clear();
    parameter_types_ = std::make_shared<hybridse::sdk::ColumnTypes>();
    str_length_ = 0;
}

bool execute(const OpenmldbHandler& handler, const std::string& sql) {
    auto rs = (handler.get_router())->ExecuteSQL(sql, handler.get_status());
    if (rs != NULL) resultset_last = rs;
    return (handler.get_status())->IsOK();
}

bool execute(const OpenmldbHandler& handler, const std::string& db, const std::string& sql) {
    auto rs = (handler.get_router())->ExecuteSQL(db, sql, handler.get_status());
    if (rs != NULL) resultset_last = rs;
    return (handler.get_status())->IsOK();
}

bool execute_parameterized(const OpenmldbHandler& handler, const std::string& db, const std::string& sql,
                           const ParameterRow& para) {
    auto pr = para.get_parameter_row();
    auto rs = (handler.get_router())->ExecuteSQLParameterized(db, sql, pr, handler.get_status());
    if (rs != NULL) resultset_last = rs;
    return (handler.get_status())->IsOK();
}

bool execute_request(const RequestRow& req) {
    auto rr = req.get_request_row();
    auto rs = ((req.get_handler())->get_router())
                  ->ExecuteSQLRequest(req.get_db(), req.get_sql(), rr, (req.get_handler())->get_status());
    if (rs != NULL) resultset_last = rs;
    return ((req.get_handler())->get_status())->IsOK();
}

std::shared_ptr<hybridse::sdk::ResultSet> get_resultset() { return resultset_last; }

void print_resultset(std::shared_ptr<hybridse::sdk::ResultSet> rs = resultset_last) {
    if (rs == nullptr) {
        std::cout << "resultset is NULL\n";
        return;
    }
    std::ostringstream oss;
    ::hybridse::base::TextTable t('-', '|', '+');
    auto schema = rs->GetSchema();
    // Add Header
    for (int i = 0; i < schema->GetColumnCnt(); i++) {
        t.add(schema->GetColumnName(i));
    }
    t.end_of_row();
    if (0 == rs->Size()) {
        t.add("Empty set");
        t.end_of_row();
        return;
    }
    rs->Reset();
    while (rs->Next()) {
        for (int idx = 0; idx < schema->GetColumnCnt(); idx++) {
            std::string str = rs->GetAsStringUnsafe(idx);
            t.add(str);
        }
        t.end_of_row();
    }
    oss << t << std::endl;
    std::cout << "\n" << oss.str() << "\n";
}
