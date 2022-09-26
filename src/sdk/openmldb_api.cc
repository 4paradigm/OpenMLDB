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

int64_t TimeStamp::get_Timestamp() const { return value; }

int32_t Date::get_year() const { return year; }

int32_t Date::get_month() const { return month; }

int32_t Date::get_day() const { return day; }

OpenmldbHandler::OpenmldbHandler(std::string _zk_cluster, std::string _zk_path) {
    cluster = new openmldb::sdk::SQLRouterOptions;
    cluster->zk_cluster = _zk_cluster;
    cluster->zk_path = _zk_path;
    status = new hybridse::sdk::Status;
    router = new openmldb::sdk::SQLClusterRouter(*cluster);
    router->Init();
}

OpenmldbHandler::OpenmldbHandler(std::string _host, uint32_t _port) {
    standalone = new openmldb::sdk::StandaloneOptions;
    standalone->host = _host;
    standalone->port = _port;
    status = new hybridse::sdk::Status;
    router = new openmldb::sdk::SQLClusterRouter(*standalone);
    router->Init();
}

OpenmldbHandler::~OpenmldbHandler() {
    if (cluster != nullptr) delete cluster;
    if (standalone != nullptr) delete standalone;
    delete status;
    delete router;
}

openmldb::sdk::SQLClusterRouter* OpenmldbHandler::get_router() const { return router; }

hybridse::sdk::Status* OpenmldbHandler::get_status() const { return status; }

ParameterRow::ParameterRow(const OpenmldbHandler* _handler) : handler(_handler) {
    parameter_types = std::make_shared<hybridse::sdk::ColumnTypes>();
}

std::shared_ptr<openmldb::sdk::SQLRequestRow> ParameterRow::get_parameter_row() const {
    sql_parameter_row = ::openmldb::sdk::SQLRequestRow::CreateSQLRequestRowFromColumnTypes(parameter_types);
    sql_parameter_row->Init(str_length);
    for (int i = 0; i < record.size(); ++i) {
        auto type = parameter_types->GetColumnType(i);
        switch (type) {
            case ::hybridse::sdk::kTypeBool:
                sql_parameter_row->AppendBool(std::any_cast<bool>(record[i]));
                break;
            case ::hybridse::sdk::kTypeInt16:
                sql_parameter_row->AppendInt16(std::any_cast<int16_t>(record[i]));
                break;
            case ::hybridse::sdk::kTypeInt32:
                sql_parameter_row->AppendInt32(std::any_cast<int32_t>(record[i]));
                break;
            case ::hybridse::sdk::kTypeInt64:
                sql_parameter_row->AppendInt64(std::any_cast<int64_t>(record[i]));
                break;
            case ::hybridse::sdk::kTypeTimestamp:
                sql_parameter_row->AppendTimestamp(std::any_cast<int64_t>(record[i]));
                break;
            case ::hybridse::sdk::kTypeDate:
                sql_parameter_row->AppendDate(std::any_cast<int32_t>(record[i]), std::any_cast<int32_t>(record[i + 1]),
                                              std::any_cast<int32_t>(record[i + 2]));
                i = i + 2;
                break;
            case ::hybridse::sdk::kTypeFloat:
                sql_parameter_row->AppendFloat(std::any_cast<float>(record[i]));
                break;
            case ::hybridse::sdk::kTypeDouble:
                sql_parameter_row->AppendDouble(std::any_cast<double>(record[i]));
                break;
            case ::hybridse::sdk::kTypeString:
                sql_parameter_row->AppendString(std::any_cast<std::string>(record[i]));
                break;
            case ::hybridse::sdk::kTypeUnknow:
                sql_parameter_row->AppendNULL();
                break;
            default:
                break;
        }
    }
    sql_parameter_row->Build();
    return sql_parameter_row;
}

ParameterRow& ParameterRow::operator<<(const bool& value) {
    parameter_types->AddColumnType(::hybridse::sdk::kTypeBool);
    record.push_back(value);
    return *this;
}

ParameterRow& ParameterRow::operator<<(const int16_t value) {
    parameter_types->AddColumnType(::hybridse::sdk::kTypeInt16);
    record.push_back(value);
    return *this;
}

ParameterRow& ParameterRow::operator<<(const int32_t value) {
    parameter_types->AddColumnType(::hybridse::sdk::kTypeInt32);
    record.push_back(value);
    return *this;
}

ParameterRow& ParameterRow::operator<<(const int64_t value) {
    parameter_types->AddColumnType(::hybridse::sdk::kTypeInt64);
    record.push_back(value);
    return *this;
}

ParameterRow& ParameterRow::operator<<(const TimeStamp value) {
    parameter_types->AddColumnType(::hybridse::sdk::kTypeTimestamp);
    record.push_back(value.get_Timestamp());
    return *this;
}

ParameterRow& ParameterRow::operator<<(const Date value) {
    parameter_types->AddColumnType(::hybridse::sdk::kTypeDate);
    record.push_back(value.get_year());
    record.push_back(value.get_month());
    record.push_back(value.get_day());
    return *this;
}

ParameterRow& ParameterRow::operator<<(const float value) {
    parameter_types->AddColumnType(::hybridse::sdk::kTypeFloat);
    record.push_back(value);
    return *this;
}

ParameterRow& ParameterRow::operator<<(const double value) {
    parameter_types->AddColumnType(::hybridse::sdk::kTypeDouble);
    record.push_back(value);
    return *this;
}

ParameterRow& ParameterRow::operator<<(const std::string value) {
    parameter_types->AddColumnType(::hybridse::sdk::kTypeString);
    str_length += value.length();
    record.push_back(value);
    return *this;
}

ParameterRow& ParameterRow::operator<<(const char* value) {
    parameter_types->AddColumnType(::hybridse::sdk::kTypeString);
    str_length += strlen(value);
    record.push_back((std::string)value);
    return *this;
}

ParameterRow& ParameterRow::operator<<(const OpenmldbNull value) {
    parameter_types->AddColumnType(::hybridse::sdk::kTypeUnknow);
    record.push_back(value);
    return *this;
}

void ParameterRow::reset() {
    record.clear();
    parameter_types = std::make_shared<hybridse::sdk::ColumnTypes>();
    str_length = 0;
}

RequestRow::RequestRow(OpenmldbHandler* _handler, const std::string& _db, const std::string& _sql)
    : handler(_handler), db(_db), sql(_sql) {
    parameter_types = std::make_shared<hybridse::sdk::ColumnTypes>();
}

std::shared_ptr<openmldb::sdk::SQLRequestRow> RequestRow::get_request_row() const {
    sql_request_row = (handler->get_router())->GetRequestRow(db, sql, handler->get_status());
    sql_request_row->Init(str_length);
    for (int i = 0; i < record.size(); ++i) {
        auto type = parameter_types->GetColumnType(i);
        switch (type) {
            case ::hybridse::sdk::kTypeBool:
                sql_request_row->AppendBool(std::any_cast<bool>(record[i]));
                break;
            case ::hybridse::sdk::kTypeInt16:
                sql_request_row->AppendInt16(std::any_cast<int16_t>(record[i]));
                break;
            case ::hybridse::sdk::kTypeInt32:
                sql_request_row->AppendInt32(std::any_cast<int32_t>(record[i]));
                break;
            case ::hybridse::sdk::kTypeInt64:
                sql_request_row->AppendInt64(std::any_cast<int64_t>(record[i]));
                break;
            case ::hybridse::sdk::kTypeTimestamp:
                sql_request_row->AppendTimestamp(std::any_cast<int64_t>(record[i]));
                break;
            case ::hybridse::sdk::kTypeDate:
                sql_request_row->AppendDate(std::any_cast<int32_t>(record[i]), std::any_cast<int32_t>(record[++i]),
                                            std::any_cast<int32_t>(record[++i]));
                break;
            case ::hybridse::sdk::kTypeFloat:
                sql_request_row->AppendFloat(std::any_cast<float>(record[i]));
                break;
            case ::hybridse::sdk::kTypeDouble:
                sql_request_row->AppendDouble(std::any_cast<double>(record[i]));
                break;
            case ::hybridse::sdk::kTypeString:
                sql_request_row->AppendString(std::any_cast<std::string>(record[i]));
                break;
            case ::hybridse::sdk::kTypeUnknow:
                sql_request_row->AppendNULL();
                break;
            default:
                break;
        }
    }
    sql_request_row->Build();
    return sql_request_row;
}

OpenmldbHandler* RequestRow::get_handler() const { return handler; }
std::string RequestRow::get_db() const { return db; }
std::string RequestRow::get_sql() const { return sql; }

RequestRow& RequestRow::operator<<(const bool& value) {
    parameter_types->AddColumnType(::hybridse::sdk::kTypeBool);
    record.push_back(value);
    return *this;
}

RequestRow& RequestRow::operator<<(const int16_t value) {
    parameter_types->AddColumnType(::hybridse::sdk::kTypeInt16);
    record.push_back(value);
    return *this;
}

RequestRow& RequestRow::operator<<(const int32_t value) {
    parameter_types->AddColumnType(::hybridse::sdk::kTypeInt32);
    record.push_back(value);
    return *this;
}

RequestRow& RequestRow::operator<<(const int64_t value) {
    parameter_types->AddColumnType(::hybridse::sdk::kTypeInt64);
    record.push_back(value);
    return *this;
}

RequestRow& RequestRow::operator<<(const TimeStamp value) {
    parameter_types->AddColumnType(::hybridse::sdk::kTypeTimestamp);
    record.push_back(value.get_Timestamp());
    return *this;
}

RequestRow& RequestRow::operator<<(const Date value) {
    parameter_types->AddColumnType(::hybridse::sdk::kTypeDate);
    record.push_back(value.get_year());
    record.push_back(value.get_month());
    record.push_back(value.get_day());
    return *this;
}

RequestRow& RequestRow::operator<<(const float value) {
    parameter_types->AddColumnType(::hybridse::sdk::kTypeFloat);
    record.push_back(value);
    return *this;
}

RequestRow& RequestRow::operator<<(const double value) {
    parameter_types->AddColumnType(::hybridse::sdk::kTypeDouble);
    record.push_back(value);
    return *this;
}

RequestRow& RequestRow::operator<<(const std::string value) {
    parameter_types->AddColumnType(::hybridse::sdk::kTypeString);
    str_length += value.length();
    record.push_back(value);
    return *this;
}

RequestRow& RequestRow::operator<<(const char* value) {
    parameter_types->AddColumnType(::hybridse::sdk::kTypeString);
    str_length += strlen(value);
    record.push_back((std::string)value);
    return *this;
}

RequestRow& RequestRow::operator<<(const OpenmldbNull value) {
    parameter_types->AddColumnType(::hybridse::sdk::kTypeUnknow);
    record.push_back(value);
    return *this;
}

void RequestRow::reset() {
    record.clear();
    parameter_types = std::make_shared<hybridse::sdk::ColumnTypes>();
    str_length = 0;
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
