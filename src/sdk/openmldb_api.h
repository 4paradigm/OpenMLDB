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

/*
 *In order to avoid the appearance of too many header files,
 *which makes it more difficult for users to use,
 *we create some classes that users need here.
 */

#ifndef OPENMLDB_API_H
#define OPENMLDB_API_H

#include <stdint.h>

#include <any>
#include <memory>
#include <vector>

#include "sdk/result_set.h"

namespace openmldb {
namespace sdk {
class SQLClusterRouter;
struct SQLRouterOptions;
struct StandaloneOptions;
struct SQLRequestRow;
}  // namespace sdk
}  // namespace openmldb

namespace hybridse {
namespace sdk {
struct Status;
class ColumnTypes;
}  // namespace sdk
}  // namespace hybridse

// save the results of SQL query
static std::shared_ptr<hybridse::sdk::ResultSet> resultset_last;

// TimeStamp corresponds to TIMESTAMP in SQL, as a parameter inserted into ParameterRow or RequestRow
// constructor : only one parameter of type int_64
class TimeStamp {
 public:
    TimeStamp(int64_t _val) : value(_val) {}
    ~TimeStamp() {}
    int64_t get_Timestamp() const;

 private:
    int64_t value;
};

// Date corresponds to DATE in SQL, as a parameter inserted into ParameterRow or RequestRow
// constructor : first parameter           int_32    year
//                    second parameter      int_32    month
//                    third parameter          int_32    day
class Date {
 public:
    Date(int32_t _year, int32_t _month, int32_t _day) : year(_year), month(_month), day(_day) {}
    ~Date() {}
    int32_t get_year() const;
    int32_t get_month() const;
    int32_t get_day() const;

 private:
    int32_t year;
    int32_t month;
    int32_t day;
};

// OpenmldbNull corresponds to NULL in SQL, as a parameter inserted into ParameterRow or RequestRow
// constructor : no parameters
class OpenmldbNull {
 public:
    OpenmldbNull() {}
};

// OpenmldbHandler is used to link openmldb server. to use openmldb, you must create an object of this type
// cluster :
// constructor : first parameter         string      format : "IP:Port"    eg : "127.0.0.1:2181"
//                    second parameter    string      format : "path"       eg : "/openmldb"
// standalone :
// constructor : first parameter         string      format : "IP"           eg : "127.0.0.1"
//                    second parameter    string      format : "Port"        eg : 6527
class OpenmldbHandler {
 public:
    OpenmldbHandler(std::string _zk_cluster, std::string _zk_path);
    OpenmldbHandler(std::string _host, uint32_t _port);
    ~OpenmldbHandler();
    openmldb::sdk::SQLClusterRouter* get_router() const;
    hybridse::sdk::Status* get_status() const;

 private:
    OpenmldbHandler(const OpenmldbHandler&);
    OpenmldbHandler& operator=(const OpenmldbHandler&);

 private:
    openmldb::sdk::SQLRouterOptions* cluster = nullptr;
    openmldb::sdk::StandaloneOptions* standalone = nullptr;
    openmldb::sdk::SQLClusterRouter* router = nullptr;
    hybridse::sdk::Status* status = nullptr;
};

// In the request with parameter and request mode of openmldb, the position of parameters to be filled is indicated by
// the symbol "?" to express

// ParameterRow is used to create a parameter line. insert data into objects of type ParameterRow, and replace the "?"
// constructor : only one parameter of type OpenmldbHandler*
// insert data into objects of type ParameterRow using operator '<<'     eg : para << 1 << "hello";
// reset() is used to clear the inserted data in an object of type ParameterRow
class ParameterRow {
 public:
    ParameterRow(const OpenmldbHandler* _handler);
    std::shared_ptr<openmldb::sdk::SQLRequestRow> get_parameter_row() const;
    ParameterRow& operator<<(const bool& value);
    ParameterRow& operator<<(const int16_t value);
    ParameterRow& operator<<(const int32_t value);
    ParameterRow& operator<<(const int64_t value);
    ParameterRow& operator<<(const TimeStamp value);
    ParameterRow& operator<<(const Date value);
    ParameterRow& operator<<(const float value);
    ParameterRow& operator<<(const double value);
    ParameterRow& operator<<(const std::string value);
    ParameterRow& operator<<(const char* value);
    ParameterRow& operator<<(const OpenmldbNull value);
    void reset();

 private:
    const OpenmldbHandler* handler;
    mutable std::shared_ptr<hybridse::sdk::ColumnTypes> parameter_types = nullptr;
    mutable std::shared_ptr<openmldb::sdk::SQLRequestRow> sql_parameter_row = nullptr;
    std::vector<std::any> record;
    uint32_t str_length = 0;
};

// RequestRow is used to create a request line. insert data into objects of type RequestRow, and replace the "?"
// constructor : first parameter           OpenmldbHandler*
//                    second parameter      string                        name of database
//                    third parameter          string                        SQL statement
// insert data into objects of type requets_row using operator '<<'     eg : req << 1 << "hello";
// reset() is used to clear the inserted data in an object of type RequestRow
class RequestRow {
 public:
    RequestRow(OpenmldbHandler* _handler, const std::string& _db, const std::string& _sql);
    std::shared_ptr<openmldb::sdk::SQLRequestRow> get_request_row() const;
    OpenmldbHandler* get_handler() const;
    std::string get_db() const;
    std::string get_sql() const;
    RequestRow& operator<<(const bool& value);
    RequestRow& operator<<(const int16_t value);
    RequestRow& operator<<(const int32_t value);
    RequestRow& operator<<(const int64_t value);
    RequestRow& operator<<(const TimeStamp value);
    RequestRow& operator<<(const Date value);
    RequestRow& operator<<(const float value);
    RequestRow& operator<<(const double value);
    RequestRow& operator<<(const std::string value);
    RequestRow& operator<<(const char* value);
    RequestRow& operator<<(const OpenmldbNull value);
    void reset();

 private:
    OpenmldbHandler* handler;
    mutable std::shared_ptr<hybridse::sdk::ColumnTypes> parameter_types = nullptr;
    mutable std::shared_ptr<openmldb::sdk::SQLRequestRow> sql_request_row = nullptr;
    std::vector<std::any> record;
    std::string db;
    std::string sql;
    uint32_t str_length = 0;
};

// execute() is used to execute SQL statements without parameters
// first parameter :         OpenmldbHandler
// second parameter :    string                        pass SQL statement
bool execute(const OpenmldbHandler& handler, const std::string& sql);

// execute() is used to execute SQL statements without parameters
// first parameter :         OpenmldbHandler     a object of type OpenmldbHandler
// second parameter :    string                        name of database
// third parameter :        string                        SQL statement
bool execute(const OpenmldbHandler& handler, const std::string& db, const std::string& sql);

// execute_parameterized() is used to execute SQL statements with parameters
// first parameter :        OpenmldbHandler     a object of type OpenmldbHandler
// second parameter :    string                        name of database
// third parameter :        string                        SQL statement
// forth parameter :        ParameterRow          a object of type ParameterRow
bool execute_parameterized(const OpenmldbHandler& handler, const std::string& db, const std::string& sql,
                           const ParameterRow& para);

// execute_request() is used to execute SQL of request mode
// only one parameter,  a object of type RequestRow
bool execute_request(const RequestRow& req);

// get the results of the latest SQL query
std::shared_ptr<hybridse::sdk::ResultSet> get_resultset();

// print_resultset() is used to print the results of SQL query
// only one parameter,  a object of type shared_ptr<hybridse::sdk::ResultSet>
void print_resultset(std::shared_ptr<hybridse::sdk::ResultSet> rs);

#endif