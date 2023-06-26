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
 * In order to avoid the appearance of too many header files,
 * which makes it more difficult for users to use,
 * we create some classes that users need here.
 */

#ifndef SRC_SDK_OPENMLDB_API_H_
#define SRC_SDK_OPENMLDB_API_H_

#include <stdint.h>

#include <any>
#include <memory>
#include <string>
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

// TimeStamp corresponds to TIMESTAMP in SQL, as a parameter inserted into ParameterRow or RequestRow
// constructor : only one parameter of type int_64
class TimeStamp {
 public:
    explicit TimeStamp(int64_t val) : value_(val) {}
    ~TimeStamp() {}
    int64_t get_Timestamp() const { return value_; }

 private:
    int64_t value_;
};

// Date corresponds to DATE in SQL, as a parameter inserted into ParameterRow or RequestRow
// constructor : first parameter           int_32    year
//                    second parameter      int_32    month
//                    third parameter          int_32    day
class Date {
 public:
    Date(int32_t year, int32_t month, int32_t day) : year_(year), month_(month), day_(day) {}
    ~Date() {}
    int32_t get_year() const { return year_; }
    int32_t get_month() const { return month_; }
    int32_t get_day() const { return day_; }

 private:
    int32_t year_;
    int32_t month_;
    int32_t day_;
};

// OpenmldbNull corresponds to NULL in SQL, as a parameter inserted into ParameterRow or RequestRow
// constructor : no parameters
class OpenmldbNull {
 public:
    OpenmldbNull() {}
};

class OpenmldbHandler;
// In the request with parameter and request mode of openmldb, the position of parameters to be filled is indicated by
// the symbol "?" to express

// ParameterRow is used to create a parameter line. insert data into objects of type ParameterRow, and replace the "?"
// constructor : only one parameter of type OpenmldbHandler*
// insert data into objects of type ParameterRow using operator '<<'     eg : para << 1 << "hello";
// reset() is used to clear the inserted data in an object of type ParameterRow
class ParameterRow {
 public:
    explicit ParameterRow(const OpenmldbHandler* handler);
    std::shared_ptr<openmldb::sdk::SQLRequestRow> get_parameter_row() const;
    ParameterRow& operator<<(const bool& value);
    ParameterRow& operator<<(const int16_t value);
    ParameterRow& operator<<(const int32_t value);
    ParameterRow& operator<<(const int64_t value);
    ParameterRow& operator<<(const TimeStamp value);
    ParameterRow& operator<<(const Date value);
    ParameterRow& operator<<(const float value);
    ParameterRow& operator<<(const double value);
    ParameterRow& operator<<(const std::string& value);
    ParameterRow& operator<<(const char* value);
    ParameterRow& operator<<(const OpenmldbNull value);
    void reset();

 private:
    const OpenmldbHandler* handler_;
    mutable std::shared_ptr<hybridse::sdk::ColumnTypes> parameter_types_ = nullptr;
    mutable std::shared_ptr<openmldb::sdk::SQLRequestRow> sql_parameter_row_ = nullptr;
    std::vector<std::any> record_;
    uint32_t str_length_ = 0;
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
    OpenmldbHandler* get_handler() const { return handler_; }
    const std::string& get_db() const { return db_; }
    const std::string& get_sql() const { return sql_; }
    RequestRow& operator<<(const bool& value);
    RequestRow& operator<<(const int16_t value);
    RequestRow& operator<<(const int32_t value);
    RequestRow& operator<<(const int64_t value);
    RequestRow& operator<<(const TimeStamp value);
    RequestRow& operator<<(const Date value);
    RequestRow& operator<<(const float value);
    RequestRow& operator<<(const double value);
    RequestRow& operator<<(const std::string& value);
    RequestRow& operator<<(const char* value);
    RequestRow& operator<<(const OpenmldbNull value);
    void reset();

 private:
    OpenmldbHandler* handler_;
    mutable std::shared_ptr<hybridse::sdk::ColumnTypes> parameter_types_ = nullptr;
    mutable std::shared_ptr<openmldb::sdk::SQLRequestRow> sql_request_row_ = nullptr;
    std::vector<std::any> record_;
    std::string db_;
    std::string sql_;
    uint32_t str_length_ = 0;
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
    OpenmldbHandler(std::string zk_cluster, std::string zk_path);
    OpenmldbHandler(std::string host, uint32_t _port);
    OpenmldbHandler(std::shared_ptr<openmldb::sdk::SQLClusterRouter> router);
    ~OpenmldbHandler();
    std::shared_ptr<openmldb::sdk::SQLClusterRouter> get_router() const { return router_; }

    // execute() is used to execute SQL statements without parameters
    // first parameter :         OpenmldbHandler
    // second parameter :    string                        pass SQL statement
    bool execute(const std::string& sql);

    // execute() is used to execute SQL statements without parameters
    // first parameter :         OpenmldbHandler     a object of type OpenmldbHandler
    // second parameter :    string                        name of database
    // third parameter :        string                        SQL statement
    bool execute(const std::string& db, const std::string& sql);

    // execute_parameterized() is used to execute SQL statements with parameters
    // first parameter :        OpenmldbHandler     a object of type OpenmldbHandler
    // second parameter :    string                        name of database
    // third parameter :        string                        SQL statement
    // forth parameter :        ParameterRow          a object of type ParameterRow
    bool execute_parameterized(const std::string& db, const std::string& sql, const ParameterRow& para);

    // execute_request() is used to execute SQL of request mode
    // only one parameter,  a object of type RequestRow
    bool execute_request(const RequestRow& req);

    // get the results of the latest SQL query
    std::shared_ptr<hybridse::sdk::ResultSet> get_resultset() { return resultset_last_; }
    hybridse::sdk::Status* get_status() const { return status_; }

 private:
    OpenmldbHandler(const OpenmldbHandler&);
    OpenmldbHandler& operator=(const OpenmldbHandler&);

 private:
    hybridse::sdk::Status* status_;
    std::shared_ptr<openmldb::sdk::SQLClusterRouter> router_ = nullptr;
    std::shared_ptr<hybridse::sdk::ResultSet> resultset_last_;
};

// print_resultset() is used to print the results of SQL query
// only one parameter,  a object of type shared_ptr<hybridse::sdk::ResultSet>
void print_resultset(std::shared_ptr<hybridse::sdk::ResultSet> rs);

#endif  // SRC_SDK_OPENMLDB_API_H_
