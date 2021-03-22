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

#ifndef SRC_TEST_BASE_TEST_H_
#define SRC_TEST_BASE_TEST_H_
#include <algorithm>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "base/texttable.h"
#include "case/sql_case.h"
#include "gtest/gtest.h"
#include "sdk/base.h"
#include "sdk/result_set.h"

namespace fedb {
namespace test {
class SQLCaseTest : public ::testing::TestWithParam<hybridse::sqlcase::SQLCase> {
 public:
    SQLCaseTest() {}
    virtual ~SQLCaseTest() {}

    static std::string GenRand(const std::string &prefix);
    static const std::string AutoTableName();
    static std::string GetYAMLBaseDir();
    static std::vector<hybridse::sqlcase::SQLCase> InitCases(const std::string &yaml_path);
    static void InitCases(const std::string &dir_path, const std::string &yaml_path,
                          std::vector<hybridse::sqlcase::SQLCase> &cases);  // NOLINT

    static void PrintSchema(const hybridse::vm::Schema &schema);
    static void PrintSdkSchema(const hybridse::sdk::Schema &schema);

    static void CheckSchema(const hybridse::vm::Schema &schema, const hybridse::vm::Schema &exp_schema);
    static void CheckSchema(const hybridse::vm::Schema &schema, const hybridse::sdk::Schema &exp_schema);
    static void PrintRows(const hybridse::vm::Schema &schema, const std::vector<hybridse::codec::Row> &rows);
    static const std::vector<hybridse::codec::Row> SortRows(const hybridse::vm::Schema &schema,
                                                         const std::vector<hybridse::codec::Row> &rows,
                                                         const std::string &order_col);
    static void CheckRow(hybridse::codec::RowView &row_view,  // NOLINT
                         std::shared_ptr<hybridse::sdk::ResultSet> rs);
    static void CheckRows(const hybridse::vm::Schema &schema, const std::string &order_col,
                          const std::vector<hybridse::codec::Row> &rows, std::shared_ptr<hybridse::sdk::ResultSet> rs);
    static void CheckRows(const hybridse::vm::Schema &schema, const std::vector<hybridse::codec::Row> &rows,
                          const std::vector<hybridse::codec::Row> &exp_rows);
    static void CheckRows(const hybridse::vm::Schema &schema, const std::string &order_col,
                          const std::vector<hybridse::codec::Row> &rows,
                          std::vector<std::shared_ptr<hybridse::sdk::ResultSet>> results);

    static void PrintResultSet(std::shared_ptr<hybridse::sdk::ResultSet> rs);
    static void PrintResultSetYamlFormat(std::shared_ptr<hybridse::sdk::ResultSet> rs);
    static void PrintResultSetYamlFormat(std::vector<std::shared_ptr<hybridse::sdk::ResultSet>> results);
    static void PrintResultSet(std::vector<std::shared_ptr<hybridse::sdk::ResultSet>> results);
    static bool IsNaN(float x) { return x != x; }
    static bool IsNaN(double x) { return x != x; }
};

}  // namespace test
}  // namespace fedb
#endif  // SRC_TEST_BASE_TEST_H_
