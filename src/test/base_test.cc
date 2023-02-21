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

#include "test/base_test.h"

#include "boost/lexical_cast.hpp"
#include "glog/logging.h"
#include "sdk/base.h"
#include "sdk/result_set.h"

#define MAX_DEBUG_LINES_CNT 20
#define MAX_DEBUG_COLUMN_CNT 20

namespace openmldb {
namespace test {

std::string SQLCaseTest::GenRand(const std::string &prefix) {
    return prefix + std::to_string(rand() % 10000000 + 1);  // NOLINT
}
const std::string SQLCaseTest::AutoTableName() { return GenRand("auto_t"); }

std::string SQLCaseTest::GetYAMLBaseDir() {
    std::string yaml_base_dir;
    const char *env_name = "YAML_CASE_BASE_DIR";
    char *value = getenv(env_name);
    if (value != nullptr) {
        yaml_base_dir.assign(value);
        if (yaml_base_dir.back() != '/') {
            yaml_base_dir.append("/");
        }
    } else {
        yaml_base_dir = "/openmldb";
    }
    DLOG(INFO) << "InitCases YAML_CASE_BASE_DIR: " << yaml_base_dir;
    return yaml_base_dir;
}

std::vector<hybridse::sqlcase::SqlCase> SQLCaseTest::InitCases(const std::string &yaml_path) {
    std::vector<hybridse::sqlcase::SqlCase> cases;
    InitCases(GetYAMLBaseDir(), yaml_path, cases);
    std::vector<hybridse::sqlcase::SqlCase> level_cases;

    int skip_case_cnt = 0;
    std::set<std::string> levels = hybridse::sqlcase::SqlCase::HYBRIDSE_LEVEL();
    for (const auto &sql_case : cases) {
        if (levels.find(std::to_string(sql_case.level())) != levels.cend()) {
            level_cases.push_back(sql_case);
        } else {
            skip_case_cnt++;
        }
    }
    if (skip_case_cnt > 0) {
        DLOG(INFO) << "InitCases done: HYBRIDSE_LEVEL skip cases cnt: " << skip_case_cnt;
    }
    return level_cases;
}
void SQLCaseTest::InitCases(const std::string &dir_path, const std::string &yaml_path,
                            std::vector<hybridse::sqlcase::SqlCase> &cases) {  // NOLINT
    std::vector<std::string> filters = {"hybridse-only"};
    if (!hybridse::sqlcase::SqlCase::CreateSqlCasesFromYaml(dir_path, yaml_path, cases, filters)) {
        FAIL() << "load cases from path " << yaml_path << " fail!";
    }
}

void SQLCaseTest::PrintSchema(const hybridse::vm::Schema &schema) {
    std::ostringstream oss;
    hybridse::codec::RowView row_view(schema);
    ::hybridse::base::TextTable t('-', '|', '+');
    // Add ColumnName
    for (int i = 0; i < schema.size(); i++) {
        t.add(schema.Get(i).name() + ":" + hybridse::type::Type_Name(schema.Get(i).type()));
        if (t.current_columns_size() >= MAX_DEBUG_COLUMN_CNT) {
            t.add("...");
            break;
        }
    }
    // Add ColumnType
    t.end_of_row();
    for (int i = 0; i < schema.size(); i++) {
        t.add(hybridse::sqlcase::SqlCase::TypeString(schema.Get(i).type()));
        if (t.current_columns_size() >= MAX_DEBUG_COLUMN_CNT) {
            t.add("...");
            break;
        }
    }
    t.end_of_row();
    oss << t << std::endl;
    LOG(INFO) << "\n" << oss.str() << "\n";
}

void SQLCaseTest::PrintSdkSchema(const hybridse::sdk::Schema &schema) {
    std::ostringstream oss;
    ::hybridse::base::TextTable t('-', '|', '+');
    // Add Header
    for (int i = 0; i < schema.GetColumnCnt(); i++) {
        t.add(schema.GetColumnName(i));
        if (t.current_columns_size() >= 20) {
            t.add("...");
            break;
        }
    }
    t.end_of_row();
}

void SQLCaseTest::CheckSchema(const hybridse::vm::Schema &schema, const hybridse::vm::Schema &exp_schema) {
    LOG(INFO) << "expect schema:\n";
    PrintSchema(exp_schema);
    LOG(INFO) << "real schema:\n";
    PrintSchema(schema);
    ASSERT_EQ(schema.size(), exp_schema.size());
    for (int i = 0; i < schema.size(); i++) {
        ASSERT_EQ(schema.Get(i).name(), exp_schema.Get(i).name());
        ASSERT_EQ(schema.Get(i).type(), exp_schema.Get(i).type());
    }
}

void SQLCaseTest::CheckSchema(const hybridse::vm::Schema &exp_schema, const hybridse::sdk::Schema &schema) {
    LOG(INFO) << "expect schema:\n";
    PrintSchema(exp_schema);
    LOG(INFO) << "real schema:\n";
    PrintSdkSchema(schema);
    ASSERT_EQ(schema.GetColumnCnt(), exp_schema.size());
    for (int i = 0; i < schema.GetColumnCnt(); i++) {
        ASSERT_EQ(exp_schema.Get(i).name(), schema.GetColumnName(i));
        switch (exp_schema.Get(i).type()) {
            case hybridse::type::kInt32: {
                ASSERT_EQ(hybridse::sdk::kTypeInt32, schema.GetColumnType(i));
                break;
            }
            case hybridse::type::kInt64: {
                ASSERT_EQ(hybridse::sdk::kTypeInt64, schema.GetColumnType(i));
                break;
            }
            case hybridse::type::kInt16: {
                ASSERT_EQ(hybridse::sdk::kTypeInt16, schema.GetColumnType(i));
                break;
            }
            case hybridse::type::kFloat: {
                ASSERT_EQ(hybridse::sdk::kTypeFloat, schema.GetColumnType(i));
                break;
            }
            case hybridse::type::kDouble: {
                ASSERT_EQ(hybridse::sdk::kTypeDouble, schema.GetColumnType(i));
                break;
            }
            case hybridse::type::kVarchar: {
                ASSERT_EQ(hybridse::sdk::kTypeString, schema.GetColumnType(i));
                break;
            }
            case hybridse::type::kTimestamp: {
                ASSERT_EQ(hybridse::sdk::kTypeTimestamp, schema.GetColumnType(i));
                break;
            }
            case hybridse::type::kDate: {
                ASSERT_EQ(hybridse::sdk::kTypeDate, schema.GetColumnType(i));
                break;
            }
            case hybridse::type::kBool: {
                ASSERT_EQ(hybridse::sdk::kTypeBool, schema.GetColumnType(i));
                break;
            }
            default: {
                FAIL() << "Invalid Column Type";
                break;
            }
        }
    }
}

void SQLCaseTest::PrintRows(const hybridse::vm::Schema &schema, const std::vector<hybridse::codec::Row> &rows) {
    std::ostringstream oss;
    hybridse::codec::RowView row_view(schema);
    ::hybridse::base::TextTable t('-', '|', '+');
    // Add Header
    for (int i = 0; i < schema.size(); i++) {
        t.add(schema.Get(i).name());
        if (t.current_columns_size() >= MAX_DEBUG_COLUMN_CNT) {
            t.add("...");
            break;
        }
    }
    t.end_of_row();
    if (rows.empty()) {
        t.add("Empty set");
        t.end_of_row();
        return;
    }

    for (auto row : rows) {
        row_view.Reset(row.buf());
        for (int idx = 0; idx < schema.size(); idx++) {
            std::string str = row_view.GetAsString(idx);
            t.add(str);
            if (t.current_columns_size() >= MAX_DEBUG_COLUMN_CNT) {
                t.add("...");
                break;
            }
        }
        t.end_of_row();
        if (t.rows().size() >= MAX_DEBUG_LINES_CNT) {
            break;
        }
    }
    oss << t << std::endl;
    LOG(INFO) << "\n" << oss.str() << "\n";
}

const std::vector<hybridse::codec::Row> SQLCaseTest::SortRows(const hybridse::vm::Schema &schema,
                                                              const std::vector<hybridse::codec::Row> &rows,
                                                              const std::string &order_col) {
    DLOG(INFO) << "sort rows start";
    hybridse::codec::RowView row_view(schema);
    int idx = -1;
    for (int i = 0; i < schema.size(); i++) {
        if (schema.Get(i).name() == order_col) {
            idx = i;
            break;
        }
    }
    if (-1 == idx) {
        return rows;
    }

    if (schema.Get(idx).type() == hybridse::type::kVarchar) {
        std::vector<std::pair<std::string, hybridse::codec::Row>> sort_rows;
        for (auto row : rows) {
            row_view.Reset(row.buf());
            row_view.GetAsString(idx);
            sort_rows.push_back(std::make_pair(row_view.GetAsString(idx), row));
        }
        std::sort(sort_rows.begin(), sort_rows.end(),
                  [](std::pair<std::string, hybridse::codec::Row> &a, std::pair<std::string, hybridse::codec::Row> &b) {
                      return a.first < b.first;
                  });
        std::vector<hybridse::codec::Row> output_rows;
        for (auto row : sort_rows) {
            output_rows.push_back(row.second);
        }
        DLOG(INFO) << "sort rows done!";
        return output_rows;
    } else {
        std::vector<std::pair<int64_t, hybridse::codec::Row>> sort_rows;
        for (auto row : rows) {
            row_view.Reset(row.buf());
            row_view.GetAsString(idx);
            sort_rows.push_back(std::make_pair(boost::lexical_cast<int64_t>(row_view.GetAsString(idx)), row));
        }
        std::sort(sort_rows.begin(), sort_rows.end(),
                  [](std::pair<int64_t, hybridse::codec::Row> &a, std::pair<int64_t, hybridse::codec::Row> &b) {
                      return a.first < b.first;
                  });
        std::vector<hybridse::codec::Row> output_rows;
        for (auto row : sort_rows) {
            output_rows.push_back(row.second);
        }
        DLOG(INFO) << "sort rows done!";
        return output_rows;
    }
}

void SQLCaseTest::PrintResultSetYamlFormat(std::shared_ptr<hybridse::sdk::ResultSet> rs) {
    PrintResultSetYamlFormat(std::vector<std::shared_ptr<hybridse::sdk::ResultSet>>({rs}));
}
void SQLCaseTest::PrintResultSetYamlFormat(std::vector<std::shared_ptr<hybridse::sdk::ResultSet>> results) {
    std::ostringstream oss;
    if (results.empty()) {
        LOG(INFO) << "EMPTY results";
    }

    auto schema = results[0] ? results[0]->GetSchema() : nullptr;

    if (nullptr == schema) {
        LOG(INFO) << "NULL Schema";
        return;
    }
    LOG(INFO) << "\n" << oss.str() << "\n";
    oss << "columns:\n[";
    for (int i = 0; i < schema->GetColumnCnt(); i++) {
        oss << "\"" << schema->GetColumnName(i) << " " << DataTypeName(schema->GetColumnType(i)) << "\"";
        if (i + 1 != schema->GetColumnCnt()) {
            oss << ", ";
        }
    }

    oss << "\nrows:\n";
    for (auto rs : results) {
        rs->Reset();
        while (rs->Next()) {
            oss << "- [";
            for (int idx = 0; idx < schema->GetColumnCnt(); idx++) {
                std::string str = rs->GetAsStringUnsafe(idx);
                auto col = schema->GetColumnName(idx);
                auto type = schema->GetColumnType(idx);
                if (DataTypeName(type) == "string" || DataTypeName(type) == "date") {
                    oss << "\"" << str << "\"";
                } else {
                    oss << str;
                }
                if (idx + 1 != schema->GetColumnCnt()) {
                    oss << ", ";
                }
            }
            oss << "]\n";
        }
    }
    LOG(INFO) << "\n" << oss.str() << "\n";
}
void SQLCaseTest::PrintResultSet(std::shared_ptr<hybridse::sdk::ResultSet> rs) {
    std::ostringstream oss;
    ::hybridse::base::TextTable t('-', '|', '+');
    auto schema = rs->GetSchema();
    // Add Header
    for (int i = 0; i < schema->GetColumnCnt(); i++) {
        t.add(schema->GetColumnName(i));
        if (t.current_columns_size() >= 20) {
            t.add("...");
            break;
        }
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
            if (t.current_columns_size() >= 20) {
                t.add("...");
                break;
            }
        }
        t.end_of_row();
        if (t.rows().size() > 10) {
            break;
        }
    }
    oss << t << std::endl;
    LOG(INFO) << "\n" << oss.str() << "\n";
}
void SQLCaseTest::PrintResultSet(std::vector<std::shared_ptr<hybridse::sdk::ResultSet>> results) {
    if (results.empty()) {
        LOG(WARNING) << "Fail to PrintResultSet: ResultSet List is Empty";
        return;
    }
    std::ostringstream oss;
    ::hybridse::base::TextTable t('-', '|', '+');
    auto rs = results[0];
    auto schema = rs->GetSchema();
    // Add Header
    for (int i = 0; i < schema->GetColumnCnt(); i++) {
        t.add(schema->GetColumnName(i));
        if (t.current_columns_size() >= 20) {
            t.add("...");
            break;
        }
    }
    t.end_of_row();
    if (0 == rs->Size()) {
        t.add("Empty set");
        t.end_of_row();
        return;
    }

    for (auto rs : results) {
        while (rs->Next()) {
            for (int idx = 0; idx < schema->GetColumnCnt(); idx++) {
                std::string str = rs->GetAsStringUnsafe(idx);
                t.add(str);
                if (t.current_columns_size() >= 20) {
                    t.add("...");
                    break;
                }
            }
            t.end_of_row();
            if (t.rows().size() > 10) {
                break;
            }
        }
    }
    oss << t << std::endl;
    LOG(INFO) << "\n" << oss.str() << "\n";
}

void SQLCaseTest::CheckRow(hybridse::codec::RowView &row_view,  // NOLINT
                           std::shared_ptr<hybridse::sdk::ResultSet> rs) {
    for (int i = 0; i < row_view.GetSchema()->size(); i++) {
        DLOG(INFO) << "Check Column Idx: " << i;
        std::string column_name = row_view.GetSchema()->Get(i).name();
        if (row_view.IsNULL(i)) {
            ASSERT_TRUE(rs->IsNULL(i)) << " At " << i;
            continue;
        }
        switch (row_view.GetSchema()->Get(i).type()) {
            case hybridse::type::kInt32: {
                ASSERT_EQ(row_view.GetInt32Unsafe(i), rs->GetInt32Unsafe(i)) << " At " << i;
                break;
            }
            case hybridse::type::kInt64: {
                ASSERT_EQ(row_view.GetInt64Unsafe(i), rs->GetInt64Unsafe(i)) << " At " << i;
                break;
            }
            case hybridse::type::kInt16: {
                ASSERT_EQ(row_view.GetInt16Unsafe(i), rs->GetInt16Unsafe(i)) << " At " << i;
                break;
            }
            case hybridse::type::kFloat: {
                float act = row_view.GetFloatUnsafe(i);
                float exp = rs->GetFloatUnsafe(i);
                if (IsNaN(exp)) {
                    ASSERT_TRUE(IsNaN(act)) << " At " << i;
                } else {
                    ASSERT_FLOAT_EQ(act, exp) << " At " << i;
                }
                break;
            }
            case hybridse::type::kDouble: {
                double act = row_view.GetDoubleUnsafe(i);
                double exp = rs->GetDoubleUnsafe(i);
                if (IsNaN(exp)) {
                    ASSERT_TRUE(IsNaN(act)) << " At " << i << ":" << column_name;
                } else {
                    ASSERT_DOUBLE_EQ(act, exp) << " At " << i << ":" << column_name;
                }

                break;
            }
            case hybridse::type::kVarchar: {
                ASSERT_EQ(row_view.GetStringUnsafe(i), rs->GetStringUnsafe(i)) << " At " << i;
                break;
            }
            case hybridse::type::kTimestamp: {
                ASSERT_EQ(row_view.GetTimestampUnsafe(i), rs->GetTimeUnsafe(i)) << " At " << i;
                break;
            }
            case hybridse::type::kDate: {
                ASSERT_EQ(row_view.GetDateUnsafe(i), rs->GetDateUnsafe(i)) << " At " << i;
                break;
            }
            case hybridse::type::kBool: {
                ASSERT_EQ(row_view.GetBoolUnsafe(i), rs->GetBoolUnsafe(i)) << " At " << i;
                break;
            }
            default: {
                FAIL() << "Invalid Column Type";
                break;
            }
        }
    }
}
void SQLCaseTest::CheckRows(const hybridse::vm::Schema &schema, const std::string &order_col,
                            const std::vector<hybridse::codec::Row> &rows,
                            std::shared_ptr<hybridse::sdk::ResultSet> rs) {
    LOG(INFO) << "Expected Rows: \n";
    PrintRows(schema, rows);
    LOG(INFO) << "ResultSet Rows: \n";
    PrintResultSet(rs);
    if (hybridse::sqlcase::SqlCase::IsDebug()) {
        PrintResultSetYamlFormat(rs);
    }
    LOG(INFO) << "order: " << order_col;

    ASSERT_EQ(static_cast<int32_t>(rows.size()), rs->Size());
    hybridse::codec::RowView row_view(schema);
    int order_idx = -1;
    for (int i = 0; i < schema.size(); i++) {
        if (schema.Get(i).name() == order_col) {
            order_idx = i;
            break;
        }
    }
    std::map<std::string, std::pair<hybridse::codec::Row, bool>> rows_map;
    if (order_idx >= 0) {
        int32_t row_id = 0;
        for (auto &row : rows) {
            row_view.Reset(row.buf());
            std::string key = row_view.GetAsString(order_idx);
            LOG(INFO) << "Get Order String: " << row_id++ << " key: " << key;
            rows_map.try_emplace(key, row, false);
        }
    }
    int32_t index = 0;
    rs->Reset();
    std::vector<hybridse::codec::Row> result_rows;
    while (rs->Next()) {
        if (order_idx >= 0) {
            std::string key = rs->GetAsStringUnsafe(order_idx);
            LOG(INFO) << "key : " << key;
            ASSERT_TRUE(rows_map.find(key) != rows_map.cend())
                << "CheckRows fail: row[" << index << "] order not expected";
            ASSERT_FALSE(rows_map[key].second) << "CheckRows fail: row[" << index << "] duplicate key";
            row_view.Reset(rows_map[key].first.buf());
            CheckRow(row_view, rs);
            rows_map[key].second = true;
        } else {
            row_view.Reset(rows[index++].buf());
            CheckRow(row_view, rs);
        }
    }
}

void SQLCaseTest::CheckRows(const hybridse::vm::Schema &schema, const std::vector<hybridse::codec::Row> &rows,
                            const std::vector<hybridse::codec::Row> &exp_rows) {
    LOG(INFO) << "expect result:\n";
    PrintRows(schema, exp_rows);
    LOG(INFO) << "real result:\n";
    PrintRows(schema, rows);
    ASSERT_EQ(rows.size(), exp_rows.size());
    hybridse::codec::RowView row_view(schema);
    hybridse::codec::RowView row_view_exp(schema);
    for (size_t row_index = 0; row_index < rows.size(); row_index++) {
        row_view.Reset(rows[row_index].buf());
        row_view_exp.Reset(exp_rows[row_index].buf());
        for (int i = 0; i < schema.size(); i++) {
            if (row_view_exp.IsNULL(i)) {
                ASSERT_TRUE(row_view.IsNULL(i)) << " At " << i;
                continue;
            }
            switch (schema.Get(i).type()) {
                case hybridse::type::kInt32: {
                    ASSERT_EQ(row_view.GetInt32Unsafe(i), row_view_exp.GetInt32Unsafe(i)) << " At " << i;
                    break;
                }
                case hybridse::type::kInt64: {
                    ASSERT_EQ(row_view.GetInt64Unsafe(i), row_view_exp.GetInt64Unsafe(i)) << " At " << i;
                    break;
                }
                case hybridse::type::kInt16: {
                    ASSERT_EQ(row_view.GetInt16Unsafe(i), row_view_exp.GetInt16Unsafe(i)) << " At " << i;
                    break;
                }
                case hybridse::type::kFloat: {
                    float act = row_view.GetFloatUnsafe(i);
                    float exp = row_view_exp.GetFloatUnsafe(i);
                    if (IsNaN(exp)) {
                        ASSERT_TRUE(IsNaN(act)) << " At " << i;
                    } else {
                        ASSERT_FLOAT_EQ(act, exp) << " At " << i;
                    }
                    break;
                }
                case hybridse::type::kDouble: {
                    double act = row_view.GetDoubleUnsafe(i);
                    double exp = row_view_exp.GetDoubleUnsafe(i);
                    if (IsNaN(exp)) {
                        ASSERT_TRUE(IsNaN(act)) << " At " << i;
                    } else {
                        ASSERT_DOUBLE_EQ(act, exp) << " At " << i;
                    }
                    break;
                }
                case hybridse::type::kVarchar: {
                    ASSERT_EQ(row_view.GetStringUnsafe(i), row_view_exp.GetStringUnsafe(i)) << " At " << i;
                    break;
                }
                case hybridse::type::kDate: {
                    ASSERT_EQ(row_view.GetDateUnsafe(i), row_view_exp.GetDateUnsafe(i)) << " At " << i;
                    break;
                }
                case hybridse::type::kTimestamp: {
                    ASSERT_EQ(row_view.GetTimestampUnsafe(i), row_view_exp.GetTimestampUnsafe(i)) << " At " << i;
                    break;
                }
                case hybridse::type::kBool: {
                    ASSERT_EQ(row_view.GetBoolUnsafe(i), row_view_exp.GetBoolUnsafe(i)) << " At " << i;
                    break;
                }
                default: {
                    FAIL() << "Invalid Column Type";
                    break;
                }
            }
        }
    }
}

void SQLCaseTest::CheckRows(const hybridse::vm::Schema &schema, const std::string &order_col,
                            const std::vector<hybridse::codec::Row> &rows,
                            std::vector<std::shared_ptr<hybridse::sdk::ResultSet>> results) {
    ASSERT_EQ(rows.size(), results.size());
    LOG(INFO) << "Expected Rows: \n";
    PrintRows(schema, rows);
    LOG(INFO) << "ResultSet Rows: \n";
    PrintResultSet(results);
    if (hybridse::sqlcase::SqlCase::IsDebug()) {
        PrintResultSetYamlFormat(results);
    }
    LOG(INFO) << "order col key: " << order_col;
    hybridse::codec::RowView row_view(schema);
    int order_idx = -1;
    for (int i = 0; i < schema.size(); i++) {
        if (schema.Get(i).name() == order_col) {
            order_idx = i;
            break;
        }
    }
    std::map<std::string, hybridse::codec::Row> rows_map;
    if (order_idx >= 0) {
        int32_t row_id = 0;
        for (auto row : rows) {
            DLOG(INFO) << "Get Order String: " << row_id++;
            row_view.Reset(row.buf());
            std::string key = row_view.GetAsString(order_idx);
            rows_map.insert(std::make_pair(key, row));
        }
    }
    int32_t index = 0;
    for (auto rs : results) {
        rs->Reset();
        while (rs->Next()) {
            if (order_idx >= 0) {
                std::string key = rs->GetAsStringUnsafe(order_idx);
                row_view.Reset(rows_map[key].buf());
            } else {
                row_view.Reset(rows[index++].buf());
            }
            CheckRow(row_view, rs);
        }
    }
}

}  // namespace test
}  // namespace openmldb
