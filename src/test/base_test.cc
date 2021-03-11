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

#include "glog/logging.h"
#include "sdk/base.h"
#include "sdk/result_set.h"
#define MAX_DEBUG_LINES_CNT 20
#define MAX_DEBUG_COLUMN_CNT 20
namespace rtidb {
namespace test {
std::string SQLCaseTest::GenRand(const std::string &prefix) {
    return prefix + std::to_string(rand() % 10000000 + 1);  // NOLINT
}
const std::string SQLCaseTest::AutoTableName() { return GenRand("auto_t"); }
std::string SQLCaseTest::FindRtidbDirPath(const std::string &dirname) {
    boost::filesystem::path current_path(boost::filesystem::current_path());
    boost::filesystem::path fesql_path;

    if (current_path.filename() == dirname) {
        return current_path.string();
    }
    while (current_path.has_parent_path()) {
        current_path = current_path.parent_path();
        if (current_path.filename().string() == dirname) {
            break;
        }
    }
    if (current_path.filename().string() == dirname) {
        return current_path.string();
    }
    return std::string();
}

std::vector<fesql::sqlcase::SQLCase> SQLCaseTest::InitCases(const std::string &yaml_path) {
    std::vector<fesql::sqlcase::SQLCase> cases;
    InitCases(FindRtidbDirPath("rtidb") + "/fesql/", yaml_path, cases);
    std::vector<fesql::sqlcase::SQLCase> level_cases;

    int skip_case_cnt = 0;
    std::set<std::string> levels = fesql::sqlcase::SQLCase::FESQL_LEVEL();
    for (const auto &sql_case : cases) {
        if (levels.find(std::to_string(sql_case.level())) != levels.cend()) {
            level_cases.push_back(sql_case);
        } else {
            skip_case_cnt++;
        }
    }
    if (skip_case_cnt > 0) {
        DLOG(INFO) << "InitCases done: FESQL_LEVEL skip cases cnt: " << skip_case_cnt;
    }
    return level_cases;
}
void SQLCaseTest::InitCases(const std::string &dir_path, const std::string &yaml_path,
                            std::vector<fesql::sqlcase::SQLCase> &cases) {  // NOLINT
    if (!fesql::sqlcase::SQLCase::CreateSQLCasesFromYaml(dir_path, yaml_path, cases)) {
        FAIL();
    }
}

void SQLCaseTest::PrintSchema(const fesql::vm::Schema &schema) {
    std::ostringstream oss;
    fesql::codec::RowView row_view(schema);
    ::fesql::base::TextTable t('-', '|', '+');
    // Add ColumnName
    for (int i = 0; i < schema.size(); i++) {
        t.add(schema.Get(i).name() + ":" + fesql::type::Type_Name(schema.Get(i).type()));
        if (t.current_columns_size() >= MAX_DEBUG_COLUMN_CNT) {
            t.add("...");
            break;
        }
    }
    // Add ColumnType
    t.endOfRow();
    for (int i = 0; i < schema.size(); i++) {
        t.add(fesql::sqlcase::SQLCase::TypeString(schema.Get(i).type()));
        if (t.current_columns_size() >= MAX_DEBUG_COLUMN_CNT) {
            t.add("...");
            break;
        }
    }
    t.endOfRow();
    oss << t << std::endl;
    LOG(INFO) << "\n" << oss.str() << "\n";
}

void SQLCaseTest::PrintSdkSchema(const fesql::sdk::Schema &schema) {
    std::ostringstream oss;
    ::fesql::base::TextTable t('-', '|', '+');
    // Add Header
    for (int i = 0; i < schema.GetColumnCnt(); i++) {
        t.add(schema.GetColumnName(i));
        if (t.current_columns_size() >= 20) {
            t.add("...");
            break;
        }
    }
    t.endOfRow();
}

void SQLCaseTest::CheckSchema(const fesql::vm::Schema &schema, const fesql::vm::Schema &exp_schema) {
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

void SQLCaseTest::CheckSchema(const fesql::vm::Schema &exp_schema, const fesql::sdk::Schema &schema) {
    LOG(INFO) << "expect schema:\n";
    PrintSchema(exp_schema);
    LOG(INFO) << "real schema:\n";
    PrintSdkSchema(schema);
    ASSERT_EQ(schema.GetColumnCnt(), exp_schema.size());
    for (int i = 0; i < schema.GetColumnCnt(); i++) {
        ASSERT_EQ(exp_schema.Get(i).name(), schema.GetColumnName(i));
        switch (exp_schema.Get(i).type()) {
            case fesql::type::kInt32: {
                ASSERT_EQ(fesql::sdk::kTypeInt32, schema.GetColumnType(i));
                break;
            }
            case fesql::type::kInt64: {
                ASSERT_EQ(fesql::sdk::kTypeInt64, schema.GetColumnType(i));
                break;
            }
            case fesql::type::kInt16: {
                ASSERT_EQ(fesql::sdk::kTypeInt16, schema.GetColumnType(i));
                break;
            }
            case fesql::type::kFloat: {
                ASSERT_EQ(fesql::sdk::kTypeFloat, schema.GetColumnType(i));
                break;
            }
            case fesql::type::kDouble: {
                ASSERT_EQ(fesql::sdk::kTypeDouble, schema.GetColumnType(i));
                break;
            }
            case fesql::type::kVarchar: {
                ASSERT_EQ(fesql::sdk::kTypeString, schema.GetColumnType(i));
                break;
            }
            case fesql::type::kTimestamp: {
                ASSERT_EQ(fesql::sdk::kTypeTimestamp, schema.GetColumnType(i));
                break;
            }
            case fesql::type::kDate: {
                ASSERT_EQ(fesql::sdk::kTypeDate, schema.GetColumnType(i));
                break;
            }
            case fesql::type::kBool: {
                ASSERT_EQ(fesql::sdk::kTypeBool, schema.GetColumnType(i));
                break;
            }
            default: {
                FAIL() << "Invalid Column Type";
                break;
            }
        }
    }
}

void SQLCaseTest::PrintRows(const fesql::vm::Schema &schema, const std::vector<fesql::codec::Row> &rows) {
    std::ostringstream oss;
    fesql::codec::RowView row_view(schema);
    ::fesql::base::TextTable t('-', '|', '+');
    // Add Header
    for (int i = 0; i < schema.size(); i++) {
        t.add(schema.Get(i).name());
        if (t.current_columns_size() >= MAX_DEBUG_COLUMN_CNT) {
            t.add("...");
            break;
        }
    }
    t.endOfRow();
    if (rows.empty()) {
        t.add("Empty set");
        t.endOfRow();
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
        t.endOfRow();
        if (t.rows().size() >= MAX_DEBUG_LINES_CNT) {
            break;
        }
    }
    oss << t << std::endl;
    LOG(INFO) << "\n" << oss.str() << "\n";
}

const std::vector<fesql::codec::Row> SQLCaseTest::SortRows(const fesql::vm::Schema &schema,
                                                           const std::vector<fesql::codec::Row> &rows,
                                                           const std::string &order_col) {
    DLOG(INFO) << "sort rows start";
    fesql::codec::RowView row_view(schema);
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

    if (schema.Get(idx).type() == fesql::type::kVarchar) {
        std::vector<std::pair<std::string, fesql::codec::Row>> sort_rows;
        for (auto row : rows) {
            row_view.Reset(row.buf());
            row_view.GetAsString(idx);
            sort_rows.push_back(std::make_pair(row_view.GetAsString(idx), row));
        }
        std::sort(sort_rows.begin(), sort_rows.end(),
                  [](std::pair<std::string, fesql::codec::Row> &a, std::pair<std::string, fesql::codec::Row> &b) {
                      return a.first < b.first;
                  });
        std::vector<fesql::codec::Row> output_rows;
        for (auto row : sort_rows) {
            output_rows.push_back(row.second);
        }
        DLOG(INFO) << "sort rows done!";
        return output_rows;
    } else {
        std::vector<std::pair<int64_t, fesql::codec::Row>> sort_rows;
        for (auto row : rows) {
            row_view.Reset(row.buf());
            row_view.GetAsString(idx);
            sort_rows.push_back(std::make_pair(boost::lexical_cast<int64_t>(row_view.GetAsString(idx)), row));
        }
        std::sort(sort_rows.begin(), sort_rows.end(),
                  [](std::pair<int64_t, fesql::codec::Row> &a, std::pair<int64_t, fesql::codec::Row> &b) {
                      return a.first < b.first;
                  });
        std::vector<fesql::codec::Row> output_rows;
        for (auto row : sort_rows) {
            output_rows.push_back(row.second);
        }
        DLOG(INFO) << "sort rows done!";
        return output_rows;
    }
}

void SQLCaseTest::PrintResultSetYamlFormat(std::shared_ptr<fesql::sdk::ResultSet> rs) {
    PrintResultSetYamlFormat(std::vector<std::shared_ptr<fesql::sdk::ResultSet>>({rs}));
}
void SQLCaseTest::PrintResultSetYamlFormat(std::vector<std::shared_ptr<fesql::sdk::ResultSet>> results) {
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
                std::string str = rs->GetAsString(idx);
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
void SQLCaseTest::PrintResultSet(std::shared_ptr<fesql::sdk::ResultSet> rs) {
    std::ostringstream oss;
    ::fesql::base::TextTable t('-', '|', '+');
    auto schema = rs->GetSchema();
    // Add Header
    for (int i = 0; i < schema->GetColumnCnt(); i++) {
        t.add(schema->GetColumnName(i));
        if (t.current_columns_size() >= 20) {
            t.add("...");
            break;
        }
    }
    t.endOfRow();
    if (0 == rs->Size()) {
        t.add("Empty set");
        t.endOfRow();
        return;
    }
    rs->Reset();
    while (rs->Next()) {
        for (int idx = 0; idx < schema->GetColumnCnt(); idx++) {
            std::string str = rs->GetAsString(idx);
            t.add(str);
            if (t.current_columns_size() >= 20) {
                t.add("...");
                break;
            }
        }
        t.endOfRow();
        if (t.rows().size() > 10) {
            break;
        }
    }
    oss << t << std::endl;
    LOG(INFO) << "\n" << oss.str() << "\n";
}
void SQLCaseTest::PrintResultSet(std::vector<std::shared_ptr<fesql::sdk::ResultSet>> results) {
    if (results.empty()) {
        LOG(WARNING) << "Fail to PrintResultSet: ResultSet List is Empty";
        return;
    }
    std::ostringstream oss;
    ::fesql::base::TextTable t('-', '|', '+');
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
    t.endOfRow();
    if (0 == rs->Size()) {
        t.add("Empty set");
        t.endOfRow();
        return;
    }

    for (auto rs : results) {
        while (rs->Next()) {
            for (int idx = 0; idx < schema->GetColumnCnt(); idx++) {
                std::string str = rs->GetAsString(idx);
                t.add(str);
                if (t.current_columns_size() >= 20) {
                    t.add("...");
                    break;
                }
            }
            t.endOfRow();
            if (t.rows().size() > 10) {
                break;
            }
        }
    }
    oss << t << std::endl;
    LOG(INFO) << "\n" << oss.str() << "\n";
}

void SQLCaseTest::CheckRow(fesql::codec::RowView &row_view,  // NOLINT
                           std::shared_ptr<fesql::sdk::ResultSet> rs) {
    for (int i = 0; i < row_view.GetSchema()->size(); i++) {
        DLOG(INFO) << "Check Column Idx: " << i;
        std::string column_name = row_view.GetSchema()->Get(i).name();
        if (row_view.IsNULL(i)) {
            ASSERT_TRUE(rs->IsNULL(i)) << " At " << i;
            continue;
        }
        switch (row_view.GetSchema()->Get(i).type()) {
            case fesql::type::kInt32: {
                ASSERT_EQ(row_view.GetInt32Unsafe(i), rs->GetInt32Unsafe(i)) << " At " << i;
                break;
            }
            case fesql::type::kInt64: {
                ASSERT_EQ(row_view.GetInt64Unsafe(i), rs->GetInt64Unsafe(i)) << " At " << i;
                break;
            }
            case fesql::type::kInt16: {
                ASSERT_EQ(row_view.GetInt16Unsafe(i), rs->GetInt16Unsafe(i)) << " At " << i;
                break;
            }
            case fesql::type::kFloat: {
                float act = row_view.GetFloatUnsafe(i);
                float exp = rs->GetFloatUnsafe(i);
                if (IsNaN(exp)) {
                    ASSERT_TRUE(IsNaN(act)) << " At " << i;
                } else {
                    ASSERT_FLOAT_EQ(act, exp) << " At " << i;
                }
                break;
            }
            case fesql::type::kDouble: {
                double act = row_view.GetDoubleUnsafe(i);
                double exp = rs->GetDoubleUnsafe(i);
                if (IsNaN(exp)) {
                    ASSERT_TRUE(IsNaN(act)) << " At " << i << ":" << column_name;
                } else {
                    ASSERT_DOUBLE_EQ(act, exp) << " At " << i << ":" << column_name;
                }

                break;
            }
            case fesql::type::kVarchar: {
                ASSERT_EQ(row_view.GetStringUnsafe(i), rs->GetStringUnsafe(i)) << " At " << i;
                break;
            }
            case fesql::type::kTimestamp: {
                ASSERT_EQ(row_view.GetTimestampUnsafe(i), rs->GetTimeUnsafe(i)) << " At " << i;
                break;
            }
            case fesql::type::kDate: {
                ASSERT_EQ(row_view.GetDateUnsafe(i), rs->GetDateUnsafe(i)) << " At " << i;
                break;
            }
            case fesql::type::kBool: {
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
void SQLCaseTest::CheckRows(const fesql::vm::Schema &schema, const std::string &order_col,
                            const std::vector<fesql::codec::Row> &rows, std::shared_ptr<fesql::sdk::ResultSet> rs) {
    LOG(INFO) << "Expected Rows: \n";
    PrintRows(schema, rows);
    LOG(INFO) << "ResultSet Rows: \n";
    PrintResultSet(rs);
    if (fesql::sqlcase::SQLCase::IS_DEBUG()) {
        PrintResultSetYamlFormat(rs);
    }
    LOG(INFO) << "order: " << order_col;

    ASSERT_EQ(static_cast<int32_t>(rows.size()), rs->Size());
    fesql::codec::RowView row_view(schema);
    int order_idx = -1;
    for (int i = 0; i < schema.size(); i++) {
        if (schema.Get(i).name() == order_col) {
            order_idx = i;
            break;
        }
    }
    std::map<std::string, std::pair<fesql::codec::Row, bool>> rows_map;
    if (order_idx >= 0) {
        int32_t row_id = 0;
        for (auto row : rows) {
            row_view.Reset(row.buf());
            std::string key = row_view.GetAsString(order_idx);
            LOG(INFO) << "Get Order String: " << row_id++ << " key: " << key;
            rows_map.insert(std::make_pair(key, std::make_pair(row, false)));
        }
    }
    int32_t index = 0;
    rs->Reset();
    std::vector<fesql::codec::Row> result_rows;
    while (rs->Next()) {
        if (order_idx >= 0) {
            std::string key = rs->GetAsString(order_idx);
            LOG(INFO) << "key : " << key;
            ASSERT_TRUE(rows_map.find(key) != rows_map.cend())
                << "CheckRows fail: row[" << index << "] order not expected";
            ASSERT_FALSE(rows_map[key].second) << "CheckRows fail: row[" << index << "] duplicate key";
            row_view.Reset(rows_map[key].first.buf());
            CheckRow(row_view, rs);
            rows_map[key].second = true;
        } else {
            row_view.Reset(rows[index++].buf());
        }
    }
}

void SQLCaseTest::CheckRows(const fesql::vm::Schema &schema, const std::vector<fesql::codec::Row> &rows,
                            const std::vector<fesql::codec::Row> &exp_rows) {
    LOG(INFO) << "expect result:\n";
    PrintRows(schema, exp_rows);
    LOG(INFO) << "real result:\n";
    PrintRows(schema, rows);
    ASSERT_EQ(rows.size(), exp_rows.size());
    fesql::codec::RowView row_view(schema);
    fesql::codec::RowView row_view_exp(schema);
    for (size_t row_index = 0; row_index < rows.size(); row_index++) {
        row_view.Reset(rows[row_index].buf());
        row_view_exp.Reset(exp_rows[row_index].buf());
        for (int i = 0; i < schema.size(); i++) {
            if (row_view_exp.IsNULL(i)) {
                ASSERT_TRUE(row_view.IsNULL(i)) << " At " << i;
                continue;
            }
            switch (schema.Get(i).type()) {
                case fesql::type::kInt32: {
                    ASSERT_EQ(row_view.GetInt32Unsafe(i), row_view_exp.GetInt32Unsafe(i)) << " At " << i;
                    break;
                }
                case fesql::type::kInt64: {
                    ASSERT_EQ(row_view.GetInt64Unsafe(i), row_view_exp.GetInt64Unsafe(i)) << " At " << i;
                    break;
                }
                case fesql::type::kInt16: {
                    ASSERT_EQ(row_view.GetInt16Unsafe(i), row_view_exp.GetInt16Unsafe(i)) << " At " << i;
                    break;
                }
                case fesql::type::kFloat: {
                    float act = row_view.GetFloatUnsafe(i);
                    float exp = row_view_exp.GetFloatUnsafe(i);
                    if (IsNaN(exp)) {
                        ASSERT_TRUE(IsNaN(act)) << " At " << i;
                    } else {
                        ASSERT_FLOAT_EQ(act, exp) << " At " << i;
                    }
                    break;
                }
                case fesql::type::kDouble: {
                    double act = row_view.GetDoubleUnsafe(i);
                    double exp = row_view_exp.GetDoubleUnsafe(i);
                    if (IsNaN(exp)) {
                        ASSERT_TRUE(IsNaN(act)) << " At " << i;
                    } else {
                        ASSERT_DOUBLE_EQ(act, exp) << " At " << i;
                    }
                    break;
                }
                case fesql::type::kVarchar: {
                    ASSERT_EQ(row_view.GetStringUnsafe(i), row_view_exp.GetStringUnsafe(i)) << " At " << i;
                    break;
                }
                case fesql::type::kDate: {
                    ASSERT_EQ(row_view.GetDateUnsafe(i), row_view_exp.GetDateUnsafe(i)) << " At " << i;
                    break;
                }
                case fesql::type::kTimestamp: {
                    ASSERT_EQ(row_view.GetTimestampUnsafe(i), row_view_exp.GetTimestampUnsafe(i)) << " At " << i;
                    break;
                }
                case fesql::type::kBool: {
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

void SQLCaseTest::CheckRows(const fesql::vm::Schema &schema, const std::string &order_col,
                            const std::vector<fesql::codec::Row> &rows,
                            std::vector<std::shared_ptr<fesql::sdk::ResultSet>> results) {
    ASSERT_EQ(rows.size(), results.size());
    LOG(INFO) << "Expected Rows: \n";
    PrintRows(schema, rows);
    LOG(INFO) << "ResultSet Rows: \n";
    PrintResultSet(results);
    if (fesql::sqlcase::SQLCase::IS_DEBUG()) {
        PrintResultSetYamlFormat(results);
    }
    LOG(INFO) << "order col key: " << order_col;
    fesql::codec::RowView row_view(schema);
    int order_idx = -1;
    for (int i = 0; i < schema.size(); i++) {
        if (schema.Get(i).name() == order_col) {
            order_idx = i;
            break;
        }
    }
    std::map<std::string, fesql::codec::Row> rows_map;
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
                std::string key = rs->GetAsString(order_idx);
                row_view.Reset(rows_map[key].buf());
            } else {
                row_view.Reset(rows[index++].buf());
            }
            CheckRow(row_view, rs);
        }
    }
}

}  // namespace test
}  // namespace rtidb
