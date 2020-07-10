/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * base_test.h
 *
 * Author: chenjing
 * Date: 2020/7/10
 *--------------------------------------------------------------------------
 **/

#ifndef SRC_TEST_BASE_TEST_H_
#define SRC_TEST_BASE_TEST_H_
#include <vector>

#include "base/texttable.h"
#include "case/sql_case.h"
#include "codec/fe_row_codec.h"
#include "glog/logging.h"
#include "gtest/gtest.h"
#include "gtest/internal/gtest-param-util.h"
#include "proto/fe_common.pb.h"
#include "vm/engine.h"
#define MAX_DEBUG_LINES_CNT 20
#define MAX_DEBUG_COLUMN_CNT 20
namespace rtidb {
namespace test {
class SQLCaseTest : public ::testing::TestWithParam<fesql::sqlcase::SQLCase> {
 public:
    SQLCaseTest() {}
    virtual ~SQLCaseTest() {}
};
std::string FindRtidbDirPath(const std::string &dirname);
std::vector<fesql::sqlcase::SQLCase> InitCases(const std::string &yaml_path);
void InitCases(const std::string &dir_path, const std::string &yaml_path,
               std::vector<fesql::sqlcase::SQLCase> &cases);  // NOLINT
void CheckSchema(const fesql::vm::Schema &schema,
                 const fesql::vm::Schema &exp_schema);
void CheckRows(const fesql::vm::Schema &schema,
               const std::vector<fesql::codec::Row> &rows,
               const std::vector<fesql::codec::Row> &exp_rows);
void PrintRows(const fesql::vm::Schema &schema,
               const std::vector<fesql::codec::Row> &rows);
void PrintSchema(const fesql::vm::Schema &schema);
const std::vector<fesql::codec::Row> SortRows(
    const fesql::vm::Schema &schema, const std::vector<fesql::codec::Row> &rows,
    const std::string &order_col);

void InitCases(const std::string &dir_path, const std::string &yaml_path,
               std::vector<fesql::sqlcase::SQLCase> &cases) {  // NOLINT
    if (!fesql::sqlcase::SQLCase::CreateSQLCasesFromYaml(dir_path, yaml_path,
                                                         cases)) {
        FAIL();
    }
}
std::vector<fesql::sqlcase::SQLCase> InitCases(const std::string &yaml_path) {
    std::vector<fesql::sqlcase::SQLCase> cases;
    InitCases(FindRtidbDirPath("rtidb") + "/fesql/", yaml_path, cases);
    return cases;
}
std::string FindRtidbDirPath(const std::string &dirname) {
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
        LOG(INFO) << "Dir Path is : " << current_path.string() << std::endl;
        return current_path.string();
    }
    return std::string();
}

void CheckSchema(const fesql::vm::Schema &schema,
                 const fesql::vm::Schema &exp_schema) {
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
void PrintSchema(const fesql::vm::Schema &schema) {
    std::ostringstream oss;
    fesql::codec::RowView row_view(schema);
    ::fesql::base::TextTable t('-', '|', '+');
    // Add ColumnName
    for (int i = 0; i < schema.size(); i++) {
        t.add(schema.Get(i).name());
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
void PrintRows(const fesql::vm::Schema &schema,
               const std::vector<fesql::codec::Row> &rows) {
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

const std::vector<fesql::codec::Row> SortRows(
    const fesql::vm::Schema &schema, const std::vector<fesql::codec::Row> &rows,
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
                  [](std::pair<std::string, fesql::codec::Row> &a,
                     std::pair<std::string, fesql::codec::Row> &b) {
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
            sort_rows.push_back(std::make_pair(
                boost::lexical_cast<int64_t>(row_view.GetAsString(idx)), row));
        }
        std::sort(sort_rows.begin(), sort_rows.end(),
                  [](std::pair<int64_t, fesql::codec::Row> &a,
                     std::pair<int64_t, fesql::codec::Row> &b) {
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
void CheckRows(const fesql::vm::Schema &schema,
               const std::vector<fesql::codec::Row> &rows,
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
                    ASSERT_EQ(row_view.GetInt32Unsafe(i),
                              row_view_exp.GetInt32Unsafe(i))
                        << " At " << i;
                    break;
                }
                case fesql::type::kInt64: {
                    ASSERT_EQ(row_view.GetInt64Unsafe(i),
                              row_view_exp.GetInt64Unsafe(i))
                        << " At " << i;
                    break;
                }
                case fesql::type::kInt16: {
                    ASSERT_EQ(row_view.GetInt16Unsafe(i),
                              row_view_exp.GetInt16Unsafe(i))
                        << " At " << i;
                    break;
                }
                case fesql::type::kFloat: {
                    ASSERT_FLOAT_EQ(row_view.GetFloatUnsafe(i),
                                    row_view_exp.GetFloatUnsafe(i))
                        << " At " << i;
                    break;
                }
                case fesql::type::kDouble: {
                    ASSERT_DOUBLE_EQ(row_view.GetDoubleUnsafe(i),
                                     row_view_exp.GetDoubleUnsafe(i))
                        << " At " << i;
                    break;
                }
                case fesql::type::kVarchar: {
                    ASSERT_EQ(row_view.GetStringUnsafe(i),
                              row_view_exp.GetStringUnsafe(i))
                        << " At " << i;
                    break;
                }
                case fesql::type::kDate: {
                    ASSERT_EQ(row_view.GetDateUnsafe(i),
                              row_view_exp.GetDateUnsafe(i))
                        << " At " << i;
                    break;
                }
                case fesql::type::kTimestamp: {
                    ASSERT_EQ(row_view.GetTimestampUnsafe(i),
                              row_view_exp.GetTimestampUnsafe(i))
                        << " At " << i;
                    break;
                }
                case fesql::type::kBool: {
                    ASSERT_EQ(row_view.GetBoolUnsafe(i),
                              row_view_exp.GetBoolUnsafe(i))
                        << " At " << i;
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
}  // namespace test
}  // namespace rtidb
#endif  // SRC_TEST_BASE_TEST_H_
