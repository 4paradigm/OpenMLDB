/*
 * engine_test.cc
 * Copyright (C) 4paradigm.com 2019 wangtaize <wangtaize@4paradigm.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "vm/engine.h"
#include <algorithm>
#include <utility>
#include <vector>
#include "base/texttable.h"
#include "boost/algorithm/string.hpp"
#include "case/sql_case.h"
#include "codec/fe_row_codec.h"
#include "codec/list_iterator_codec.h"
#include "gtest/gtest.h"
#include "gtest/internal/gtest-param-util.h"
#include "llvm/ExecutionEngine/Orc/LLJIT.h"
#include "llvm/IR/Function.h"
#include "llvm/IR/IRBuilder.h"
#include "llvm/IR/InstrTypes.h"
#include "llvm/IR/LegacyPassManager.h"
#include "llvm/IR/Module.h"
#include "llvm/Support/InitLLVM.h"
#include "llvm/Support/TargetSelect.h"
#include "llvm/Support/raw_ostream.h"
#include "llvm/Transforms/AggressiveInstCombine/AggressiveInstCombine.h"
#include "llvm/Transforms/InstCombine/InstCombine.h"
#include "llvm/Transforms/Scalar.h"
#include "llvm/Transforms/Scalar/GVN.h"
#include "parser/parser.h"
#include "plan/planner.h"
#include "vm/test_base.h"
#define MAX_DEBUG_LINES_CNT 20
#define MAX_DEBUG_COLUMN_CNT 20
using namespace llvm;       // NOLINT (build/namespaces)
using namespace llvm::orc;  // NOLINT (build/namespaces)

namespace fesql {
namespace vm {
using fesql::codec::ArrayListV;
using fesql::codec::Row;
using fesql::sqlcase::SQLCase;
enum EngineRunMode { RUNBATCH, RUNONE };

const bool IS_DEBUG = true;
std::vector<SQLCase> InitCases(std::string yaml_path);
void InitCases(std::string yaml_path, std::vector<SQLCase>& cases);  // NOLINT

void InitCases(std::string yaml_path, std::vector<SQLCase>& cases) {  // NOLINT
    if (!SQLCase::CreateSQLCasesFromYaml(
            fesql::sqlcase::FindFesqlDirPath() + "/" + yaml_path, cases)) {
        FAIL();
    }
}
std::vector<SQLCase> InitCases(std::string yaml_path) {
    std::vector<SQLCase> cases;
    InitCases(yaml_path, cases);
    return cases;
}

std::string sqliteStr = "";

int callback(void *NotUsed, int argc, char **argv, char **azColName){
    int i;
    for(i = 0; i < argc; i++){
        sqliteStr +=  NULL == argv[i] ? "NULL" : argv[i];
        sqliteStr += ", ";
        std::cout<<sqliteStr<<std::endl;
    }

    sqliteStr.pop_back();
    sqliteStr.pop_back();
    sqliteStr+="\n";
    return 0;
}

void CheckSchema(const vm::Schema& schema, const vm::Schema& exp_schema);
void CheckRows(const vm::Schema& schema, const std::vector<Row>& rows,
               const std::vector<Row>& exp_rows);
void PrintRows(const vm::Schema& schema, const std::vector<Row>& rows);
void StoreData(::fesql::storage::Table* table, const std::vector<Row>& rows);

void CheckSchema(const vm::Schema& schema, const vm::Schema& exp_schema) {
    ASSERT_EQ(schema.size(), exp_schema.size());
    for (int i = 0; i < schema.size(); i++) {
        ASSERT_EQ(schema.Get(i).DebugString(), exp_schema.Get(i).DebugString());
    }
}
void PrintRows(const vm::Schema& schema, const std::vector<Row>& rows) {
    std::ostringstream oss;
    RowView row_view(schema);
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

const std::vector<Row> SortRows(const vm::Schema& schema,
                                const std::vector<Row>& rows,
                                const std::string& order_col) {
    RowView row_view(schema);
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

    std::vector<std::pair<int64_t, Row>> sort_rows;
    for (auto row : rows) {
        row_view.Reset(row.buf());
        row_view.GetAsString(idx);
        sort_rows.push_back(std::make_pair(
            boost::lexical_cast<int64_t>(row_view.GetAsString(idx)), row));
    }
    std::sort(sort_rows.begin(), sort_rows.end(),
              [](std::pair<int64_t, Row>& a, std::pair<int64_t, Row>& b) {
                  return a.first < b.first;
              });
    std::vector<Row> output_rows;
    for (auto row : sort_rows) {
        output_rows.push_back(row.second);
    }
    return output_rows;
}
void CheckRows(const vm::Schema& schema, const std::vector<Row>& rows,
               const std::vector<Row>& exp_rows) {
    LOG(INFO) << "expect result:\n";
    PrintRows(schema, exp_rows);
    LOG(INFO) << "real result:\n";
    PrintRows(schema, rows);
    ASSERT_EQ(rows.size(), exp_rows.size());
    RowView row_view(schema);
    RowView row_view_exp(schema);
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
void StoreData(::fesql::storage::Table* table, const std::vector<Row>& rows) {
    ASSERT_TRUE(table->Init());
    for (auto row : rows) {
        ASSERT_TRUE(table->Put(reinterpret_cast<char*>(row.buf()), row.size()));
    }
}

class EngineTest : public ::testing::TestWithParam<SQLCase> {
 public:
    EngineTest() {}
    virtual ~EngineTest() {}
};
const std::vector<Row> SortRows(const vm::Schema& schema,
                                const std::vector<Row>& rows,
                                const std::string& order_col);
void RequestModeCheck(SQLCase& sql_case) {  // NOLINT
    int32_t input_cnt = sql_case.CountInputs();
    // Init catalog
    std::map<std::string, std::shared_ptr<::fesql::storage::Table>>
        name_table_map;
    auto catalog = BuildCommonCatalog();
    for (int32_t i = 0; i < input_cnt; i++) {
        type::TableDef table_def;
        sql_case.ExtractInputTableDef(table_def, i);
        std::shared_ptr<::fesql::storage::Table> table(
            new ::fesql::storage::Table(i + 1, 1, table_def));
        name_table_map[table_def.name()] = table;
        ASSERT_TRUE(AddTable(catalog, table_def, table));
    }

    // Init engine and run session
    std::cout << sql_case.sql_str() << std::endl;
    base::Status get_status;

    Engine engine(catalog);
    RequestRunSession session;
    if (IS_DEBUG) {
        session.EnableDebug();
    }

    bool ok =
        engine.Get(sql_case.sql_str(), sql_case.db(), session, get_status);
    ASSERT_TRUE(ok);

    const std::string& request_name = session.GetRequestName();
    std::vector<Row> request_data;
    for (int32_t i = 0; i < input_cnt; i++) {
        auto input = sql_case.inputs()[i];
        if (input.name_ == request_name) {
            ASSERT_TRUE(sql_case.ExtractInputData(request_data, i));
            continue;
        }
        std::vector<Row> rows;
        sql_case.ExtractInputData(rows, i);
        if (!rows.empty()) {
            StoreData(name_table_map[input.name_].get(), rows);
        }
    }

    int32_t ret = -1;
    DLOG(INFO) << "RUN IN MODE REQUEST";
    std::vector<Row> output;
    vm::Schema schema;
    schema = session.GetSchema();
    PrintSchema(schema);


    std::ostringstream oss;
    session.GetPhysicalPlan()->Print(oss, "");
    LOG(INFO) << "physical plan:\n" << oss.str() << std::endl;

    if (!sql_case.request_plan().empty()) {
        ASSERT_EQ(oss.str(), sql_case.request_plan());
    }

    std::ostringstream runner_oss;
    session.GetRunner()->Print(runner_oss, "");
    LOG(INFO) << "runner plan:\n" << runner_oss.str() << std::endl;

    // Check Output Schema
    std::vector<Row> case_output_data;
    type::TableDef case_output_table;
    ASSERT_TRUE(sql_case.ExtractOutputData(case_output_data));
    ASSERT_TRUE(sql_case.ExtractOutputSchema(case_output_table));
    CheckSchema(schema, case_output_table.columns());

    // Check Output Data
    auto request_table = name_table_map[request_name];
    ASSERT_TRUE(request_table->Init());
    for (auto in_row : request_data) {
        Row out_row;
        ret = session.Run(in_row, &out_row);
        ASSERT_EQ(0, ret);
        ASSERT_TRUE(request_table->Put(
            reinterpret_cast<const char*>(in_row.buf()), in_row.size()));
        output.push_back(out_row);
    }

    CheckRows(schema, output, case_output_data);
}

void BatchModeCheck(SQLCase& sql_case) {  // NOLINT
    int32_t input_cnt = sql_case.CountInputs();

    // Init catalog
    std::map<std::string, std::shared_ptr<::fesql::storage::Table>>
        name_table_map;
    auto catalog = BuildCommonCatalog();
    for (int32_t i = 0; i < input_cnt; i++) {
        type::TableDef table_def;
        sql_case.ExtractInputTableDef(table_def, i);
        std::shared_ptr<::fesql::storage::Table> table(
            new ::fesql::storage::Table(i + 1, 1, table_def));
        name_table_map[table_def.name()] = table;
        ASSERT_TRUE(AddTable(catalog, table_def, table));
    }

    // Init engine and run session
    std::cout << sql_case.sql_str() << std::endl;
    base::Status get_status;

    Engine engine(catalog);
    BatchRunSession session;
    if (IS_DEBUG) {
        session.EnableDebug();
    }

    bool ok =
        engine.Get(sql_case.sql_str(), sql_case.db(), session, get_status);
    ASSERT_TRUE(ok);
    std::vector<Row> request_data;
    for (int32_t i = 0; i < input_cnt; i++) {
        auto input = sql_case.inputs()[i];
        std::vector<Row> rows;
        sql_case.ExtractInputData(rows, i);
        if (!rows.empty()) {
            StoreData(name_table_map[input.name_].get(), rows);
        }
    }

    DLOG(INFO) << "RUN IN MODE BATCH";
    vm::Schema schema;
    schema = session.GetSchema();
    PrintSchema(schema);
    std::ostringstream oss;
    session.GetPhysicalPlan()->Print(oss, "");
    LOG(INFO) << "physical plan:\n" << oss.str() << std::endl;

    if (!sql_case.batch_plan().empty()) {
        ASSERT_EQ(oss.str(), sql_case.batch_plan());
    }

    std::ostringstream runner_oss;
    session.GetRunner()->Print(runner_oss, "");
    LOG(INFO) << "runner plan:\n" << runner_oss.str() << std::endl;

    // Check Output Schema
    std::vector<Row> case_output_data;
    type::TableDef case_output_table;
    ASSERT_TRUE(sql_case.ExtractOutputData(case_output_data));
    ASSERT_TRUE(sql_case.ExtractOutputSchema(case_output_table));
    CheckSchema(schema, case_output_table.columns());

    // Check Output Data
    std::vector<Row> output;
    ASSERT_EQ(0, session.Run(output));
    CheckRows(schema, SortRows(schema, output, sql_case.output().order_),
              case_output_data);

    /* Compare with SQLite*/
    
    //Determine whether to compare with SQLite
    if (sql_case.standard_sql()) {

        // Use SQLite to get output
        sqlite3 *db;
        char *zErrMsg = 0;
        int rc;

        // Create database in the memory
        rc = sqlite3_open(":memory:", &db);
        if( rc ){
            fprintf(stderr, "Can't open database: %s\n", sqlite3_errmsg(db));
            exit(0);
        }else{
            fprintf(stderr, "Database Create successfully\n");
        }

        // Create SQL statement to create a table schema
        type::TableDef output_table;
        sql_case.ExtractInputTableDef(output_table);
        std::string create_table_sql;
        SQLCase::BuildCreateSQLFromSchema(output_table, &create_table_sql, false);
        //LOG(INFO) << create_table_sql;

        //Create a table schema
        const char* create_table_sql_ch = create_table_sql.c_str();
        rc = sqlite3_exec(db, create_table_sql_ch, callback, 0, &zErrMsg);
        if( rc != SQLITE_OK ){
        fprintf(stderr, "SQL error: %s\n", zErrMsg);
            sqlite3_free(zErrMsg);
        }else{
            fprintf(stdout, "Table schema created successfully\n");
        }

        //Create SQL statements to insert data to the table (One insert)
        std::string create_insert_sql = "";
        std::vector<std::string> data_line;
        SQLCase::BuildInsertSQLFromMultipleRows(output_table, sql_case.inputs()[0].data_, &create_insert_sql);

        // Insert data into the table
        const char* create_insert_sql_ch = create_insert_sql.c_str();

        std::cout << create_insert_sql_ch <<std::endl;
        rc = sqlite3_exec(db, create_insert_sql_ch, callback, 0, &zErrMsg);
        if( rc != SQLITE_OK ){
            fprintf(stderr, "SQL error: %s\n", zErrMsg);
            sqlite3_free(zErrMsg);
        }else{
            fprintf(stdout, "Records created successfully\n");
        }

        // Execute SQL statement 
        const char* create_execute_sql_ch = sql_case.sql_str().c_str();
        
        sqliteStr = "";
        
        rc = sqlite3_exec(db, create_execute_sql_ch, callback, 0, &zErrMsg);
        if( rc != SQLITE_OK ){
            fprintf(stderr, "SQL error: %s\n", zErrMsg);
            sqlite3_free(zErrMsg);
        }else{
            fprintf(stdout, "Operation done successfully\n");
        }
        sqlite3_close(db);

        sqliteStr.pop_back();

        // Transfer Sqlite outcome to Fesql row
        std::vector<fesql::codec::Row> sqliteRows;
        SQLCase::ExtractRows(schema, sqliteStr, sqliteRows);

        // Compare Fesql output with SQLite output. 
        CheckRows(schema, SortRows(schema, sqliteRows, sql_case.output().order_),
              SortRows(schema, output, sql_case.output().order_));
    }          
}

INSTANTIATE_TEST_CASE_P(
    EngineSimpleQuery, EngineTest,
    testing::ValuesIn(InitCases("/cases/query/simple_query.yaml")));
INSTANTIATE_TEST_CASE_P(
    EngineUdfQuery, EngineTest,
    testing::ValuesIn(InitCases("/cases/query/udf_query.yaml")));
INSTANTIATE_TEST_CASE_P(
    EngineUdafQuery, EngineTest,
    testing::ValuesIn(InitCases("/cases/query/udaf_query.yaml")));
INSTANTIATE_TEST_CASE_P(
    EngineExtreamQuery, EngineTest,
    testing::ValuesIn(InitCases("/cases/query/extream_query.yaml")));

INSTANTIATE_TEST_CASE_P(
    EngineLastJoinQuery, EngineTest,
    testing::ValuesIn(InitCases("/cases/query/last_join_query.yaml")));

INSTANTIATE_TEST_CASE_P(
    EngineLastJoinWindowQuery, EngineTest,
    testing::ValuesIn(InitCases("/cases/query/last_join_window_query.yaml")));

INSTANTIATE_TEST_CASE_P(
    EngineRequestLastJoinWindowQuery, EngineTest,
    testing::ValuesIn(InitCases("/cases/query/last_join_window_query.yaml")));

INSTANTIATE_TEST_CASE_P(
    EngineWindowQuery, EngineTest,
    testing::ValuesIn(InitCases("/cases/query/window_query.yaml")));

INSTANTIATE_TEST_CASE_P(
    EngineWindowWithUnionQuery, EngineTest,
    testing::ValuesIn(InitCases("/cases/query/window_with_union_query.yaml")));

INSTANTIATE_TEST_CASE_P(
    EngineBatchGroupQuery, EngineTest,
    testing::ValuesIn(InitCases("/cases/query/group_query.yaml")));

TEST_P(EngineTest, test_request_engine) {
    ParamType sql_case = GetParam();
    LOG(INFO) << sql_case.desc();
    if (!boost::contains(sql_case.mode(), "request-unsupport")) {
        RequestModeCheck(sql_case);
    }
}
TEST_P(EngineTest, test_batch_engine) {
    ParamType sql_case = GetParam();
    LOG(INFO) << sql_case.desc();
    if (!boost::contains(sql_case.mode(), "batch-unsupport")) {
        BatchModeCheck(sql_case);
    }
}

TEST_F(EngineTest, EngineCompileOnlyTest) {
    const fesql::base::Status exp_status(::fesql::common::kOk, "ok");

    fesql::type::TableDef table_def;
    fesql::type::TableDef table_def2;

    BuildTableDef(table_def);
    BuildTableDef(table_def2);

    table_def.set_name("t1");
    table_def2.set_name("t2");

    std::shared_ptr<::fesql::storage::Table> table(
        new ::fesql::storage::Table(1, 1, table_def));
    std::shared_ptr<::fesql::storage::Table> table2(
        new ::fesql::storage::Table(2, 1, table_def2));
    ::fesql::type::IndexDef* index = table_def.add_indexes();
    index->set_name("index12");
    index->add_first_keys("col1");
    index->add_first_keys("col2");
    index->set_second_key("col5");
    auto catalog = BuildCommonCatalog(table_def, table);
    AddTable(catalog, table_def2, table2);

    {
        std::vector<std::string> sql_str_list = {
            "SELECT t1.COL1, t1.COL2, t2.COL1, t2.COL2 FROM t1 full join t2 on "
            "t1.col1 = t2.col2;",
            "SELECT t1.COL1, t1.COL2, t2.COL1, t2.COL2 FROM t1 left join t2 on "
            "t1.col1 = t2.col2;",
            "SELECT t1.COL1, t1.COL2, t2.COL1, t2.COL2 FROM t1 right join t2 "
            "on "
            "t1.col1 = t2.col2;",
            "SELECT t1.COL1, t1.COL2, t2.COL1, t2.COL2 FROM t1 last join t2 "
            "order by t2.col5 on t1.col1 = t2.col2;"};
        EngineOptions options;
        options.set_compile_only(true);
        Engine engine(catalog, options);
        base::Status get_status;
        for (auto sqlstr : sql_str_list) {
            boost::to_lower(sqlstr);
            LOG(INFO) << sqlstr;
            std::cout << sqlstr << std::endl;
            BatchRunSession session;
            ASSERT_TRUE(
                engine.Get(sqlstr, table_def.catalog(), session, get_status));
        }
    }

    {
        std::vector<std::string> sql_str_list = {
            "SELECT t1.COL1, t1.COL2, t2.COL1, t2.COL2 FROM t1 full join t2 on "
            "t1.col1 = t2.col2;",
            "SELECT t1.COL1, t1.COL2, t2.COL1, t2.COL2 FROM t1 left join t2 on "
            "t1.col1 = t2.col2;",
            "SELECT t1.COL1, t1.COL2, t2.COL1, t2.COL2 FROM t1 right join t2 "
            "on "
            "t1.col1 = t2.col2;"};
        EngineOptions options;
        Engine engine(catalog, options);
        base::Status get_status;
        for (auto sqlstr : sql_str_list) {
            boost::to_lower(sqlstr);
            LOG(INFO) << sqlstr;
            std::cout << sqlstr << std::endl;
            BatchRunSession session;
            ASSERT_FALSE(
                engine.Get(sqlstr, table_def.catalog(), session, get_status));
        }
    }
}
}  // namespace vm
}  // namespace fesql

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    InitializeNativeTarget();
    InitializeNativeTargetAsmPrinter();
    return RUN_ALL_TESTS();
}
/*
    // Callback function in "SELECT"
    static int callback(void *data, int argc, char **argv, char **azColName){
        int i;
        fprintf(stderr, "%s: ", (const char*)data);
        for(i=0; i<argc; i++){
            printf("%s = %s\n", azColName[i], argv[i] ? argv[i] : "NULL");
        }
        printf("\n");
        return 0;
    }
    
    // Determine whether to compare with SQLite
    if (sql_case.standard_sql_) {

        // Use SQLite to get output
        sqlite3 *db;
        char *zErrMsg = 0;
        int rc;
        char const *table;
        char const *sql;
        char const *sql3;
        const char* data = "Callback function called";

        // Create database in memory
        rc = sqlite3_open(":memory:", &db);
        if( rc ){
            fprintf(stderr, "Can't open database: %s\n", sqlite3_errmsg(db));
            exit(0);
        }else{
            fprintf(stderr, "Opened database successfully\n");
        }

        table = ;

        sql = "SELECT * from COMPANY";

        // Insert data into database
        rc = sqlite3_exec(db, table, callback, (void*)data, &zErrMsg);
        if( rc != SQLITE_OK ){
            fprintf(stderr, "SQL error: %s\n", zErrMsg);
            sqlite3_free(zErrMsg);
        }else{
            fprintf(stdout, "1 Operation done successfully\n");
        }

        // Execute SQL statement 
        rc = sqlite3_exec(db, sql, callback, (void*)data, &zErrMsg);
        if( rc != SQLITE_OK ){
            fprintf(stderr, "SQL error: %s\n", zErrMsg);
            sqlite3_free(zErrMsg);
        }else{
            fprintf(stdout, "2 Operation done successfully\n");
        }

        sqlite3_close(db);

    }*/