/*
 * engine_test_base.cc.c
 * Copyright (C) 4paradigm 2021 chenjing <chenjing@4paradigm.com>
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
#include "testing/engine_test_base.h"

#include <utility>

#include "absl/cleanup/cleanup.h"
#include "absl/strings/ascii.h"
#include "absl/time/clock.h"
#include "base/texttable.h"
#include "google/protobuf/util/message_differencer.h"
#include "plan/plan_api.h"
#include "vm/sql_compiler.h"
namespace hybridse {
namespace vm {

bool IsNaN(float x) { return x != x; }
bool IsNaN(double x) { return x != x; }

void CheckSchema(const codec::Schema& schema, const codec::Schema& exp_schema) {
    ASSERT_EQ(schema.size(), exp_schema.size());
    ::google::protobuf::util::MessageDifferencer differ;
    // approximate equal for float values
    differ.set_float_comparison(::google::protobuf::util::MessageDifferencer::FloatComparison::APPROXIMATE);
    // equivalent avoid the issue that some optional bool fields that may contains a default value
    differ.set_message_field_comparison(
        ::google::protobuf::util::MessageDifferencer::MessageFieldComparison::EQUIVALENT);
    for (int i = 0; i < schema.size(); i++) {
        std::string diff_str;
        differ.ReportDifferencesToString(&diff_str);
        ASSERT_TRUE(differ.Compare(schema.Get(i), exp_schema.Get(i)))
            << "Fail column type at " << i
            << "\ngot: " << schema.Get(i).ShortDebugString()
            << "\nbut expect: " << exp_schema.Get(i).ShortDebugString()
            << "\ndifference: " << diff_str;
    }
}

std::string YamlTypeName(type::Type type) { return hybridse::sqlcase::SqlCase::TypeString(type); }

// 打印符合yaml测试框架格式的预期结果
void PrintYamlResult(const vm::Schema& schema, const std::vector<Row>& rows) {
    std::ostringstream oss;
    oss << "columns: [";
    for (int i = 0; i < schema.size(); i++) {
        auto col = schema.Get(i);
        oss << "\"" << col.name() << " " << YamlTypeName(col.type()) << "\"";
        if (i + 1 != schema.size()) {
            oss << ", ";
        }
    }
    oss << " ]\n";
    oss << "\nrows:\n";
    RowView row_view(schema);
    for (auto row : rows) {
        row_view.Reset(row.buf());
        oss << " - [";
        for (int idx = 0; idx < schema.size(); idx++) {
            std::string str = row_view.GetAsString(idx);
            oss << str;
            if (idx + 1 != schema.size()) {
                oss << ", ";
            }
        }
        oss << "]\n";
    }
    LOG(INFO) << "\n" << oss.str() << "\n";
}
void PrintRows(const vm::Schema& schema, const std::vector<Row>& rows) {
    std::ostringstream oss;
    RowView row_view(schema);
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
        oss << t << std::endl;
        LOG(INFO) << "\n" << oss.str() << "\n";
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

const std::vector<Row> SortRows(const vm::Schema& schema, const std::vector<Row>& rows, const std::string& order_col) {
    DLOG(INFO) << "sort rows start";
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

    if (schema.Get(idx).type() == hybridse::type::kVarchar) {
        std::vector<std::pair<std::string, Row>> sort_rows;
        for (auto row : rows) {
            row_view.Reset(row.buf());
            row_view.GetAsString(idx);
            sort_rows.push_back(std::make_pair(row_view.GetAsString(idx), row));
        }
        std::sort(sort_rows.begin(), sort_rows.end(),
                  [](std::pair<std::string, Row>& a, std::pair<std::string, Row>& b) { return a.first < b.first; });
        std::vector<Row> output_rows;
        for (auto row : sort_rows) {
            output_rows.push_back(row.second);
        }
        DLOG(INFO) << "sort rows done!";
        return output_rows;
    } else {
        std::vector<std::pair<int64_t, Row>> sort_rows;
        for (auto row : rows) {
            row_view.Reset(row.buf());
            row_view.GetAsString(idx);
            sort_rows.push_back(std::make_pair(boost::lexical_cast<int64_t>(row_view.GetAsString(idx)), row));
        }
        std::sort(sort_rows.begin(), sort_rows.end(),
                  [](std::pair<int64_t, Row>& a, std::pair<int64_t, Row>& b) { return a.first < b.first; });
        std::vector<Row> output_rows;
        for (auto row : sort_rows) {
            output_rows.push_back(row.second);
        }
        DLOG(INFO) << "sort rows done!";
        return output_rows;
    }
}

void CheckRows(const vm::Schema& schema, const std::vector<Row>& rows, const std::vector<Row>& exp_rows) {
    ASSERT_EQ(rows.size(), exp_rows.size());
    RowView row_view(schema);
    RowView row_view_exp(schema);
    for (size_t row_index = 0; row_index < rows.size(); row_index++) {
        ASSERT_TRUE(nullptr != rows[row_index].buf());
        row_view.Reset(rows[row_index].buf());
        row_view_exp.Reset(exp_rows[row_index].buf());
        for (int i = 0; i < schema.size(); i++) {
            if (row_view_exp.IsNULL(i)) {
                ASSERT_TRUE(row_view.IsNULL(i)) << " At " << i << schema.Get(i).name();
                continue;
            }
            ASSERT_FALSE(row_view.IsNULL(i)) << " At " << i;
            switch (schema.Get(i).type()) {
                case hybridse::type::kInt32: {
                    ASSERT_EQ(row_view.GetInt32Unsafe(i), row_view_exp.GetInt32Unsafe(i))
                        << " At " << i << " " << schema.Get(i).name();
                    break;
                }
                case hybridse::type::kInt64: {
                    ASSERT_EQ(row_view.GetInt64Unsafe(i), row_view_exp.GetInt64Unsafe(i))
                        << " At " << i << " " << schema.Get(i).name();
                    break;
                }
                case hybridse::type::kInt16: {
                    ASSERT_EQ(row_view.GetInt16Unsafe(i), row_view_exp.GetInt16Unsafe(i))
                        << " At " << i << " " << schema.Get(i).name();
                    break;
                }
                case hybridse::type::kFloat: {
                    float act = row_view.GetFloatUnsafe(i);
                    float exp = row_view_exp.GetFloatUnsafe(i);
                    if (IsNaN(exp)) {
                        ASSERT_TRUE(IsNaN(act)) << " At " << i << " " << schema.Get(i).name();
                    } else {
                        ASSERT_FLOAT_EQ(act, exp) << " At " << i << " " << schema.Get(i).name();
                    }
                    break;
                }
                case hybridse::type::kDouble: {
                    double act = row_view.GetDoubleUnsafe(i);
                    double exp = row_view_exp.GetDoubleUnsafe(i);
                    if (IsNaN(exp)) {
                        ASSERT_TRUE(IsNaN(act)) << " At " << i << " " << schema.Get(i).name();
                    } else {
                        ASSERT_DOUBLE_EQ(act, exp) << " At " << i << " " << schema.Get(i).name();
                    }
                    break;
                }
                case hybridse::type::kVarchar: {
                    ASSERT_EQ(row_view.GetStringUnsafe(i), row_view_exp.GetStringUnsafe(i))
                        << " At " << i << " " << schema.Get(i).name();
                    break;
                }
                case hybridse::type::kDate: {
                    ASSERT_EQ(row_view.GetDateUnsafe(i), row_view_exp.GetDateUnsafe(i))
                        << " At " << i << " " << schema.Get(i).name();
                    break;
                }
                case hybridse::type::kTimestamp: {
                    ASSERT_EQ(row_view.GetTimestampUnsafe(i), row_view_exp.GetTimestampUnsafe(i))
                        << " At " << i << " " << schema.Get(i).name();
                    break;
                }
                case hybridse::type::kBool: {
                    ASSERT_EQ(row_view.GetBoolUnsafe(i), row_view_exp.GetBoolUnsafe(i))
                        << " At " << i << " " << schema.Get(i).name();
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

const std::string GenerateTableName(int32_t id) { return "auto_t" + std::to_string(id); }

void DoEngineCheckExpect(const SqlCase& sql_case, std::shared_ptr<RunSession> session, const std::vector<Row>& output) {
    if (sql_case.expect().count_ >= 0) {
        ASSERT_EQ(static_cast<size_t>(sql_case.expect().count_), output.size());
    }
    const Schema& schema = session->GetSchema();
    std::vector<Row> sorted_output;

    bool is_batch_request = session->engine_mode() == kBatchRequestMode;
    if (is_batch_request) {
        const auto& sql_ctx = std::dynamic_pointer_cast<SqlCompileInfo>(session->GetCompileInfo())->get_sql_context();
        const auto& output_common_column_indices = sql_ctx.batch_request_info.output_common_column_indices;
        if (!output_common_column_indices.empty() &&
            output_common_column_indices.size() != static_cast<size_t>(schema.size()) &&
            sql_ctx.is_batch_request_optimized) {
            DLOG(INFO) << "Reorder batch request outputs for non-trival columns";

            auto& expect_common_column_indices = sql_case.expect().common_column_indices_;
            if (!expect_common_column_indices.empty()) {
                ASSERT_EQ(expect_common_column_indices, output_common_column_indices);
            }

            std::vector<Row> reordered;
            std::vector<std::pair<size_t, size_t>> select_indices;
            size_t common_col_idx = 0;
            size_t non_common_col_idx = 0;
            auto plan = sql_ctx.physical_plan;
            for (size_t i = 0; i < plan->GetOutputSchemaSize(); ++i) {
                if (output_common_column_indices.find(i) != output_common_column_indices.end()) {
                    select_indices.push_back(std::make_pair(0, common_col_idx));
                    common_col_idx += 1;
                } else {
                    select_indices.push_back(std::make_pair(1, non_common_col_idx));
                    non_common_col_idx += 1;
                }
            }
            codec::RowSelector selector(
                {plan->GetOutputSchemaSource(0)->GetSchema(), plan->GetOutputSchemaSource(1)->GetSchema()},
                select_indices);
            for (const auto& row : output) {
                int8_t* reordered_buf = nullptr;
                size_t reordered_size;
                ASSERT_TRUE(selector.Select(row, &reordered_buf, &reordered_size));
                reordered.push_back(Row(codec::RefCountedSlice::Create(reordered_buf, reordered_size)));
            }
            sorted_output = reordered;
        } else {
            sorted_output = SortRows(schema, output, sql_case.expect().order_);
        }
    } else {
        sorted_output = SortRows(schema, output, sql_case.expect().order_);
    }
    if (sql_case.expect().schema_.empty() && sql_case.expect().columns_.empty()) {
        LOG(INFO) << "Expect result columns empty, Real result:\n";
        PrintRows(schema, sorted_output);
        PrintYamlResult(schema, sorted_output);
    } else {
        // Check Output Schema
        type::TableDef case_output_table;
        ASSERT_TRUE(sql_case.ExtractOutputSchema(case_output_table));
        ASSERT_NO_FATAL_FAILURE(CheckSchema(schema, case_output_table.columns()));

        LOG(INFO) << "Real result:\n";
        PrintRows(schema, sorted_output);

        std::vector<Row> case_output_data;
        ASSERT_TRUE(sql_case.ExtractOutputData(case_output_data));
        // for batch request mode, trivally compare last result
        if (is_batch_request && sql_case.batch_request().columns_.empty()) {
            if (!case_output_data.empty()) {
                case_output_data = {case_output_data.back()};
            }
        }

        LOG(INFO) << "Expect result:\n";
        PrintRows(schema, case_output_data);
        PrintYamlResult(schema, sorted_output);

        ASSERT_NO_FATAL_FAILURE(CheckRows(schema, sorted_output, case_output_data));
    }
}

Status EngineTestRunner::ExtractTableInfoFromCreateString(const std::string& create,
                                                          sqlcase::SqlCase::TableInfo* table_info) {
    CHECK_TRUE(table_info != nullptr, common::kNullPointer, "Fail extract with null table info");
    CHECK_TRUE(!create.empty(), common::kTestEngineError, "Fail extract with empty create string");

    node::NodeManager manager;
    base::Status status;
    node::PlanNodeList plan_trees;
    CHECK_TRUE(plan::PlanAPI::CreatePlanTreeFromScript(create, plan_trees, &manager, status), common::kPlanError,
               "Fail to resolve logical plan", status.msg);
    CHECK_TRUE(1u == plan_trees.size(), common::kPlanError, "Fail to extract table info with multi logical plan tree");
    CHECK_TRUE(nullptr != plan_trees[0] && node::kPlanTypeCreate == plan_trees[0]->type_, common::kPlanError,
               "Fail to extract table info with invalid SQL, CREATE SQL is required");
    node::CreatePlanNode* create_plan = dynamic_cast<node::CreatePlanNode*>(plan_trees[0]);
    table_info->name_ = create_plan->GetTableName();
    CHECK_TRUE(create_plan->ExtractColumnsAndIndexs(table_info->columns_, table_info->indexs_), common::kPlanError,
               "Invalid Create Plan Node");
    std::ostringstream oss;
    oss << "name: " << table_info->name_ << "\n";
    oss << "columns: [";
    for (auto column : table_info->columns_) {
        oss << column << ",";
    }
    oss << "]\n";
    oss << "indexs: [";
    for (auto index : table_info->indexs_) {
        oss << index << ",";
    }
    oss << "]\n";
    LOG(INFO) << oss.str();
    return Status::OK();
}
Status EngineTestRunner::PrepareParameter() {
    if (!sql_case_.parameters().columns_.empty()) {
        CHECK_TRUE(sql_case_.parameters().rows_.size() <= 1, common::kUnSupport,
                   "Multiple parameter-rows aren't supported currently");
        this->parameter_schema_ = sql_case_.ExtractParameterTypes();
        CHECK_TRUE(parameter_schema_.size() > 0, common::kUnSupport, "Invalid parameter schema!")
        CHECK_TRUE(sql_case_.ExtractRows(parameter_schema_, sql_case_.parameters().rows_, this->parameter_rows_),
                   kTestEngineError, "Extract case parameters rows failed");
        CHECK_TRUE(this->parameter_rows_.size() <= 1, common::kUnSupport,
                   "Multiple parameter-rows aren't supported currently");
    }
    return base::Status::OK();
}
void EngineTestRunner::InitSqlCase() {
    for (size_t idx = 0; idx < sql_case_.inputs_.size(); idx++) {
        if (!sql_case_.inputs_[idx].create_.empty()) {
            auto status = ExtractTableInfoFromCreateString(sql_case_.inputs_[idx].create_, &sql_case_.inputs_[idx]);
            ASSERT_TRUE(status.isOK()) << status;
        }
    }

    if (!sql_case_.batch_request_.create_.empty()) {
        auto status = ExtractTableInfoFromCreateString(sql_case_.batch_request_.create_, &sql_case_.batch_request_);
        ASSERT_TRUE(status.isOK()) << status;
    }
}

Status EngineTestRunner::Compile() {
    CHECK_TRUE(engine_ != nullptr, common::kTestEngineError, "Engine is not init");
    CHECK_STATUS(PrepareParameter())
    std::string sql_str = sql_case_.sql_str();
    for (int j = 0; j < sql_case_.CountInputs(); ++j) {
        std::string placeholder = "{" + std::to_string(j) + "}";
        boost::replace_all(sql_str, placeholder, sql_case_.inputs_[j].name_);
    }
    DLOG(INFO) << "Compile SQL:\n" << sql_str;
    CHECK_TRUE(session_ != nullptr, common::kTestEngineError, "Session is not set");
    if (hybridse::sqlcase::SqlCase::IsDebug() || sql_case_.debug()) {
        session_->EnableDebug();
    }
    if (session_->engine_mode() == kBatchMode) {
        auto batch_session =
            std::dynamic_pointer_cast<BatchRunSession>(session_);
        batch_session->SetParameterSchema(parameter_schema_);
    } else {
        CHECK_TRUE(parameter_schema_.empty(), common::kUnSupport,
                   "Request or BatchRequest mode do not support parameterized query currently")
    }

    base::Status status;
    bool ok = false;
    {
        absl::Time start = absl::Now();
        absl::Cleanup clean = [&start]() { DLOG(INFO) << "compile takes " << absl::Now() - start; };
        ok = engine_->Get(sql_str, sql_case_.db(), *session_, status);
    }

    if (!ok || !status.isOK()) {
        DLOG(INFO) << status;
        if (!sql_case_.expect().msg_.empty()) {
            EXPECT_EQ(absl::StripAsciiWhitespace(sql_case_.expect().msg_), status.msg);
        }
        return_code_ = ENGINE_TEST_RET_COMPILE_ERROR;
    } else {
        DLOG(INFO) << "SQL output schema:";
        std::ostringstream oss;
        std::dynamic_pointer_cast<SqlCompileInfo>(session_->GetCompileInfo())->GetPhysicalPlan()->Print(oss, "");
        DLOG(INFO) << "Physical plan:\n" << oss.str();

        std::ostringstream runner_oss;
        std::dynamic_pointer_cast<SqlCompileInfo>(session_->GetCompileInfo())->GetClusterJob()->Print(runner_oss, "");
        DLOG(INFO) << "Runner plan:\n" << runner_oss.str();
    }
    return status;
}

void EngineTestRunner::RunCheck() {
    bool ok = InitEngineCatalog();
    if (!ok) {
        return_code_ = ENGINE_TEST_INIT_CATALOG_ERROR;
        FAIL() << "Engine Test Init Catalog Error";
        return;
    }
    auto engine_mode = session_->engine_mode();
    Status status = Compile();
    ASSERT_EQ(sql_case_.expect().success_, status.isOK());
    if (!status.isOK()) {
        return_code_ = ENGINE_TEST_RET_COMPILE_ERROR;
        return;
    }
    std::ostringstream oss;
    std::dynamic_pointer_cast<SqlCompileInfo>(session_->GetCompileInfo())->GetPhysicalPlan()->Print(oss, "");
    if (!sql_case_.batch_plan().empty() && engine_mode == kBatchMode) {
        ASSERT_EQ(oss.str(), sql_case_.batch_plan());
    } else if (!sql_case_.cluster_request_plan().empty() && engine_mode == kRequestMode &&
               options_.IsClusterOptimzied()) {
        ASSERT_EQ(oss.str(), sql_case_.cluster_request_plan());
    } else if (!sql_case_.request_plan().empty() && engine_mode == kRequestMode && !options_.IsClusterOptimzied()) {
        ASSERT_EQ(oss.str(), sql_case_.request_plan());
    }
    status = PrepareData();
    ASSERT_TRUE(status.isOK()) << "Prepare data error: " << status;
    if (!status.isOK()) {
        return;
    }
    std::vector<Row> output_rows;
    status = Compute(&output_rows);
    ASSERT_TRUE(status.isOK()) << "Session run error: " << status;
    if (!status.isOK()) {
        return_code_ = ENGINE_TEST_RET_EXECUTION_ERROR;
        return;
    }
    ASSERT_NO_FATAL_FAILURE(DoEngineCheckExpect(sql_case_, session_, output_rows));
    return_code_ = ENGINE_TEST_RET_SUCCESS;
}

void EngineTestRunner::RunBenchmark(size_t iters) {
    if (!InitEngineCatalog()) {
        LOG(ERROR) << "Engine Test Init Catalog Error";
        return;
    }
    auto engine_mode = session_->engine_mode();
    if (engine_mode == kRequestMode) {
        LOG(WARNING) << "Request mode case can not properly run many times";
        return;
    }
    Status status = Compile();
    if (!status.isOK()) {
        LOG(WARNING) << "Compile error: " << status;
        return;
    }
    status = PrepareData();
    if (!status.isOK()) {
        LOG(WARNING) << "Prepare data error: " << status;
        return;
    }

    std::vector<Row> output_rows;
    status = Compute(&output_rows);
    if (!status.isOK()) {
        LOG(WARNING) << "Run error: " << status;
        return;
    }
    ASSERT_NO_FATAL_FAILURE(DoEngineCheckExpect(sql_case_, session_, output_rows));

    struct timeval st;
    struct timeval et;
    gettimeofday(&st, nullptr);
    for (size_t i = 0; i < iters; ++i) {
        output_rows.clear();
        status = Compute(&output_rows);
        if (!status.isOK()) {
            LOG(WARNING) << "Run error at " << i << "th iter: " << status;
            return;
        }
    }
    gettimeofday(&et, nullptr);
    if (iters != 0) {
        double mill = (et.tv_sec - st.tv_sec) * 1000 + (et.tv_usec - st.tv_usec) / 1000.0;
        printf("Engine run take approximately %.5f ms per run\n", mill / iters);
    }
}

GTEST_ALLOW_UNINSTANTIATED_PARAMETERIZED_TEST(EngineTest);
GTEST_ALLOW_UNINSTANTIATED_PARAMETERIZED_TEST(BatchRequestEngineTest);

INSTANTIATE_TEST_SUITE_P(EngineTestBug, EngineTest,
                         testing::ValuesIn(sqlcase::InitCases("cases/debug/bug.yaml")));
INSTANTIATE_TEST_SUITE_P(EngineFailQuery, EngineTest,
                        testing::ValuesIn(sqlcase::InitCases("cases/query/fail_query.yaml")));

INSTANTIATE_TEST_SUITE_P(EngineTestFzTest, EngineTest,
                        testing::ValuesIn(sqlcase::InitCases("cases/query/fz_sql.yaml")));

INSTANTIATE_TEST_SUITE_P(LimitClauseQuery, EngineTest,
                        testing::ValuesIn(sqlcase::InitCases("cases/query/limit.yaml")));

INSTANTIATE_TEST_SUITE_P(EngineSimpleQuery, EngineTest,
                        testing::ValuesIn(sqlcase::InitCases("cases/query/simple_query.yaml")));
INSTANTIATE_TEST_SUITE_P(EngineConstQuery, EngineTest,
                        testing::ValuesIn(sqlcase::InitCases("cases/query/const_query.yaml")));
INSTANTIATE_TEST_SUITE_P(EngineUdfQuery, EngineTest,
                        testing::ValuesIn(sqlcase::InitCases("cases/query/udf_query.yaml")));
INSTANTIATE_TEST_SUITE_P(EngineOperatorQuery, EngineTest,
                        testing::ValuesIn(sqlcase::InitCases("cases/query/operator_query.yaml")));
INSTANTIATE_TEST_SUITE_P(EngineParameterizedQuery, EngineTest,
                         testing::ValuesIn(sqlcase::InitCases("cases/query/parameterized_query.yaml")));
INSTANTIATE_TEST_SUITE_P(EngineUdafQuery, EngineTest,
                        testing::ValuesIn(sqlcase::InitCases("cases/query/udaf_query.yaml")));
INSTANTIATE_TEST_SUITE_P(EngineExtreamQuery, EngineTest,
                        testing::ValuesIn(sqlcase::InitCases("cases/query/extream_query.yaml")));

INSTANTIATE_TEST_SUITE_P(EngineLastJoinQuery, EngineTest,
                        testing::ValuesIn(sqlcase::InitCases("cases/query/last_join_query.yaml")));
INSTANTIATE_TEST_SUITE_P(EngineLeftJoin, EngineTest,
                        testing::ValuesIn(sqlcase::InitCases("cases/query/left_join.yml")));

INSTANTIATE_TEST_SUITE_P(EngineLastJoinWindowQuery, EngineTest,
                        testing::ValuesIn(sqlcase::InitCases("cases/query/last_join_window_query.yaml")));
INSTANTIATE_TEST_SUITE_P(EngineLastJoinSubqueryWindow, EngineTest,
                        testing::ValuesIn(sqlcase::InitCases("cases/query/last_join_subquery_window.yml")));
INSTANTIATE_TEST_SUITE_P(EngineLastJoinWhere, EngineTest,
                        testing::ValuesIn(sqlcase::InitCases("cases/query/last_join_where.yaml")));
INSTANTIATE_TEST_SUITE_P(EngineWindowQuery, EngineTest,
                        testing::ValuesIn(sqlcase::InitCases("cases/query/window_query.yaml")));

INSTANTIATE_TEST_SUITE_P(EngineWindowWithUnionQuery, EngineTest,
                        testing::ValuesIn(sqlcase::InitCases("cases/query/window_with_union_query.yaml")));

INSTANTIATE_TEST_SUITE_P(EngineBatchGroupQuery, EngineTest,
                        testing::ValuesIn(sqlcase::InitCases("cases/query/group_query.yaml")));
INSTANTIATE_TEST_SUITE_P(EngineBatchHavingQuery, EngineTest,
                         testing::ValuesIn(sqlcase::InitCases("cases/query/having_query.yaml")));
INSTANTIATE_TEST_SUITE_P(EngineBatchWhereGroupQuery, EngineTest,
                         testing::ValuesIn(sqlcase::InitCases("cases/query/where_group_query.yaml")));
INSTANTIATE_TEST_SUITE_P(WithClause, EngineTest,
                         testing::ValuesIn(sqlcase::InitCases("cases/query/with.yaml")));
INSTANTIATE_TEST_SUITE_P(UnionQuery, EngineTest,
                         testing::ValuesIn(sqlcase::InitCases("cases/query/union_query.yml")));

INSTANTIATE_TEST_SUITE_P(EngineTestWindowRowQuery, EngineTest,
                        testing::ValuesIn(sqlcase::InitCases("cases/function/window/test_window_row.yaml")));

INSTANTIATE_TEST_SUITE_P(
    EngineTestWindowRowsRangeQuery, EngineTest,
    testing::ValuesIn(sqlcase::InitCases("cases/function/window/test_window_row_range.yaml")));

INSTANTIATE_TEST_SUITE_P(
    EngineTestWindowRowsCurrentRow, EngineTest,
    testing::ValuesIn(sqlcase::InitCases("cases/function/window/test_current_row.yaml")));

INSTANTIATE_TEST_SUITE_P(EngineTestWindowAttributes, EngineTest,
                         testing::ValuesIn(sqlcase::InitCases("cases/function/window/window_attributes.yaml")));
INSTANTIATE_TEST_SUITE_P(EngineTestWindowUnion, EngineTest,
                        testing::ValuesIn(sqlcase::InitCases("cases/function/window/test_window_union.yaml")));
INSTANTIATE_TEST_SUITE_P(EngineTestWindowMaxSize, EngineTest,
                        testing::ValuesIn(sqlcase::InitCases("cases/function/window/test_maxsize.yaml")));

INSTANTIATE_TEST_SUITE_P(EngineTestMultipleDatabases, EngineTest,
    testing::ValuesIn(sqlcase::InitCases("cases/function/multiple_databases/test_multiple_databases.yaml")));
INSTANTIATE_TEST_SUITE_P(EngineTestLastJoinSimple, EngineTest,
                         testing::ValuesIn(sqlcase::InitCases("cases/function/join/test_lastjoin_simple.yaml")));
INSTANTIATE_TEST_SUITE_P(EngineTestLastJoinComplex, EngineTest,
                        testing::ValuesIn(sqlcase::InitCases("cases/function/join/test_lastjoin_complex.yaml")));
INSTANTIATE_TEST_SUITE_P(EngineTestArithmetic, EngineTest,
                        testing::ValuesIn(sqlcase::InitCases("cases/function/expression/test_arithmetic.yaml")));
INSTANTIATE_TEST_SUITE_P(EngineTestPredicate, EngineTest,
                        testing::ValuesIn(sqlcase::InitCases("cases/function/expression/test_predicate.yaml")));
INSTANTIATE_TEST_SUITE_P(EngineTestCondition, EngineTest,
                        testing::ValuesIn(sqlcase::InitCases("cases/function/expression/test_condition.yaml")));
INSTANTIATE_TEST_SUITE_P(EngineTestLogic, EngineTest,
                        testing::ValuesIn(sqlcase::InitCases("cases/function/expression/test_logic.yaml")));
INSTANTIATE_TEST_SUITE_P(EngineTestType, EngineTest,
                        testing::ValuesIn(sqlcase::InitCases("cases/function/expression/test_type.yaml")));

INSTANTIATE_TEST_SUITE_P(EngineTestSubSelect, EngineTest,
                        testing::ValuesIn(sqlcase::InitCases("cases/function/select/test_sub_select.yaml")));

INSTANTIATE_TEST_SUITE_P(EngineTestUdfFunction, EngineTest,
                        testing::ValuesIn(sqlcase::InitCases("cases/function/function/test_udf_function.yaml")));
INSTANTIATE_TEST_SUITE_P(
    EngineTestUdafFunction, EngineTest,
    testing::ValuesIn(sqlcase::InitCases("cases/function/function/test_udaf_function.yaml")));
INSTANTIATE_TEST_SUITE_P(EngineTestCalculateFunction, EngineTest,
                        testing::ValuesIn(sqlcase::InitCases("cases/function/function/test_calculate.yaml")));
INSTANTIATE_TEST_SUITE_P(EngineTestDateFunction, EngineTest,
                        testing::ValuesIn(sqlcase::InitCases("cases/function/function/test_date.yaml")));
INSTANTIATE_TEST_SUITE_P(EngineTestStringFunction, EngineTest,
                        testing::ValuesIn(sqlcase::InitCases("cases/function/function/test_string.yaml")));

INSTANTIATE_TEST_SUITE_P(EngineTestSelectSample, EngineTest,
                        testing::ValuesIn(sqlcase::InitCases("cases/function/select/test_select_sample.yaml")));
INSTANTIATE_TEST_SUITE_P(EngineTestWhere, EngineTest,
                        testing::ValuesIn(sqlcase::InitCases("cases/function/select/test_where.yaml")));

INSTANTIATE_TEST_SUITE_P(EngineTestFzFunction, EngineTest,
                        testing::ValuesIn(sqlcase::InitCases("cases/function/test_feature_zero_function.yaml")));

INSTANTIATE_TEST_SUITE_P(EngineTestFzSQLFunction, EngineTest,
                        testing::ValuesIn(sqlcase::InitCases("cases/function/test_fz_sql.yaml")));
INSTANTIATE_TEST_SUITE_P(EngineTestClusterWindowAndLastJoin, EngineTest,
                        testing::ValuesIn(sqlcase::InitCases("cases/function/cluster/window_and_lastjoin.yaml")));
INSTANTIATE_TEST_SUITE_P(EngineTestClusterWindowRow, EngineTest,
                        testing::ValuesIn(sqlcase::InitCases("cases/function/cluster/test_window_row.yaml")));
INSTANTIATE_TEST_SUITE_P(EngineTestClusterWindowRowRange, EngineTest,
                        testing::ValuesIn(sqlcase::InitCases("cases/function/cluster/test_window_row_range.yaml")));

INSTANTIATE_TEST_SUITE_P(
    EngineTestWindowExcludeCurrentTime, EngineTest,
    testing::ValuesIn(sqlcase::InitCases("cases/function/window/test_window_exclude_current_time.yaml")));

INSTANTIATE_TEST_SUITE_P(EngineTestIndexOptimized, EngineTest,
                        testing::ValuesIn(sqlcase::InitCases("cases/function/test_index_optimized.yaml")));
INSTANTIATE_TEST_SUITE_P(EngineTestErrorWindow, EngineTest,
                        testing::ValuesIn(sqlcase::InitCases("cases/function/window/error_window.yaml")));

// myhug 场景正确性验证
INSTANTIATE_TEST_SUITE_P(EngineTestFzMyhug, EngineTest,
                        testing::ValuesIn(sqlcase::InitCases("cases/function/fz_ddl/test_myhug.yaml")));

// luoji 场景正确性验证
INSTANTIATE_TEST_SUITE_P(EngineTestFzLuoji, EngineTest,
                        testing::ValuesIn(sqlcase::InitCases("cases/function/fz_ddl/test_luoji.yaml")));
// bank 场景正确性验证
INSTANTIATE_TEST_SUITE_P(EngineTestFzBank, EngineTest,
                        testing::ValuesIn(sqlcase::InitCases("cases/function/fz_ddl/test_bank.yaml")));
// TODO(qiliguo) #229 sql 语句加一个大 select, 选取其中几列，
//   添加到 expect 中的做验证
// imported from spark offline test
// 单表反欺诈场景
INSTANTIATE_TEST_SUITE_P(EngineTestSparkFQZ, EngineTest,
                        testing::ValuesIn(sqlcase::InitCases("cases/function/spark/test_fqz_studio.yaml")));
// 单表-广告场景
INSTANTIATE_TEST_SUITE_P(EngineTestSparkAds, EngineTest,
                        testing::ValuesIn(sqlcase::InitCases("cases/function/spark/test_ads.yaml")));
// 单表-新闻场景
INSTANTIATE_TEST_SUITE_P(EngineTestSparkNews, EngineTest,
                        testing::ValuesIn(sqlcase::InitCases("cases/function/spark/test_news.yaml")));
// 多表-京东数据场景
INSTANTIATE_TEST_SUITE_P(EngineTestSparkJD, EngineTest,
                        testing::ValuesIn(sqlcase::InitCases("cases/function/spark/test_jd.yaml")));
// 多表-信用卡用户转借记卡预测场景
INSTANTIATE_TEST_SUITE_P(EngineTestSparkCredit, EngineTest,
                        testing::ValuesIn(sqlcase::InitCases("cases/function/spark/test_credit.yaml")));

// AUTOX
INSTANTIATE_TEST_SUITE_P(EngineTestAutoXSQLFunction, EngineTest,
                         testing::ValuesIn(sqlcase::InitCases("cases/usecase/autox.yaml")));

INSTANTIATE_TEST_SUITE_P(BatchRequestEngineTest, BatchRequestEngineTest,
                        testing::ValuesIn(sqlcase::InitCases("cases/function/test_batch_request.yaml")));
}  // namespace vm
}  // namespace hybridse
