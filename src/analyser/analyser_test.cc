/*-------------------------------------------------------------------------
 * Copyright (C) 2019, 4paradigm
 * analyser_test.cc
 *
 * Author: chenjing
 * Date: 2019/10/30
 *--------------------------------------------------------------------------
 **/
#include "analyser/analyser.h"
#include <string>
#include <utility>
#include "gtest/gtest.h"
#include "parser/parser.h"
namespace fesql {
namespace analyser {

using fesql::node::NodeManager;
using fesql::node::NodePointVector;
using fesql::node::PlanNode;
using fesql::node::SQLNode;

void GetSchema(::fesql::type::TableDef &table) {  // NOLINT (runtime/references)
    {
        ::fesql::type::ColumnDef *column = table.add_columns();
        column->set_name("col1");
        column->set_type(::fesql::type::kFloat);
    }

    {
        ::fesql::type::ColumnDef *column = table.add_columns();
        column->set_name("col2");
        column->set_type(::fesql::type::kInt16);
    }

    {
        ::fesql::type::ColumnDef *column = table.add_columns();
        column->set_name("col3");
        column->set_type(::fesql::type::kInt32);
    }

    {
        ::fesql::type::ColumnDef *column = table.add_columns();
        column->set_name("col4");
        column->set_type(::fesql::type::kInt64);
    }

    {
        ::fesql::type::ColumnDef *column = table.add_columns();
        column->set_name("col5");
        column->set_type(::fesql::type::kDouble);
    }
    table.set_name("t1");
}

class AnalyserTest : public ::testing::TestWithParam<
                         std::pair<error::ErrorType, std::string>> {
 public:
    AnalyserTest() {
        parser_ = new parser::FeSQLParser();
        manager_ = new NodeManager();
        table.clear_columns();
        GetSchema(table);
        analyser = new FeSQLAnalyser(manager_, &table);
    }

    ~AnalyserTest() {
        delete parser_;
        delete manager_;
        delete analyser;
    }

 protected:
    parser::FeSQLParser *parser_;
    NodeManager *manager_;
    FeSQLAnalyser *analyser;
    ::fesql::type::TableDef table;
};

INSTANTIATE_TEST_CASE_P(
    AnalyserValidate, AnalyserTest,
    testing::Values(
        std::make_pair(
            error::kSucess,
            "SELECT t1.col1 c1,  TRIM(col3) as trimCol3, col2 FROM t1"
            " limit 10;"),
        std::make_pair(error::kAnalyserErrorTableNotExist,
                       "SELECT t2.COL1 c1 FROM t2 limit 10;"),
        std::make_pair(error::kAnalyserErrorQueryMultiTable,
                       "SELECT t2.col1 c1 "
                       "FROM t2, t1 limit 10;"),
        std::make_pair(error::kAnalyserErrorColumnNotExist,
                       "SELECT t1.col100 c1"
                       " FROM t1 limit 10;"),
        std::make_pair(
            error::kAnalyserErrorGlobalAggFunction,
            "SELECT t1.col1 c1,  MIN(col3) as trimCol3, col2 FROM t1 "
            "limit 10;"),
        std::make_pair(error::kSucess,
                       "create table test(\n"
                       "    column1 int NOT NULL,\n"
                       "    column2 timestamp NOT NULL,\n"
                       "    column3 int NOT NULL,\n"
                       "    column4 string NOT NULL,\n"
                       "    column5 int NOT NULL\n"
                       ");"),
        std::make_pair(error::kSucess,
                       "create table IF NOT EXISTS test(\n"
                       "    column1 int NOT NULL,\n"
                       "    column2 timestamp NOT NULL,\n"
                       "    column3 int NOT NULL,\n"
                       "    column4 string NOT NULL,\n"
                       "    column5 int NOT NULL\n"
                       ");"),

        std::make_pair(error::kSucess,
                       "create table test(\n"
                       "    column1 int NOT NULL,\n"
                       "    column2 timestamp NOT NULL,\n"
                       "    column3 int NOT NULL,\n"
                       "    column4 string NOT NULL,\n"
                       "    column5 int NOT NULL,\n"
                       "    index(key=(column4))\n"
                       ");"),

        std::make_pair(error::kSucess,
                       "create table test(\n"
                       "    column1 int NOT NULL,\n"
                       "    column2 timestamp NOT NULL,\n"
                       "    column3 int NOT NULL,\n"
                       "    column4 string NOT NULL,\n"
                       "    column5 int NOT NULL,\n"
                       "    index(key=(column4, column3))\n"
                       ");"),
        std::make_pair(error::kSucess,
                       "create table test(\n"
                       "    column1 int NOT NULL,\n"
                       "    column2 timestamp NOT NULL,\n"
                       "    column3 int NOT NULL,\n"
                       "    column4 string NOT NULL,\n"
                       "    column5 int NOT NULL,\n"
                       "    index(key=(column4, column3), ts=column5)\n"
                       ");"),
        std::make_pair(
            error::kSucess,
            "create table test(\n"
            "    column1 int NOT NULL,\n"
            "    column2 timestamp NOT NULL,\n"
            "    column3 int NOT NULL,\n"
            "    column4 string NOT NULL,\n"
            "    column5 int NOT NULL,\n"
            "    index(key=(column4, column3), ts=column2, ttl=60d)\n"
            ");"),
        std::make_pair(error::kSucess,
                       "create table test(\n"
                       "    column1 int NOT NULL,\n"
                       "    column2 timestamp NOT NULL,\n"
                       "    column3 int NOT NULL,\n"
                       "    column4 string NOT NULL,\n"
                       "    column5 int NOT NULL,\n"
                       "    index(key=(column4, column3), version=(column5, "
                       "3), ts=column2, ttl=60d)\n"
                       ");")));

TEST_P(AnalyserTest, RunAnalyseTest) {
    ParamType param = GetParam();
    NodePointVector list;
    base::Status status;
    int ret = parser_->parse(param.second, list, manager_, status);
    ASSERT_EQ(0, ret);
    ASSERT_EQ(1, list.size());

    NodePointVector query_tree;
    ret = analyser->Analyse(list, query_tree, status);
    if (0 != status.code) {
        std::cout << status.msg << std::endl;
    }
    ASSERT_EQ(param.first, ret);
    if (query_tree.size() > 0) {
        std::cout << *query_tree[0] << std::endl;
    }
}

TEST_F(AnalyserTest, ColumnRefValidateTest) {
    ASSERT_TRUE(analyser->IsColumnExistInTable("col1", "t1"));
    ASSERT_FALSE(analyser->IsColumnExistInTable("col100", "t1"));
    ASSERT_FALSE(analyser->IsColumnExistInTable("col1", "t2"));
}

}  // namespace analyser
}  // namespace fesql
int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
