/*-------------------------------------------------------------------------
 * Copyright (C) 2019, 4paradigm
 * analyser_test.cc
 *      
 * Author: chenjing
 * Date: 2019/10/30 
 *--------------------------------------------------------------------------
**/
#include "parser/parser.h"
#include "analyser.h"
#include "gtest/gtest.h"

namespace fesql {
namespace analyser {

using fesql::node::SQLNode;
using fesql::node::NodePointVector;
using fesql::node::PlanNode;
using fesql::node::NodeManager;

void GetSchema(::fesql::type::TableDef &table) {
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

class AnalyserTest : public ::testing::TestWithParam<std::pair<error::ErrorType, std::string>> {
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

INSTANTIATE_TEST_CASE_P(PairReturn, AnalyserTest, testing::Values(
    std::make_pair(error::kSucess,
                   "SELECT t1.COL1 c1,  trim(COL3) as trimCol3, COL2 FROM t1 limit 10;"),
    std::make_pair(error::kAnalyserErrorTableNotExist,
                   "SELECT t2.COL1 c1 FROM t2 limit 10;"),
    std::make_pair(error::kAnalyserErrorQueryMultiTable, "SELECT t2.COL1 c1 FROM t2, t1 limit 10;")
));

TEST_P(AnalyserTest, RunAnalyseTest) {
    ParamType param = GetParam();
    NodePointVector list;
    int ret = parser_->parse(param.second, list, manager_);
    ASSERT_EQ(0, ret);
    ASSERT_EQ(1, list.size());

    NodePointVector query_tree;
    ret = analyser->Analyse(list, query_tree);
    ASSERT_EQ(param.first, ret);
}

}
}
int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
