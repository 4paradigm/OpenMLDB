/*
 * test_base.cc.c
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
#include "testing/test_base.h"

#include <unordered_map>

#include "plan/plan_api.h"
namespace hybridse {
namespace vm {

void BuildAggTableDef(::hybridse::type::TableDef& table, const std::string& aggr_table, // NOLINT
                      const std::string& aggr_db) {
    table.set_name(aggr_table);
    table.set_catalog(aggr_db);
    {
        ::hybridse::type::ColumnDef* column = table.add_columns();
        column->set_type(::hybridse::type::kVarchar);
        column->set_name("key");
    }
    {
        ::hybridse::type::ColumnDef* column = table.add_columns();
        column->set_type(::hybridse::type::kTimestamp);
        column->set_name("ts_start");
    }
    {
        ::hybridse::type::ColumnDef* column = table.add_columns();
        column->set_type(::hybridse::type::kTimestamp);
        column->set_name("ts_end");
    }
    {
        ::hybridse::type::ColumnDef* column = table.add_columns();
        column->set_type(::hybridse::type::kInt32);
        column->set_name("num_rows");
    }
    {
        ::hybridse::type::ColumnDef* column = table.add_columns();
        column->set_type(::hybridse::type::kVarchar);
        column->set_name("agg_val");
    }
    {
        ::hybridse::type::ColumnDef* column = table.add_columns();
        column->set_type(::hybridse::type::kInt64);
        column->set_name("binlog_offset");
    }
    {
        ::hybridse::type::ColumnDef* column = table.add_columns();
        column->set_type(::hybridse::type::kVarchar);
        column->set_name("filter_key");
    }
}

void BuildTableDef(::hybridse::type::TableDef& table) {  // NOLINT
    table.set_name("t1");
    table.set_catalog("db");
    {
        ::hybridse::type::ColumnDef* column = table.add_columns();
        column->set_type(::hybridse::type::kVarchar);
        column->set_name("col0");
    }
    {
        ::hybridse::type::ColumnDef* column = table.add_columns();
        column->set_type(::hybridse::type::kInt32);
        column->set_name("col1");
    }
    {
        ::hybridse::type::ColumnDef* column = table.add_columns();
        column->set_type(::hybridse::type::kInt16);
        column->set_name("col2");
    }
    {
        ::hybridse::type::ColumnDef* column = table.add_columns();
        column->set_type(::hybridse::type::kFloat);
        column->set_name("col3");
    }
    {
        ::hybridse::type::ColumnDef* column = table.add_columns();
        column->set_type(::hybridse::type::kDouble);
        column->set_name("col4");
    }

    {
        ::hybridse::type::ColumnDef* column = table.add_columns();
        column->set_type(::hybridse::type::kInt64);
        column->set_name("col5");
    }

    {
        ::hybridse::type::ColumnDef* column = table.add_columns();
        column->set_type(::hybridse::type::kVarchar);
        column->set_name("col6");
    }
}

void BuildTableA(::hybridse::type::TableDef& table) {  // NOLINT
    table.set_name("ta");
    table.set_catalog("db");
    {
        ::hybridse::type::ColumnDef* column = table.add_columns();
        column->set_type(::hybridse::type::kVarchar);
        column->set_name("c0");
    }
    {
        ::hybridse::type::ColumnDef* column = table.add_columns();
        column->set_type(::hybridse::type::kInt32);
        column->set_name("c1");
    }
    {
        ::hybridse::type::ColumnDef* column = table.add_columns();
        column->set_type(::hybridse::type::kInt16);
        column->set_name("c2");
    }
    {
        ::hybridse::type::ColumnDef* column = table.add_columns();
        column->set_type(::hybridse::type::kFloat);
        column->set_name("c3");
    }
    {
        ::hybridse::type::ColumnDef* column = table.add_columns();
        column->set_type(::hybridse::type::kDouble);
        column->set_name("c4");
    }

    {
        ::hybridse::type::ColumnDef* column = table.add_columns();
        column->set_type(::hybridse::type::kInt64);
        column->set_name("c5");
    }

    {
        ::hybridse::type::ColumnDef* column = table.add_columns();
        column->set_type(::hybridse::type::kVarchar);
        column->set_name("c6");
    }
}
void BuildTableT2Def(::hybridse::type::TableDef& table) {  // NOLINT
    table.set_name("t2");
    table.set_catalog("db");
    {
        ::hybridse::type::ColumnDef* column = table.add_columns();
        column->set_type(::hybridse::type::kVarchar);
        column->set_name("str0");
    }
    {
        ::hybridse::type::ColumnDef* column = table.add_columns();
        column->set_type(::hybridse::type::kVarchar);
        column->set_name("str1");
    }
    {
        ::hybridse::type::ColumnDef* column = table.add_columns();
        column->set_type(::hybridse::type::kFloat);
        column->set_name("col3");
    }
    {
        ::hybridse::type::ColumnDef* column = table.add_columns();
        column->set_type(::hybridse::type::kDouble);
        column->set_name("col4");
    }
    {
        ::hybridse::type::ColumnDef* column = table.add_columns();
        column->set_type(::hybridse::type::kInt16);
        column->set_name("col2");
    }
    {
        ::hybridse::type::ColumnDef* column = table.add_columns();
        column->set_type(::hybridse::type::kInt32);
        column->set_name("col1");
    }
    {
        ::hybridse::type::ColumnDef* column = table.add_columns();
        column->set_type(::hybridse::type::kInt64);
        column->set_name("col5");
    }
}

void BuildBuf(int8_t** buf, uint32_t* size) {
    ::hybridse::type::TableDef table;
    BuildTableDef(table);
    codec::RowBuilder builder(table.columns());
    uint32_t total_size = builder.CalTotalLength(2);
    int8_t* ptr = static_cast<int8_t*>(malloc(total_size));
    builder.SetBuffer(ptr, total_size);
    builder.AppendString("0", 1);
    builder.AppendInt32(32);
    builder.AppendInt16(16);
    builder.AppendFloat(2.1f);
    builder.AppendDouble(3.1);
    builder.AppendInt64(64);
    builder.AppendString("1", 1);
    *buf = ptr;
    *size = total_size;
}

void BuildT2Buf(int8_t** buf, uint32_t* size) {
    ::hybridse::type::TableDef table;
    BuildTableT2Def(table);
    codec::RowBuilder builder(table.columns());
    uint32_t total_size = builder.CalTotalLength(2);
    int8_t* ptr = static_cast<int8_t*>(malloc(total_size));
    builder.SetBuffer(ptr, total_size);
    builder.AppendString("A", 1);
    builder.AppendString("B", 1);
    builder.AppendFloat(22.1f);
    builder.AppendDouble(33.1);
    builder.AppendInt16(160);
    builder.AppendInt32(32);
    builder.AppendInt64(640);
    *buf = ptr;
    *size = total_size;
}
void BuildRows(::hybridse::type::TableDef& table,  // NOLINT
               std::vector<Row>& rows) {           // NOLINT
    BuildTableDef(table);
    {
        codec::RowBuilder builder(table.columns());
        std::string str = "1";
        std::string str0 = "0";
        uint32_t total_size = builder.CalTotalLength(str.size() + str0.size());
        int8_t* ptr = static_cast<int8_t*>(malloc(total_size));

        builder.SetBuffer(ptr, total_size);
        builder.AppendString("0", 1);
        builder.AppendInt32(1);
        builder.AppendInt16(5);
        builder.AppendFloat(1.1f);
        builder.AppendDouble(11.1);
        builder.AppendInt64(1);
        builder.AppendString(str.c_str(), 1);
        rows.push_back(Row(base::RefCountedSlice::Create(ptr, total_size)));
    }
    {
        codec::RowBuilder builder(table.columns());
        std::string str = "22";
        std::string str0 = "0";
        uint32_t total_size = builder.CalTotalLength(str.size() + str0.size());
        int8_t* ptr = static_cast<int8_t*>(malloc(total_size));
        builder.SetBuffer(ptr, total_size);
        builder.AppendString("0", 1);
        builder.AppendInt32(2);
        builder.AppendInt16(5);
        builder.AppendFloat(2.2f);
        builder.AppendDouble(22.2);
        builder.AppendInt64(2);
        builder.AppendString(str.c_str(), str.size());
        rows.push_back(Row(base::RefCountedSlice::Create(ptr, total_size)));
    }
    {
        codec::RowBuilder builder(table.columns());
        std::string str = "333";
        std::string str0 = "0";
        uint32_t total_size = builder.CalTotalLength(str.size() + str0.size());
        int8_t* ptr = static_cast<int8_t*>(malloc(total_size));
        builder.SetBuffer(ptr, total_size);
        builder.AppendString("1", 1);
        builder.AppendInt32(3);
        builder.AppendInt16(55);
        builder.AppendFloat(3.3f);
        builder.AppendDouble(33.3);
        builder.AppendInt64(1);
        builder.AppendString(str.c_str(), str.size());
        rows.push_back(Row(base::RefCountedSlice::Create(ptr, total_size)));
    }
    {
        codec::RowBuilder builder(table.columns());
        std::string str = "4444";
        std::string str0 = "0";
        uint32_t total_size = builder.CalTotalLength(str.size() + str0.size());
        int8_t* ptr = static_cast<int8_t*>(malloc(total_size));
        builder.SetBuffer(ptr, total_size);
        builder.AppendString("1", 1);
        builder.AppendInt32(4);
        builder.AppendInt16(55);
        builder.AppendFloat(4.4f);
        builder.AppendDouble(44.4);
        builder.AppendInt64(2);
        builder.AppendString("4444", str.size());
        rows.push_back(Row(base::RefCountedSlice::Create(ptr, total_size)));
    }
    {
        codec::RowBuilder builder(table.columns());
        std::string str =
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            "a";
        std::string str0 = "0";
        uint32_t total_size = builder.CalTotalLength(str.size() + str0.size());
        int8_t* ptr = static_cast<int8_t*>(malloc(total_size));
        builder.SetBuffer(ptr, total_size);
        builder.AppendString("2", 1);
        builder.AppendInt32(5);
        builder.AppendInt16(55);
        builder.AppendFloat(5.5f);
        builder.AppendDouble(55.5);
        builder.AppendInt64(3);
        builder.AppendString(str.c_str(), str.size());
        rows.push_back(Row(base::RefCountedSlice::Create(ptr, total_size)));
    }
}
void BuildT2Rows(::hybridse::type::TableDef& table,  // NOLINT
                 std::vector<Row>& rows) {           // NOLINT
    BuildTableT2Def(table);
    {
        codec::RowBuilder builder(table.columns());
        std::string str = "A";
        std::string str0 = "0";
        uint32_t total_size = builder.CalTotalLength(str.size() + str0.size());
        int8_t* ptr = static_cast<int8_t*>(malloc(total_size));

        builder.SetBuffer(ptr, total_size);
        builder.AppendString(str0.c_str(), 1);
        builder.AppendString(str.c_str(), 1);
        builder.AppendFloat(1.1f);
        builder.AppendDouble(11.1);
        builder.AppendInt16(50);
        builder.AppendInt32(1);
        builder.AppendInt64(1);
        rows.push_back(Row(base::RefCountedSlice::Create(ptr, total_size)));
    }
    {
        codec::RowBuilder builder(table.columns());
        std::string str = "BB";
        std::string str0 = "0";
        uint32_t total_size = builder.CalTotalLength(str.size() + str0.size());
        int8_t* ptr = static_cast<int8_t*>(malloc(total_size));
        builder.SetBuffer(ptr, total_size);
        builder.AppendString(str0.c_str(), 1);
        builder.AppendString(str.c_str(), str.size());
        builder.AppendFloat(2.2f);
        builder.AppendDouble(22.2);
        builder.AppendInt16(50);
        builder.AppendInt32(2);
        builder.AppendInt64(2);
        rows.push_back(Row(base::RefCountedSlice::Create(ptr, total_size)));
    }
    {
        codec::RowBuilder builder(table.columns());
        std::string str = "CCC";
        std::string str0 = "1";
        uint32_t total_size = builder.CalTotalLength(str.size() + str0.size());
        int8_t* ptr = static_cast<int8_t*>(malloc(total_size));
        builder.SetBuffer(ptr, total_size);
        builder.AppendString(str0.c_str(), 1);
        builder.AppendString(str.c_str(), str.size());
        builder.AppendFloat(3.3f);
        builder.AppendDouble(33.3);
        builder.AppendInt16(550);
        builder.AppendInt32(3);
        builder.AppendInt64(1);
        rows.push_back(Row(base::RefCountedSlice::Create(ptr, total_size)));
    }
    {
        codec::RowBuilder builder(table.columns());
        std::string str = "DDDD";
        std::string str0 = "1";
        uint32_t total_size = builder.CalTotalLength(str.size() + str0.size());
        int8_t* ptr = static_cast<int8_t*>(malloc(total_size));
        builder.SetBuffer(ptr, total_size);
        builder.AppendString(str0.c_str(), 1);
        builder.AppendString(str.c_str(), str.size());
        builder.AppendFloat(4.4f);
        builder.AppendDouble(44.4);
        builder.AppendInt16(550);
        builder.AppendInt32(4);
        builder.AppendInt64(2);
        rows.push_back(Row(base::RefCountedSlice::Create(ptr, total_size)));
    }
    {
        codec::RowBuilder builder(table.columns());
        std::string str = "EEEEE";
        std::string str0 = "2";
        uint32_t total_size = builder.CalTotalLength(str.size() + str0.size());
        int8_t* ptr = static_cast<int8_t*>(malloc(total_size));
        builder.SetBuffer(ptr, total_size);
        builder.AppendString(str0.c_str(), 1);
        builder.AppendString(str.c_str(), str.size());
        builder.AppendFloat(5.5f);
        builder.AppendDouble(55.5);
        builder.AppendInt16(550);
        builder.AppendInt32(5);
        builder.AppendInt64(3);
        rows.push_back(Row(base::RefCountedSlice::Create(ptr, total_size)));
    }
}
void ExtractExprListFromSimpleSql(::hybridse::node::NodeManager* nm,
                                  const std::string& sql,
                                  node::ExprListNode* output) {
    std::cout << sql << std::endl;
    ::hybridse::node::PlanNodeList plan_trees;
    ::hybridse::base::Status base_status;
    if (::hybridse::plan::PlanAPI::CreatePlanTreeFromScript(sql, plan_trees, nm,
                                                            base_status)) {
        std::cout << base_status.str();
        std::cout << *(plan_trees[0]) << std::endl;
    } else {
        std::cout << base_status.str();
    }
    ASSERT_EQ(0, base_status.code);
    std::cout.flush();

    ASSERT_EQ(node::kPlanTypeProject, plan_trees[0]->GetChildren()[0]->type_);
    auto project_plan_node =
        dynamic_cast<node::ProjectPlanNode*>(plan_trees[0]->GetChildren()[0]);
    ASSERT_EQ(1u, project_plan_node->project_list_vec_.size());

    auto project_list = dynamic_cast<node::ProjectListNode*>(
        project_plan_node->project_list_vec_[0]);
    for (auto project : project_list->GetProjects()) {
        output->AddChild(
            dynamic_cast<node::ProjectNode*>(project)->GetExpression());
    }
}
void ExtractExprFromSimpleSql(::hybridse::node::NodeManager* nm,
                              const std::string& sql, node::ExprNode** output) {
    std::cout << sql << std::endl;
    ::hybridse::node::PlanNodeList plan_trees;
    ::hybridse::base::Status base_status;
    ASSERT_EQ(0, base_status.code);
    if (::hybridse::plan::PlanAPI::CreatePlanTreeFromScript(sql, plan_trees, nm,
                                                            base_status)) {
        std::cout << base_status.str();
        std::cout << *(plan_trees[0]) << std::endl;
    } else {
        std::cout << base_status.str();
    }
    ASSERT_EQ(0, base_status.code);
    std::cout.flush();

    ASSERT_EQ(node::kPlanTypeProject, plan_trees[0]->GetChildren()[0]->type_);
    auto project_plan_node =
        dynamic_cast<node::ProjectPlanNode*>(plan_trees[0]->GetChildren()[0]);
    ASSERT_EQ(1u, project_plan_node->project_list_vec_.size());

    auto project_list = dynamic_cast<node::ProjectListNode*>(
        project_plan_node->project_list_vec_[0]);
    auto project =
        dynamic_cast<node::ProjectNode*>(project_list->GetProjects()[0]);
    *output = project->GetExpression();
}
bool AddTable(hybridse::type::Database& db,  // NOLINT
              const hybridse::type::TableDef& table_def) {
    *(db.add_tables()) = table_def;
    return true;
}
std::shared_ptr<SimpleCatalog> BuildSimpleCatalog(
    const hybridse::type::Database& database) {
    std::shared_ptr<SimpleCatalog> catalog(new SimpleCatalog(true));
    catalog->AddDatabase(database);
    return catalog;
}
std::shared_ptr<SimpleCatalog> BuildSimpleCatalogIndexUnsupport(
    const hybridse::type::Database& database) {
    std::shared_ptr<SimpleCatalog> catalog(new SimpleCatalog(false));
    catalog->AddDatabase(database);
    return catalog;
}
std::shared_ptr<SimpleCatalog> BuildSimpleCatalog() {
    return std::make_shared<SimpleCatalog>(true);
}
bool InitSimpleCataLogFromSqlCase(SqlCase& sql_case,  // NOLINT
                                  std::shared_ptr<SimpleCatalog> catalog) {
    if (sql_case.db_.empty()) {
        sql_case.db_ =  sqlcase::SqlCase::GenRand("auto_db");
    }

    std::unordered_map<std::string, hybridse::type::Database> db_map;
    hybridse::type::Database db;
    db.set_name(sql_case.db());
    db_map[sql_case.db()] = db;
    for (int32_t i = 0; i < sql_case.CountInputs(); i++) {
        sql_case.inputs_[i].name_ = sql_case.inputs()[i].name_;
        if (sql_case.inputs_[i].name_.empty()) {
            sql_case.inputs_[i].name_ = SqlCase::GenRand("auto_t");
        }
        type::TableDef table_def;
        if (!sql_case.ExtractInputTableDef(table_def, i)) {
            return false;
        }
        table_def.set_name(sql_case.inputs_[i].name_);
        if (db_map.find(table_def.catalog()) == db_map.end()) {
            hybridse::type::Database new_db;
            new_db.set_name(table_def.catalog());
            db_map[table_def.catalog()] = new_db;
        }
        if (!AddTable(db_map[table_def.catalog()], table_def)) {
            return false;
        }
    }
    for (auto iter = db_map.begin(); iter != db_map.end(); iter++) {
        catalog->AddDatabase(iter->second);
    }
    return true;
}

void PrintSchema(std::ostringstream& ss, const Schema& schema) {
    for (int32_t i = 0; i < schema.size(); i++) {
        if (i > 0) {
            ss << "\n";
        }
        const type::ColumnDef& column = schema.Get(i);
        ss << column.name() << " " << type::Type_Name(column.type());
    }
}

void PrintAllSchema(std::ostringstream& ss, const PhysicalOpNode* op) {
    for (size_t i = 0; i < op->GetProducerCnt(); i++) {
        PrintAllSchema(ss, op->GetProducer(i));
    }
    ss << "[";
    op->Print(ss, "\t");
    ss << "]";
    ss << ": " << std::endl;
    auto schema = op->GetOutputSchema();
    PrintSchema(ss, *schema);
    ss << std::endl;
}

void PrintSchema(const Schema& schema) {
    std::ostringstream ss;
    PrintSchema(ss, schema);
    LOG(INFO) << "\n" << ss.str();
}

}  // namespace vm
}  // namespace hybridse
