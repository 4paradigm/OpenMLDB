/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * schema_test.cc
 *
 * Author: chenjing
 * Date: 2020/4/20
 *--------------------------------------------------------------------------
 **/
#include "vm/schemas_context.h"
#include "glog/logging.h"
#include "gtest/gtest.h"
namespace fesql {
namespace vm {
void BuildTableDef(::fesql::type::TableDef& table) {  // NOLINT
    table.set_name("t1");
    table.set_catalog("db");
    {
        ::fesql::type::ColumnDef* column = table.add_columns();
        column->set_type(::fesql::type::kVarchar);
        column->set_name("col0");
    }
    {
        ::fesql::type::ColumnDef* column = table.add_columns();
        column->set_type(::fesql::type::kInt32);
        column->set_name("col1");
    }
    {
        ::fesql::type::ColumnDef* column = table.add_columns();
        column->set_type(::fesql::type::kInt16);
        column->set_name("col2");
    }
    {
        ::fesql::type::ColumnDef* column = table.add_columns();
        column->set_type(::fesql::type::kFloat);
        column->set_name("col3");
    }
    {
        ::fesql::type::ColumnDef* column = table.add_columns();
        column->set_type(::fesql::type::kDouble);
        column->set_name("col4");
    }

    {
        ::fesql::type::ColumnDef* column = table.add_columns();
        column->set_type(::fesql::type::kInt64);
        column->set_name("col5");
    }

    {
        ::fesql::type::ColumnDef* column = table.add_columns();
        column->set_type(::fesql::type::kVarchar);
        column->set_name("col6");
    }
}

void BuildTableT2Def(::fesql::type::TableDef& table) {  // NOLINT
    table.set_name("t2");
    table.set_catalog("db");
    {
        ::fesql::type::ColumnDef* column = table.add_columns();
        column->set_type(::fesql::type::kVarchar);
        column->set_name("str0");
    }
    {
        ::fesql::type::ColumnDef* column = table.add_columns();
        column->set_type(::fesql::type::kVarchar);
        column->set_name("str1");
    }
    {
        ::fesql::type::ColumnDef* column = table.add_columns();
        column->set_type(::fesql::type::kFloat);
        column->set_name("col3");
    }
    {
        ::fesql::type::ColumnDef* column = table.add_columns();
        column->set_type(::fesql::type::kDouble);
        column->set_name("col4");
    }
    {
        ::fesql::type::ColumnDef* column = table.add_columns();
        column->set_type(::fesql::type::kInt16);
        column->set_name("col2");
    }
    {
        ::fesql::type::ColumnDef* column = table.add_columns();
        column->set_type(::fesql::type::kInt32);
        column->set_name("col1");
    }
    {
        ::fesql::type::ColumnDef* column = table.add_columns();
        column->set_type(::fesql::type::kInt64);
        column->set_name("col5");
    }
}

class SchemasContextTest : public ::testing::Test {};

TEST_F(SchemasContextTest, NewSchemasContextTest) {
    vm::SchemaSourceList name_schemas;
    type::TableDef t1;
    type::TableDef t2;
    {
        BuildTableDef(t1);
        name_schemas.AddSchemaSource("t1", &t1.columns());
    }
    {
        BuildTableT2Def(t2);
        name_schemas.AddSchemaSource("t2", &t2.columns());
    }
    SchemasContext ctx(name_schemas);

    ASSERT_EQ(0u, ctx.table_context_id_map_["t1"]);
    ASSERT_EQ(1u, ctx.table_context_id_map_["t2"]);

    ASSERT_EQ(std::vector<uint32_t>({0u, 1u}), ctx.col_context_id_map_["col1"]);
    ASSERT_EQ(std::vector<uint32_t>({1u}), ctx.col_context_id_map_["str0"]);

    ASSERT_EQ(0u, ctx.row_schema_info_list_[0].idx_);
    ASSERT_EQ(name_schemas.schema_source_list_[0].schema_,
              ctx.row_schema_info_list_[0].schema_);
    ASSERT_EQ("t1", ctx.row_schema_info_list_[0].table_name_);

    ASSERT_EQ(1u, ctx.row_schema_info_list_[1].idx_);
    ASSERT_EQ(name_schemas.schema_source_list_[1].schema_,
              ctx.row_schema_info_list_[1].schema_);
    ASSERT_EQ("t2", ctx.row_schema_info_list_[1].table_name_);
}

TEST_F(SchemasContextTest, ColumnResolvedTest) {
    vm::SchemaSourceList name_schemas;
    type::TableDef t1;
    type::TableDef t2;
    {
        BuildTableDef(t1);
        name_schemas.AddSchemaSource("t1", &t1.columns());
    }
    {
        BuildTableT2Def(t2);
        name_schemas.AddSchemaSource("t2", &t2.columns());
    }
    SchemasContext ctx(name_schemas);
    RowSchemaInfo* info_t1 = &ctx.row_schema_info_list_[0];
    RowSchemaInfo* info_t2 = &ctx.row_schema_info_list_[1];
    {
        const RowSchemaInfo* info;
        ASSERT_TRUE(ctx.ColumnRefResolved("t1", "col1", &info));
        ASSERT_EQ(info, info_t1);
    }
    {
        const RowSchemaInfo* info;
        ASSERT_TRUE(ctx.ColumnRefResolved("t2", "col1", &info));
        ASSERT_EQ(info, info_t2);
    }

    {
        const RowSchemaInfo* info;
        ASSERT_TRUE(ctx.ColumnRefResolved("", "str0", &info));
        ASSERT_EQ(info, info_t2);
    }

    {
        const RowSchemaInfo* info;
        ASSERT_FALSE(ctx.ColumnRefResolved("", "col1", &info));
    }

    {
        const RowSchemaInfo* info;
        ASSERT_FALSE(ctx.ColumnRefResolved("", "col2", &info));
    }
}

TEST_F(SchemasContextTest, ColumnOffsetResolvedTest) {
    vm::SchemaSourceList name_schemas;
    type::TableDef t1;
    type::TableDef t2;
    {
        BuildTableDef(t1);
        name_schemas.AddSchemaSource("t1", &t1.columns());
    }
    {
        BuildTableT2Def(t2);
        name_schemas.AddSchemaSource("t2", &t2.columns());
    }
    SchemasContext ctx(name_schemas);
    ASSERT_EQ(0, ctx.ColumnOffsetResolved(0, 0));
    ASSERT_EQ(1, ctx.ColumnOffsetResolved(0, 1));
    ASSERT_EQ(2, ctx.ColumnOffsetResolved(0, 2));
    ASSERT_EQ(3, ctx.ColumnOffsetResolved(0, 3));
    ASSERT_EQ(7, ctx.ColumnOffsetResolved(1, 0));
    ASSERT_EQ(8, ctx.ColumnOffsetResolved(1, 1));
}
}  // namespace vm
}  // namespace fesql
int main(int argc, char** argv) {
    ::testing::GTEST_FLAG(color) = "yes";
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
