/*
 * batch_catalog_test.cc
 * Copyright (C) 4paradigm.com 2020 wangtaize <wangtaize@4paradigm.com>
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

#include "catalog/batch_catalog.h"
#include <stdlib.h> /* srand, rand */
#include <time.h>   /* time */
#include <sstream>
#include "arrow/api.h"
#include "arrow/filesystem/localfs.h"
#include "arrow/io/api.h"
#include "gtest/gtest.h"
#include "parquet/arrow/reader.h"
#include "parquet/arrow/writer.h"

namespace fesql {
namespace catalog {

class BatchCatalogTest : public ::testing::Test {};

std::shared_ptr<arrow::Table> GenerateTable() {
    arrow::Int64Builder i64builder;
    PARQUET_THROW_NOT_OK(i64builder.AppendValues({1, 2, 3, 4, 5}));
    std::shared_ptr<arrow::Array> i64array;
    PARQUET_THROW_NOT_OK(i64builder.Finish(&i64array));
    arrow::StringBuilder strbuilder;
    PARQUET_THROW_NOT_OK(strbuilder.Append("some"));
    PARQUET_THROW_NOT_OK(strbuilder.Append("string"));
    PARQUET_THROW_NOT_OK(strbuilder.Append("content"));
    PARQUET_THROW_NOT_OK(strbuilder.Append("in"));
    PARQUET_THROW_NOT_OK(strbuilder.Append("rows"));
    std::shared_ptr<arrow::Array> strarray;
    PARQUET_THROW_NOT_OK(strbuilder.Finish(&strarray));
    std::shared_ptr<arrow::Schema> schema =
        arrow::schema({arrow::field("int", arrow::int64()),
                       arrow::field("str", arrow::utf8())});
    return arrow::Table::Make(schema, {i64array, strarray});
}

void WriteParquetFile(const arrow::Table& table, const std::string& path) {
    std::shared_ptr<arrow::io::FileOutputStream> outfile;
    PARQUET_THROW_NOT_OK(arrow::io::FileOutputStream::Open(path, &outfile));
    // The last argument to the function call is the size of the RowGroup in
    // the parquet file. Normally you would choose this to be rather large but
    // for the example, we use a small value to have multiple RowGroups.
    PARQUET_THROW_NOT_OK(parquet::arrow::WriteTable(
        table, arrow::default_memory_pool(), outfile, 3));
}

TEST_F(BatchCatalogTest, test_init) {
    std::shared_ptr<::arrow::fs::FileSystem> fs(
        new arrow::fs::LocalFileSystem());
    InputTables tables;
    int id = rand() % 1000;
    std::stringstream ss;
    ss << "batch_catalog_test" << id << ".parquet";
    std::string file_path = ss.str();
    std::shared_ptr<arrow::Table> table = GenerateTable();
    WriteParquetFile(*table, file_path);
    tables.push_back(std::make_pair("t1", file_path));
    BatchCatalog bcl(fs, tables);
    bool ok = bcl.Init();
    ASSERT_TRUE(ok);
    std::shared_ptr<TableHandler> table_handler = bcl.GetTable("db", "t1");
    ASSERT_FALSE(!table_handler);
    ASSERT_EQ("t1", table_handler->GetName());
    const Schema& schema = table_handler->GetSchema();
    ASSERT_EQ(2, schema.size());
}

}  // namespace catalog
}  // namespace fesql

int main(int argc, char** argv) {
    srand(time(NULL));
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
