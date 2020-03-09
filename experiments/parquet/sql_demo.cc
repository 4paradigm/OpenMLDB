/*
 * sql_demo.cc
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

#include <memory>
#include <utility>
#include <vector>
#include <iostream>
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/exception.h>
#include "analyser/analyser.h"
#include "codegen/ir_base_builder.h"
#include "glog/logging.h"
#include "parser/parser.h"
#include "plan/planner.h"
#include "storage/type_ir_builder.h"
#include "udf/udf.h"
#include "vm/op_generator.h"

std::shared_ptr<arrow::Table> GenTable() {
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
    std::shared_ptr<arrow::Schema> schema = arrow::schema(
        {arrow::field("int", arrow::int64()), arrow::field("str", arrow::utf8())});
    return arrow::Table::Make(schema, {i64array, strarray});
}

void WriteParquetFile(const arrow::Table& table, 
        const std::string& path) {
    std::shared_ptr<arrow::io::FileOutputStream> outfile;
    PARQUET_THROW_NOT_OK(
      arrow::io::FileOutputStream::Open(path, &outfile));
    // The last argument to the function call is the size of the RowGroup in
    // the parquet file. Normally you would choose this to be rather large but
    // for the example, we use a small value to have multiple RowGroups.
    PARQUET_THROW_NOT_OK(
      parquet::arrow::WriteTable(table, arrow::default_memory_pool(), outfile, 3));
}

void ExecSQL(const std::string& path, const std::string& table_name,
             const std::string& sql) {
    ::fesql::type::TableDef table; 
}



