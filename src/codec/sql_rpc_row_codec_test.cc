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

#include "codec/sql_rpc_row_codec.h"

#include <memory>
#include <string>
#include <vector>

#include "codec/fe_row_codec.h"
#include "gflags/gflags.h"
#include "gtest/gtest.h"

namespace openmldb {
namespace codec {

class SqlRpcRowCodecTest : public ::testing::Test {};

void InitSchema(hybridse::codec::Schema* schema) {
    hybridse::type::ColumnDef* column;
    column = schema->Add();
    column->set_name("col_0");
    column->set_type(hybridse::type::kInt32);
    column = schema->Add();
    column->set_name("col_1");
    column->set_type(hybridse::type::kFloat);
    column = schema->Add();
    column->set_name("col_2");
    column->set_type(hybridse::type::kVarchar);
}

TEST_F(SqlRpcRowCodecTest, TestSingleSlice) {
    hybridse::codec::Schema schema;
    InitSchema(&schema);

    size_t buf_size;
    hybridse::codec::RowBuilder builder(schema);

    buf_size = builder.CalTotalLength(5);
    int8_t* buf = reinterpret_cast<int8_t*>(malloc(buf_size));
    builder.SetBuffer(buf, buf_size);
    builder.AppendInt32(42);
    builder.AppendFloat(3.14);
    builder.AppendString("hello", 5);
    hybridse::codec::Row row(hybridse::codec::RefCountedSlice::CreateManaged(buf, buf_size));

    butil::IOBuf iobuf;
    iobuf.append("test prefix string");
    size_t total_size;
    ASSERT_TRUE(EncodeRpcRow(row, &iobuf, &total_size));
    ASSERT_EQ(buf_size, total_size);

    hybridse::codec::Row decoded;
    ASSERT_TRUE(DecodeRpcRow(iobuf, 18, buf_size, 1, &decoded));

    hybridse::codec::RowView row_view(schema);
    row_view.Reset(decoded.buf(0), decoded.size(0));
    ASSERT_EQ(42, row_view.GetInt32Unsafe(0));
    ASSERT_FLOAT_EQ(3.14, row_view.GetFloatUnsafe(1));
    ASSERT_EQ("hello", row_view.GetStringUnsafe(2));
}

TEST_F(SqlRpcRowCodecTest, TestMultiSlice) {
    hybridse::codec::Schema schema;
    InitSchema(&schema);

    size_t buf_size;
    hybridse::codec::RowBuilder builder(schema);

    buf_size = builder.CalTotalLength(5);
    int8_t* buf1 = reinterpret_cast<int8_t*>(malloc(buf_size));
    builder.SetBuffer(buf1, buf_size);
    builder.AppendInt32(42);
    builder.AppendFloat(3.14);
    builder.AppendString("hello", 5);
    hybridse::codec::Row row(hybridse::codec::RefCountedSlice::CreateManaged(buf1, buf_size));
    row.Append(hybridse::codec::RefCountedSlice());

    int8_t* buf2 = reinterpret_cast<int8_t*>(malloc(buf_size));
    builder.SetBuffer(buf2, buf_size);
    builder.AppendInt32(99);
    builder.AppendFloat(0.618);
    builder.AppendString("world", 5);
    row.Append(hybridse::codec::RefCountedSlice::CreateManaged(buf2, buf_size));
    row.Append(hybridse::codec::RefCountedSlice());

    butil::IOBuf iobuf;
    size_t total_size;
    ASSERT_TRUE(EncodeRpcRow(row, &iobuf, &total_size));
    ASSERT_EQ(2 * buf_size + 2 * 6, total_size);

    hybridse::codec::Row decoded;
    ASSERT_TRUE(DecodeRpcRow(iobuf, 0, total_size, 4, &decoded));

    hybridse::codec::RowView row_view(schema);
    row_view.Reset(decoded.buf(0), decoded.size(0));
    ASSERT_EQ(42, row_view.GetInt32Unsafe(0));
    ASSERT_FLOAT_EQ(3.14, row_view.GetFloatUnsafe(1));
    ASSERT_EQ("hello", row_view.GetStringUnsafe(2));

    ASSERT_EQ(nullptr, decoded.buf(1));
    ASSERT_EQ(0, decoded.size(1));

    row_view.Reset(decoded.buf(2), decoded.size(2));
    ASSERT_EQ(99, row_view.GetInt32Unsafe(0));
    ASSERT_FLOAT_EQ(0.618, row_view.GetFloatUnsafe(1));
    ASSERT_EQ("world", row_view.GetStringUnsafe(2));

    ASSERT_EQ(nullptr, decoded.buf(3));
    ASSERT_EQ(0, decoded.size(3));
}

}  // namespace codec
}  // namespace openmldb

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    return RUN_ALL_TESTS();
}
