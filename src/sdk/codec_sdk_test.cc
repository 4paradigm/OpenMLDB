/*
 * Copyright (c) 2021 4Paradigm
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


#include "sdk/codec_sdk.h"
#include <string>
#include <vector>
#include "gtest/gtest.h"

namespace fesql {
namespace sdk {

class CodecSDKTest : public ::testing::Test {};

TEST_F(CodecSDKTest, NULLTest) {
    codec::Schema schema;
    ::fesql::type::ColumnDef* col = schema.Add();
    col->set_name("col1");
    col->set_type(::fesql::type::kInt16);
    col = schema.Add();
    col->set_name("col2");
    col->set_type(::fesql::type::kBool);
    col = schema.Add();
    col->set_name("col3");
    col->set_type(::fesql::type::kVarchar);
    codec::RowBuilder builder(schema);
    uint32_t size = builder.CalTotalLength(1);
    std::string row;
    row.resize(size);
    builder.SetBuffer(reinterpret_cast<int8_t*>(&(row[0])), size);
    std::string st("1");
    ASSERT_TRUE(builder.AppendNULL());
    ASSERT_TRUE(builder.AppendBool(false));
    ASSERT_TRUE(builder.AppendString(st.c_str(), 1));
    butil::IOBuf io_buf;
    io_buf.append(reinterpret_cast<const void*>(row.c_str()), size);
    {
        RowIOBufView rv(schema);
        rv.Reset(io_buf);
        ASSERT_TRUE(rv.IsNULL(0));
        bool val1 = true;
        ASSERT_EQ(rv.GetBool(1, &val1), 0);
        ASSERT_FALSE(val1);
        butil::IOBuf tmp;
        ASSERT_EQ(rv.GetString(2, &tmp), 0);
    }
}

TEST_F(CodecSDKTest, Normal) {
    codec::Schema schema;
    ::fesql::type::ColumnDef* col = schema.Add();
    col->set_name("col1");
    col->set_type(::fesql::type::kInt32);
    col = schema.Add();
    col->set_name("col2");
    col->set_type(::fesql::type::kInt16);
    col = schema.Add();
    col->set_name("col3");
    col->set_type(::fesql::type::kFloat);
    col = schema.Add();
    col->set_name("col4");
    col->set_type(::fesql::type::kDouble);
    col = schema.Add();
    col->set_name("col5");
    col->set_type(::fesql::type::kInt64);
    codec::RowBuilder builder(schema);
    uint32_t size = builder.CalTotalLength(0);
    std::string row;
    row.resize(size);
    builder.SetBuffer(reinterpret_cast<int8_t*>(&(row[0])), size);
    ASSERT_TRUE(builder.AppendInt32(1));
    ASSERT_TRUE(builder.AppendInt16(2));
    ASSERT_TRUE(builder.AppendFloat(3.1));
    ASSERT_TRUE(builder.AppendDouble(4.1));
    ASSERT_TRUE(builder.AppendInt64(5));
    {
        butil::IOBuf buf;
        buf.append(row);
        RowIOBufView view(schema);
        view.Reset(buf);
        int32_t val = 0;
        ASSERT_EQ(view.GetInt32(0, &val), 0);
        ASSERT_EQ(val, 1);
        int16_t val1 = 0;
        ASSERT_EQ(view.GetInt16(1, &val1), 0);
        ASSERT_EQ(val1, 2);
    }
}

TEST_F(CodecSDKTest, TimestampTest) {
    codec::Schema schema;
    ::fesql::type::ColumnDef* col = schema.Add();
    col->set_name("col1");
    col->set_type(::fesql::type::kInt32);
    col = schema.Add();
    col->set_name("std_ts");
    col->set_type(::fesql::type::kTimestamp);
    codec::RowBuilder builder(schema);
    uint32_t size = builder.CalTotalLength(0);
    std::string row;
    row.resize(size);
    builder.SetBuffer(reinterpret_cast<int8_t*>(&(row[0])), size);
    ASSERT_TRUE(builder.AppendInt32(1));
    ASSERT_TRUE(builder.AppendTimestamp(1590115420000L));
    {
        butil::IOBuf buf;
        buf.append(row);
        RowIOBufView view(schema);
        view.Reset(buf);
        int32_t val = 0;
        ASSERT_EQ(view.GetInt32(0, &val), 0);
        ASSERT_EQ(val, 1);
        int64_t val1 = 0;
        ASSERT_EQ(view.GetTimestamp(1, &val1), 0);
        ASSERT_EQ(val1, 1590115420000L);
    }
}

TEST_F(CodecSDKTest, DateTest) {
    codec::Schema schema;
    ::fesql::type::ColumnDef* col = schema.Add();
    col->set_name("col1");
    col->set_type(::fesql::type::kInt32);
    col = schema.Add();
    col->set_name("std_date");
    col->set_type(::fesql::type::kDate);
    codec::RowBuilder builder(schema);
    uint32_t size = builder.CalTotalLength(0);
    std::string row;
    row.resize(size);
    builder.SetBuffer(reinterpret_cast<int8_t*>(&(row[0])), size);
    ASSERT_TRUE(builder.AppendInt32(1));
    ASSERT_TRUE(builder.AppendDate(2020, 05, 27));
    {
        butil::IOBuf buf;
        buf.append(row);
        RowIOBufView view(schema);
        view.Reset(buf);
        int32_t val = 0;
        ASSERT_EQ(view.GetInt32(0, &val), 0);
        ASSERT_EQ(val, 1);
        int32_t year;
        int32_t month;
        int32_t day;
        ASSERT_EQ(view.GetDate(1, &year, &month, &day), 0);
        ASSERT_EQ(year, 2020);
        ASSERT_EQ(month, 05);
        ASSERT_EQ(day, 27);
    }
}

TEST_F(CodecSDKTest, Encode) {
    codec::Schema schema;
    for (int i = 0; i < 10; i++) {
        ::fesql::type::ColumnDef* col = schema.Add();
        col->set_name("col" + std::to_string(i));
        if (i % 3 == 0) {
            col->set_type(::fesql::type::kInt16);
        } else if (i % 3 == 1) {
            col->set_type(::fesql::type::kDouble);
        } else {
            col->set_type(::fesql::type::kVarchar);
        }
    }
    codec::RowBuilder builder(schema);
    uint32_t size = builder.CalTotalLength(30);
    std::string row;
    row.resize(size);
    builder.SetBuffer(reinterpret_cast<int8_t*>(&(row[0])), size);
    for (int i = 0; i < 10; i++) {
        if (i % 3 == 0) {
            ASSERT_TRUE(builder.AppendInt16(i));
        } else if (i % 3 == 1) {
            ASSERT_TRUE(builder.AppendDouble(2.3));
        } else {
            std::string str(10, 'a' + i);
            ASSERT_TRUE(builder.AppendString(str.c_str(), str.length()));
        }
    }
    ASSERT_FALSE(builder.AppendInt16(1234));
    {
        butil::IOBuf buf;
        buf.append(row);
        RowIOBufView view(schema);
        view.Reset(buf);
        for (int i = 0; i < 10; i++) {
            if (i % 3 == 0) {
                int16_t val = 0;
                ASSERT_EQ(view.GetInt16(i, &val), 0);
                ASSERT_EQ(val, i);
            } else if (i % 3 == 1) {
                double val = 0.0;
                ASSERT_EQ(view.GetDouble(i, &val), 0);
                ASSERT_EQ(val, 2.3);
            } else {
                butil::IOBuf tmp;
                ASSERT_EQ(view.GetString(i, &tmp), 0);
                ASSERT_STREQ(tmp.to_string().c_str(),
                             std::string(10, 'a' + i).c_str());
            }
        }
    }
}

}  // namespace sdk
}  // namespace fesql

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
