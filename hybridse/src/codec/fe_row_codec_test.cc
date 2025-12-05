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

#include "codec/fe_row_codec.h"

#include <string>
#include <vector>

#include "gflags/gflags.h"
#include "gtest/gtest.h"

DECLARE_bool(enable_spark_unsaferow_format);

namespace hybridse {
namespace codec {

class CodecTest : public ::testing::Test {};

TEST_F(CodecTest, NULLTest) {
    Schema schema;
    ::hybridse::type::ColumnDef* col = schema.Add();
    col->set_name("col1");
    col->set_type(::hybridse::type::kInt16);
    col = schema.Add();
    col->set_name("col2");
    col->set_type(::hybridse::type::kBool);
    col = schema.Add();
    col->set_name("col3");
    col->set_type(::hybridse::type::kVarchar);
    RowBuilder builder(schema);
    uint32_t size = builder.CalTotalLength(1);
    std::string row;
    row.resize(size);
    builder.SetBuffer(reinterpret_cast<int8_t*>(&(row[0])), size);
    std::string st("1");
    ASSERT_TRUE(builder.AppendNULL());
    ASSERT_TRUE(builder.AppendBool(false));
    ASSERT_TRUE(builder.AppendString(st.c_str(), 1));
    {
        RowView view(schema, reinterpret_cast<int8_t*>(&(row[0])), size);
        ASSERT_TRUE(view.IsNULL(0));
        const char* ch = NULL;
        uint32_t length = 0;
        bool val1 = true;
        ASSERT_EQ(view.GetBool(1, &val1), 0);
        ASSERT_FALSE(val1);
        ASSERT_EQ(view.GetString(2, &ch, &length), 0);
    }
}

TEST_F(CodecTest, Normal) {
    Schema schema;
    ::hybridse::type::ColumnDef* col = schema.Add();
    col->set_name("col1");
    col->set_type(::hybridse::type::kInt32);
    col = schema.Add();
    col->set_name("col2");
    col->set_type(::hybridse::type::kInt16);
    col = schema.Add();
    col->set_name("col3");
    col->set_type(::hybridse::type::kFloat);
    col = schema.Add();
    col->set_name("col4");
    col->set_type(::hybridse::type::kDouble);
    col = schema.Add();
    col->set_name("col5");
    col->set_type(::hybridse::type::kInt64);
    RowBuilder builder(schema);
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
        RowView view(schema, reinterpret_cast<int8_t*>(&(row[0])), size);
        int32_t val = 0;
        ASSERT_EQ(view.GetInt32(0, &val), 0);
        ASSERT_EQ(val, 1);
        int16_t val1 = 0;
        ASSERT_EQ(view.GetInt16(1, &val1), 0);
        ASSERT_EQ(val1, 2);
    }
}

TEST_F(CodecTest, TimestampTest) {
    Schema schema;
    ::hybridse::type::ColumnDef* col = schema.Add();
    col->set_name("col1");
    col->set_type(::hybridse::type::kInt32);
    col = schema.Add();
    col->set_name("std_ts");
    col->set_type(::hybridse::type::kTimestamp);
    RowBuilder builder(schema);
    uint32_t size = builder.CalTotalLength(0);
    std::string row;
    row.resize(size);
    builder.SetBuffer(reinterpret_cast<int8_t*>(&(row[0])), size);
    ASSERT_TRUE(builder.AppendInt32(1));
    ASSERT_TRUE(builder.AppendTimestamp(1590115420000L));
    {
        RowView view(schema, reinterpret_cast<int8_t*>(&(row[0])), size);
        int32_t val = 0;
        ASSERT_EQ(view.GetInt32(0, &val), 0);
        ASSERT_EQ(val, 1);
        int64_t val1 = 0;
        ASSERT_EQ(view.GetTimestamp(1, &val1), 0);
        ASSERT_EQ(val1, 1590115420000L);
        ASSERT_EQ("1590115420000", view.GetAsString(1));
    }
}

TEST_F(CodecTest, DateTest) {
    Schema schema;
    ::hybridse::type::ColumnDef* col = schema.Add();
    col->set_name("col1");
    col->set_type(::hybridse::type::kInt32);
    col = schema.Add();
    col->set_name("std_date");
    col->set_type(::hybridse::type::kDate);
    RowBuilder builder(schema);
    uint32_t size = builder.CalTotalLength(0);
    std::string row;
    row.resize(size);
    builder.SetBuffer(reinterpret_cast<int8_t*>(&(row[0])), size);
    ASSERT_TRUE(builder.AppendInt32(1));
    ASSERT_TRUE(builder.AppendDate(2020, 05, 27));

    {
        RowView view(schema, reinterpret_cast<int8_t*>(&(row[0])), size);
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
        ASSERT_EQ("2020-05-27", view.GetAsString(1));

        ASSERT_EQ(view.GetDateUnsafe(1), 7865371);
        ASSERT_EQ(view.GetYearUnsafe(7865371), 2020);
        ASSERT_EQ(view.GetMonthUnsafe(7865371), 5);
        ASSERT_EQ(view.GetDayUnsafe(7865371), 27);
    }
}

TEST_F(CodecTest, Encode) {
    Schema schema;
    for (int i = 0; i < 10; i++) {
        ::hybridse::type::ColumnDef* col = schema.Add();
        col->set_name("col" + std::to_string(i));
        if (i % 3 == 0) {
            col->set_type(::hybridse::type::kInt16);
        } else if (i % 3 == 1) {
            col->set_type(::hybridse::type::kDouble);
        } else {
            col->set_type(::hybridse::type::kVarchar);
        }
    }
    RowBuilder builder(schema);
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
        RowView view(schema, reinterpret_cast<int8_t*>(&(row[0])), size);
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
                const char* ch = NULL;
                uint32_t length = 0;
                ASSERT_EQ(view.GetString(i, &ch, &length), 0);
                std::string str(ch, length);
                ASSERT_STREQ(str.c_str(), std::string(10, 'a' + i).c_str());
            }
        }
        int16_t val = 0;
        ASSERT_EQ(view.GetInt16(10, &val), -1);
    }
}

TEST_F(CodecTest, AppendNULL) {
    Schema schema;
    for (int i = 0; i < 20; i++) {
        ::hybridse::type::ColumnDef* col = schema.Add();
        col->set_name("col" + std::to_string(i));
        if (i % 3 == 0) {
            col->set_type(::hybridse::type::kInt16);
        } else if (i % 3 == 1) {
            col->set_type(::hybridse::type::kDouble);
        } else {
            col->set_type(::hybridse::type::kVarchar);
        }
    }
    RowBuilder builder(schema);
    uint32_t size = builder.CalTotalLength(30);
    std::string row;
    row.resize(size);
    builder.SetBuffer(reinterpret_cast<int8_t*>(&(row[0])), size);
    for (int i = 0; i < 20; i++) {
        if (i % 2 == 0) {
            ASSERT_TRUE(builder.AppendNULL());
            continue;
        }
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
    RowView view(schema, reinterpret_cast<int8_t*>(&(row[0])), size);
    for (int i = 0; i < 20; i++) {
        if (i % 3 == 0) {
            int16_t val = 0;
            int ret = view.GetInt16(i, &val);
            if (i % 2 == 0) {
                ASSERT_TRUE(view.IsNULL(i));
                ASSERT_EQ(ret, 1);
            } else {
                ASSERT_EQ(ret, 0);
                ASSERT_EQ(val, i);
            }
        } else if (i % 3 == 1) {
            double val = 0.0;
            int ret = view.GetDouble(i, &val);
            if (i % 2 == 0) {
                ASSERT_TRUE(view.IsNULL(i));
                ASSERT_EQ(ret, 1);
            } else {
                ASSERT_EQ(ret, 0);
                ASSERT_EQ(val, 2.3);
            }
        } else {
            const char* ch = NULL;
            uint32_t length = 0;
            int ret = view.GetString(i, &ch, &length);
            if (i % 2 == 0) {
                ASSERT_TRUE(view.IsNULL(i));
                ASSERT_EQ(ret, 1);
            } else {
                ASSERT_EQ(ret, 0);
                std::string str(ch, length);
                ASSERT_STREQ(str.c_str(), std::string(10, 'a' + i).c_str());
            }
        }
    }
    int16_t val = 0;
    ASSERT_EQ(view.GetInt16(20, &val), -1);
}

TEST_F(CodecTest, AppendNULLAndEmpty) {
    Schema schema;
    for (int i = 0; i < 20; i++) {
        ::hybridse::type::ColumnDef* col = schema.Add();
        col->set_name("col" + std::to_string(i));
        if (i % 2 == 0) {
            col->set_type(::hybridse::type::kInt16);
        } else {
            col->set_type(::hybridse::type::kVarchar);
        }
    }
    RowBuilder builder(schema);
    uint32_t size = builder.CalTotalLength(30);
    std::string row;
    row.resize(size);
    builder.SetBuffer(reinterpret_cast<int8_t*>(&(row[0])), size);
    for (int i = 0; i < 20; i++) {
        if (i % 2 == 0) {
            if (i % 3 == 0) {
                ASSERT_TRUE(builder.AppendNULL());
            } else {
                ASSERT_TRUE(builder.AppendInt16(i));
            }
        } else {
            std::string str(10, 'a' + i);
            if (i % 3 == 0) {
                ASSERT_TRUE(builder.AppendNULL());
            } else if (i % 3 == 1) {
                ASSERT_TRUE(builder.AppendString(str.c_str(), 0));
            } else {
                ASSERT_TRUE(builder.AppendString(str.c_str(), str.length()));
            }
        }
    }
    ASSERT_FALSE(builder.AppendInt16(1234));
    RowView view(schema, reinterpret_cast<int8_t*>(&(row[0])), size);
    for (int i = 0; i < 20; i++) {
        if (i % 2 == 0) {
            int16_t val = 0;
            int ret = view.GetInt16(i, &val);
            if (i % 3 == 0) {
                ASSERT_TRUE(view.IsNULL(i));
                ASSERT_EQ(ret, 1);
            } else {
                ASSERT_EQ(ret, 0);
                ASSERT_EQ(val, i);
            }
        } else {
            const char* ch = NULL;
            uint32_t length = 0;
            int ret = view.GetString(i, &ch, &length);
            if (i % 3 == 0) {
                ASSERT_TRUE(view.IsNULL(i));
                ASSERT_EQ(ret, 1);
            } else if (i % 3 == 1) {
                ASSERT_EQ(ret, 0);
                ASSERT_EQ(length, 0u);
            } else {
                ASSERT_EQ(ret, 0);
                std::string str(ch, length);
                ASSERT_STREQ(str.c_str(), std::string(10, 'a' + i).c_str());
            }
        }
    }
    int16_t val = 0;
    ASSERT_EQ(view.GetInt16(20, &val), -1);
}

TEST_F(CodecTest, ManyCol) {
    std::vector<int> num_vec = {10, 20, 50, 100, 1000, 10000, 100000};
    for (auto col_num : num_vec) {
        ::hybridse::type::TableDef def;
        for (int i = 0; i < col_num; i++) {
            ::hybridse::type::ColumnDef* col = def.add_columns();
            col->set_name("col" + std::to_string(i + 1));
            col->set_type(::hybridse::type::kVarchar);
            col = def.add_columns();
            col->set_name("col" + std::to_string(i + 2));
            col->set_type(::hybridse::type::kInt64);
            col = def.add_columns();
            col->set_name("col" + std::to_string(i + 3));
            col->set_type(::hybridse::type::kDouble);
        }
        RowBuilder builder(def.columns());
        uint32_t size = builder.CalTotalLength(10 * col_num);
        uint64_t base = 1000000000;
        uint64_t ts = 1576811755000;
        std::string row;
        row.resize(size);
        row.clear();
        row.resize(size);
        builder.SetBuffer(reinterpret_cast<int8_t*>(&(row[0])), size);
        for (int idx = 0; idx < col_num; idx++) {
            ASSERT_TRUE(
                builder.AppendString(std::to_string(base + idx).c_str(), 10));
            ASSERT_TRUE(builder.AppendInt64(ts + idx));
            ASSERT_TRUE(builder.AppendDouble(1.3));
        }
        RowView view(def.columns(), reinterpret_cast<int8_t*>(&(row[0])), size);
        for (int idx = 0; idx < col_num; idx++) {
            const char* ch = NULL;
            uint32_t length = 0;
            int ret = view.GetString(idx * 3, &ch, &length);
            ASSERT_EQ(ret, 0);
            std::string str(ch, length);
            ASSERT_STREQ(str.c_str(), std::to_string(base + idx).c_str());
            int64_t val = 0;
            ret = view.GetInt64(idx * 3 + 1, &val);
            ASSERT_EQ(ret, 0);
            ASSERT_EQ(val, static_cast<int64_t>(ts + idx));
            double d = 0.0;
            ret = view.GetDouble(idx * 3 + 2, &d);
            ASSERT_EQ(ret, 0);
            ASSERT_DOUBLE_EQ(d, 1.3);
        }
    }
}

TEST_F(CodecTest, SliceFormatTest) {
    std::vector<int> num_vec = {10, 20, 50, 100, 1000};
    for (auto col_num : num_vec) {
        ::hybridse::type::TableDef def;
        for (int i = 0; i < col_num; i++) {
            ::hybridse::type::ColumnDef* col = def.add_columns();
            col->set_name("col" + std::to_string(i));
            if (i % 3 == 0) {
                col->set_type(::hybridse::type::kVarchar);
            } else if (i % 3 == 1) {
                col->set_type(::hybridse::type::kInt64);
            } else if (i % 3 == 2) {
                col->set_type(::hybridse::type::kDouble);
            }
        }

        SliceFormat decoder(&def.columns());
        for (int i = 0; i < col_num; i++) {
            if (i % 3 == 0) {
                const codec::ColInfo* info = decoder.GetColumnInfo(i);
                ASSERT_TRUE(info != nullptr);
                ASSERT_EQ(::hybridse::type::kVarchar, info->type());

                ASSERT_TRUE(decoder.GetStringColumnInfo(i).ok());
            } else if (i % 3 == 1) {
                const codec::ColInfo* info = decoder.GetColumnInfo(i);
                ASSERT_TRUE(info != nullptr);
                ASSERT_EQ(::hybridse::type::kInt64, info->type());
            } else if (i % 3 == 2) {
                const codec::ColInfo* info = decoder.GetColumnInfo(i);
                ASSERT_TRUE(info != nullptr);
                ASSERT_EQ(::hybridse::type::kDouble, info->type());
            }
        }
    }
}

TEST_F(CodecTest, SliceFormatOffsetTest) {
    type::TableDef table;
    table.set_name("t1");
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
    {
        ::hybridse::type::ColumnDef* column = table.add_columns();
        column->set_type(::hybridse::type::kVarchar);
        column->set_name("col7");
    }

    SliceFormat decoder(&table.columns());
    {
        const codec::ColInfo* info = decoder.GetColumnInfo(0);
        ASSERT_EQ(::hybridse::type::kInt32, info->type());
        LOG(INFO) << "offset: " << info->offset;
        ASSERT_EQ(7u, info->offset);
    }
    {
        const codec::ColInfo* info = decoder.GetColumnInfo(1);
        ASSERT_EQ(::hybridse::type::kInt16, info->type());
        LOG(INFO) << "offset: " << info->offset;
        ASSERT_EQ(7u + 4u, info->offset);
    }
    {
        const codec::ColInfo* info = decoder.GetColumnInfo(2);
        ASSERT_EQ(::hybridse::type::kFloat, info->type());
        LOG(INFO) << "offset: " << info->offset;
        ASSERT_EQ(7u + 4u + 2u, info->offset);
    }
    {
        const codec::ColInfo* info = decoder.GetColumnInfo(3);
        ASSERT_EQ(::hybridse::type::kDouble, info->type());
        LOG(INFO) << "offset: " << info->offset;
        ASSERT_EQ(7u + 4u + 2u + 4u, info->offset);
    }
    {
        const codec::ColInfo* info = decoder.GetColumnInfo(4);
        ASSERT_EQ(::hybridse::type::kInt64, info->type());
        LOG(INFO) << "offset: " << info->offset;
        ASSERT_EQ(7u + 4u + 2u + 4u + 8u, info->offset);
    }
    {
        const codec::ColInfo* info = decoder.GetColumnInfo(5);
        ASSERT_EQ(::hybridse::type::kVarchar, info->type());

        auto rs = decoder.GetStringColumnInfo(5);
        ASSERT_TRUE(rs.ok());
        auto& str_info = rs.value();
        LOG(INFO) << "offset: " << str_info.offset
                  << " next_offset: " << str_info.str_next_offset
                  << " str_start_offset " << str_info.str_start_offset;
        ASSERT_EQ(0u, str_info.offset);
        ASSERT_EQ(1u, str_info.str_next_offset);
        ASSERT_EQ(33u, str_info.str_start_offset);
    }
    {
        const codec::ColInfo* info = decoder.GetColumnInfo(6);
        ASSERT_EQ(::hybridse::type::kVarchar, info->type());

        auto rs = decoder.GetStringColumnInfo(6);
        ASSERT_TRUE(rs.ok());
        auto& str_info = rs.value();
        LOG(INFO) << "offset: " << str_info.offset
                  << " next_offset: " << str_info.str_next_offset
                  << " str_start_offset " << str_info.str_start_offset;
        ASSERT_EQ(1u, str_info.offset);
        ASSERT_EQ(0u, str_info.str_next_offset);
        ASSERT_EQ(33u, str_info.str_start_offset);
    }
}
TEST_F(CodecTest, SliceFormatOffsetLongHeaderTest) {
    type::TableDef table;
    table.set_name("t1");
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
    {
        ::hybridse::type::ColumnDef* column = table.add_columns();
        column->set_type(::hybridse::type::kVarchar);
        column->set_name("col7");
    }
    {
        ::hybridse::type::ColumnDef* column = table.add_columns();
        column->set_type(::hybridse::type::kInt64);
        column->set_name("col8");
    }
    {
        ::hybridse::type::ColumnDef* column = table.add_columns();
        column->set_type(::hybridse::type::kDouble);
        column->set_name("col9");
    }

    SliceFormat decoder(&table.columns());
    {
        const codec::ColInfo* info = decoder.GetColumnInfo(0);
        ASSERT_EQ(::hybridse::type::kInt32, info->type());
        LOG(INFO) << "offset: " << info->offset;
        ASSERT_EQ(8u, info->offset);
    }
    {
        const codec::ColInfo* info = decoder.GetColumnInfo(1);
        ASSERT_EQ(::hybridse::type::kInt16, info->type());
        LOG(INFO) << "offset: " << info->offset;
        ASSERT_EQ(8u + 4u, info->offset);
    }
    {
        const codec::ColInfo* info = decoder.GetColumnInfo(2);
        ASSERT_EQ(::hybridse::type::kFloat, info->type());
        LOG(INFO) << "offset: " << info->offset;
        ASSERT_EQ(8u + 4u + 2u, info->offset);
    }
    {
        const codec::ColInfo* info = decoder.GetColumnInfo(3);
        ASSERT_EQ(::hybridse::type::kDouble, info->type());
        LOG(INFO) << "offset: " << info->offset;
        ASSERT_EQ(8u + 4u + 2u + 4u, info->offset);
    }
    {
        const codec::ColInfo* info = decoder.GetColumnInfo(4);
        ASSERT_EQ(::hybridse::type::kInt64, info->type());
        LOG(INFO) << "offset: " << info->offset;
        ASSERT_EQ(8u + 4u + 2u + 4u + 8u, info->offset);
    }
    {
        const codec::ColInfo* info = decoder.GetColumnInfo(5);
        ASSERT_EQ(::hybridse::type::kVarchar, info->type());

        auto str_info_wp = decoder.GetStringColumnInfo(5);
        ASSERT_TRUE(str_info_wp.ok());
        auto& str_info = str_info_wp.value();
        LOG(INFO) << "offset: " << str_info.offset
                  << " next_offset: " << str_info.str_next_offset
                  << " str_start_offset " << str_info.str_start_offset;
        ASSERT_EQ(0u, str_info.offset);
        ASSERT_EQ(1u, str_info.str_next_offset);
        ASSERT_EQ(50u, str_info.str_start_offset);
    }
    {
        const codec::ColInfo* info = decoder.GetColumnInfo(6);
        ASSERT_EQ(::hybridse::type::kVarchar, info->type());

        auto str_info_wp = decoder.GetStringColumnInfo(6);
        ASSERT_TRUE(str_info_wp.ok());
        auto& str_info = str_info_wp.value();
        LOG(INFO) << "offset: " << str_info.offset
                  << " next_offset: " << str_info.str_next_offset
                  << " str_start_offset " << str_info.str_start_offset;
        ASSERT_EQ(1u, str_info.offset);
        ASSERT_EQ(0u, str_info.str_next_offset);
        ASSERT_EQ(50u, str_info.str_start_offset);
    }
}
TEST_F(CodecTest, SparkUnsaferowBitMapSizeTest) {
    FLAGS_enable_spark_unsaferow_format = false;
    ASSERT_EQ(BitMapSize(3), 1);
    ASSERT_EQ(BitMapSize(8), 1);
    ASSERT_EQ(BitMapSize(9), 2);
    ASSERT_EQ(BitMapSize(20), 3);
    ASSERT_EQ(BitMapSize(65), 9);

    FLAGS_enable_spark_unsaferow_format = true;
    ASSERT_EQ(BitMapSize(3), 8);
    ASSERT_EQ(BitMapSize(8), 8);
    ASSERT_EQ(BitMapSize(9), 8);
    ASSERT_EQ(BitMapSize(20), 8);
    ASSERT_EQ(BitMapSize(65), 16);

    FLAGS_enable_spark_unsaferow_format = false;
}
TEST_F(CodecTest, SparkUnsaferowRowFormatTest) {
    FLAGS_enable_spark_unsaferow_format = true;

    std::vector<int> num_vec = {10, 20, 50, 100, 1000};
    for (auto col_num : num_vec) {
        ::hybridse::type::TableDef def;
        for (int i = 0; i < col_num; i++) {
            ::hybridse::type::ColumnDef* col = def.add_columns();
            col->set_name("col" + std::to_string(i));
            if (i % 3 == 0) {
                col->set_type(::hybridse::type::kVarchar);
            } else if (i % 3 == 1) {
                col->set_type(::hybridse::type::kInt64);
            } else if (i % 3 == 2) {
                col->set_type(::hybridse::type::kDouble);
            }
        }

        SliceFormat decoder(&def.columns());
        for (int i = 0; i < col_num; i++) {
            if (i % 3 == 0) {
                const codec::ColInfo* info = decoder.GetColumnInfo(i);
                ASSERT_TRUE(info != nullptr);
                ASSERT_EQ(::hybridse::type::kVarchar, info->type());

                auto rs = decoder.GetStringColumnInfo(i);
                ASSERT_TRUE(rs.ok());
            } else if (i % 3 == 1) {
                const codec::ColInfo* info = decoder.GetColumnInfo(i);
                ASSERT_TRUE(info != nullptr);
                ASSERT_EQ(::hybridse::type::kInt64, info->type());
            } else if (i % 3 == 2) {
                const codec::ColInfo* info = decoder.GetColumnInfo(i);
                ASSERT_TRUE(info != nullptr);
                ASSERT_EQ(::hybridse::type::kDouble, info->type());
            }
        }
    }

    FLAGS_enable_spark_unsaferow_format = false;
}

}  // namespace codec
}  // namespace hybridse

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
