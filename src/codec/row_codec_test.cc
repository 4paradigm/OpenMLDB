/*
 * row_codec_test.cc
 * Copyright (C) 4paradigm.com 2019 denglong <denglong@4paradigm.com>
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

#include "codec/row_codec.h"
#include <string>
#include <vector>
#include "gtest/gtest.h"

namespace fesql {
namespace codec {

class CodecTest : public ::testing::Test {};

TEST_F(CodecTest, NULLTest) {
    Schema schema;
    ::fesql::type::ColumnDef* col = schema.Add();
    col->set_name("col1");
    col->set_type(::fesql::type::kInt16);
    col = schema.Add();
    col->set_name("col2");
    col->set_type(::fesql::type::kBool);
    col = schema.Add();
    col->set_name("col3");
    col->set_type(::fesql::type::kVarchar);
    RowBuilder builder(schema);
    uint32_t size = builder.CalTotalLength(1);
    std::string row;
    row.resize(size);
    builder.SetBuffer(reinterpret_cast<int8_t*>(&(row[0])), size);
    std::string st("1");
    ASSERT_TRUE(builder.AppendNULL());
    ASSERT_TRUE(builder.AppendBool(false));
    ASSERT_TRUE(builder.AppendString(st.c_str(), 1));
    RowView view(schema, reinterpret_cast<int8_t*>(&(row[0])), size);
    ASSERT_TRUE(view.IsNULL(0));
    char* ch = NULL;
    uint32_t length = 0;
    bool val1 = true;
    ASSERT_EQ(view.GetBool(1, &val1), 0);
    ASSERT_FALSE(val1);
    ASSERT_EQ(view.GetString(2, &ch, &length), 0);
}

TEST_F(CodecTest, Normal) {
    Schema schema;
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
    RowView view(schema, reinterpret_cast<int8_t*>(&(row[0])), size);
    int32_t val = 0;
    ASSERT_EQ(view.GetInt32(0, &val), 0);
    ASSERT_EQ(val, 1);
    int16_t val1 = 0;
    ASSERT_EQ(view.GetInt16(1, &val1), 0);
    ASSERT_EQ(val1, 2);
}

TEST_F(CodecTest, Encode) {
    Schema schema;
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
            char* ch = NULL;
            uint32_t length = 0;
            ASSERT_EQ(view.GetString(i, &ch, &length), 0);
            std::string str(ch, length);
            ASSERT_STREQ(str.c_str(), std::string(10, 'a' + i).c_str());
        }
    }
    int16_t val = 0;
    ASSERT_EQ(view.GetInt16(10, &val), -1);
}

TEST_F(CodecTest, AppendNULL) {
    Schema schema;
    for (int i = 0; i < 20; i++) {
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
            char* ch = NULL;
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
        ::fesql::type::ColumnDef* col = schema.Add();
        col->set_name("col" + std::to_string(i));
        if (i % 2 == 0) {
            col->set_type(::fesql::type::kInt16);
        } else {
            col->set_type(::fesql::type::kVarchar);
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
            char* ch = NULL;
            uint32_t length = 0;
            int ret = view.GetString(i, &ch, &length);
            if (i % 3 == 0) {
                ASSERT_TRUE(view.IsNULL(i));
                ASSERT_EQ(ret, 1);
            } else if (i % 3 == 1) {
                ASSERT_EQ(ret, 0);
                ASSERT_EQ(length, 0);
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
        ::fesql::type::TableDef def;
        for (int i = 0; i < col_num; i++) {
            ::fesql::type::ColumnDef* col = def.add_columns();
            col->set_name("col" + std::to_string(i + 1));
            col->set_type(::fesql::type::kVarchar);
            col = def.add_columns();
            col->set_name("col" + std::to_string(i + 2));
            col->set_type(::fesql::type::kInt64);
            col = def.add_columns();
            col->set_name("col" + std::to_string(i + 3));
            col->set_type(::fesql::type::kDouble);
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
            char* ch = NULL;
            uint32_t length = 0;
            int ret = view.GetString(idx * 3, &ch, &length);
            ASSERT_EQ(ret, 0);
            std::string str(ch, length);
            ASSERT_STREQ(str.c_str(), std::to_string(base + idx).c_str());
            int64_t val = 0;
            ret = view.GetInt64(idx * 3 + 1, &val);
            ASSERT_EQ(ret, 0);
            ASSERT_EQ(val, ts + idx);
            double d = 0.0;
            ret = view.GetDouble(idx * 3 + 2, &d);
            ASSERT_EQ(ret, 0);
            ASSERT_DOUBLE_EQ(d, 1.3);
        }
    }
}

}  // namespace codec
}  // namespace fesql

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
