/*
 * codec_test.cc
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

#include "storage/codec.h"
#include <string>
#include <vector>
#include "gtest/gtest.h"

namespace fesql {
namespace storage {
class CodecTest : public ::testing::Test {};

TEST_F(CodecTest, NULL) {
    Schema schema;
    ::fesql::type::ColumnDef* col = schema.Add();
    col->set_name("col1");
    col->set_type(::fesql::type::kInt16);
    col = schema.Add();
    col->set_name("col2");
    col->set_type(::fesql::type::kBool);
    col = schema.Add();
    col->set_name("col3");
    col->set_type(::fesql::type::kString);
    RowBuilder builder(schema);
    uint32_t size = builder.CalTotalLength(2);
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
            col->set_type(::fesql::type::kString);
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
            col->set_type(::fesql::type::kString);
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
            col->set_type(::fesql::type::kString);
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

}  // namespace storage
}  // namespace fesql
int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
