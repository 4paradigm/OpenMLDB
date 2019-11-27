#include "codec.h"
#include <string>
#include <vector>
#include "gtest/gtest.h"

namespace fesql {
namespace storage {
class CodecTest : public ::testing::Test {
};

TEST_F(CodecTest, encode) {
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
    uint32_t size = RowBuilder::CalTotalLength(schema, 30);
    std::string row;
    row.resize(size);
    RowBuilder builder(schema, (int8_t*)(&(row[0])), size);
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
    RowView view(schema, (int8_t*)(&(row[0])), size);
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
    uint32_t size = RowBuilder::CalTotalLength(schema, 30);
    std::string row;
    row.resize(size);
    RowBuilder builder(schema, (int8_t*)(&(row[0])), size);
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
    RowView view(schema, (int8_t*)(&(row[0])), size);
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
    uint32_t size = RowBuilder::CalTotalLength(schema, 30);
    std::string row;
    row.resize(size);
    RowBuilder builder(schema, (int8_t*)(&(row[0])), size);
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
            } else if (i % 3 == 1){
                ASSERT_TRUE(builder.AppendString(str.c_str(), 0));
            } else {
                ASSERT_TRUE(builder.AppendString(str.c_str(), str.length()));
            }
        }
    }
    ASSERT_FALSE(builder.AppendInt16(1234));
    RowView view(schema, (int8_t*)(&(row[0])), size);
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
            } else if (i % 3 == 1){
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
