//
// schema_codec_test.cc
// Copyright 2017 4paradigm.com 
//


#include "base/schema_codec.h"
#include "gtest/gtest.h"
#include "base/strings.h"
#include <iostream>

namespace rtidb {
namespace base {

class SchemaCodecTest : public ::testing::Test {

public:
    SchemaCodecTest() {}
    ~SchemaCodecTest() {}
};

TEST_F(SchemaCodecTest, Encode) {

    std::vector<ColumnDesc> columns;
    ColumnDesc desc1;
    desc1.name = "uname";
    desc1.type = ::rtidb::base::ColType::kString;
    desc1.add_ts_idx = true;
    columns.push_back(desc1);
    
    ColumnDesc desc2;
    desc2.name = "age";
    desc2.type = ::rtidb::base::ColType::kInt32;
    desc2.add_ts_idx = false;
    columns.push_back(desc2);

    SchemaCodec codec;
    std::string buffer;
    codec.Encode(columns, buffer);
    std::vector<ColumnDesc > decoded_columns;
    codec.Decode(buffer, decoded_columns);
    ASSERT_EQ(2, decoded_columns.size());
    ASSERT_EQ(::rtidb::base::ColType::kString, decoded_columns[0].type);
    ASSERT_EQ("uname", decoded_columns[0].name);
    ASSERT_EQ(::rtidb::base::ColType::kInt32, decoded_columns[1].type);
    ASSERT_EQ("age", decoded_columns[1].name);
}

TEST_F(SchemaCodecTest, Timestamp) {

    std::vector<ColumnDesc> columns;
    ColumnDesc desc1;
    desc1.name = "card";
    desc1.type = ::rtidb::base::ColType::kString;
    desc1.add_ts_idx = true;
    columns.push_back(desc1);
    
    ColumnDesc desc2;
    desc2.name = "ts";
    desc2.type = ::rtidb::base::ColType::kTimestamp;
    desc2.add_ts_idx = false;
    columns.push_back(desc2);

    SchemaCodec codec;
    std::string buffer;
    codec.Encode(columns, buffer);
    std::vector<ColumnDesc > decoded_columns;
    codec.Decode(buffer, decoded_columns);
    ASSERT_EQ(2, decoded_columns.size());
    ASSERT_EQ(::rtidb::base::ColType::kString, decoded_columns[0].type);
    ASSERT_EQ("card", decoded_columns[0].name);
    ASSERT_EQ(::rtidb::base::ColType::kTimestamp, decoded_columns[1].type);
    ASSERT_EQ("ts", decoded_columns[1].name);
}


}
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
