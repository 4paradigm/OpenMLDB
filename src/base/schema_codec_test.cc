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
    std::vector<std::pair<ColType, std::string> > columns;
    columns.push_back(std::pair<ColType, std::string>(::rtidb::base::ColType::kString, "uname"));
    columns.push_back(std::pair<ColType, std::string>(::rtidb::base::ColType::kInt32, "age"));
    SchemaCodec codec;
    std::string buffer;
    codec.Encode(columns, buffer);
    std::vector<std::pair<ColType, std::string> > decoded_columns;
    codec.Decode(buffer, decoded_columns);
    ASSERT_EQ(2, decoded_columns.size());
    ASSERT_EQ(::rtidb::base::ColType::kString, decoded_columns[0].first);
    ASSERT_EQ("uname", decoded_columns[0].second);
    ASSERT_EQ(::rtidb::base::ColType::kInt32, decoded_columns[1].first);
    ASSERT_EQ("age", decoded_columns[1].second);
}

}
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
