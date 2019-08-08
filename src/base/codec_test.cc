/*
 * codec_test.cc
 */


#include "base/codec.h"
#include "storage/segment.h"
#include "gtest/gtest.h"
#include "proto/common.pb.h"
#include "proto/tablet.pb.h"
#include "base/kv_iterator.h"


namespace rtidb {
namespace base {

class CodecTest : public ::testing::Test {

public:
    CodecTest(){}
    ~CodecTest() {}
};

TEST_F(CodecTest, EncodeRows_empty) {
    std::vector<std::pair<uint64_t, ::rtidb::base::Slice>> data;
    std::string pairs;
    int32_t size = ::rtidb::base::EncodeRows(data, 0, &pairs);
    ASSERT_EQ(size, 0);
}


TEST_F(CodecTest, EncodeRows_invalid) {
    std::vector<std::pair<uint64_t, ::rtidb::base::Slice>> data;
    int32_t size = ::rtidb::base::EncodeRows(data, 0, NULL);
    ASSERT_EQ(size, -1);
}

TEST_F(CodecTest, EncodeRows) {
    std::vector<std::pair<uint64_t, ::rtidb::base::Slice>> data;
    std::string test1 = "value1";
    std::string test2 = "value2";
    std::string empty;
    uint32_t total_block_size = test1.length() + test2.length() + empty.length();
    data.push_back(std::make_pair(1, ::rtidb::base::Slice(test1.c_str(), test1.length())));
    data.push_back(std::make_pair(2, ::rtidb::base::Slice(test2.c_str(), test2.length())));
    data.push_back(std::make_pair(3, ::rtidb::base::Slice(empty.c_str(), empty.length())));
    std::string pairs;
    int32_t size = ::rtidb::base::EncodeRows(data, total_block_size, &pairs);
    ASSERT_EQ(size, 3 * 12 + 6 + 6);
    std::vector<std::pair<uint64_t, std::string*>> new_data;
    ::rtidb::base::Decode(&pairs, new_data);
    ASSERT_EQ(data.size(), new_data.size());
    ASSERT_EQ(new_data[0].second->compare(test1), 0);
    ASSERT_EQ(new_data[1].second->compare(test2), 0);
    ASSERT_EQ(new_data[2].second->compare(empty), 0);
}

}
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
