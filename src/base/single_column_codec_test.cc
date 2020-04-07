/**
 * single_column_codec_test.h
 */

#include "base/single_column_codec.h" 
#include "gtest/gtest.h"

namespace rtidb {
namespace base {

class SingleColumnCodecTest : public ::testing::Test {
 public:
    SingleColumnCodecTest() {}
    ~SingleColumnCodecTest() {}
};

TEST_F(SingleColumnCodecTest, TestEncodec) {
    std::vector<std::string> vec;
    {
        /**
         * encode part
         */
        vec.push_back(::rtidb::base::Append(true));
        vec.push_back(::rtidb::base::Append(false));
        int16_t int16_val = 33;
        vec.push_back(::rtidb::base::Append(int16_val));
        int32_t int32_val = 44;
        vec.push_back(::rtidb::base::Append(int32_val));
        int64_t int64_val = 55;
        vec.push_back(::rtidb::base::Append(int64_val));
        float float_val = 3.3;
        vec.push_back(::rtidb::base::Append(float_val));
        double double_val = 4.4;
        vec.push_back(::rtidb::base::Append(double_val));
    }
    {
        /**
         * decode part
         */
        ASSERT_EQ(::rtidb::base::GetBool(vec[0]), true); 
        ASSERT_EQ(::rtidb::base::GetBool(vec[1]), false); 
        ASSERT_EQ(::rtidb::base::GetInt16(vec[2]), 33); 
        ASSERT_EQ(::rtidb::base::GetInt32(vec[3]), 44); 
        ASSERT_EQ(::rtidb::base::GetInt64(vec[4]), 55); 
        ASSERT_EQ(::rtidb::base::GetFloat(vec[5]), 3.3f); 
        ASSERT_EQ(::rtidb::base::GetDouble(vec[6]), 4.4); 
    }
}

} //namespace base
} //namespace rtidb

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
