/**
 * Copyright (C) 2017 4paradigm.com
 * field_codec_test.cc
 */
#include <vector>
#include "base/field_codec.h"
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
        {
            std::string val_1 = "";
            val_1.resize(1);
            char* buf = const_cast<char*>(val_1.data());
            ::rtidb::base::Convert(true, buf);
            vec.push_back(val_1);
        }
        {
            std::string val_2 = "";
            val_2.resize(1);
            char* buf = const_cast<char*>(val_2.data());
            ::rtidb::base::Convert(false, buf);
            vec.push_back(val_2);
        }
        {
            int16_t int16_val = 33;
            std::string val_3 = "";
            val_3.resize(2);
            char* buf = const_cast<char*>(val_3.data());
            ::rtidb::base::Convert(int16_val, buf);
            vec.push_back(val_3);
        }
        {
            int32_t int32_val = 44;
            std::string val_4 = "";
            val_4.resize(4);
            char* buf = const_cast<char*>(val_4.data());
            ::rtidb::base::Convert(int32_val, buf);
            vec.push_back(val_4);
        }
        {
            int64_t int64_val = 55;
            std::string val_5 = "";
            val_5.resize(8);
            char* buf = const_cast<char*>(val_5.data());
            ::rtidb::base::Convert(int64_val, buf);
            vec.push_back(val_5);
        }
        {
            float float_val = 3.3;
            std::string val_6 = "";
            val_6.resize(4);
            char* buf = const_cast<char*>(val_6.data());
            ::rtidb::base::Convert(float_val, buf);
            vec.push_back(val_6);
        }
        {
            double double_val = 4.4;
            std::string val_7 = "";
            val_7.resize(8);
            char* buf = const_cast<char*>(val_7.data());
            ::rtidb::base::Convert(double_val, buf);
            vec.push_back(val_7);
        }
    }
    {
        /**
         * decode part
         */
        bool v1 = false;
        ::rtidb::base::GetBool(vec[0].data(), &v1);
        ASSERT_EQ(v1, true);

        bool v2 = true;
        ::rtidb::base::GetBool(vec[1].data(), &v2);
        ASSERT_EQ(v2, false);

        int16_t v3 = 0;
        ::rtidb::base::GetInt16(vec[2].data(), &v3);
        ASSERT_EQ(v3, 33);

        int32_t v4 = 0;
        ::rtidb::base::GetInt32(vec[3].data(), &v4);
        ASSERT_EQ(v4, 44);

        int64_t v5 = 0;
        ::rtidb::base::GetInt64(vec[4].data(), &v5);
        ASSERT_EQ(v5, 55);

        float v6 = 0.0;
        ::rtidb::base::GetFloat(vec[5].data(), &v6);
        ASSERT_EQ(v6, 3.3f);

        double v7 = 0.0;
        ::rtidb::base::GetDouble(vec[6].data(), &v7);
        ASSERT_EQ(v7, 4.4);
    }
}

}  // namespace base
}  // namespace rtidb

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
