/*
 * memcomparable_format_test.cc
 */

#include "base/memcomparable_format.h"
#include "gtest/gtest.h"

namespace rtidb {
namespace base {

class MemComFormatTest : public ::testing::Test {
 public:
    MemComFormatTest() {}
    ~MemComFormatTest() {}
};

TEST_F(MemComFormatTest, TestIntegersNum) {
    {
        // test positive small int
        std::vector<std::string> vec;
        int16_t arr[3] = {-10, 10, 0};
        for (int i = 0; i < 3; i++) {
            int16_t small_int = arr[i];
            std::string str;
            str.resize(sizeof(int16_t));
            char* to = const_cast<char*>(str.data());
            ASSERT_EQ(PackInteger(&small_int, sizeof(small_int), false, to), 0);
            int16_t dst;
            UnpackInteger(to, sizeof(int16_t), false, &dst);
            ASSERT_EQ(dst, arr[i]);
            vec.push_back(str);
        }
        std::vector<std::string> sort_vec = vec;
        std::sort(sort_vec.begin(), sort_vec.end());
        ASSERT_EQ(sort_vec.at(0), vec.at(0));
        ASSERT_EQ(sort_vec.at(1), vec.at(2));
        ASSERT_EQ(sort_vec.at(2), vec.at(1));
    }
    {
        // test int
        std::vector<std::string> vec;
        int32_t arr[3] = {-100, 100, 0};
        for (int i = 0; i < 3; i++) {
            int32_t int_32 = arr[i];
            std::string str;
            str.resize(sizeof(int32_t));
            char* to = const_cast<char*>(str.data());
            ASSERT_EQ(PackInteger(&int_32, sizeof(int_32), false, to), 0);
            int32_t dst;
            UnpackInteger(to, sizeof(int32_t), false, &dst);
            ASSERT_EQ(dst, arr[i]);
            vec.push_back(str);
        }
        std::vector<std::string> sort_vec = vec;
        std::sort(sort_vec.begin(), sort_vec.end());
        ASSERT_EQ(sort_vec.at(0), vec.at(0));
        ASSERT_EQ(sort_vec.at(1), vec.at(2));
        ASSERT_EQ(sort_vec.at(2), vec.at(1));
    }
    {
        // test big int
        std::vector<std::string> vec;
        int64_t arr[3] = {-1000, 1000,  0};
        for (int i = 0; i < 3; i++) {
            int64_t int_64 = arr[i];
            std::string str;
            str.resize(sizeof(int64_t));
            char* to = const_cast<char*>(str.data());
            ASSERT_EQ(PackInteger(&int_64, sizeof(int_64), false, to), 0);
            int64_t dst;
            UnpackInteger(to, sizeof(int64_t), false, &dst);
            ASSERT_EQ(dst, arr[i]);
            vec.push_back(str);
        }
        std::vector<std::string> sort_vec = vec;
        std::sort(sort_vec.begin(), sort_vec.end());
        ASSERT_EQ(sort_vec.at(0), vec.at(0));
        ASSERT_EQ(sort_vec.at(1), vec.at(2));
        ASSERT_EQ(sort_vec.at(2), vec.at(1));
    }
}

TEST_F(MemComFormatTest, TestFloatingNum) {
    {
        //  test float 
        std::vector<std::string> vec;
        float arr[3] = {-6.66, 6.66, 0};
        for (int i = 0; i < 3; i++) {
            float f = arr[i];
            std::string str;
            str.resize(sizeof(float));
            char* to = const_cast<char*>(str.data());
            ASSERT_EQ(PackFloat(&f, sizeof(float), to), 0);
            float dst;
            UnpackFloat(to, &dst);
            ASSERT_EQ(dst, (float)arr[i]);
            vec.push_back(str);
        }
        std::vector<std::string> sort_vec = vec;
        std::sort(sort_vec.begin(), sort_vec.end());
        ASSERT_EQ(sort_vec.at(0), vec.at(0));
        ASSERT_EQ(sort_vec.at(1), vec.at(2));
        ASSERT_EQ(sort_vec.at(2), vec.at(1));
    }
    {
        // test double
        std::vector<std::string> vec;
        double arr[3] = {-6.66, 6.66, 0};
        for (int i = 0; i < 3; i++) {
            double d = arr[i];
            std::string str;
            str.resize(sizeof(double));
            char* to = const_cast<char*>(str.data());
            ASSERT_EQ(PackDouble(&d, sizeof(double), to), 0);
            double dst;
            UnpackDouble(to, &dst);
            ASSERT_EQ(dst, arr[i]);
            vec.push_back(str);
        }
        std::vector<std::string> sort_vec = vec;
        std::sort(sort_vec.begin(), sort_vec.end());
        ASSERT_EQ(sort_vec.at(0), vec.at(0));
        ASSERT_EQ(sort_vec.at(1), vec.at(2));
        ASSERT_EQ(sort_vec.at(2), vec.at(1));
    }
}

TEST_F(MemComFormatTest, TestVarchar) {
    std::vector<std::string> vec;
    int arr_len = 6;
    std::string arr[arr_len] 
        = {"12345678", "123456789", "1234567890123456789", 
            "",         " ",         "1234567"};
    for (int i = 0; i < arr_len; i++) {
        //  pack varchar
        std::string str = arr[i];
        const char* src = str.c_str();
        size_t str_len = str.length();
        int32_t dst_len = GetDstStrSize(str_len);
        //printf("--------------step1 dst_len %d\n", dst_len);
        std::string from;
        from.resize(dst_len);
        char* dst = const_cast<char*>(from.data());
        ASSERT_EQ(PackString(src, str_len, (void**)&dst), 0);
        vec.push_back(from);

        // unpack varchar
        int32_t size = 0;
        char* to = new char[from.length()];
        ASSERT_EQ(UnpackString(from.c_str(), to, &size), 0);
        std::string res(to, size);
        ASSERT_EQ(res, arr[i]);
    }
    std::vector<std::string> sort_vec = vec;
    std::sort(sort_vec.begin(), sort_vec.end());
    ASSERT_EQ(sort_vec.at(0), vec.at(3));
    ASSERT_EQ(sort_vec.at(1), vec.at(4));
    ASSERT_EQ(sort_vec.at(2), vec.at(5));
    ASSERT_EQ(sort_vec.at(3), vec.at(0));
    ASSERT_EQ(sort_vec.at(4), vec.at(1));
    ASSERT_EQ(sort_vec.at(5), vec.at(2));
}

}  // namespace base
}  // namespace rtidb

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
