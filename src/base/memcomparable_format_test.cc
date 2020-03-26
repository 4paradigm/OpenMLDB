/*
 * memcomparable_format_test.cc
*/


#include "base/memcomparable_format.h"
#include "gtest/gtest.h"

namespace rtidb {
namespace base {

class MemComFormatTest : public ::testing::Test {

public:
    MemComFormatTest(){}
    ~MemComFormatTest() {}
};

TEST_F(MemComFormatTest, TestIntegersNum) {
    char* to;
    {
        /* test positive small int*/
        int16_t arr[3] = {-10, 0, 10};
        for (int i = 0; i < 3; i++) {
            int16_t small_int = arr[i];
            to = new char[sizeof(int16_t)];
            ASSERT_EQ(PackInteger(&small_int, sizeof(small_int), false, to), 0);
            int16_t dst;
            UnpackInteger(to, sizeof(int16_t), false, &dst);
            ASSERT_EQ(dst, arr[i]);
        }
    }
    {
        /* test int*/
        int32_t arr[3] = {-100, 0, 100};
        for (int i = 0; i < 3; i++) {
            int32_t int_32 = arr[i];
            to = new char[sizeof(int32_t)];
            ASSERT_EQ(PackInteger(&int_32, sizeof(int_32), false, to), 0);
            int32_t dst;
            UnpackInteger(to, sizeof(int32_t), false, &dst);
            ASSERT_EQ(dst, arr[i]);
        }
    }
    {
        /* test big int*/
        int64_t arr[3] = {-1000, 0, 1000};
        for (int i = 0; i < 3; i++) {
            int64_t int_64 = arr[i];
            to = new char[sizeof(int64_t)];
            ASSERT_EQ(PackInteger(&int_64, sizeof(int_64), false, to), 0);
            int64_t dst;
            UnpackInteger(to, sizeof(int64_t), false, &dst);
            ASSERT_EQ(dst, arr[i]);
        }
    }
    delete to;
}

TEST_F(MemComFormatTest, TestFloatingNum) {
    char* to;
    {
        /* test float */
        float arr[3] = {-6.66, 0, 6.66};
        for (int i = 0; i < 3; i++) {
            float f = arr[i];
            to = new char[sizeof(f)];
            ASSERT_EQ(PackFloat(&f, sizeof(float), to), 0);
            float dst;
            UnpackFloat(to, &dst);
            ASSERT_EQ(dst, (float)arr[i]);
        }
    }
    {
        /* test double */
        double arr[3] = {-6.66, 0, 6.66};
        for (int i = 0; i < 3; i++) {
            double d = arr[i];
            to = new char[sizeof(d)];
            ASSERT_EQ(PackDouble(&d, sizeof(double), to), 0);
            double dst;
            UnpackDouble(to, &dst);
            ASSERT_EQ(dst, arr[i]);
        }
    }
    delete to;
}

TEST_F(MemComFormatTest, TestVarchar) {
    int arr_len = 6;
    std::string arr[arr_len] 
        = {"", " ", "1234567", "12345678", "123456789", "1234567890123456789"};
    for (int i = 0; i < arr_len; i++) {
        /* pack varchar*/
        std::string str = arr[i];
        const char* src = str.c_str();
        size_t str_len = str.length();
        int32_t dst_len = GetDstStrSize(str_len);
        printf("--------------step1 dst_len %d\n", dst_len);
        std::string from;
        from.resize(dst_len);
        char* dst = const_cast<char*>(from.data());
        ASSERT_EQ(PackString(src, str_len, (void**)&dst), 0);

        /*unpack varchar*/
        int32_t size = 0;
        char* to = new char[from.length()];
        ASSERT_EQ(UnpackString(from.c_str(), to, &size), 0);
        std::string res(to, size);
        ASSERT_EQ(res, arr[i]);
    }
}

}
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
