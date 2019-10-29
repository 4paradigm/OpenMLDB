#include "list.h"
#include "gtest/gtest.h"
#include <time.h>
#include <sys/time.h>
#include <string>
namespace fesql {
namespace storage {
DefaultComparator cmp;

class ListTest : public ::testing::Test {

public:
    ListTest(){}
    ~ListTest() {}
};

static inline long get_micros() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return static_cast<long>(tv.tv_sec) * 1000000 + tv.tv_usec;
}

uint64_t loop_time = 10000;
uint64_t record_cnt = 1000;

TEST_F(ListTest, ArrayList) {
    ArrayList<uint64_t, uint64_t, DefaultComparator> list(cmp);
    uint64_t arr[] = {8, 6, 5, 4};
    uint64_t value1 = 5;
    list.Insert(arr[2], value1);
    uint64_t value2 = 8;
    list.Insert(arr[0], value2);
    uint64_t value3 = 4;
    list.Insert(arr[3], value3);
    uint64_t value4 = 6;
    list.Insert(arr[1], value4);
    Iterator<uint64_t, uint64_t>* iter = list.NewIterator();
    iter->SeekToFirst();
    int count = 0;
    while(iter->Valid()) {
        ASSERT_EQ(arr[count], iter->GetKey());
        iter->Next();
        count ++;
    }
    ASSERT_EQ(count, 4);
    delete iter;
}

TEST_F(ListTest, LinkList) {
    LinkList<uint64_t, uint64_t, DefaultComparator> list(cmp);
    uint64_t arr[] = {8, 6, 5, 4};
    uint64_t value1 = 5;
    list.Insert(arr[2], value1);
    uint64_t value2 = 8;
    list.Insert(arr[0], value2);
    uint64_t value3 = 4;
    list.Insert(arr[3], value3);
    uint64_t value4 = 6;
    list.Insert(arr[1], value4);
    Iterator<uint64_t, uint64_t>* iter = list.NewIterator();
    iter->SeekToFirst();
    int count = 0;
    while(iter->Valid()) {
        ASSERT_EQ(arr[count], iter->GetKey());
        iter->Next();
        count ++;
    }
    ASSERT_EQ(count, 4);
    delete iter;
}

TEST_F(ListTest, LinkListEqualItem) {
    LinkList<uint64_t, uint64_t, DefaultComparator> list(cmp);
    uint64_t value1 = 1;
    list.Insert(5, value1);
    uint64_t value2 = 8;
    list.Insert(8, value2);
    uint64_t value3 = 2;
    list.Insert(5, value3);
    uint64_t value4 = 3;
    list.Insert(5, value4);
    Iterator<uint64_t, uint64_t>* iter = list.NewIterator();
    iter->SeekToFirst();
    int count = 0;
    while(iter->Valid()) {
        if (count > 0) {
            ASSERT_EQ(iter->GetValue(), 4 - count);
        }
        iter->Next();
        count ++;
    }
    ASSERT_EQ(count, 4);
    delete iter;
}

TEST_F(ListTest, ArrayListEqualItem) {
    ArrayList<uint64_t, uint64_t, DefaultComparator> list(cmp);
    uint64_t value1 = 1;
    list.Insert(5, value1);
    uint64_t value2 = 8;
    list.Insert(8, value2);
    uint64_t value3 = 2;
    list.Insert(5, value3);
    uint64_t value4 = 3;
    list.Insert(5, value4);
    Iterator<uint64_t, uint64_t>* iter = list.NewIterator();
    iter->SeekToFirst();
    int count = 0;
    while(iter->Valid()) {
        if (count > 0) {
            ASSERT_EQ(iter->GetValue(), 4 - count);
        }
        iter->Next();
        count ++;
    }
    ASSERT_EQ(count, 4);
    delete iter;
}

/*TEST_F(ListTest, LinkListPerform) {
    {
        uint64_t cur_time = get_micros();
        for (uint64_t idx = 0; idx < loop_time; idx++) {
            LinkList<uint64_t, uint64_t, DefaultComparator> list(cmp);
            uint64_t value = 1;
            for (uint64_t i = 0; i < record_cnt; i++) {
                list.Insert(i, value);
            }
        }
        uint64_t time_used = get_micros() - cur_time;
        printf("linklist insert time: %lu avg: %lu\n", time_used, time_used / loop_time);
    }
    LinkList<uint64_t, uint64_t, DefaultComparator> list(cmp);
    uint64_t value = 1;
    for (uint64_t i = 0; i < record_cnt; i++) {
        list.Insert(i, value);
    }
    uint64_t cur_time = get_micros();
    for (uint64_t idx = 0; idx < loop_time; idx++) {
        Iterator<uint64_t, uint64_t>* iter = list.NewIterator();
        iter->SeekToFirst();
        int count = 0;
        while(iter->Valid()) {
            iter->Next();
            count ++;
        }
        ASSERT_EQ(count, record_cnt);
        delete iter;
    }
    uint64_t time_used = get_micros() - cur_time;
    printf("linklist time: %lu avg: %lu\n", time_used, time_used / loop_time);
}

TEST_F(ListTest, ArrayListPerform) {
    {
        uint64_t cur_time = get_micros();
        for (uint64_t idx = 0; idx < loop_time; idx++) {
            ArrayList<uint64_t, uint64_t, DefaultComparator> list(cmp);
            uint64_t value = 1;
            for (uint64_t i = 0; i < record_cnt; i++) {
                list.Insert(i, value);
            }
        }
        uint64_t time_used = get_micros() - cur_time;
        printf("arraylist insert time: %lu avg: %lu\n", time_used, time_used / loop_time);
    }
    ArrayList<uint64_t, uint64_t, DefaultComparator> list(cmp);
    uint64_t value = 1;
    for (uint64_t i = 0; i < record_cnt; i++) {
        list.Insert(i, value);
    }

    uint64_t cur_time = get_micros();
    for (uint64_t idx = 0; idx < loop_time; idx++) {
        Iterator<uint64_t, uint64_t>* iter = list.NewIterator();
        iter->SeekToFirst();
        int count = 0;
        while(iter->Valid()) {
            iter->Next();
            count ++;
        }
        ASSERT_EQ(count, record_cnt);
        delete iter;
    }
    uint64_t time_used = get_micros() - cur_time;
    printf("arraylist time: %lu avg: %lu\n", time_used, time_used / loop_time);
}*/

}
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

