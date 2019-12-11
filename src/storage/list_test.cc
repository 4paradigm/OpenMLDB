// list.h
// Copyright (C) 2017 4paradigm.com
// Author denglong
// Date 2019-10-24
//

#include "storage/list.h"
#include <sys/time.h>
#include <time.h>
#include <random>
#include <string>
#include "gtest/gtest.h"
#include "storage/skiplist.h"

namespace fesql {
namespace storage {

using ::fesql::base::DefaultComparator;
DefaultComparator cmp;

class ListTest : public ::testing::Test {
 public:
    ListTest() {}
    ~ListTest() {}
};

static inline int64_t get_micros() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return static_cast<int64_t>(tv.tv_sec) * 1000000 + tv.tv_usec;
}

uint64_t loop_time = 10000;
uint64_t record_cnt = 300;

TEST_F(ListTest, Size) {
    ArrayList<uint64_t, void*, DefaultComparator> list(cmp);
    ASSERT_EQ(32ul, sizeof(list));
}

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
    while (iter->Valid()) {
        ASSERT_EQ(arr[count], iter->GetKey());
        iter->Next();
        count++;
    }
    ASSERT_EQ(count, 4);
    delete iter;
}

TEST_F(ListTest, ArrayListSplitByPos) {
    ArrayList<uint64_t, uint64_t, DefaultComparator> list(cmp);
    list.SplitByPos(1111);
    uint64_t value1 = 5;
    list.Insert(5, value1);
    list.SplitByPos(1111);
    ASSERT_EQ(list.GetSize(), 1u);
    list.SplitByPos(1);
    ASSERT_EQ(list.GetSize(), 1u);
    list.Insert(6, value1);
    ASSERT_EQ(list.GetSize(), 2u);
    list.SplitByPos(1);
    ASSERT_EQ(list.GetSize(), 1u);
    for (int i = 0; i < 50; i++) {
        list.Insert(6, value1);
    }
    ASSERT_EQ(list.GetSize(), 51u);
    list.SplitByPos(52);
    ASSERT_EQ(list.GetSize(), 51u);
    list.SplitByPos(51);
    ASSERT_EQ(list.GetSize(), 51u);
    list.SplitByPos(20);
    ASSERT_EQ(list.GetSize(), 20u);
    list.SplitByPos(0);
    ASSERT_EQ(list.GetSize(), 0u);
}

TEST_F(ListTest, LinkListSplitByPos) {
    LinkList<uint64_t, uint64_t, DefaultComparator> list(cmp);
    list.SplitByPos(1111);
    uint64_t value1 = 5;
    list.Insert(5, value1);
    list.SplitByPos(1111);
    ASSERT_EQ(list.GetSize(), 1u);
    list.SplitByPos(1);
    ASSERT_EQ(list.GetSize(), 1u);
    list.Insert(6, value1);
    ASSERT_EQ(list.GetSize(), 2u);
    list.SplitByPos(1);
    ASSERT_EQ(list.GetSize(), 1u);
    for (int i = 0; i < 50; i++) {
        list.Insert(6, value1);
    }
    ASSERT_EQ(list.GetSize(), 51u);
    list.SplitByPos(52);
    ASSERT_EQ(list.GetSize(), 51u);
    list.SplitByPos(51);
    ASSERT_EQ(list.GetSize(), 51u);
    list.SplitByPos(20);
    ASSERT_EQ(list.GetSize(), 20u);
    list.SplitByPos(0);
    ASSERT_EQ(list.GetSize(), 0u);
}

TEST_F(ListTest, ArrayListSplit) {
    ArrayList<uint64_t, uint64_t, DefaultComparator> list(cmp);
    list.Split(1111);
    uint64_t value1 = 5;
    list.Insert(5, value1);
    list.Split(1);
    ASSERT_EQ(list.GetSize(), 1u);
    list.Split(1111);
    ASSERT_EQ(list.GetSize(), 0u);
    for (int i = 0; i < 10; i++) {
        list.Insert(i, value1);
    }
    ASSERT_EQ(list.GetSize(), 10u);
    list.Split(4);
    ASSERT_EQ(list.GetSize(), 5u);
    list.Insert(8, value1);
    list.Insert(7, value1);
    ASSERT_EQ(list.GetSize(), 7u);
    list.Split(7);
    ASSERT_EQ(list.GetSize(), 3u);
}

TEST_F(ListTest, LinkListSplit) {
    LinkList<uint64_t, uint64_t, DefaultComparator> list(cmp);
    list.Split(1111);
    uint64_t value1 = 5;
    list.Insert(5, value1);
    list.Split(1);
    ASSERT_EQ(list.GetSize(), 1u);
    list.Split(1111);
    ASSERT_EQ(list.GetSize(), 0u);
    for (int i = 0; i < 10; i++) {
        list.Insert(i, value1);
    }
    ASSERT_EQ(list.GetSize(), 10u);
    list.Split(4);
    ASSERT_EQ(list.GetSize(), 5u);
    list.Insert(8, value1);
    list.Insert(7, value1);
    ASSERT_EQ(list.GetSize(), 7u);
    list.Split(7);
    ASSERT_EQ(list.GetSize(), 3u);
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
    while (iter->Valid()) {
        ASSERT_EQ(arr[count], iter->GetKey());
        iter->Next();
        count++;
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
    while (iter->Valid()) {
        if (count > 0) {
            ASSERT_EQ(iter->GetValue(), 4 - count);
        }
        iter->Next();
        count++;
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
    while (iter->Valid()) {
        if (count > 0) {
            ASSERT_EQ(iter->GetValue(), 4 - count);
        }
        iter->Next();
        count++;
    }
    ASSERT_EQ(count, 4);
    delete iter;
}

/*TEST_F(ListTest, SkipListPerform) {
    {
        std::vector<::fesql::base::Skiplist<uint64_t, uint64_t,
DefaultComparator>*> vec; uint64_t value = 1; for (uint64_t idx = 0; idx <
loop_time; idx++) {
            ::fesql::base::Skiplist<uint64_t, uint64_t, DefaultComparator>* list
= new ::fesql::base::Skiplist<uint64_t, uint64_t, DefaultComparator>(12, 4,
cmp); for (uint64_t i = 0; i < record_cnt; i++) { list->Insert(i, value);
            }
            vec.push_back(list);
        }
        std::default_random_engine engine;
        uint64_t cur_time = get_micros();
        for (uint64_t idx = 0; idx < loop_time; idx++) {
            vec[idx]->Insert(engine() % record_cnt, value);
        }
        uint64_t time_used = get_micros() - cur_time;
        printf("skiplist insert time: %lu avg: %lu\n", time_used, time_used /
loop_time);
    }
    SkipList<uint64_t, uint64_t, DefaultComparator> list(12, 4, cmp);
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
    printf("skiplist time: %lu avg: %lu\n", time_used, time_used / loop_time);
}*/

/*TEST_F(ListTest, LinkListPerform) {
    {
        std::vector<LinkList<uint64_t, uint64_t, DefaultComparator>*> vec;
        uint64_t value = 1;
        for (uint64_t idx = 0; idx < loop_time; idx++) {
            LinkList<uint64_t, uint64_t, DefaultComparator>* list =
                new LinkList<uint64_t, uint64_t, DefaultComparator>(cmp);
            for (uint64_t i = 0; i < record_cnt; i++) {
                list->Insert(i, value);
            }
            vec.push_back(list);
        }
        std::default_random_engine engine;
        uint64_t cur_time = get_micros();
        for (uint64_t idx = 0; idx < loop_time; idx++) {
            vec[idx]->Insert(engine() % record_cnt, value);
        }
        uint64_t time_used = get_micros() - cur_time;
        printf("linklist insert time: %lu avg: %lu\n", time_used, time_used /
loop_time);
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
}*/

/*TEST_F(ListTest, ArrayListPerform) {
    {
        std::vector<ArrayList<uint64_t, uint64_t, DefaultComparator>*> vec;
        uint64_t value = 1;
        for (uint64_t idx = 0; idx < loop_time; idx++) {
            ArrayList<uint64_t, uint64_t, DefaultComparator>* list =
                new ArrayList<uint64_t, uint64_t, DefaultComparator>(cmp);
            for (uint64_t i = 0; i < record_cnt; i++) {
                list->Insert(i, value);
            }
            vec.push_back(list);
        }
        std::default_random_engine engine;
        uint64_t cur_time = get_micros();
        for (uint64_t idx = 0; idx < loop_time; idx++) {
            vec[idx]->Insert(engine() % record_cnt, value);
        }
        uint64_t time_used = get_micros() - cur_time;
        printf("arraylist insert time: %lu avg: %lu\n", time_used, time_used /
loop_time);
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

}  // namespace storage
}  // namespace fesql

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
