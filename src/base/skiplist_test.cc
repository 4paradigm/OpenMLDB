//
// skip_list_test.cc
// Copyright 2017 4paradigm.com 

#include "base/skiplist.h"
#include "gtest/gtest.h"

namespace rtidb {
namespace base {

class NodeTest : public ::testing::Test {

public:
    NodeTest(){}
    ~NodeTest() {}
};

class SkiplistTest : public ::testing::Test {

public:
    SkiplistTest(){}
    ~SkiplistTest() {}

};

struct Comparator {
    int operator()(const uint32_t a, const uint32_t b) const {
        if (a > b) {
            return 1;
        }else if (a == b) {
            return 0;
        }
        return -1;
    }
};

TEST_F(NodeTest, SetNext) {
    Node<uint32_t> node(1, 2);
    Node<uint32_t> node2(3, 2);
    node.SetNext(1, &node2);
    const Node<uint32_t>* node_ptr = node.GetNext(1);
    const uint32_t result = node_ptr->GetData();
    ASSERT_EQ(3, result);
}

TEST_F(NodeTest, SetNext2) {
    uint32_t data1 = 1;
    uint32_t data2 = 3;
    Node<uint32_t*> node(&data1, 2);
    Node<uint32_t*> node2(&data2, 2);
    node.SetNext(1, &node2);
    const Node<uint32_t*>* node_ptr = node.GetNext(1);
    const uint32_t* result = node_ptr->GetData();
    ASSERT_EQ(3, *result);
}

TEST_F(SkiplistTest, InsertAndIterator) {
    Comparator cmp;
    Skiplist<uint32_t, Comparator> sl(12, 4, cmp);
    sl.Insert(1);
    sl.Insert(2);
    sl.Insert(2);
    sl.Insert(3);
    Skiplist<uint32_t, Comparator>::Iterator* it = sl.NewIterator();
    it->Seek(0);
    ASSERT_EQ(1, it->GetData());
    it->Next();
    ASSERT_EQ(2, it->GetData());
    it->Next();
    ASSERT_EQ(2, it->GetData());
    it->Next();
    ASSERT_EQ(3, it->GetData());
    it->Next();
    ASSERT_FALSE(it->Valid());
    it->Seek(2);
    ASSERT_EQ(2, it->GetData());
    delete it;
}

TEST_F(SkiplistTest, Iterator) {
    Comparator cmp;
    Skiplist<uint32_t, Comparator> sl(12, 4, cmp);
    Skiplist<uint32_t, Comparator>::Iterator* it = sl.NewIterator();
    it->Seek(0);
    ASSERT_FALSE(it->Valid());
    delete it;
    sl.Insert(1);
    it = sl.NewIterator();
    it->SeekToFirst();
    ASSERT_EQ(1, it->GetData());
    it->Next();
    ASSERT_FALSE(it->Valid());
    delete it;
}

TEST_F(SkiplistTest, Split1) {
    Comparator cmp;
    Skiplist<uint32_t, Comparator> sl(12, 4, cmp);
    sl.Insert(0);
    sl.Insert(1);
    sl.Insert(2);
    sl.Insert(3);
    Node<uint32_t>* node = sl.Split(1);
    ASSERT_EQ(1, node->GetData());
    Skiplist<uint32_t, Comparator>::Iterator* it = sl.NewIterator();
    it->Seek(0);
    ASSERT_EQ(0, it->GetData());
    it->Next();
    ASSERT_FALSE(it->Valid());
}

}
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
