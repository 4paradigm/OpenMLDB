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

struct StrComparator {
    int operator()(const std::string& a, const std::string& b) const {
        return a.compare(b);
    }
};



TEST_F(NodeTest, SetNext) {
    Node<uint32_t, uint32_t> node(1, 1, 2);
    Node<uint32_t, uint32_t> node2(3, 3, 2);
    node.SetNext(1, &node2);
    const Node<uint32_t, uint32_t>* node_ptr = node.GetNext(1);
    ASSERT_EQ(3, node_ptr->GetValue());
    ASSERT_EQ(3, node_ptr->GetKey());
}


TEST_F(SkiplistTest, InsertAndIterator) {
    Comparator cmp;
    Skiplist<uint32_t, uint32_t, Comparator> sl(12, 4, cmp);
    sl.Insert(1, 2);
    sl.Insert(2, 4);
    sl.Insert(2, 5);
    sl.Insert(3, 6);
    Skiplist<uint32_t, uint32_t, Comparator>::Iterator* it = sl.NewIterator();
    it->Seek(0);
    ASSERT_EQ(1, it->GetKey());
    ASSERT_EQ(2, it->GetValue());
    it->Next();
    ASSERT_EQ(2, it->GetKey());
    ASSERT_EQ(5, it->GetValue());
    it->Next();
    ASSERT_EQ(2, it->GetKey());
    ASSERT_EQ(4, it->GetValue());
    it->Next();
    ASSERT_EQ(3, it->GetKey());
    ASSERT_EQ(6, it->GetValue());
    it->Next();
    ASSERT_FALSE(it->Valid());
    it->Seek(2);
    ASSERT_EQ(2, it->GetKey());
    ASSERT_EQ(5, it->GetValue());
    delete it;
}

TEST_F(SkiplistTest, Iterator) {
    Comparator cmp;
    Skiplist<uint32_t, uint32_t, Comparator> sl(12, 4, cmp);
    Skiplist<uint32_t, uint32_t, Comparator>::Iterator* it = sl.NewIterator();
    it->Seek(0);
    ASSERT_FALSE(it->Valid());
    delete it;
    sl.Insert(1, 2);
    it = sl.NewIterator();
    it->SeekToFirst();
    ASSERT_EQ(1, it->GetKey());
    ASSERT_EQ(2, it->GetValue());
    it->Next();
    ASSERT_FALSE(it->Valid());
    delete it;
}

TEST_F(SkiplistTest, Split1) {
    Comparator cmp;
    Skiplist<uint32_t, uint32_t, Comparator> sl(12, 4, cmp);
    sl.Insert(0, 0);
    sl.Insert(1, 1);
    sl.Insert(2, 2);
    sl.Insert(3, 3);
    Node<uint32_t, uint32_t>* node = sl.Split(1);
    ASSERT_EQ(1, node->GetKey());
    Skiplist<uint32_t, uint32_t, Comparator>::Iterator* it = sl.NewIterator();
    it->Seek(0);
    ASSERT_EQ(0, it->GetKey());
    it->Next();
    ASSERT_FALSE(it->Valid());
    // Can not find the node deleted
    it->Seek(2);
    ASSERT_FALSE(it->Valid());
}

TEST_F(SkiplistTest, Split2) {
    StrComparator cmp;
    Skiplist<std::string, std::string, StrComparator> sl(12, 4, cmp);
    sl.Insert("h", "b");
    sl.Insert("a", "b");
    Skiplist<std::string, std::string, StrComparator>::Iterator* it = sl.NewIterator();
    it->Seek("h");
    ASSERT_EQ("h", it->GetKey());
    it->Next();
    ASSERT_FALSE(it->Valid());
}

TEST_F(SkiplistTest, Get) {
    Comparator cmp;
    Skiplist<uint32_t, uint32_t, Comparator> sl(12, 4, cmp);
    sl.Insert(1, 1);
    uint32_t ret = sl.Get(1);
    ASSERT_EQ(1, ret);
    ASSERT_FALSE(sl.Get(2) == 2);
}

}
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
