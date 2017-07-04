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

struct DescComparator {
    int operator()(const uint32_t a, const uint32_t b) const {
        if (a > b) {
            return -1;
        }else if (a == b) {
            return 0;
        }
        return 1;
    }
};



struct StrComparator {
    int operator()(const std::string& a, const std::string& b) const {
        return a.compare(b);
    }
};



TEST_F(NodeTest, SetNext) {
    uint32_t key = 1;
    uint32_t value = 2;
    Node<uint32_t, uint32_t> node(key, value, 2);
    uint32_t key2 = 3;
    uint32_t value2 = 3;
    Node<uint32_t, uint32_t> node2(key2, value2, 2);
    node.SetNext(1, &node2);
    Node<uint32_t, uint32_t>* node_ptr = node.GetNext(1);
    ASSERT_EQ(3, node_ptr->GetValue());
    ASSERT_EQ(3, node_ptr->GetKey());
}

TEST_F(NodeTest, AddToFirst) {
    Comparator cmp;
    Skiplist<uint32_t, uint32_t, Comparator> sl(12, 4, cmp);
    uint32_t key3 = 2;
    uint32_t value3 = 5;
    sl.Insert(key3, value3);
    uint32_t key4 = 3;
    uint32_t value4= 6;
    sl.Insert(key4, value4);
    uint32_t key1 = 1;
    uint32_t value1 = 1;
    bool ok = sl.AddToFirst(key1, value1);
    ASSERT_TRUE(ok);
    Skiplist<uint32_t, uint32_t, Comparator>::Iterator* it = sl.NewIterator();
    it->SeekToFirst();
    ASSERT_TRUE(it->Valid());
    ASSERT_EQ(1, it->GetKey());
    ASSERT_EQ(1, it->GetValue());
    it->Next();
    ASSERT_TRUE(it->Valid());
    ASSERT_EQ(2, it->GetKey());
    ASSERT_EQ(5, it->GetValue());
    delete it;

    uint32_t key_bad = 2;
    uint32_t value_bad = 2;
    ok = sl.AddToFirst(key_bad, value_bad);
    ASSERT_FALSE(ok);
}


TEST_F(SkiplistTest, InsertAndIterator) {
    Comparator cmp;
    Skiplist<uint32_t, uint32_t, Comparator> sl(12, 4, cmp);
    uint32_t key1 = 1;
    uint32_t value1 = 2;
    sl.Insert(key1, value1);
    uint32_t key2 = 2;
    uint32_t value2 = 4;
    sl.Insert(key2, value2);
    uint32_t key3 = 2;
    uint32_t value3 = 5;
    sl.Insert(key3, value3);
    uint32_t key4 = 3;
    uint32_t value4= 6;
    sl.Insert(key4, value4);
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

TEST_F(SkiplistTest, GetSize) {
    Comparator cmp;
    Skiplist<uint32_t, uint32_t, Comparator> sl(12, 4, cmp);
    ASSERT_EQ(0, sl.GetSize());
    uint32_t key1 = 1;
    uint32_t value1 = 2;
    sl.Insert(key1, value1);
    uint32_t key3 = 2;
    uint32_t value3 = 5;
    sl.Insert(key3, value3);
    uint32_t key4 = 3;
    uint32_t value4= 6;
    sl.Insert(key4, value4);
    ASSERT_EQ(3, sl.GetSize());
}

TEST_F(SkiplistTest, Iterator) {
    Comparator cmp;
    Skiplist<uint32_t, uint32_t, Comparator> sl(12, 4, cmp);
    Skiplist<uint32_t, uint32_t, Comparator>::Iterator* it = sl.NewIterator();
    it->Seek(0);
    ASSERT_FALSE(it->Valid());
    delete it;
    {
        uint32_t key = 1;
        uint32_t value=  2;
        sl.Insert(key, value);
    }
    {
        uint32_t key = 2;
        uint32_t value=  3;
        sl.Insert(key, value);
    }
    {
        uint32_t key = 3;
        uint32_t value=  4;
        sl.Insert(key, value);
    }
    it = sl.NewIterator();
    it->SeekToFirst();
    ASSERT_EQ(1, it->GetKey());
    ASSERT_EQ(2, it->GetValue());
    it->Next();
    ASSERT_TRUE(it->Valid());
    ASSERT_EQ(2, it->GetKey());
    ASSERT_EQ(3, it->GetValue());
    it->Next();
    ASSERT_TRUE(it->Valid());
    ASSERT_EQ(3, it->GetKey());
    ASSERT_EQ(4, it->GetValue());
    it->Next();
    ASSERT_FALSE(it->Valid());
    delete it;
}

TEST_F(SkiplistTest, Split1) {
    Comparator cmp;
    Skiplist<uint32_t, uint32_t, Comparator> sl(12, 4, cmp);
    uint32_t key1 = 0;
    uint32_t value1= 0;
    sl.Insert(key1, value1);
    uint32_t key2 = 1;
    uint32_t value2= 1;
    sl.Insert(key2, value2);
    uint32_t key3 = 2;
    uint32_t value3= 2;
    sl.Insert(key3, value3);
    uint32_t key4 = 3;
    uint32_t value4= 6;
    sl.Insert(key4, value4);
    Node<uint32_t, uint32_t>* node = sl.Split(4);
    ASSERT_EQ(NULL, node);
    node = sl.Split(1);
    ASSERT_EQ(1, node->GetKey());
    node = node->GetNext(0);
    ASSERT_TRUE(node != NULL);
    ASSERT_EQ(2, node->GetKey());
    node = node->GetNext(0);
    ASSERT_TRUE(node != NULL);
    ASSERT_EQ(3, node->GetKey());
    node = node->GetNext(0);
    ASSERT_TRUE(node == NULL);
    Skiplist<uint32_t, uint32_t, Comparator>::Iterator* it = sl.NewIterator();
    it->Seek(0);
    ASSERT_EQ(0, it->GetKey());
    it->Next();
    ASSERT_FALSE(it->Valid());
    // Can not find the node deleted
    it->Seek(2);
    ASSERT_FALSE(it->Valid());
}

TEST_F(SkiplistTest, Iterator2) {
    StrComparator cmp;
    Skiplist<std::string, std::string, StrComparator> sl(12, 4, cmp);
    std::string k = "h";
    std::string v= "b";
    sl.Insert(k, v);
    std::string k1 = "a";
    std::string v2="b";
    sl.Insert(k1, v2);
    Skiplist<std::string, std::string, StrComparator>::Iterator* it = sl.NewIterator();
    it->Seek("h");
    ASSERT_EQ("h", it->GetKey());
    it->Next();
    ASSERT_FALSE(it->Valid());
}

TEST_F(SkiplistTest, Clear) {
    StrComparator cmp;
    Skiplist<std::string, std::string, StrComparator> sl(12, 4, cmp);
    std::string k = "h";
    std::string v= "b";
    sl.Insert(k, v);
    std::string k1 = "a";
    std::string v2="b";
    sl.Insert(k1, v2);
    ASSERT_EQ(2,sl.Clear());
}

TEST_F(SkiplistTest, Remove) {
    StrComparator cmp;
    Skiplist<std::string, std::string, StrComparator> sl(12, 4, cmp);
    std::string k = "h";
    std::string v= "b";
    sl.Insert(k, v);
    std::string k1 = "a";
    std::string v2="b";
    sl.Insert(k1, v2);
    std::string k2 = "b";
    std::string v3="c";
    sl.Insert(k2, v3);
    std::string k3 = "c";
    Node<std::string, std::string>* none_exist_node = sl.Remove(k3);
    ASSERT_FALSE(none_exist_node != NULL);
    Node<std::string, std::string>* node = sl.Remove(k2);
    ASSERT_FALSE(node == NULL);
    ASSERT_EQ("b", node->GetKey());
    ASSERT_EQ("c", node->GetValue());
    Skiplist<std::string, std::string, StrComparator>::Iterator* it = sl.NewIterator();
    it->SeekToFirst();
    ASSERT_TRUE(it->Valid());
    ASSERT_EQ("a", it->GetKey());
    it->Next();
    ASSERT_TRUE(it->Valid());
    ASSERT_EQ("h", it->GetKey());
    it->Next();
    ASSERT_FALSE(it->Valid());
}



TEST_F(SkiplistTest, Get) {
    Comparator cmp;
    Skiplist<uint32_t, uint32_t, Comparator> sl(12, 4, cmp);
    uint32_t key = 1;
    uint32_t value = 1;
    sl.Insert(key, value);
    uint32_t ret = sl.Get(1);
    ASSERT_EQ(1, ret);
    ASSERT_FALSE(sl.Get(2) == 2);
}

TEST_F(SkiplistTest, Duplicate) {
    DescComparator cmp;
    Skiplist<uint32_t, uint32_t, DescComparator> sl(12, 4, cmp);
    {
        uint32_t key = 1;
        uint32_t value = 1;
        sl.Insert(key, value);
    }
    {
        uint32_t key = 1;
        uint32_t value = 2;
        sl.Insert(key, value);
    }
    {
        uint32_t key = 2;
        uint32_t value = 3;
        sl.Insert(key, value);
    }

    Skiplist<uint32_t, uint32_t, DescComparator>::Iterator* it = sl.NewIterator();
    it->SeekToFirst();
    ASSERT_TRUE(it->Valid());
    ASSERT_EQ(2, it->GetKey());
    ASSERT_EQ(3, it->GetValue());
    it->Next();
    ASSERT_TRUE(it->Valid());
    ASSERT_EQ(1, it->GetKey());
    ASSERT_EQ(2, it->GetValue());
    it->Next();
    ASSERT_TRUE(it->Valid());
    ASSERT_EQ(1, it->GetKey());
    ASSERT_EQ(1, it->GetValue());
    it->Next();
    ASSERT_FALSE(it->Valid());
}

}
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
