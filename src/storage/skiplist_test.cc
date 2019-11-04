//
// skip_list_test.cc
// Copyright 2017 4paradigm.com 

#include "skiplist.h"
#include "gtest/gtest.h"
#include "util/slice.h"
#include <vector>

namespace fesql {
namespace storage {

class NodeTest : public ::testing::Test {

public:
    NodeTest(){}
    ~NodeTest() {}
};

class SkipListTest : public ::testing::Test {

public:
    SkipListTest(){}
    ~SkipListTest() {}

};


struct SliceComparator {
    int operator()(const Slice& a, const Slice& b) const {
        return a.compare(b);
    }
};

std::vector<uint8_t> vec = {1, 2, 5, 10, 12};

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

struct KE {
    Slice k;
    uint32_t v;
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

TEST_F(NodeTest, NodeByteSize) {
    std::atomic<Node<Slice, std::string*>* > node0[12];
    ASSERT_EQ(96, sizeof(node0));
    ASSERT_EQ(32, sizeof(Node<uint64_t, void*>));
    ASSERT_EQ(40, sizeof(Node<Slice, void*>));
}



TEST_F(NodeTest, SliceTest)  {
    SliceComparator cmp;
    SkipList<Slice, KE*, SliceComparator> sl(12, 4, cmp);
    Slice key("test1");
    KE* v = new KE();
    v->k = key;
    v->v = 1;
    sl.Insert(key, v);
    Slice pk("test1");
    KE* n = sl.Get(pk);
    ASSERT_TRUE(pk.compare(n->k) == 0);
}

TEST_F(NodeTest, AddToFirst) {
    Comparator cmp;
    for (auto height : vec) {
        SkipList<uint32_t, uint32_t, Comparator> sl(height, 4, cmp);
        ASSERT_EQ(24, sizeof(sl));
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
        Iterator<uint32_t, uint32_t>* it = sl.NewIterator();
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
}


TEST_F(SkipListTest, InsertAndIterator) {
    Comparator cmp;
    for (auto height : vec) {
        SkipList<uint32_t, uint32_t, Comparator> sl(height, 4, cmp);
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
        Iterator<uint32_t, uint32_t>* it = sl.NewIterator();
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
}

TEST_F(SkipListTest, GetSize) {
    Comparator cmp;
    SkipList<uint32_t, uint32_t, Comparator> sl(12, 4, cmp);
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

TEST_F(SkipListTest, Iterator) {
    Comparator cmp;
    SkipList<uint32_t, uint32_t, Comparator> sl(12, 4, cmp);
    Iterator<uint32_t, uint32_t>* it = sl.NewIterator();
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

TEST_F(SkipListTest, Split1) {
    for (auto height : vec) {
        Comparator cmp;
        SkipList<uint32_t, uint32_t, Comparator> sl(height, 4, cmp);
        uint32_t key1 = 0;
        uint32_t value1= 0;
        sl.Insert(key1, value1);
        uint32_t key2 = 1;
        uint32_t value2= 1;
        sl.Insert(key2, value2);
        uint32_t key3 = 2;
        uint32_t value3= 2;
        sl.Insert(key3, value3);
        ASSERT_EQ(2, sl.GetLast()->GetKey());
        uint32_t key4 = 3;
        uint32_t value4= 6;
        sl.Insert(key4, value4);
        ASSERT_EQ(3, sl.GetLast()->GetKey());
        Node<uint32_t, uint32_t>* node = sl.Split(4);
        ASSERT_EQ(3, sl.GetLast()->GetKey());
        ASSERT_EQ(NULL, node);
        node = sl.Split(1);
        ASSERT_EQ(0, sl.GetLast()->GetKey());
        ASSERT_EQ(1, node->GetKey());
        node = node->GetNext(0);
        ASSERT_TRUE(node != NULL);
        ASSERT_EQ(2, node->GetKey());
        node = node->GetNext(0);
        ASSERT_TRUE(node != NULL);
        ASSERT_EQ(3, node->GetKey());
        node = node->GetNext(0);
        ASSERT_TRUE(node == NULL);
        Iterator<uint32_t, uint32_t>* it = sl.NewIterator();
        it->Seek(0);
        ASSERT_EQ(0, it->GetKey());
        it->Next();
        ASSERT_FALSE(it->Valid());
        // Can not find the node deleted
        it->Seek(2);
        ASSERT_FALSE(it->Valid());
    }
}

TEST_F(SkipListTest, SplitByPos) {
    Comparator cmp;
    for (auto height : vec) {
        SkipList<uint32_t, uint32_t, Comparator> sl(height, 4, cmp);
        uint32_t key1 = 0;
        uint32_t value1= 0;
        sl.Insert(key1, value1);
        uint32_t key2 = 1;
        uint32_t value2= 1;
        sl.Insert(key2, value2);
        uint32_t key3 = 2;
        uint32_t value3= 2;
        sl.Insert(key3, value3);
        // insert the same key
        uint32_t value3_an= 22;
        sl.Insert(key3, value3_an);
        uint32_t key4 = 3;
        uint32_t value4= 6;
        sl.Insert(key4, value4);
        ASSERT_EQ(3, sl.GetLast()->GetKey());

        Node<uint32_t, uint32_t>* node = sl.SplitByPos(6);
        ASSERT_TRUE(node == NULL);
        ASSERT_EQ(3, sl.GetLast()->GetKey());
        node = sl.SplitByPos(3);
        ASSERT_EQ(2, sl.GetLast()->GetKey());
        ASSERT_EQ(2, node->GetKey());
        node = node->GetNext(0);
        ASSERT_TRUE(node != NULL);
        ASSERT_EQ(3, node->GetKey());
        node = node->GetNext(0);
        ASSERT_TRUE(node == NULL);
        Iterator<uint32_t, uint32_t>* it = sl.NewIterator();
        it->Seek(0);
        ASSERT_EQ(0, it->GetKey());
        it->Next();
        ASSERT_TRUE(it->Valid());

        it->Seek(2);
        ASSERT_EQ(2, it->GetKey());
        it->Next();
        ASSERT_FALSE(it->Valid());
    }    
}

TEST_F(SkipListTest, SplitByPos1) {
    Comparator cmp;
    for (auto height : vec) {
        SkipList<uint32_t, uint32_t, Comparator> sl(height, 4, cmp);
        uint32_t key1 = 1;
        uint32_t value1= 1;
        sl.Insert(key1, value1);
        uint32_t key2 = 2;
        uint32_t value2= 2;
        sl.Insert(key2, value2);
        uint32_t key3 = 3;
        uint32_t value3= 3;
        sl.Insert(key3, value3);
        uint32_t key4 = 4;
        uint32_t value4= 4;
        sl.Insert(key4, value4);
        ASSERT_EQ(4, sl.GetLast()->GetKey());
        Node<uint32_t, uint32_t>* node = sl.SplitByPos(2);
        ASSERT_EQ(3, node->GetKey());
        ASSERT_EQ(2, sl.GetLast()->GetKey());
        ASSERT_EQ(2, sl.GetLast()->GetValue());
    }
}    

TEST_F(SkipListTest, Iterator2) {
    StrComparator cmp;
    SkipList<std::string, std::string, StrComparator> sl(12, 4, cmp);
    std::string k = "h";
    std::string v= "b";
    sl.Insert(k, v);
    std::string k1 = "a";
    std::string v2="b";
    sl.Insert(k1, v2);
    Iterator<std::string, std::string>* it = sl.NewIterator();
    it->Seek("h");
    ASSERT_EQ("h", it->GetKey());
    it->Next();
    ASSERT_FALSE(it->Valid());
}

TEST_F(SkipListTest, Clear) {
    StrComparator cmp;
    SkipList<std::string, std::string, StrComparator> sl(12, 4, cmp);
    std::string k = "h";
    std::string v= "b";
    sl.Insert(k, v);
    std::string k1 = "a";
    std::string v2="b";
    sl.Insert(k1, v2);
    ASSERT_EQ(2,sl.Clear());
}

TEST_F(SkipListTest, Remove) {
    StrComparator cmp;
    SkipList<std::string, std::string, StrComparator> sl(12, 4, cmp);
    std::string k = "h";
    std::string v= "b";
    sl.Insert(k, v);
    std::string k1 = "a";
    std::string v2="b";
    sl.Insert(k1, v2);
    std::string k2 = "b";
    std::string v3="c";
    sl.Insert(k2, v3);
    ASSERT_EQ("h", sl.GetLast()->GetKey());
    std::string k3="c";
    Node<std::string, std::string>* none_exist_node = sl.Remove(k3);
    ASSERT_FALSE(none_exist_node != NULL);
    Node<std::string, std::string>* node = sl.Remove(k2);
    ASSERT_EQ("h", sl.GetLast()->GetKey());
    ASSERT_FALSE(node == NULL);
    ASSERT_EQ("b", node->GetKey());
    ASSERT_EQ("c", node->GetValue());
    Iterator<std::string, std::string>* it = sl.NewIterator();
    it->SeekToFirst();
    ASSERT_TRUE(it->Valid());
    ASSERT_EQ("a", it->GetKey());
    it->Next();
    ASSERT_TRUE(it->Valid());
    ASSERT_EQ("h", it->GetKey());
    it->Next();
    ASSERT_FALSE(it->Valid());
    node = sl.Remove(k);
    ASSERT_EQ("a", sl.GetLast()->GetKey());
    ASSERT_FALSE(node == NULL);
    ASSERT_EQ("h", node->GetKey());
    ASSERT_EQ("b", node->GetValue());
    node = sl.Remove(k1);
    ASSERT_TRUE(sl.GetLast() == NULL);
}

TEST_F(SkipListTest, Get) {
    Comparator cmp;
    SkipList<uint32_t, uint32_t, Comparator> sl(12, 4, cmp);
    uint32_t key = 1;
    uint32_t value = 1;
    sl.Insert(key, value);
    uint32_t ret = sl.Get(1);
    ASSERT_EQ(1, ret);
    ASSERT_FALSE(sl.Get(2) == 2);
}

TEST_F(SkipListTest, GetLast) {
    Comparator cmp;
    SkipList<uint32_t, uint32_t, Comparator> sl(12, 4, cmp);
    ASSERT_TRUE(sl.GetLast() == NULL);
    uint32_t value = 1111111;
    for (uint32_t idx = 100; idx < 10000; idx++) {
        sl.Insert(idx, value);
        ASSERT_EQ(idx, sl.GetLast()->GetKey());
        ASSERT_EQ(value, sl.GetLast()->GetValue());
    }
    for (uint32_t idx = 0; idx < 100; idx++) {
        sl.Insert(idx, value);
        ASSERT_EQ(9999, sl.GetLast()->GetKey());
        ASSERT_EQ(value, sl.GetLast()->GetValue());
    }
    sl.Clear();
    ASSERT_TRUE(sl.GetLast() == NULL);
}

TEST_F(SkipListTest, Duplicate) {
    DescComparator cmp;
    SkipList<uint32_t, uint32_t, DescComparator> sl(12, 4, cmp);
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

    Iterator<uint32_t, uint32_t>* it = sl.NewIterator();

    //ASSERT_EQ(3, it->GetSize());
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
