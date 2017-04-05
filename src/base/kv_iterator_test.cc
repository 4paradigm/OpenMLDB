//
// kv_iterator_test.cc
// Copyright 2017 4paradigm.com 


#include "base/kv_iterator.h"
#include "base/codec.h"
#include "gtest/gtest.h"
#include "base/strings.h"
#include <iostream>

namespace rtidb {
namespace base {

class KvIteratorTest : public ::testing::Test {

public:
    KvIteratorTest() {}
    ~KvIteratorTest() {}
};


TEST_F(KvIteratorTest, Iterator) {

    char* data = new char[12 * 2 + 10];
    DataBlock* db1 = new DataBlock();
    db1->data = "hello";
    db1->size = 5;

    DataBlock* db2 = new DataBlock();
    db2->data = "hell1";
    db2->size = 5;
    Encode(9527, db1, data);
    Encode(9528, db2, data);

    std::cout <<DebugString(debug) << std::endl;
    KvIterator kv_it(static_cast<void*>(data), data, 34);
    ASSERT_TRUE(kv_it.Valid());
    kv_it.Next();
    ASSERT_EQ(9527, kv_it.GetKey());
    ASSERT_EQ("hello", kv_it.GetValue().data());
    ASSERT_TRUE(kv_it.Valid());
    kv_it.Next();
    ASSERT_EQ(9528, kv_it.GetKey());
    ASSERT_EQ("hell1", kv_it.GetValue().ToString());

}


}
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
