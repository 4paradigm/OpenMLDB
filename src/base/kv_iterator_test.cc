//
// kv_iterator_test.cc
// Copyright 2017 4paradigm.com 


#include "base/kv_iterator.h"
#include "base/codec.h"
#include "gtest/gtest.h"
#include "base/strings.h"
#include "proto/tablet.pb.h"
#include <iostream>

namespace rtidb {
namespace base {

class KvIteratorTest : public ::testing::Test {

public:
    KvIteratorTest() {}
    ~KvIteratorTest() {}
};


TEST_F(KvIteratorTest, Iterator_NULL) {
    ::rtidb::api::ScanResponse* response = new ::rtidb::api::ScanResponse();
    KvIterator kv_it(response);
    ASSERT_FALSE(kv_it.Valid());
}

TEST_F(KvIteratorTest, Iterator_ONE) {
    ::rtidb::api::ScanResponse* response = new ::rtidb::api::ScanResponse();
    std::string* pairs = response->mutable_pairs();
    pairs->resize(17);
    char* data = reinterpret_cast<char*>(& ((*pairs)[0])) ;
    DataBlock* db1 = new DataBlock("hello", 5);
    Encode(9527, db1, data, 0);
    KvIterator kv_it(response);
    ASSERT_TRUE(kv_it.Valid());
    ASSERT_EQ(9527, kv_it.GetKey());
    ASSERT_EQ("hello", kv_it.GetValue().ToString());
    kv_it.Next();
    ASSERT_FALSE(kv_it.Valid());
}

TEST_F(KvIteratorTest, Iterator) {
    ::rtidb::api::ScanResponse* response = new ::rtidb::api::ScanResponse();

    std::string* pairs = response->mutable_pairs();
    pairs->resize(34);
    char* data = reinterpret_cast<char*>(& ((*pairs)[0])) ;
    DataBlock* db1 = new DataBlock("hello", 5);
    DataBlock* db2 = new DataBlock("hell1", 5);
    Encode(9527, db1, data, 0);
    Encode(9528, db2, data, 17);
    KvIterator kv_it(response);
    ASSERT_TRUE(kv_it.Valid());
    ASSERT_EQ(9527, kv_it.GetKey());
    ASSERT_EQ("hello", kv_it.GetValue().ToString());
    kv_it.Next();
    ASSERT_TRUE(kv_it.Valid());
    ASSERT_EQ(9528, kv_it.GetKey());
    ASSERT_EQ("hell1", kv_it.GetValue().ToString());
    kv_it.Next();
    ASSERT_FALSE(kv_it.Valid());
}


}
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
