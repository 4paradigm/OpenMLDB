//
// segment_test.cc
// Copyright (C) 2017 4paradigm.com
// Author wangtaize 
// Date 2017-03-31
//

#include <iostream>
#include "storage/segment.h"

#include "gtest/gtest.h"
#include "logging.h"

namespace rtidb {
namespace storage {

class SegmentTest : public ::testing::Test {

public:
    SegmentTest(){}
    ~SegmentTest() {}
};

TEST_F(SegmentTest, DataBlock) {
    const char* test = "test";
    DataBlock* db = new DataBlock(test, 4);
    ASSERT_EQ(4, db->size);
    ASSERT_EQ('t', db->data[0]);
    ASSERT_EQ('e', db->data[1]);
    ASSERT_EQ('s', db->data[2]);
    ASSERT_EQ('t', db->data[3]);
    ASSERT_EQ(4, db->Release());
    ASSERT_EQ(NULL, db->data);
}

TEST_F(SegmentTest, TestRelease) {
    Segment* seg2 = new Segment();
    const char* test = "test";
    seg2->Put(test, 9527, test, 4);
    uint64_t size = seg2->Release();
    // 4 bytes pk size
    // 4 bytes value size
    // 8 bytes time size
    // 4 bytes value size size
    ASSERT_EQ(4 + 4 + 8 + 4, size);
    delete seg2;
}

TEST_F(SegmentTest, PutAndGet) {
   Segment segment; 
   const char* test = "test";
   std::string pk = "pk";
   segment.Put(pk, 9768, test, 4);
   DataBlock* db = NULL;
   bool ret = segment.Get(pk, 9768, &db);
   ASSERT_TRUE(ret);
   ASSERT_TRUE(db != NULL);
   ASSERT_EQ(4, db->size);
   std::string t(db->data, db->size);
   std::string e = "test";
   ASSERT_EQ(e, t);
}

TEST_F(SegmentTest, MultiGet) {
   Segment segment; 
   {
       const char* test = "test1";
       std::string pk = "pk1";
       segment.Put(pk, 1, test, 5);
   }
   {
       const char* test = "test2";
       std::string pk = "pk1";
       segment.Put(pk, 2, test, 5);
   }

   {
       const char* test = "test2";
       std::string pk = "pk2";
       segment.Put(pk, 2, test, 5);
   }

   std::vector<std::string> keys;
   keys.push_back("pk1");
   keys.push_back("pk2");
   std::map<uint32_t, DataBlock*> datas;
   Ticket ticket;
   segment.BatchGet(keys, datas, ticket);
   ASSERT_EQ(2, datas.size());

   {
       DataBlock* value1 = datas[0];
       std::string value1_str(value1->data, value1->size);
       ASSERT_EQ("test2", value1_str);
   }

   {
       DataBlock* value1 = datas[1];
       std::string value1_str(value1->data, value1->size);
       ASSERT_EQ("test2", value1_str);
   }
}

TEST_F(SegmentTest, Iterator) {
   Segment segment; 
   segment.Put("pk", 9768, "test1", 5);
   segment.Put("pk", 9769, "test2", 5);
   Ticket ticket;
   Segment::Iterator* it = segment.NewIterator("pk", ticket);
   it->Seek(9769);
   ASSERT_EQ(9769, it->GetKey());
   DataBlock* value = it->GetValue();
   std::string result(value->data, value->size);
   ASSERT_EQ("test2", result);
   it->Next();
   value = it->GetValue();
   std::string result2(value->data, value->size);
   ASSERT_EQ("test1", result2);
   it->Next();
   ASSERT_FALSE(it->Valid());
   char data[400];
   for (uint32_t i = 0; i < 50000; i++) {
       std::string pk = "pk" + i;
       segment.Put(pk, i, data, 400);
   }
}

TEST_F(SegmentTest, TestGc4TTL) {
    Segment segment;
    segment.Put("PK", 9768, "test1", 5);
    segment.Put("PK", 9769, "test2", 5);
    uint64_t count = segment.Gc4TTL(9765);
    ASSERT_EQ(0, count);
    count = segment.Gc4TTL(9768);
    ASSERT_EQ(1, count);
    count = segment.Gc4TTL(9770);
    ASSERT_EQ(1, count);
}

TEST_F(SegmentTest, TestStat) {
    Segment segment;
    segment.Put("PK", 9768, "test1", 5);
    segment.Put("PK", 9769, "test2", 5);
    ASSERT_EQ(2, segment.GetDataCnt());
    uint64_t count = segment.Gc4TTL(9765);
    ASSERT_EQ(0, count);
    count = segment.Gc4TTL(9768);
    ASSERT_EQ(1, segment.GetDataCnt());
    ASSERT_EQ(1, count);
    count = segment.Gc4TTL(9770);
    ASSERT_EQ(1, count);
    ASSERT_EQ(0, segment.GetDataCnt());
}



}
}

int main(int argc, char** argv) {
    ::baidu::common::SetLogLevel(::baidu::common::DEBUG);
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}


