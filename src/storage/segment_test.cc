//
// segment_test.cc
// Copyright (C) 2017 4paradigm.com
// Author wangtaize 
// Date 2017-03-31
//

#include <iostream>
#include "storage/segment.h"
#include "base/slice.h"
#include "storage/record.h"

#include "gtest/gtest.h"
#include "logging.h"

using ::rtidb::base::Slice;

namespace rtidb {
namespace storage {

class SegmentTest : public ::testing::Test {

public:
    SegmentTest(){}
    ~SegmentTest() {}
};

TEST_F(SegmentTest, Size) {
    ASSERT_EQ(16, sizeof(DataBlock));
    ASSERT_EQ(40, sizeof(KeyEntry));
}

TEST_F(SegmentTest, DataBlock) {
    const char* test = "test";
    DataBlock* db = new DataBlock(1, test, 4);
    ASSERT_EQ(4, db->size);
    ASSERT_EQ('t', db->data[0]);
    ASSERT_EQ('e', db->data[1]);
    ASSERT_EQ('s', db->data[2]);
    ASSERT_EQ('t', db->data[3]);
    delete db;
}

TEST_F(SegmentTest, PutAndGet) {
   Segment segment; 
   const char* test = "test";
   Slice pk("pk");
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

TEST_F(SegmentTest, PutAndScan) {
   Segment segment; 
   Slice pk("test1");
   std::string value = "test0";
   segment.Put(pk, 9527, value.c_str(), value.size());
   segment.Put(pk, 9528, value.c_str(), value.size());
   segment.Put(pk, 9528, value.c_str(), value.size());
   segment.Put(pk, 9529, value.c_str(), value.size());
   ASSERT_EQ(1, segment.GetPkCnt());
   Ticket ticket;
   Iterator* it = segment.NewIterator("test1", ticket);
   ASSERT_EQ(4, it->GetSize());
   it->Seek(9530);
   ASSERT_TRUE(it->Valid());
   ASSERT_EQ(9529, it->GetKey());
   DataBlock* val = it->GetValue();
   std::string result(val->data, val->size);
   ASSERT_EQ("test0", result);
   it->Next();
   val = it->GetValue();
   std::string result2(val->data, val->size);
   ASSERT_EQ("test0", result2);
   it->Next();
   ASSERT_TRUE(it->Valid());
}

TEST_F(SegmentTest, Delete) {
   Segment segment; 
   Slice pk("test1");
   std::string value = "test0";
   segment.Put(pk, 9527, value.c_str(), value.size());
   segment.Put(pk, 9528, value.c_str(), value.size());
   segment.Put(pk, 9528, value.c_str(), value.size());
   segment.Put(pk, 9529, value.c_str(), value.size());
   ASSERT_EQ(1, segment.GetPkCnt());
   Ticket ticket;
   Iterator* it = segment.NewIterator("test1", ticket);
   ASSERT_EQ(4, it->GetSize());
   delete it;
   ASSERT_TRUE(segment.Delete(pk));
   it = segment.NewIterator("test1", ticket);
   ASSERT_EQ(0, it->GetSize());
   delete it;
   uint64_t gc_idx_cnt = 0;
   uint64_t gc_record_cnt = 0;
   uint64_t gc_record_byte_size = 0;
   segment.IncrGcVersion();
   segment.IncrGcVersion();
   segment.GcFreeList(gc_idx_cnt, gc_record_cnt, gc_record_byte_size);
   ASSERT_EQ(4, gc_idx_cnt);
   ASSERT_EQ(4, gc_record_cnt);
   ASSERT_EQ(84, gc_record_byte_size);
}

TEST_F(SegmentTest, GetCount) {
    Segment segment; 
    Slice pk("test1");
    std::string value = "test0";
    uint64_t count = 0;
    segment.Put(pk, 9527, value.c_str(), value.size());
    segment.Put(pk, 9528, value.c_str(), value.size());
    segment.Put(pk, 9529, value.c_str(), value.size());
    ASSERT_EQ(0, segment.GetCount(pk, count));
    ASSERT_EQ(3, count);
    segment.Put(pk, 9530, value.c_str(), value.size());
    ASSERT_EQ(0, segment.GetCount(pk, count));
    ASSERT_EQ(4, count);

    uint64_t gc_idx_cnt = 0;
    uint64_t gc_record_cnt = 0;
    uint64_t gc_record_byte_size = 0;
    segment.Gc4TTL(9528, gc_idx_cnt, gc_record_cnt, gc_record_byte_size);
    ASSERT_EQ(0, segment.GetCount(pk, count));
    ASSERT_EQ(2, count);

    std::vector<uint32_t> ts_idx_vec = {1, 3, 5};
    Segment segment1(8, ts_idx_vec); 
    Slice pk1("pk");
    ::rtidb::api::LogEntry logEntry;
    for (int i = 0; i < 6; i++) {
       ::rtidb::api::TSDimension* ts = logEntry.add_ts_dimensions();
       ts->set_ts(1100 + i);
       ts->set_idx(i);
    }
    DataBlock db(1, "test1", 5);
    segment1.Put(pk1, logEntry.ts_dimensions(), &db);
    ASSERT_EQ(-1, segment1.GetCount(pk1, 0, count));
    ASSERT_EQ(0, segment1.GetCount(pk1, 1, count));
    ASSERT_EQ(1, count);
    ASSERT_EQ(0, segment1.GetCount(pk1, 3, count));
    ASSERT_EQ(1, count);

    logEntry.Clear();
    for (int i = 0; i < 6; i++) {
        if (i == 3) {
            continue;
        }
        ::rtidb::api::TSDimension* ts = logEntry.add_ts_dimensions();
        ts->set_ts(1200 + i);
        ts->set_idx(i);
    }
    segment1.Put(pk1, logEntry.ts_dimensions(), &db);
    ASSERT_EQ(0, segment1.GetCount(pk1, 1, count));
    ASSERT_EQ(2, count);
    ASSERT_EQ(0, segment1.GetCount(pk1, 3, count));
    ASSERT_EQ(1, count);
}

TEST_F(SegmentTest, Iterator) {
   Segment segment; 
   Slice pk("test1");
   segment.Put(pk, 9768, "test1", 5);
   segment.Put(pk, 9768, "test1", 5);
   segment.Put(pk, 9768, "test1", 5);
   segment.Put(pk, 9769, "test2", 5);
   ASSERT_EQ(1, segment.GetPkCnt());
   Ticket ticket;
   Iterator* it = segment.NewIterator("test1", ticket);
   ASSERT_EQ(4, it->GetSize());
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
   ASSERT_TRUE(it->Valid());
}

TEST_F(SegmentTest, TestGc4Head) {
    Segment segment;
    uint64_t gc_idx_cnt = 0;
    uint64_t gc_record_cnt = 0;
    uint64_t gc_record_byte_size = 0;
    Slice pk("PK");
    segment.Put(pk, 9768, "test1", 5);
    segment.Put(pk, 9769, "test2", 5);
    segment.Gc4Head(1, gc_idx_cnt, gc_record_cnt, gc_record_byte_size);
    ASSERT_EQ(1, gc_idx_cnt);
    ASSERT_EQ(1, gc_record_cnt);
    ASSERT_EQ(GetRecordSize(5), gc_record_byte_size);
    Ticket ticket;
    Iterator* it = segment.NewIterator(pk, ticket);
    it->Seek(9769);
    ASSERT_TRUE(it->Valid());
    ASSERT_EQ(9769, it->GetKey());
    DataBlock* value = it->GetValue();
    std::string result(value->data, value->size);
    ASSERT_EQ("test2", result);
    it->Next();
    ASSERT_FALSE(it->Valid());
}

TEST_F(SegmentTest, TestGc4TTL) {
    Segment segment;
    segment.Put("PK", 9768, "test1", 5);
    segment.Put("PK", 9769, "test2", 5);
    uint64_t gc_idx_cnt = 0;
    uint64_t gc_record_cnt = 0;
    uint64_t gc_record_byte_size = 0;
    segment.Gc4TTL(9765, gc_idx_cnt, gc_record_cnt, gc_record_byte_size);
    ASSERT_EQ(0, gc_idx_cnt);
    ASSERT_EQ(0, gc_record_cnt);
    ASSERT_EQ(0, gc_record_byte_size);
    segment.Gc4TTL(9768, gc_idx_cnt, gc_record_cnt, gc_record_byte_size);
    ASSERT_EQ(1, gc_idx_cnt);
    ASSERT_EQ(1, gc_record_cnt);
    ASSERT_EQ(GetRecordSize(5), gc_record_byte_size);
    segment.Gc4TTL(9770, gc_idx_cnt, gc_record_cnt, gc_record_byte_size);
    ASSERT_EQ(2, gc_idx_cnt);
    ASSERT_EQ(2, gc_record_cnt);
    ASSERT_EQ(2 * GetRecordSize(5), gc_record_byte_size);
}

TEST_F(SegmentTest, TestStat) {
    Segment segment;
    segment.Put("PK", 9768, "test1", 5);
    segment.Put("PK", 9769, "test2", 5);
    uint64_t gc_idx_cnt = 0;
    uint64_t gc_record_cnt = 0;
    uint64_t gc_record_byte_size = 0;
    ASSERT_EQ(2, segment.GetIdxCnt());
    ASSERT_EQ(1, segment.GetPkCnt());
    segment.Gc4TTL(9765, gc_idx_cnt, gc_record_cnt, gc_record_byte_size);
    ASSERT_EQ(0, gc_idx_cnt);
    segment.Gc4TTL(9768, gc_idx_cnt, gc_record_cnt, gc_record_byte_size);
    ASSERT_EQ(1, segment.GetIdxCnt());
    ASSERT_EQ(1, gc_idx_cnt);
    segment.Gc4TTL(9770, gc_idx_cnt, gc_record_cnt, gc_record_byte_size);
    ASSERT_EQ(2, gc_idx_cnt);
    ASSERT_EQ(0, segment.GetIdxCnt());
}

TEST_F(SegmentTest, GetTsIdx) {
    std::vector<uint32_t> ts_idx_vec = {1, 3, 5};
    Segment segment(8, ts_idx_vec);
    ASSERT_EQ(3, segment.GetTsCnt());
    uint32_t real_idx = 0;
    ASSERT_EQ(-1, segment.GetTsIdx(0, real_idx));
    ASSERT_EQ(0, segment.GetTsIdx(1, real_idx));
    ASSERT_EQ(0, real_idx);
    ASSERT_EQ(-1, segment.GetTsIdx(2, real_idx));
    ASSERT_EQ(0, segment.GetTsIdx(3, real_idx));
    ASSERT_EQ(1, real_idx);
    ASSERT_EQ(-1, segment.GetTsIdx(4, real_idx));
    ASSERT_EQ(0, segment.GetTsIdx(5, real_idx));
    ASSERT_EQ(2, real_idx);
}

TEST_F(SegmentTest, PutAndGetTS) {
   std::vector<uint32_t> ts_idx_vec = {1, 3, 5};
   Segment segment(8, ts_idx_vec); 
   Slice pk("pk");
   ::rtidb::api::LogEntry logEntry;
   for (int i = 0; i < 6; i++) {
       ::rtidb::api::TSDimension* ts = logEntry.add_ts_dimensions();
       ts->set_ts(1100 + i);
       ts->set_idx(i);
   }
   DataBlock db(1, "test1", 5);
   segment.Put(pk, logEntry.ts_dimensions(), &db);
   DataBlock* result = NULL;
   bool ret = segment.Get(pk, 0, 1101, &result);
   ASSERT_FALSE(ret);
   ret = segment.Get(pk, 1, 1101, &result);
   ASSERT_TRUE(ret);
   ASSERT_TRUE(result != NULL);
   ASSERT_EQ(5, result->size);
   std::string t(result->data, result->size);
   std::string e = "test1";
   ASSERT_EQ(e, t);
}

}
}

int main(int argc, char** argv) {
    ::baidu::common::SetLogLevel(::baidu::common::INFO);
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}


