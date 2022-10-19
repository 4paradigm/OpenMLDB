/*
 * Copyright 2021 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "storage/segment.h"

#include <iostream>
#include <string>

#include "base/glog_wrapper.h"
#include "base/slice.h"
#include "gtest/gtest.h"
#include "storage/record.h"

using ::openmldb::base::Slice;

namespace openmldb {
namespace storage {

class SegmentTest : public ::testing::Test {
 public:
    SegmentTest() {}
    ~SegmentTest() {}
};

TEST_F(SegmentTest, Size) {
    ASSERT_EQ(16, (int64_t)sizeof(DataBlock));
    ASSERT_EQ(40, (int64_t)sizeof(KeyEntry));
}

TEST_F(SegmentTest, DataBlock) {
    const char* test = "test";
    DataBlock* db = new DataBlock(1, test, 4);
    ASSERT_EQ(4, (int64_t)db->size);
    ASSERT_EQ('t', db->data[0]);
    ASSERT_EQ('e', db->data[1]);
    ASSERT_EQ('s', db->data[2]);
    ASSERT_EQ('t', db->data[3]);
    delete db;
}

TEST_F(SegmentTest, PutAndScan) {
    Segment segment;
    Slice pk("test1");
    std::string value = "test0";
    segment.Put(pk, 9527, value.c_str(), value.size());
    segment.Put(pk, 9528, value.c_str(), value.size());
    segment.Put(pk, 9528, value.c_str(), value.size());
    segment.Put(pk, 9529, value.c_str(), value.size());
    ASSERT_EQ(1, (int64_t)segment.GetPkCnt());
    Ticket ticket;
    MemTableIterator* it = segment.NewIterator("test1", ticket);
    it->Seek(9530);
    ASSERT_TRUE(it->Valid());
    ASSERT_EQ(9529, (int64_t)it->GetKey());
    ::openmldb::base::Slice val = it->GetValue();
    std::string result(val.data(), val.size());
    ASSERT_EQ("test0", result);
    it->Next();
    val = it->GetValue();
    std::string result2(val.data(), val.size());
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
    ASSERT_EQ(1, (int64_t)segment.GetPkCnt());
    Ticket ticket;
    MemTableIterator* it = segment.NewIterator("test1", ticket);
    int size = 0;
    it->SeekToFirst();
    while (it->Valid()) {
        it->Next();
        size++;
    }
    ASSERT_EQ(4, size);
    delete it;
    ASSERT_TRUE(segment.Delete(pk));
    it = segment.NewIterator("test1", ticket);
    ASSERT_FALSE(it->Valid());
    delete it;
    uint64_t gc_idx_cnt = 0;
    uint64_t gc_record_cnt = 0;
    uint64_t gc_record_byte_size = 0;
    segment.IncrGcVersion();
    segment.IncrGcVersion();
    segment.GcFreeList(gc_idx_cnt, gc_record_cnt, gc_record_byte_size);
    ASSERT_EQ(4, (int64_t)gc_idx_cnt);
    ASSERT_EQ(4, (int64_t)gc_record_cnt);
    ASSERT_EQ(84, (int64_t)gc_record_byte_size);
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
    ASSERT_EQ(3, (int64_t)count);
    segment.Put(pk, 9530, value.c_str(), value.size());
    ASSERT_EQ(0, segment.GetCount(pk, count));
    ASSERT_EQ(4, (int64_t)count);

    uint64_t gc_idx_cnt = 0;
    uint64_t gc_record_cnt = 0;
    uint64_t gc_record_byte_size = 0;
    segment.Gc4TTL(9528, gc_idx_cnt, gc_record_cnt, gc_record_byte_size);
    ASSERT_EQ(0, segment.GetCount(pk, count));
    ASSERT_EQ(2, (int64_t)count);

    std::vector<uint32_t> ts_idx_vec = {1, 3, 5};
    Segment segment1(8, ts_idx_vec);
    Slice pk1("pk");
    std::map<int32_t, uint64_t> ts_map;
    for (int i = 0; i < 6; i++) {
        ts_map.emplace(i, 1100 + i);
    }
    DataBlock db(1, "test1", 5);
    segment1.Put(pk1, ts_map, &db);
    ASSERT_EQ(-1, segment1.GetCount(pk1, 0, count));
    ASSERT_EQ(0, segment1.GetCount(pk1, 1, count));
    ASSERT_EQ(1, (int64_t)count);
    ASSERT_EQ(0, segment1.GetCount(pk1, 3, count));
    ASSERT_EQ(1, (int64_t)count);

    ts_map.clear();
    for (int i = 0; i < 6; i++) {
        if (i == 3) {
            continue;
        }
        ts_map.emplace(i, 1200 + i);
    }
    segment1.Put(pk1, ts_map, &db);
    ASSERT_EQ(0, segment1.GetCount(pk1, 1, count));
    ASSERT_EQ(2, (int64_t)count);
    ASSERT_EQ(0, segment1.GetCount(pk1, 3, count));
    ASSERT_EQ(1, (int64_t)count);
}

TEST_F(SegmentTest, Iterator) {
    Segment segment;
    Slice pk("test1");
    segment.Put(pk, 9768, "test1", 5);
    segment.Put(pk, 9768, "test1", 5);
    segment.Put(pk, 9768, "test1", 5);
    segment.Put(pk, 9769, "test2", 5);
    ASSERT_EQ(1, (int64_t)segment.GetPkCnt());
    Ticket ticket;
    MemTableIterator* it = segment.NewIterator("test1", ticket);
    it->SeekToFirst();
    int size = 0;
    while (it->Valid()) {
        it->Next();
        size++;
    }
    ASSERT_EQ(4, size);
    it->Seek(9769);
    ASSERT_EQ(9769, (int64_t)it->GetKey());
    ::openmldb::base::Slice value = it->GetValue();
    std::string result(value.data(), value.size());
    ASSERT_EQ("test2", result);
    it->Next();
    value = it->GetValue();
    std::string result2(value.data(), value.size());
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
    ASSERT_EQ(1, (int64_t)gc_idx_cnt);
    ASSERT_EQ(1, (int64_t)gc_record_cnt);
    ASSERT_EQ(GetRecordSize(5), (int64_t)gc_record_byte_size);
    Ticket ticket;
    MemTableIterator* it = segment.NewIterator(pk, ticket);
    it->Seek(9769);
    ASSERT_TRUE(it->Valid());
    ASSERT_EQ(9769, (int64_t)it->GetKey());
    ::openmldb::base::Slice value = it->GetValue();
    std::string result(value.data(), value.size());
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
    ASSERT_EQ(0, (int64_t)gc_idx_cnt);
    ASSERT_EQ(0, (int64_t)gc_record_cnt);
    ASSERT_EQ(0, (int64_t)gc_record_byte_size);
    segment.Gc4TTL(9768, gc_idx_cnt, gc_record_cnt, gc_record_byte_size);
    ASSERT_EQ(1, (int64_t)gc_idx_cnt);
    ASSERT_EQ(1, (int64_t)gc_record_cnt);
    ASSERT_EQ(GetRecordSize(5), (int64_t)gc_record_byte_size);
    segment.Gc4TTL(9770, gc_idx_cnt, gc_record_cnt, gc_record_byte_size);
    ASSERT_EQ(2, (int64_t)gc_idx_cnt);
    ASSERT_EQ(2, (int64_t)gc_record_cnt);
    ASSERT_EQ(2 * GetRecordSize(5), (int64_t)gc_record_byte_size);
}

TEST_F(SegmentTest, TestGc4TTLAndHead) {
    Segment segment;
    segment.Put("PK1", 9766, "test1", 5);
    segment.Put("PK1", 9767, "test2", 5);
    segment.Put("PK1", 9768, "test3", 5);
    segment.Put("PK1", 9769, "test4", 5);
    segment.Put("PK2", 9765, "test1", 5);
    segment.Put("PK2", 9766, "test2", 5);
    segment.Put("PK2", 9767, "test3", 5);
    uint64_t gc_idx_cnt = 0;
    uint64_t gc_record_cnt = 0;
    uint64_t gc_record_byte_size = 0;
    segment.Gc4TTLAndHead(0, 0, gc_idx_cnt, gc_record_cnt, gc_record_byte_size);
    ASSERT_EQ(0, (int64_t)gc_idx_cnt);
    ASSERT_EQ(0, (int64_t)gc_record_cnt);
    ASSERT_EQ(0, (int64_t)gc_record_byte_size);
    segment.Gc4TTLAndHead(9765, 0, gc_idx_cnt, gc_record_cnt, gc_record_byte_size);
    ASSERT_EQ(0, (int64_t)gc_idx_cnt);
    ASSERT_EQ(0, (int64_t)gc_record_cnt);
    ASSERT_EQ(0, (int64_t)gc_record_byte_size);
    gc_idx_cnt = 0;
    gc_record_cnt = 0;
    gc_record_byte_size = 0;
    segment.Gc4TTLAndHead(0, 3, gc_idx_cnt, gc_record_cnt, gc_record_byte_size);
    ASSERT_EQ(0, (int64_t)gc_idx_cnt);
    ASSERT_EQ(0, (int64_t)gc_record_cnt);
    ASSERT_EQ(0, (int64_t)gc_record_byte_size);
    gc_idx_cnt = 0;
    gc_record_cnt = 0;
    gc_record_byte_size = 0;
    segment.Gc4TTLAndHead(9765, 3, gc_idx_cnt, gc_record_cnt, gc_record_byte_size);
    ASSERT_EQ(0, (int64_t)gc_idx_cnt);
    ASSERT_EQ(0, (int64_t)gc_record_cnt);
    ASSERT_EQ(0, (int64_t)gc_record_byte_size);
    gc_idx_cnt = 0;
    gc_record_cnt = 0;
    gc_record_byte_size = 0;
    segment.Gc4TTLAndHead(9766, 2, gc_idx_cnt, gc_record_cnt, gc_record_byte_size);
    ASSERT_EQ(2, (int64_t)gc_idx_cnt);
    ASSERT_EQ(2, (int64_t)gc_record_cnt);
    ASSERT_EQ(2 * GetRecordSize(5), (int64_t)gc_record_byte_size);
    gc_idx_cnt = 0;
    gc_record_cnt = 0;
    gc_record_byte_size = 0;
    segment.Gc4TTLAndHead(9770, 1, gc_idx_cnt, gc_record_cnt, gc_record_byte_size);
    ASSERT_EQ(3, (int64_t)gc_idx_cnt);
    ASSERT_EQ(3, (int64_t)gc_record_cnt);
    ASSERT_EQ(3 * GetRecordSize(5), (int64_t)gc_record_byte_size);
}

TEST_F(SegmentTest, TestGc4TTLOrHead) {
    Segment segment;
    segment.Put("PK1", 9766, "test1", 5);
    segment.Put("PK1", 9767, "test2", 5);
    segment.Put("PK1", 9768, "test3", 5);
    segment.Put("PK1", 9769, "test4", 5);
    segment.Put("PK2", 9765, "test1", 5);
    segment.Put("PK2", 9766, "test2", 5);
    segment.Put("PK2", 9767, "test3", 5);
    uint64_t gc_idx_cnt = 0;
    uint64_t gc_record_cnt = 0;
    uint64_t gc_record_byte_size = 0;
    segment.Gc4TTLOrHead(0, 0, gc_idx_cnt, gc_record_cnt, gc_record_byte_size);
    ASSERT_EQ(0, (int64_t)gc_idx_cnt);
    ASSERT_EQ(0, (int64_t)gc_record_cnt);
    ASSERT_EQ(0, (int64_t)gc_record_byte_size);
    segment.Gc4TTLOrHead(9765, 0, gc_idx_cnt, gc_record_cnt, gc_record_byte_size);
    ASSERT_EQ(1, (int64_t)gc_idx_cnt);
    ASSERT_EQ(1, (int64_t)gc_record_cnt);
    ASSERT_EQ(GetRecordSize(5), (int64_t)gc_record_byte_size);
    gc_idx_cnt = 0;
    gc_record_cnt = 0;
    gc_record_byte_size = 0;
    segment.Gc4TTLOrHead(0, 3, gc_idx_cnt, gc_record_cnt, gc_record_byte_size);
    ASSERT_EQ(1, (int64_t)gc_idx_cnt);
    ASSERT_EQ(1, (int64_t)gc_record_cnt);
    ASSERT_EQ(GetRecordSize(5), (int64_t)gc_record_byte_size);
    gc_idx_cnt = 0;
    gc_record_cnt = 0;
    gc_record_byte_size = 0;
    segment.Gc4TTLOrHead(9765, 3, gc_idx_cnt, gc_record_cnt, gc_record_byte_size);
    ASSERT_EQ(0, (int64_t)gc_idx_cnt);
    ASSERT_EQ(0, (int64_t)gc_record_cnt);
    ASSERT_EQ(0, (int64_t)gc_record_byte_size);
    gc_idx_cnt = 0;
    gc_record_cnt = 0;
    gc_record_byte_size = 0;
    segment.Gc4TTLOrHead(9766, 2, gc_idx_cnt, gc_record_cnt, gc_record_byte_size);
    ASSERT_EQ(2, (int64_t)gc_idx_cnt);
    ASSERT_EQ(2, (int64_t)gc_record_cnt);
    ASSERT_EQ(2 * GetRecordSize(5), (int64_t)gc_record_byte_size);
    gc_idx_cnt = 0;
    gc_record_cnt = 0;
    gc_record_byte_size = 0;
    segment.Gc4TTLOrHead(9770, 1, gc_idx_cnt, gc_record_cnt, gc_record_byte_size);
    ASSERT_EQ(3, (int64_t)gc_idx_cnt);
    ASSERT_EQ(3, (int64_t)gc_record_cnt);
    ASSERT_EQ(3 * GetRecordSize(5), (int64_t)gc_record_byte_size);
}

TEST_F(SegmentTest, TestStat) {
    Segment segment;
    segment.Put("PK", 9768, "test1", 5);
    segment.Put("PK", 9769, "test2", 5);
    uint64_t gc_idx_cnt = 0;
    uint64_t gc_record_cnt = 0;
    uint64_t gc_record_byte_size = 0;
    ASSERT_EQ(2, (int64_t)segment.GetIdxCnt());
    ASSERT_EQ(1, (int64_t)segment.GetPkCnt());
    segment.Gc4TTL(9765, gc_idx_cnt, gc_record_cnt, gc_record_byte_size);
    ASSERT_EQ(0, (int64_t)gc_idx_cnt);
    segment.Gc4TTL(9768, gc_idx_cnt, gc_record_cnt, gc_record_byte_size);
    ASSERT_EQ(1, (int64_t)segment.GetIdxCnt());
    ASSERT_EQ(1, (int64_t)gc_idx_cnt);
    segment.Gc4TTL(9770, gc_idx_cnt, gc_record_cnt, gc_record_byte_size);
    ASSERT_EQ(2, (int64_t)gc_idx_cnt);
    ASSERT_EQ(0, (int64_t)segment.GetIdxCnt());
}

TEST_F(SegmentTest, GetTsIdx) {
    std::vector<uint32_t> ts_idx_vec = {1, 3, 5};
    Segment segment(8, ts_idx_vec);
    ASSERT_EQ(3, (int64_t)segment.GetTsCnt());
    uint32_t real_idx = UINT32_MAX;
    ASSERT_EQ(-1, segment.GetTsIdx(0, real_idx));
    ASSERT_EQ(0, segment.GetTsIdx(1, real_idx));
    ASSERT_EQ(0, (int64_t)real_idx);
    ASSERT_EQ(-1, segment.GetTsIdx(2, real_idx));
    ASSERT_EQ(0, segment.GetTsIdx(3, real_idx));
    ASSERT_EQ(1, (int64_t)real_idx);
    ASSERT_EQ(-1, segment.GetTsIdx(4, real_idx));
    ASSERT_EQ(0, segment.GetTsIdx(5, real_idx));
    ASSERT_EQ(2, (int64_t)real_idx);
}

}  // namespace storage
}  // namespace openmldb

int main(int argc, char** argv) {
    ::openmldb::base::SetLogLevel(INFO);
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
