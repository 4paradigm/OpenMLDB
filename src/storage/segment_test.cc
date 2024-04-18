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

#include "absl/strings/str_cat.h"
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
    std::unique_ptr<DataBlock> db = std::make_unique<DataBlock>(1, test, 4);
    ASSERT_EQ(4, (int64_t)db->size);
    ASSERT_EQ('t', db->data[0]);
    ASSERT_EQ('e', db->data[1]);
    ASSERT_EQ('s', db->data[2]);
    ASSERT_EQ('t', db->data[3]);
}

TEST_F(SegmentTest, PutAndScan) {
    Segment segment(8);
    Slice pk("test1");
    std::string value = "test0";
    segment.Put(pk, 9527, value.c_str(), value.size());
    segment.Put(pk, 9528, value.c_str(), value.size());
    segment.Put(pk, 9528, value.c_str(), value.size());
    segment.Put(pk, 9529, value.c_str(), value.size());
    ASSERT_EQ(1, (int64_t)segment.GetPkCnt());
    Ticket ticket;
    std::unique_ptr<MemTableIterator> it(segment.NewIterator("test1", ticket, type::CompressType::kNoCompress));
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

// report result, don't need to print args in here, just print the failure
::testing::AssertionResult CheckStatisticsInfo(const StatisticsInfo& expect, const StatisticsInfo& value) {
    if (expect.idx_cnt_vec.size() != value.idx_cnt_vec.size()) {
        return ::testing::AssertionFailure()
               << "idx_cnt_vec size expect " << expect.idx_cnt_vec.size() << " but got " << value.idx_cnt_vec.size();
    }
    for (size_t idx = 0; idx < expect.idx_cnt_vec.size(); idx++) {
        if (expect.idx_cnt_vec[idx] != value.idx_cnt_vec[idx]) {
            return ::testing::AssertionFailure() << "idx_cnt_vec[" << idx << "] expect " << expect.idx_cnt_vec[idx]
                                                 << " but got " << value.idx_cnt_vec[idx];
        }
    }
    if (expect.record_byte_size != value.record_byte_size) {
        return ::testing::AssertionFailure()
               << "record_byte_size expect " << expect.record_byte_size << " but got " << value.record_byte_size;
    }
    if (expect.idx_byte_size != value.idx_byte_size) {
        return ::testing::AssertionFailure()
               << "idx_byte_size expect " << expect.idx_byte_size << " but got " << value.idx_byte_size;
    }
    return ::testing::AssertionSuccess();
}

// helper
::testing::AssertionResult CheckStatisticsInfo(std::initializer_list<size_t> vec, uint64_t idx_byte_size,
                                               uint64_t record_byte_size, const StatisticsInfo& value) {
    StatisticsInfo info(0);  // overwrite by set idx_cnt_vec
    info.idx_cnt_vec = vec;
    info.idx_byte_size = idx_byte_size;
    info.record_byte_size = record_byte_size;
    return CheckStatisticsInfo(info, value);
}

StatisticsInfo CreateStatisticsInfo(uint64_t idx_cnt, uint64_t idx_byte_size, uint64_t record_byte_size) {
    StatisticsInfo info(1);
    info.idx_cnt_vec[0] = idx_cnt;
    info.idx_byte_size = idx_byte_size;
    info.record_byte_size = record_byte_size;
    return info;
}

TEST_F(SegmentTest, Delete) {
    Segment segment(8);
    Slice pk("test1");
    std::string value = "test0";
    segment.Put(pk, 9527, value.c_str(), value.size());
    segment.Put(pk, 9528, value.c_str(), value.size());
    segment.Put(pk, 9528, value.c_str(), value.size());
    segment.Put(pk, 9529, value.c_str(), value.size());
    ASSERT_EQ(1, (int64_t)segment.GetPkCnt());
    Ticket ticket;
    std::unique_ptr<MemTableIterator> it(segment.NewIterator("test1", ticket, type::CompressType::kNoCompress));
    int size = 0;
    it->SeekToFirst();
    while (it->Valid()) {
        it->Next();
        size++;
    }
    ASSERT_EQ(4, size);
    ASSERT_TRUE(segment.Delete(std::nullopt, pk));
    it.reset(segment.NewIterator("test1", ticket, type::CompressType::kNoCompress));
    ASSERT_FALSE(it->Valid());
    segment.IncrGcVersion();
    segment.IncrGcVersion();
    StatisticsInfo gc_info(1);
    segment.GcFreeList(&gc_info);
    CheckStatisticsInfo(CreateStatisticsInfo(4, 365, 4 * (5 + sizeof(DataBlock))), gc_info);
}

TEST_F(SegmentTest, GetCount) {
    Segment segment(8);
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

    StatisticsInfo gc_info(1);
    segment.Gc4TTL(9528, &gc_info);
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
    Segment segment(8);
    Slice pk("test1");
    segment.Put(pk, 9768, "test1", 5);
    segment.Put(pk, 9768, "test1", 5);
    segment.Put(pk, 9768, "test1", 5);
    segment.Put(pk, 9769, "test2", 5);
    ASSERT_EQ(1, (int64_t)segment.GetPkCnt());
    Ticket ticket;
    std::unique_ptr<MemTableIterator> it(segment.NewIterator("test1", ticket, type::CompressType::kNoCompress));
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
    Segment segment(8);
    Slice pk("PK");
    segment.Put(pk, 9768, "test1", 5);
    segment.Put(pk, 9769, "test2", 5);
    StatisticsInfo gc_info(1);
    segment.Gc4Head(1, &gc_info);
    CheckStatisticsInfo(CreateStatisticsInfo(1, 0, GetRecordSize(5)), gc_info);
    Ticket ticket;
    std::unique_ptr<MemTableIterator> it(segment.NewIterator(pk, ticket, type::CompressType::kNoCompress));
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
    Segment segment(8);
    segment.Put("PK", 9768, "test1", 5);
    segment.Put("PK", 9769, "test2", 5);
    StatisticsInfo gc_info(1);
    segment.Gc4TTL(9765, &gc_info);
    CheckStatisticsInfo(CreateStatisticsInfo(0, 0, 0), gc_info);
    segment.Gc4TTL(9768, &gc_info);
    CheckStatisticsInfo(CreateStatisticsInfo(1, 0, GetRecordSize(5)), gc_info);
    segment.Gc4TTL(9770, &gc_info);
    CheckStatisticsInfo(CreateStatisticsInfo(2, 0, 2 * GetRecordSize(5)), gc_info);
    segment.IncrGcVersion();
    segment.IncrGcVersion();
    segment.GcFreeList(&gc_info);
    CheckStatisticsInfo(CreateStatisticsInfo(2, 194, 2 * GetRecordSize(5)), gc_info);
}

TEST_F(SegmentTest, TestGc4TTLAndHead) {
    Segment segment(8);
    segment.Put("PK1", 9766, "test1", 5);
    segment.Put("PK1", 9767, "test2", 5);
    segment.Put("PK1", 9768, "test3", 5);
    segment.Put("PK1", 9769, "test4", 5);
    segment.Put("PK2", 9765, "test1", 5);
    segment.Put("PK2", 9766, "test2", 5);
    segment.Put("PK2", 9767, "test3", 5);
    StatisticsInfo gc_info(1);
    // Gc4TTLAndHead only change gc_info.vec[0], check code
    // no expire
    segment.Gc4TTLAndHead(0, 0, &gc_info);
    ASSERT_TRUE(CheckStatisticsInfo({0}, 0, 0, gc_info));
    // no lat expire, so all records won't be deleted
    segment.Gc4TTLAndHead(9999, 0, &gc_info);
    ASSERT_TRUE(CheckStatisticsInfo({0}, 0, 0, gc_info));
    // no abs expire, so all records won't be deleted
    segment.Gc4TTLAndHead(0, 3, &gc_info);
    ASSERT_TRUE(CheckStatisticsInfo({0}, 0, 0, gc_info));
    // current_time > expire_time means not expired, so == is outdate and lat 2, so `9765` should be deleted
    segment.Gc4TTLAndHead(9765, 2, &gc_info);
    ASSERT_TRUE(CheckStatisticsInfo({1}, 0, GetRecordSize(5), gc_info));
    // gc again, no record expired, info won't update
    segment.Gc4TTLAndHead(9765, 2, &gc_info);
    ASSERT_TRUE(CheckStatisticsInfo({1}, 0, GetRecordSize(5), gc_info));
    // new info
    gc_info.Reset();
    // time <= 9770 is abs expired, but lat 1, so just 1 record per key left, 4 deleted
    segment.Gc4TTLAndHead(9770, 1, &gc_info);
    ASSERT_TRUE(CheckStatisticsInfo({4}, 0, 4 * GetRecordSize(5), gc_info));
    uint64_t cnt = 0;
    ASSERT_EQ(0, segment.GetCount("PK1", cnt));
    ASSERT_EQ(1, cnt);
    ASSERT_EQ(0, segment.GetCount("PK2", cnt));
    ASSERT_EQ(1, cnt);
}

TEST_F(SegmentTest, TestGc4TTLOrHead) {
    Segment segment(8);
    segment.Put("PK1", 9766, "test1", 5);
    segment.Put("PK1", 9767, "test2", 5);
    segment.Put("PK1", 9768, "test3", 5);
    segment.Put("PK1", 9769, "test4", 5);
    segment.Put("PK2", 9765, "test1", 5);
    segment.Put("PK2", 9766, "test2", 5);
    segment.Put("PK2", 9767, "test3", 5);
    StatisticsInfo gc_info(1);
    // no expire
    segment.Gc4TTLOrHead(0, 0, &gc_info);
    ASSERT_TRUE(CheckStatisticsInfo({0}, 0, 0, gc_info));
    // all record <= 9765 should be deleted, no matter the lat expire
    segment.Gc4TTLOrHead(9765, 0, &gc_info);
    ASSERT_TRUE(CheckStatisticsInfo({1}, 0, GetRecordSize(5), gc_info));
    gc_info.Reset();
    // even abs no expire, only lat 3 per key
    segment.Gc4TTLOrHead(0, 3, &gc_info);
    ASSERT_TRUE(CheckStatisticsInfo({1}, 0, GetRecordSize(5), gc_info));
    gc_info.Reset();
    segment.Gc4TTLOrHead(9765, 3, &gc_info);
    ASSERT_TRUE(CheckStatisticsInfo({0}, 0, 0, gc_info));
    segment.Gc4TTLOrHead(9766, 2, &gc_info);
    ASSERT_TRUE(CheckStatisticsInfo({2}, 0, 2 * GetRecordSize(5), gc_info));
    gc_info.Reset();
    segment.Gc4TTLOrHead(9770, 1, &gc_info);
    ASSERT_TRUE(CheckStatisticsInfo({3}, 0, 3 * GetRecordSize(5), gc_info));
}

TEST_F(SegmentTest, TestStat) {
    Segment segment(8);
    segment.Put("PK", 9768, "test1", 5);
    segment.Put("PK", 9769, "test2", 5);
    ASSERT_EQ(2, (int64_t)segment.GetIdxCnt());
    ASSERT_EQ(1, (int64_t)segment.GetPkCnt());
    StatisticsInfo gc_info(1);
    segment.Gc4TTL(9765, &gc_info);
    ASSERT_EQ(0, gc_info.GetTotalCnt());
    gc_info.Reset();
    segment.Gc4TTL(9768, &gc_info);
    ASSERT_EQ(1, (int64_t)segment.GetIdxCnt());
    ASSERT_EQ(1, gc_info.GetTotalCnt());
    segment.Gc4TTL(9770, &gc_info);
    ASSERT_EQ(2, gc_info.GetTotalCnt());
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

int GetCount(Segment* segment, int idx) {
    int count = 0;
    std::unique_ptr<KeyEntries::Iterator> pk_it(segment->GetKeyEntries()->NewIterator());
    if (!pk_it) {
        return 0;
    }
    uint32_t real_idx = idx;
    segment->GetTsIdx(idx, real_idx);
    pk_it->SeekToFirst();
    while (pk_it->Valid()) {
        KeyEntry* entry = nullptr;
        if (segment->GetTsCnt() > 1) {
            entry = reinterpret_cast<KeyEntry**>(pk_it->GetValue())[real_idx];
        } else {
            entry = reinterpret_cast<KeyEntry*>(pk_it->GetValue());
        }
        std::unique_ptr<TimeEntries::Iterator> ts_it(entry->entries.NewIterator());
        ts_it->SeekToFirst();
        while (ts_it->Valid()) {
            count++;
            ts_it->Next();
        }
        pk_it->Next();
    }
    return count;
}

TEST_F(SegmentTest, ReleaseAndCount) {
    std::vector<uint32_t> ts_idx_vec = {1, 3};
    Segment segment(8, ts_idx_vec);
    ASSERT_EQ(2, (int64_t)segment.GetTsCnt());
    for (int i = 0; i < 100; i++) {
        std::string key = "key" + std::to_string(i);
        uint64_t ts = 1669013677221000;
        for (int j = 0; j < 2; j++) {
            DataBlock* data = new DataBlock(2, key.c_str(), key.length());
            std::map<int32_t, uint64_t> ts_map = {{1, ts + j}, {3, ts + j}};
            segment.Put(Slice(key), ts_map, data);
        }
    }
    ASSERT_EQ(200, GetCount(&segment, 1));
    ASSERT_EQ(200, GetCount(&segment, 3));
    StatisticsInfo gc_info(1);
    segment.ReleaseAndCount({1}, &gc_info);
    ASSERT_EQ(0, GetCount(&segment, 1));
    ASSERT_EQ(200, GetCount(&segment, 3));
    segment.ReleaseAndCount(&gc_info);
    ASSERT_EQ(0, GetCount(&segment, 1));
    ASSERT_EQ(0, GetCount(&segment, 3));
}

TEST_F(SegmentTest, ReleaseAndCountOneTs) {
    Segment segment(8);
    for (int i = 0; i < 100; i++) {
        std::string key = "key" + std::to_string(i);
        uint64_t ts = 1669013677221000;
        for (int j = 0; j < 2; j++) {
            segment.Put(Slice(key), ts + j, key.c_str(), key.size());
        }
    }
    StatisticsInfo gc_info(1);
    ASSERT_EQ(200, GetCount(&segment, 0));
    segment.ReleaseAndCount(&gc_info);
    ASSERT_EQ(0, GetCount(&segment, 0));
}

TEST_F(SegmentTest, TestDeleteRange) {
    Segment segment(8);
    for (int idx = 0; idx < 10; idx++) {
        std::string key = absl::StrCat("key", idx);
        std::string value = absl::StrCat("value", idx);
        uint64_t ts = 1000;
        for (int i = 0; i < 10; i++) {
            segment.Put(Slice(key), ts + i, value.data(), 6);
        }
    }
    ASSERT_EQ(100, GetCount(&segment, 0));
    std::string pk = "key2";
    Ticket ticket;
    std::unique_ptr<MemTableIterator> it(segment.NewIterator(pk, ticket, type::CompressType::kNoCompress));
    it->Seek(1005);
    ASSERT_TRUE(it->Valid() && it->GetKey() == 1005);
    ASSERT_TRUE(segment.Delete(std::nullopt, pk, 1005, 1004));
    ASSERT_EQ(99, GetCount(&segment, 0));
    it->Seek(1005);
    ASSERT_FALSE(it->Valid() && it->GetKey() == 1005);
    ASSERT_TRUE(segment.Delete(std::nullopt, pk, 1005, std::nullopt));
    ASSERT_EQ(94, GetCount(&segment, 0));
    it->Seek(1005);
    ASSERT_FALSE(it->Valid());
    pk = "key3";
    ASSERT_TRUE(segment.Delete(std::nullopt, pk));
    pk = "key4";
    ASSERT_TRUE(segment.Delete(std::nullopt, pk, 1005, 1001));
    ASSERT_EQ(80, GetCount(&segment, 0));
    segment.IncrGcVersion();
    segment.IncrGcVersion();
    StatisticsInfo gc_info(1);
    segment.GcFreeList(&gc_info);
    CheckStatisticsInfo(CreateStatisticsInfo(20, 1012, 20 * (6 + sizeof(DataBlock))), gc_info);
}

TEST_F(SegmentTest, PutIfAbsent) {
    {
        Segment segment(8);  // so ts_cnt_ == 1
        // check all time == false
        segment.Put("PK", 1, "test1", 5, true);
        segment.Put("PK", 1, "test2", 5, true);  // even key&time is the same, different value means different record
        ASSERT_EQ(2, (int64_t)segment.GetIdxCnt());
        ASSERT_EQ(1, (int64_t)segment.GetPkCnt());
        segment.Put("PK", 2, "test3", 5, true);
        segment.Put("PK", 2, "test4", 5, true);
        segment.Put("PK", 3, "test5", 5, true);
        segment.Put("PK", 3, "test6", 5, true);
        ASSERT_EQ(6, (int64_t)segment.GetIdxCnt());
        // insert exists rows
        segment.Put("PK", 2, "test3", 5, true);
        segment.Put("PK", 1, "test1", 5, true);
        segment.Put("PK", 1, "test2", 5, true);
        segment.Put("PK", 3, "test6", 5, true);
        ASSERT_EQ(6, (int64_t)segment.GetIdxCnt());
        // new rows
        segment.Put("PK", 2, "test7", 5, true);
        ASSERT_EQ(7, (int64_t)segment.GetIdxCnt());
        segment.Put("PK", 0, "test8", 5, true);  // seek to last, next is empty
        ASSERT_EQ(8, (int64_t)segment.GetIdxCnt());
    }

    {
        // support when ts_cnt_ != 1 too
        std::vector<uint32_t> ts_idx_vec = {1, 3};
        Segment segment(8, ts_idx_vec);
        ASSERT_EQ(2, (int64_t)segment.GetTsCnt());
        std::string key = "PK";
        uint64_t ts = 1669013677221000;
        // the same ts
        for (int j = 0; j < 2; j++) {
            DataBlock* data = new DataBlock(2, key.c_str(), key.length());
            std::map<int32_t, uint64_t> ts_map = {{1, ts}, {3, ts}};
            segment.Put(Slice(key), ts_map, data, true);
        }
        ASSERT_EQ(1, GetCount(&segment, 1));
        ASSERT_EQ(1, GetCount(&segment, 3));
    }

    {
        // put ts_map contains DEFAULT_TS_COL_ID
        std::vector<uint32_t> ts_idx_vec = {DEFAULT_TS_COL_ID};
        Segment segment(8, ts_idx_vec);
        ASSERT_EQ(1, (int64_t)segment.GetTsCnt());
        std::string key = "PK";
        std::map<int32_t, uint64_t> ts_map = {{DEFAULT_TS_COL_ID, 100}};  // cur time == 100
        auto* block = new DataBlock(1, "test1", 5);
        segment.Put(Slice(key), ts_map, block, true);
        ASSERT_EQ(1, GetCount(&segment, DEFAULT_TS_COL_ID));
        ts_map = {{DEFAULT_TS_COL_ID, 200}};
        block = new DataBlock(1, "test1", 5);
        segment.Put(Slice(key), ts_map, block, true);
        ASSERT_EQ(1, GetCount(&segment, DEFAULT_TS_COL_ID));
    }

    {
        // put ts_map contains DEFAULT_TS_COL_ID
        std::vector<uint32_t> ts_idx_vec = {DEFAULT_TS_COL_ID, 1, 3};
        Segment segment(8, ts_idx_vec);
        ASSERT_EQ(3, (int64_t)segment.GetTsCnt());
        std::string key = "PK";
        std::map<int32_t, uint64_t> ts_map = {{DEFAULT_TS_COL_ID, 100}};  // cur time == 100
        auto* block = new DataBlock(1, "test1", 5);
        segment.Put(Slice(key), ts_map, block, true);
        ASSERT_EQ(1, GetCount(&segment, DEFAULT_TS_COL_ID));
        ts_map = {{DEFAULT_TS_COL_ID, 200}};
        block = new DataBlock(1, "test1", 5);
        segment.Put(Slice(key), ts_map, block, true);
        ASSERT_EQ(1, GetCount(&segment, DEFAULT_TS_COL_ID));
    }
}

}  // namespace storage
}  // namespace openmldb

int main(int argc, char** argv) {
    ::openmldb::base::SetLogLevel(INFO);
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
