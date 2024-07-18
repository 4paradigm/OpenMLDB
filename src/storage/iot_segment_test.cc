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

#include "storage/iot_segment.h"

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

// iotsegment is not the same with segment, so we need to test it separately
class IOTSegmentTest : public ::testing::Test {
 public:
    IOTSegmentTest() {}
    ~IOTSegmentTest() {}
};

TEST_F(IOTSegmentTest, PutAndScan) {
    IOTSegment segment(8, {1, 3, 5},
                       {common::IndexType::kClustered, common::IndexType::kSecondary, common::IndexType::kCovering});
    Slice pk("test1");
    std::string value = "test0";
    auto cblk = new DataBlock(2, value.c_str(), value.size());  // 1 clustered + 1 covering, hard copy
    auto sblk = new DataBlock(1, value.c_str(), value.size());  // 1 secondary, fake value, hard copy
    // use the frenquently used Put method
    ASSERT_TRUE(segment.Put(pk, {{1, 100}, {3, 300}, {5, 500}}, cblk, sblk));
    // if first one is clustered index, segment put will fail in the first time, no need to revert
    ASSERT_FALSE(segment.Put(pk, {{1, 100}}, cblk, sblk));
    ASSERT_FALSE(segment.Put(pk, {{1, 100}, {3, 300}, {5, 500}}, cblk, sblk));
    ASSERT_EQ(1, (int64_t)segment.GetPkCnt());
    Ticket ticket;
    // iter clustered(idx 1), not the secondary, don't create iot iter
    std::unique_ptr<MemTableIterator> it(
        segment.Segment::NewIterator("test1", 1, ticket, type::CompressType::kNoCompress));
    it->Seek(500);  // find less than
    ASSERT_TRUE(it->Valid());
    ASSERT_EQ(100, (int64_t)it->GetKey());
    ::openmldb::base::Slice val = it->GetValue();
    std::string result(val.data(), val.size());
    ASSERT_EQ("test0", result);
    it->Next();
    ASSERT_FALSE(it->Valid());  // just one row

    // if first one is not the clustered index, we can't know if it exists, be careful
    ASSERT_TRUE(segment.Put(pk, {{3, 300}, {5, 500}}, nullptr, nullptr));
}

TEST_F(IOTSegmentTest, PutAndScanWhenDefaultTs) {
    // in the same inner index, it won't have the same ts id
    IOTSegment segment(8, {DEFAULT_TS_COL_ID, 3, 5},
                       {common::IndexType::kClustered, common::IndexType::kSecondary, common::IndexType::kCovering});
    Slice pk("test1");
    std::string value = "test0";
    auto cblk = new DataBlock(2, value.c_str(), value.size());  // 1 clustered + 1 covering, hard copy
    auto sblk = new DataBlock(1, value.c_str(), value.size());  // 1 secondary, fake value, hard copy
    // use the frenquently used Put method
    ASSERT_TRUE(segment.Put(pk, {{DEFAULT_TS_COL_ID, 100}, {3, 300}, {5, 500}}, cblk, sblk));
    // if first one is clustered index, segment put will fail in the first time, no need to revert
    ASSERT_FALSE(segment.Put(pk, {{DEFAULT_TS_COL_ID, 100}}, cblk, sblk));
    ASSERT_FALSE(segment.Put(pk, {{DEFAULT_TS_COL_ID, 100}, {3, 300}, {5, 500}}, cblk, sblk));
    ASSERT_EQ(1, (int64_t)segment.GetPkCnt());
    Ticket ticket;
    // iter clustered(idx 1), not the secondary, don't create iot iter
    std::unique_ptr<MemTableIterator> it(
        segment.Segment::NewIterator("test1", DEFAULT_TS_COL_ID, ticket, type::CompressType::kNoCompress));
    it->Seek(500);  // find less than
    ASSERT_TRUE(it->Valid());
    ASSERT_EQ(100, (int64_t)it->GetKey());
    ::openmldb::base::Slice val = it->GetValue();
    std::string result(val.data(), val.size());
    ASSERT_EQ("test0", result);
    it->Next();
    ASSERT_FALSE(it->Valid());  // just one row

    // if first one is not the clustered index, we can't know if it exists, be careful
    ASSERT_TRUE(segment.Put(pk, {{3, 300}, {5, 500}}, nullptr, nullptr));
}

TEST_F(IOTSegmentTest, CheckKeyExists) {
    IOTSegment segment(8, {1, 3, 5},
                       {common::IndexType::kClustered, common::IndexType::kSecondary, common::IndexType::kCovering});
    Slice pk("test1");
    std::string value = "test0";
    auto cblk = new DataBlock(2, value.c_str(), value.size());  // 1 clustered + 1 covering, hard copy
    auto sblk = new DataBlock(1, value.c_str(), value.size());  // 1 secondary, fake value, hard copy
    // use the frenquently used Put method
    segment.Put(pk, {{1, 100}, {3, 300}, {5, 500}}, cblk, sblk);
    ASSERT_EQ(1, (int64_t)segment.GetPkCnt());
    // check if exists in cidx segment(including 'ttl expired but not gc')
    auto st = segment.CheckKeyExists(pk, {{1, 100}});
    ASSERT_TRUE(absl::IsAlreadyExists(st)) << st.ToString();
    st = segment.CheckKeyExists(pk, {{1, 300}});
    ASSERT_TRUE(absl::IsNotFound(st)) << st.ToString();
    // check sidx/covering idx will fail
    st = segment.CheckKeyExists(pk, {{3, 300}});
    ASSERT_TRUE(absl::IsInvalidArgument(st)) << st.ToString();
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

// TODO(hw): gc multi idx has bug, fix later
// TEST_F(IOTSegmentTest, TestGc4Head) {
//     IOTSegment segment(8);
//     Slice pk("PK");
//     segment.Put(pk, 9768, "test1", 5);
//     segment.Put(pk, 9769, "test2", 5);
//     StatisticsInfo gc_info(1);
//     segment.Gc4Head(1, &gc_info);
//     CheckStatisticsInfo(CreateStatisticsInfo(1, 0, GetRecordSize(5)), gc_info);
//     Ticket ticket;
//     std::unique_ptr<MemTableIterator> it(segment.NewIterator(pk, ticket, type::CompressType::kNoCompress));
//     it->Seek(9769);
//     ASSERT_TRUE(it->Valid());
//     ASSERT_EQ(9769, (int64_t)it->GetKey());
//     ::openmldb::base::Slice value = it->GetValue();
//     std::string result(value.data(), value.size());
//     ASSERT_EQ("test2", result);
//     it->Next();
//     ASSERT_FALSE(it->Valid());
// }

TEST_F(IOTSegmentTest, TestGc4TTL) {
    // cidx segment won't execute gc, gc will be done in iot gc
    // and multi idx gc `GcAllType` has bug, skip test it
    {
        std::vector<uint32_t> idx_vec = {1};
        std::vector<common::IndexType> idx_type = {common::IndexType::kClustered};
        auto segment = std::make_unique<IOTSegment>(8, idx_vec, idx_type);
        Slice pk("test1");
        std::string value = "test0";
        auto cblk = new DataBlock(1, value.c_str(), value.size());  // 1 clustered + 1 covering, hard copy
        auto sblk = new DataBlock(1, value.c_str(), value.size());  // 1 secondary, fake value, hard copy
        ASSERT_TRUE(segment->Put(pk, {{1, 100}}, cblk, sblk));
        // ref iot gc SchedGCByDelete
        StatisticsInfo statistics_info(segment->GetTsCnt());
        segment->IncrGcVersion();
        segment->GcFreeList(&statistics_info);
        segment->ExecuteGc({{1, {1, 0, TTLType::kAbsoluteTime}}}, &statistics_info, segment->ClusteredTs());
        ASSERT_TRUE(CheckStatisticsInfo({0}, 0, 0, statistics_info));
    }
    {
        std::vector<uint32_t> idx_vec = {1};
        std::vector<common::IndexType> idx_type = {common::IndexType::kSecondary};
        auto segment = std::make_unique<IOTSegment>(8, idx_vec, idx_type);
        Slice pk("test1");
        std::string value = "test0";
        auto cblk = new DataBlock(1, value.c_str(), value.size());  // 1 clustered + 1 covering, hard copy
        // execute gc will delete it
        auto sblk = new DataBlock(1, value.c_str(), value.size());  // 1 secondary, fake value, hard copy
        ASSERT_TRUE(segment->Put(pk, {{1, 100}}, cblk, sblk));
        // ref iot gc SchedGCByDelete
        StatisticsInfo statistics_info(segment->GetTsCnt());
        segment->IncrGcVersion();  // 1
        segment->GcFreeList(&statistics_info);
        segment->ExecuteGc({{1, {1, 0, TTLType::kAbsoluteTime}}}, &statistics_info, segment->ClusteredTs());
        // secondary will gc, but idx_byte_size is 0(GcFreeList change it)
        ASSERT_TRUE(CheckStatisticsInfo({1}, 0, GetRecordSize(5), statistics_info));

        segment->IncrGcVersion();               // 2
        segment->GcFreeList(&statistics_info);  // empty
        ASSERT_TRUE(CheckStatisticsInfo({1}, 0, GetRecordSize(5), statistics_info));
        segment->IncrGcVersion();  // delta default is 2, version should >=2, and node_cache free version should >= 3
        segment->GcFreeList(&statistics_info);
        // don't know why 197
        ASSERT_TRUE(CheckStatisticsInfo({1}, 197, GetRecordSize(5), statistics_info));
    }
}

// TEST_F(IOTSegmentTest, TestGc4TTLAndHead) {
//     IOTSegment segment(8);
//     segment.Put("PK1", 9766, "test1", 5);
//     segment.Put("PK1", 9767, "test2", 5);
//     segment.Put("PK1", 9768, "test3", 5);
//     segment.Put("PK1", 9769, "test4", 5);
//     segment.Put("PK2", 9765, "test1", 5);
//     segment.Put("PK2", 9766, "test2", 5);
//     segment.Put("PK2", 9767, "test3", 5);
//     StatisticsInfo gc_info(1);
//     // Gc4TTLAndHead only change gc_info.vec[0], check code
//     // no expire
//     segment.Gc4TTLAndHead(0, 0, &gc_info);
//     ASSERT_TRUE(CheckStatisticsInfo({0}, 0, 0, gc_info));
//     // no lat expire, so all records won't be deleted
//     segment.Gc4TTLAndHead(9999, 0, &gc_info);
//     ASSERT_TRUE(CheckStatisticsInfo({0}, 0, 0, gc_info));
//     // no abs expire, so all records won't be deleted
//     segment.Gc4TTLAndHead(0, 3, &gc_info);
//     ASSERT_TRUE(CheckStatisticsInfo({0}, 0, 0, gc_info));
//     // current_time > expire_time means not expired, so == is outdate and lat 2, so `9765` should be deleted
//     segment.Gc4TTLAndHead(9765, 2, &gc_info);
//     ASSERT_TRUE(CheckStatisticsInfo({1}, 0, GetRecordSize(5), gc_info));
//     // gc again, no record expired, info won't update
//     segment.Gc4TTLAndHead(9765, 2, &gc_info);
//     ASSERT_TRUE(CheckStatisticsInfo({1}, 0, GetRecordSize(5), gc_info));
//     // new info
//     gc_info.Reset();
//     // time <= 9770 is abs expired, but lat 1, so just 1 record per key left, 4 deleted
//     segment.Gc4TTLAndHead(9770, 1, &gc_info);
//     ASSERT_TRUE(CheckStatisticsInfo({4}, 0, 4 * GetRecordSize(5), gc_info));
//     uint64_t cnt = 0;
//     ASSERT_EQ(0, segment.GetCount("PK1", cnt));
//     ASSERT_EQ(1, cnt);
//     ASSERT_EQ(0, segment.GetCount("PK2", cnt));
//     ASSERT_EQ(1, cnt);
// }

// TEST_F(IOTSegmentTest, TestGc4TTLOrHead) {
//     IOTSegment segment(8);
//     segment.Put("PK1", 9766, "test1", 5);
//     segment.Put("PK1", 9767, "test2", 5);
//     segment.Put("PK1", 9768, "test3", 5);
//     segment.Put("PK1", 9769, "test4", 5);
//     segment.Put("PK2", 9765, "test1", 5);
//     segment.Put("PK2", 9766, "test2", 5);
//     segment.Put("PK2", 9767, "test3", 5);
//     StatisticsInfo gc_info(1);
//     // no expire
//     segment.Gc4TTLOrHead(0, 0, &gc_info);
//     ASSERT_TRUE(CheckStatisticsInfo({0}, 0, 0, gc_info));
//     // all record <= 9765 should be deleted, no matter the lat expire
//     segment.Gc4TTLOrHead(9765, 0, &gc_info);
//     ASSERT_TRUE(CheckStatisticsInfo({1}, 0, GetRecordSize(5), gc_info));
//     gc_info.Reset();
//     // even abs no expire, only lat 3 per key
//     segment.Gc4TTLOrHead(0, 3, &gc_info);
//     ASSERT_TRUE(CheckStatisticsInfo({1}, 0, GetRecordSize(5), gc_info));
//     gc_info.Reset();
//     segment.Gc4TTLOrHead(9765, 3, &gc_info);
//     ASSERT_TRUE(CheckStatisticsInfo({0}, 0, 0, gc_info));
//     segment.Gc4TTLOrHead(9766, 2, &gc_info);
//     ASSERT_TRUE(CheckStatisticsInfo({2}, 0, 2 * GetRecordSize(5), gc_info));
//     gc_info.Reset();
//     segment.Gc4TTLOrHead(9770, 1, &gc_info);
//     ASSERT_TRUE(CheckStatisticsInfo({3}, 0, 3 * GetRecordSize(5), gc_info));
// }

// TEST_F(IOTSegmentTest, TestStat) {
//     IOTSegment segment(8);
//     segment.Put("PK", 9768, "test1", 5);
//     segment.Put("PK", 9769, "test2", 5);
//     ASSERT_EQ(2, (int64_t)segment.GetIdxCnt());
//     ASSERT_EQ(1, (int64_t)segment.GetPkCnt());
//     StatisticsInfo gc_info(1);
//     segment.Gc4TTL(9765, &gc_info);
//     ASSERT_EQ(0, gc_info.GetTotalCnt());
//     gc_info.Reset();
//     segment.Gc4TTL(9768, &gc_info);
//     ASSERT_EQ(1, (int64_t)segment.GetIdxCnt());
//     ASSERT_EQ(1, gc_info.GetTotalCnt());
//     segment.Gc4TTL(9770, &gc_info);
//     ASSERT_EQ(2, gc_info.GetTotalCnt());
//     ASSERT_EQ(0, (int64_t)segment.GetIdxCnt());
// }

// TEST_F(IOTSegmentTest, GetTsIdx) {
//     std::vector<uint32_t> ts_idx_vec = {1, 3, 5};
//     IOTSegment segment(8, ts_idx_vec);
//     ASSERT_EQ(3, (int64_t)segment.GetTsCnt());
//     uint32_t real_idx = UINT32_MAX;
//     ASSERT_EQ(-1, segment.GetTsIdx(0, real_idx));
//     ASSERT_EQ(0, segment.GetTsIdx(1, real_idx));
//     ASSERT_EQ(0, (int64_t)real_idx);
//     ASSERT_EQ(-1, segment.GetTsIdx(2, real_idx));
//     ASSERT_EQ(0, segment.GetTsIdx(3, real_idx));
//     ASSERT_EQ(1, (int64_t)real_idx);
//     ASSERT_EQ(-1, segment.GetTsIdx(4, real_idx));
//     ASSERT_EQ(0, segment.GetTsIdx(5, real_idx));
//     ASSERT_EQ(2, (int64_t)real_idx);
// }

// int GetCount(IOTSegment* segment, int idx) {
//     int count = 0;
//     std::unique_ptr<KeyEntries::Iterator> pk_it(segment->GetKeyEntries()->NewIterator());
//     if (!pk_it) {
//         return 0;
//     }
//     uint32_t real_idx = idx;
//     segment->GetTsIdx(idx, real_idx);
//     pk_it->SeekToFirst();
//     while (pk_it->Valid()) {
//         KeyEntry* entry = nullptr;
//         if (segment->GetTsCnt() > 1) {
//             entry = reinterpret_cast<KeyEntry**>(pk_it->GetValue())[real_idx];
//         } else {
//             entry = reinterpret_cast<KeyEntry*>(pk_it->GetValue());
//         }
//         std::unique_ptr<TimeEntries::Iterator> ts_it(entry->entries.NewIterator());
//         ts_it->SeekToFirst();
//         while (ts_it->Valid()) {
//             count++;
//             ts_it->Next();
//         }
//         pk_it->Next();
//     }
//     return count;
// }

// TEST_F(IOTSegmentTest, ReleaseAndCount) {
//     std::vector<uint32_t> ts_idx_vec = {1, 3};
//     IOTSegment segment(8, ts_idx_vec);
//     ASSERT_EQ(2, (int64_t)segment.GetTsCnt());
//     for (int i = 0; i < 100; i++) {
//         std::string key = "key" + std::to_string(i);
//         uint64_t ts = 1669013677221000;
//         for (int j = 0; j < 2; j++) {
//             DataBlock* data = new DataBlock(2, key.c_str(), key.length());
//             std::map<int32_t, uint64_t> ts_map = {{1, ts + j}, {3, ts + j}};
//             segment.Put(Slice(key), ts_map, data);
//         }
//     }
//     ASSERT_EQ(200, GetCount(&segment, 1));
//     ASSERT_EQ(200, GetCount(&segment, 3));
//     StatisticsInfo gc_info(1);
//     segment.ReleaseAndCount({1}, &gc_info);
//     ASSERT_EQ(0, GetCount(&segment, 1));
//     ASSERT_EQ(200, GetCount(&segment, 3));
//     segment.ReleaseAndCount(&gc_info);
//     ASSERT_EQ(0, GetCount(&segment, 1));
//     ASSERT_EQ(0, GetCount(&segment, 3));
// }

// TEST_F(IOTSegmentTest, ReleaseAndCountOneTs) {
//     IOTSegment segment(8);
//     for (int i = 0; i < 100; i++) {
//         std::string key = "key" + std::to_string(i);
//         uint64_t ts = 1669013677221000;
//         for (int j = 0; j < 2; j++) {
//             segment.Put(Slice(key), ts + j, key.c_str(), key.size());
//         }
//     }
//     StatisticsInfo gc_info(1);
//     ASSERT_EQ(200, GetCount(&segment, 0));
//     segment.ReleaseAndCount(&gc_info);
//     ASSERT_EQ(0, GetCount(&segment, 0));
// }

// TEST_F(IOTSegmentTest, TestDeleteRange) {
//     IOTSegment segment(8);
//     for (int idx = 0; idx < 10; idx++) {
//         std::string key = absl::StrCat("key", idx);
//         std::string value = absl::StrCat("value", idx);
//         uint64_t ts = 1000;
//         for (int i = 0; i < 10; i++) {
//             segment.Put(Slice(key), ts + i, value.data(), 6);
//         }
//     }
//     ASSERT_EQ(100, GetCount(&segment, 0));
//     std::string pk = "key2";
//     Ticket ticket;
//     std::unique_ptr<MemTableIterator> it(segment.NewIterator(pk, ticket, type::CompressType::kNoCompress));
//     it->Seek(1005);
//     ASSERT_TRUE(it->Valid() && it->GetKey() == 1005);
//     ASSERT_TRUE(segment.Delete(std::nullopt, pk, 1005, 1004));
//     ASSERT_EQ(99, GetCount(&segment, 0));
//     it->Seek(1005);
//     ASSERT_FALSE(it->Valid() && it->GetKey() == 1005);
//     ASSERT_TRUE(segment.Delete(std::nullopt, pk, 1005, std::nullopt));
//     ASSERT_EQ(94, GetCount(&segment, 0));
//     it->Seek(1005);
//     ASSERT_FALSE(it->Valid());
//     pk = "key3";
//     ASSERT_TRUE(segment.Delete(std::nullopt, pk));
//     pk = "key4";
//     ASSERT_TRUE(segment.Delete(std::nullopt, pk, 1005, 1001));
//     ASSERT_EQ(80, GetCount(&segment, 0));
//     segment.IncrGcVersion();
//     segment.IncrGcVersion();
//     StatisticsInfo gc_info(1);
//     segment.GcFreeList(&gc_info);
//     CheckStatisticsInfo(CreateStatisticsInfo(20, 1012, 20 * (6 + sizeof(DataBlock))), gc_info);
// }

// TEST_F(IOTSegmentTest, PutIfAbsent) {
//     {
//         IOTSegment segment(8);  // so ts_cnt_ == 1
//         // check all time == false
//         segment.Put("PK", 1, "test1", 5, true);
//         segment.Put("PK", 1, "test2", 5, true);  // even key&time is the same, different value means different record
//         ASSERT_EQ(2, (int64_t)segment.GetIdxCnt());
//         ASSERT_EQ(1, (int64_t)segment.GetPkCnt());
//         segment.Put("PK", 2, "test3", 5, true);
//         segment.Put("PK", 2, "test4", 5, true);
//         segment.Put("PK", 3, "test5", 5, true);
//         segment.Put("PK", 3, "test6", 5, true);
//         ASSERT_EQ(6, (int64_t)segment.GetIdxCnt());
//         // insert exists rows
//         segment.Put("PK", 2, "test3", 5, true);
//         segment.Put("PK", 1, "test1", 5, true);
//         segment.Put("PK", 1, "test2", 5, true);
//         segment.Put("PK", 3, "test6", 5, true);
//         ASSERT_EQ(6, (int64_t)segment.GetIdxCnt());
//         // new rows
//         segment.Put("PK", 2, "test7", 5, true);
//         ASSERT_EQ(7, (int64_t)segment.GetIdxCnt());
//         segment.Put("PK", 0, "test8", 5, true);  // seek to last, next is empty
//         ASSERT_EQ(8, (int64_t)segment.GetIdxCnt());
//     }

//     {
//         // support when ts_cnt_ != 1 too
//         std::vector<uint32_t> ts_idx_vec = {1, 3};
//         IOTSegment segment(8, ts_idx_vec);
//         ASSERT_EQ(2, (int64_t)segment.GetTsCnt());
//         std::string key = "PK";
//         uint64_t ts = 1669013677221000;
//         // the same ts
//         for (int j = 0; j < 2; j++) {
//             DataBlock* data = new DataBlock(2, key.c_str(), key.length());
//             std::map<int32_t, uint64_t> ts_map = {{1, ts}, {3, ts}};
//             segment.Put(Slice(key), ts_map, data, true);
//         }
//         ASSERT_EQ(1, GetCount(&segment, 1));
//         ASSERT_EQ(1, GetCount(&segment, 3));
//     }

//     {
//         // put ts_map contains DEFAULT_TS_COL_ID
//         std::vector<uint32_t> ts_idx_vec = {DEFAULT_TS_COL_ID};
//         IOTSegment segment(8, ts_idx_vec);
//         ASSERT_EQ(1, (int64_t)segment.GetTsCnt());
//         std::string key = "PK";
//         std::map<int32_t, uint64_t> ts_map = {{DEFAULT_TS_COL_ID, 100}};  // cur time == 100
//         auto* block = new DataBlock(1, "test1", 5);
//         segment.Put(Slice(key), ts_map, block, true);
//         ASSERT_EQ(1, GetCount(&segment, DEFAULT_TS_COL_ID));
//         ts_map = {{DEFAULT_TS_COL_ID, 200}};
//         block = new DataBlock(1, "test1", 5);
//         segment.Put(Slice(key), ts_map, block, true);
//         ASSERT_EQ(1, GetCount(&segment, DEFAULT_TS_COL_ID));
//     }

//     {
//         // put ts_map contains DEFAULT_TS_COL_ID
//         std::vector<uint32_t> ts_idx_vec = {DEFAULT_TS_COL_ID, 1, 3};
//         IOTSegment segment(8, ts_idx_vec);
//         ASSERT_EQ(3, (int64_t)segment.GetTsCnt());
//         std::string key = "PK";
//         std::map<int32_t, uint64_t> ts_map = {{DEFAULT_TS_COL_ID, 100}};  // cur time == 100
//         auto* block = new DataBlock(1, "test1", 5);
//         segment.Put(Slice(key), ts_map, block, true);
//         ASSERT_EQ(1, GetCount(&segment, DEFAULT_TS_COL_ID));
//         ts_map = {{DEFAULT_TS_COL_ID, 200}};
//         block = new DataBlock(1, "test1", 5);
//         segment.Put(Slice(key), ts_map, block, true);
//         ASSERT_EQ(1, GetCount(&segment, DEFAULT_TS_COL_ID));
//     }
// }

}  // namespace storage
}  // namespace openmldb

int main(int argc, char** argv) {
    ::openmldb::base::SetLogLevel(INFO);
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
