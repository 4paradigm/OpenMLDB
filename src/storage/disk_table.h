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

#pragma once

#include <atomic>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "base/slice.h"
#include "base/status.h"
#include "common/timer.h"
#include "proto/common.pb.h"
#include "proto/tablet.pb.h"
#include "rocksdb/compaction_filter.h"
#include "rocksdb/db.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/status.h"
#include "rocksdb/table.h"
#include "rocksdb/utilities/checkpoint.h"
#include "storage/iterator.h"
#include "storage/key_transform.h"
#include "storage/table.h"

namespace openmldb {
namespace storage {

class KeyTSComparator : public rocksdb::Comparator {
 public:
    KeyTSComparator() {}
    const char* Name() const override { return "KeyTSComparator"; }

    int Compare(const rocksdb::Slice& a, const rocksdb::Slice& b) const override {
        rocksdb::Slice key1, key2;  // may contain the ts_pos
        uint64_t ts1 = 0, ts2 = 0;
        ParseKeyAndTs(a, &key1, &ts1);
        ParseKeyAndTs(b, &key2, &ts2);

        int ret = key1.compare(key2);
        if (ret != 0) {
            return ret;
        } else {
            if (ts1 > ts2) return -1;
            if (ts1 < ts2) return 1;
            return 0;
        }
    }
    void FindShortestSeparator(std::string* /*start*/, const rocksdb::Slice& /*limit*/) const override {}
    void FindShortSuccessor(std::string* /*key*/) const override {}
};

class KeyTsPrefixTransform : public rocksdb::SliceTransform {
 public:
    const char* Name() const override { return "KeyTsPrefixTransform"; }
    rocksdb::Slice Transform(const rocksdb::Slice& src) const override {
        assert(InDomain(src));
        return rocksdb::Slice(src.data(), src.size() - TS_LEN);
    }

    bool InDomain(const rocksdb::Slice& src) const override { return src.size() >= TS_LEN; }

    bool InRange(const rocksdb::Slice& dst) const override { return dst.size() <= TS_LEN; }

    bool FullLengthEnabled(size_t* len) const override { return false; }

    bool SameResultWhenAppended(const rocksdb::Slice& prefix) const override { return InDomain(prefix); }
};

class AbsoluteTTLCompactionFilter : public rocksdb::CompactionFilter {
 public:
    explicit AbsoluteTTLCompactionFilter(std::shared_ptr<InnerIndexSt> inner_index) : inner_index_(inner_index) {}
    virtual ~AbsoluteTTLCompactionFilter() {}

    const char* Name() const override { return "AbsoluteTTLCompactionFilter"; }

    bool Filter(int /*level*/, const rocksdb::Slice& key, const rocksdb::Slice& /*existing_value*/,
                std::string* /*new_value*/, bool* /*value_changed*/) const override {
        if (key.size() < TS_LEN) {
            return false;
        }
        uint64_t real_ttl = 0;
        const auto& indexs = inner_index_->GetIndex();
        if (indexs.size() > 1) {
            if (key.size() < TS_LEN + TS_POS_LEN) {
                return false;
            }
            uint32_t ts_idx = *((uint32_t*)(key.data() + key.size() - TS_LEN -  // NOLINT
                                            TS_POS_LEN));
            bool has_found = false;
            for (const auto& index : indexs) {
                auto ts_col = index->GetTsColumn();
                if (!ts_col) {
                    return false;
                }
                if (ts_col->GetId() == ts_idx &&
                    index->GetTTL()->ttl_type == openmldb::storage::TTLType::kAbsoluteTime) {
                    real_ttl = index->GetTTL()->abs_ttl;
                    has_found = true;
                    break;
                }
            }
            if (!has_found) {
                return false;
            }
        } else {
            real_ttl = indexs.front()->GetTTL()->abs_ttl;
        }
        if (real_ttl < 1) {
            return false;
        }
        uint64_t ts = 0;
        memcpy(static_cast<void*>(&ts), key.data() + key.size() - TS_LEN, TS_LEN);
        memrev64ifbe(static_cast<void*>(&ts));
        uint64_t cur_time = ::baidu::common::timer::get_micros() / 1000;
        if (ts < cur_time - real_ttl) {
            return true;
        }
        return false;
    }

 private:
    std::shared_ptr<InnerIndexSt> inner_index_;
};

class AbsoluteTTLFilterFactory : public rocksdb::CompactionFilterFactory {
 public:
    explicit AbsoluteTTLFilterFactory(const std::shared_ptr<InnerIndexSt>& inner_index) : inner_index_(inner_index) {}
    std::unique_ptr<rocksdb::CompactionFilter> CreateCompactionFilter(
        const rocksdb::CompactionFilter::Context& context) override {
        return std::unique_ptr<rocksdb::CompactionFilter>(new AbsoluteTTLCompactionFilter(inner_index_));
    }
    const char* Name() const override { return "AbsoluteTTLFilterFactory"; }

 private:
    std::shared_ptr<InnerIndexSt> inner_index_;
};

class DiskTable : public Table {
 public:
    DiskTable(const std::string& name, uint32_t id, uint32_t pid, const std::map<std::string, uint32_t>& mapping,
              uint64_t ttl, ::openmldb::type::TTLType ttl_type, ::openmldb::common::StorageMode storage_mode,
              const std::string& table_path);

    DiskTable(const ::openmldb::api::TableMeta& table_meta, const std::string& table_path);
    DiskTable(const DiskTable&) = delete;
    DiskTable& operator=(const DiskTable&) = delete;

    virtual ~DiskTable();

    bool InitColumnFamilyDescriptor();

    bool Init() override;

    static void initOptionTemplate();

    bool Put(const std::string& pk, uint64_t time, const char* data, uint32_t size) override;

    absl::Status Put(uint64_t time, const std::string& value, const Dimensions& dimensions,
                     bool put_if_absent) override;

    bool Get(uint32_t idx, const std::string& pk, uint64_t ts,
             std::string& value);  // NOLINT

    bool Get(const std::string& pk, uint64_t ts, std::string& value);  // NOLINT

    bool Delete(const ::openmldb::api::LogEntry& entry) override;

    base::Status Truncate();

    bool Delete(uint32_t idx, const std::string& pk, const std::optional<uint64_t>& start_ts,
                const std::optional<uint64_t>& end_ts) override;

    uint64_t GetExpireTime(const TTLSt& ttl_st) override;

    uint64_t GetRecordCnt() override {
        uint64_t count = 0;
        if (cf_hs_.size() == 1) {
            db_->GetIntProperty(cf_hs_[0], "rocksdb.estimate-num-keys", &count);
        } else {
            db_->GetIntProperty(cf_hs_[1], "rocksdb.estimate-num-keys", &count);
        }
        return count;
    }

    uint64_t GetOffset() { return offset_.load(std::memory_order_relaxed); }

    void SetOffset(uint64_t offset) { offset_.store(offset, std::memory_order_relaxed); }

    TableIterator* NewIterator(const std::string& pk, Ticket& ticket) override;

    TableIterator* NewIterator(uint32_t idx, const std::string& pk, Ticket& ticket) override;

    TraverseIterator* NewTraverseIterator(uint32_t idx) override;

    ::hybridse::vm::WindowIterator* NewWindowIterator(uint32_t idx) override;

    void SchedGc() override;

    void GcHead();
    void GcTTLAndHead();
    void GcTTLOrHead();

    bool IsExpire(const ::openmldb::api::LogEntry& entry) override;

    void CompactDB() {
        for (rocksdb::ColumnFamilyHandle* cf : cf_hs_) {
            db_->CompactRange(rocksdb::CompactRangeOptions(), cf, nullptr, nullptr);
        }
    }

    int CreateCheckPoint(const std::string& checkpoint_dir);

    bool DeleteIndex(const std::string& idx_name) override;
    uint64_t GetRecordIdxCnt() override;
    bool GetRecordIdxCnt(uint32_t idx, uint64_t** stat, uint32_t* size) override;
    uint64_t GetRecordPkCnt() override;
    uint64_t GetRecordByteSize() const override { return 0; }
    uint64_t GetRecordIdxByteSize() override;

    int GetCount(uint32_t index, const std::string& pk, uint64_t& count) override;  // NOLINT

 private:
    base::Status Delete(uint32_t idx, const std::string& pk, uint64_t start_ts, const std::optional<uint64_t>& end_ts);

 private:
    rocksdb::DB* db_;
    rocksdb::WriteOptions write_opts_;
    std::vector<rocksdb::ColumnFamilyDescriptor> cf_ds_;
    std::vector<rocksdb::ColumnFamilyHandle*> cf_hs_;
    rocksdb::Options options_;
    KeyTSComparator cmp_;
    std::atomic<uint64_t> offset_;
    std::string table_path_;
};

}  // namespace storage
}  // namespace openmldb
