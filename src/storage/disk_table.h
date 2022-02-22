//
// Copyright (C) 2017 4paradigm.com
// Created by yangjun on 12/14/18.
//

#pragma once

#include <atomic>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "gflags/gflags.h"
#include "rocksdb/compaction_filter.h"
#include "rocksdb/db.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/status.h"
#include "rocksdb/table.h"
#include "rocksdb/utilities/checkpoint.h"
#include "boost/lexical_cast.hpp"
#include "base/endianconv.h"
#include "base/slice.h"
#include "proto/common.pb.h"
#include "proto/tablet.pb.h"
#include "storage/iterator.h"
#include "storage/table.h"
#include "common/timer.h"  // NOLINT

typedef google::protobuf::RepeatedPtrField<::openmldb::api::Dimension> Dimensions;

namespace openmldb {
namespace storage {

static const uint32_t TS_LEN = sizeof(uint64_t);
static const uint32_t TS_POS_LEN = sizeof(uint8_t);

__attribute__((unused)) static int ParseKeyAndTs(bool has_ts_idx,
                                                 const rocksdb::Slice& s,
                                                 std::string& key,   // NOLINT
                                                 uint64_t& ts,       // NOLINT
                                                 uint8_t& ts_idx) {  // NOLINT
    auto len = TS_LEN;
    if (has_ts_idx) {
        len += TS_POS_LEN;
    }
    key.clear();
    if (s.size() < len) {
        return -1;
    } else if (s.size() > len) {
        key.assign(s.data(), s.size() - len);
    }
    if (has_ts_idx) {
        memcpy(static_cast<void*>(&ts_idx), s.data() + s.size() - len,
               TS_POS_LEN);
    }
    memcpy(static_cast<void*>(&ts), s.data() + s.size() - TS_LEN, TS_LEN);
    memrev64ifbe(static_cast<void*>(&ts));
    return 0;
}

static int ParseKeyAndTs(const rocksdb::Slice& s, std::string& key,  // NOLINT
                         uint64_t& ts) {                             // NOLINT
    key.clear();
    if (s.size() < TS_LEN) {
        return -1;
    } else if (s.size() > TS_LEN) {
        key.assign(s.data(), s.size() - TS_LEN);
    }
    memcpy(static_cast<void*>(&ts), s.data() + s.size() - TS_LEN, TS_LEN);
    memrev64ifbe(static_cast<void*>(&ts));
    return 0;
}

static inline std::string CombineKeyTs(const std::string& key, uint64_t ts) {
    std::string result;
    result.resize(key.size() + TS_LEN);
    char* buf = reinterpret_cast<char*>(&(result[0]));
    memrev64ifbe(static_cast<void*>(&ts));
    memcpy(buf, key.c_str(), key.size());
    memcpy(buf + key.size(), static_cast<void*>(&ts), TS_LEN);
    return result;
}

static inline std::string CombineKeyTs(const std::string& key, uint64_t ts,
                                       uint8_t ts_pos) {
    std::string result;
    result.resize(key.size() + TS_LEN + TS_POS_LEN);
    char* buf = reinterpret_cast<char*>(&(result[0]));
    memrev64ifbe(static_cast<void*>(&ts));
    memcpy(buf, key.c_str(), key.size());
    memcpy(buf + key.size(), static_cast<void*>(&ts_pos), TS_POS_LEN);
    memcpy(buf + key.size() + TS_POS_LEN, static_cast<void*>(&ts), TS_LEN);
    return result;
}

class KeyTSComparator : public rocksdb::Comparator {
 public:
    KeyTSComparator() {}
    const char* Name() const override { return "KeyTSComparator"; }

    int Compare(const rocksdb::Slice& a,
                const rocksdb::Slice& b) const override {
        std::string key1, key2;
        uint64_t ts1 = 0, ts2 = 0;
        ParseKeyAndTs(a, key1, ts1);
        ParseKeyAndTs(b, key2, ts2);

        int ret = key1.compare(key2);
        if (ret != 0) {
            return ret;
        } else {
            if (ts1 > ts2) return -1;
            if (ts1 < ts2) return 1;
            return 0;
        }
    }
    void FindShortestSeparator(std::string* /*start*/,
                               const rocksdb::Slice& /*limit*/) const override {
    }
    void FindShortSuccessor(std::string* /*key*/) const override {}
};

class KeyTsPrefixTransform : public rocksdb::SliceTransform {
 public:
    const char* Name() const override { return "KeyTsPrefixTransform"; }
    rocksdb::Slice Transform(const rocksdb::Slice& src) const override {
        assert(InDomain(src));
        return rocksdb::Slice(src.data(), src.size() - TS_LEN);
    }

    bool InDomain(const rocksdb::Slice& src) const override {
        return src.size() >= TS_LEN;
    }

    bool InRange(const rocksdb::Slice& dst) const override {
        return dst.size() <= TS_LEN;
    }

    bool FullLengthEnabled(size_t* len) const override { return false; }

    bool SameResultWhenAppended(const rocksdb::Slice& prefix) const override {
        return InDomain(prefix);
    }
};

class AbsoluteTTLCompactionFilter : public rocksdb::CompactionFilter {
 public:
    explicit AbsoluteTTLCompactionFilter(std::shared_ptr<InnerIndexSt> inner_index) : inner_index_(inner_index) {}
    virtual ~AbsoluteTTLCompactionFilter() {}

    const char* Name() const override { return "AbsoluteTTLCompactionFilter"; }

    bool Filter(int /*level*/, const rocksdb::Slice& key,
                const rocksdb::Slice& /*existing_value*/,
                std::string* /*new_value*/,
                bool* /*value_changed*/) const override {
        if (key.size() < TS_LEN) {
            return false;
        }
        uint64_t real_ttl = 0;
        const auto& indexs = inner_index_->GetIndex();
        if (indexs.size() > 1) {
            if (key.size() < TS_LEN + TS_POS_LEN) {
                return false;
            }
            uint8_t ts_idx =
                *((uint8_t*)(key.data() + key.size() - TS_LEN -  // NOLINT
                             TS_POS_LEN));
            bool has_found = false;
            for (const auto index : indexs) {
                auto ts_col = index->GetTsColumn();
                if (!ts_col) {
                    return false;
                }
                if (ts_col->GetId() == static_cast<int>(ts_idx)) {
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
        memcpy(static_cast<void*>(&ts), key.data() + key.size() - TS_LEN,
               TS_LEN);
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

class DiskTableIterator : public TableIterator {
 public:
    DiskTableIterator(rocksdb::DB* db, rocksdb::Iterator* it,
                      const rocksdb::Snapshot* snapshot, const std::string& pk);
    DiskTableIterator(rocksdb::DB* db, rocksdb::Iterator* it,
                      const rocksdb::Snapshot* snapshot, const std::string& pk,
                      uint8_t ts_idx);
    virtual ~DiskTableIterator();
    bool Valid() override;
    void Next() override;
    openmldb::base::Slice GetValue() const override;
    std::string GetPK() const override;
    uint64_t GetKey() const override;
    void SeekToFirst() override;
    void Seek(uint64_t time) override;

 private:
    rocksdb::DB* db_;
    rocksdb::Iterator* it_;
    const rocksdb::Snapshot* snapshot_;
    std::string pk_;
    uint64_t ts_;
    uint8_t ts_idx_;
    bool has_ts_idx_ = false;
};

class DiskTableTraverseIterator : public TableIterator {
 public:
    DiskTableTraverseIterator(rocksdb::DB* db, rocksdb::Iterator* it,
                              const rocksdb::Snapshot* snapshot,
                              ::openmldb::storage::TTLType ttl_type,
                              const uint64_t& expire_time,
                              const uint64_t& expire_cnt);
    DiskTableTraverseIterator(rocksdb::DB* db, rocksdb::Iterator* it,
                              const rocksdb::Snapshot* snapshot,
                              ::openmldb::storage::TTLType ttl_type,
                              const uint64_t& expire_time,
                              const uint64_t& expire_cnt, int32_t ts_idx);
    virtual ~DiskTableTraverseIterator();
    bool Valid() override;
    void Next() override;
    openmldb::base::Slice GetValue() const override;
    std::string GetPK() const override;
    uint64_t GetKey() const override;
    void SeekToFirst() override;
    void Seek(const std::string& pk, uint64_t time) override;
    uint64_t GetCount() const override;

 private:
    void NextPK();
    bool IsExpired();

 private:
    rocksdb::DB* db_;
    rocksdb::Iterator* it_;
    const rocksdb::Snapshot* snapshot_;
    uint32_t record_idx_;
    ::openmldb::storage::TTLSt expire_value_;
    std::string pk_;
    uint64_t ts_;
    bool has_ts_idx_;
    uint8_t ts_idx_;
    uint64_t traverse_cnt_;
};

class DiskTable : public Table {
 public:
    DiskTable(const std::string& name, uint32_t id, uint32_t pid,
              const std::map<std::string, uint32_t>& mapping, uint64_t ttl,
              ::openmldb::type::TTLType ttl_type,
              ::openmldb::common::StorageMode storage_mode,
              const std::string& db_root_path);

    DiskTable(const ::openmldb::api::TableMeta& table_meta,
              const std::string& db_root_path);
    DiskTable(const DiskTable&) = delete;
    DiskTable& operator=(const DiskTable&) = delete;

    virtual ~DiskTable();

    bool InitColumnFamilyDescriptor();

    bool Init() override;

    bool LoadTable();

    static void initOptionTemplate();

    bool Put(const std::string& pk, uint64_t time, const char* data,
             uint32_t size) override;

    bool Put(uint64_t time, const std::string& value,
             const Dimensions& dimensions) override;

    // bool Put(const Dimensions& dimensions, const TSDimensions& ts_dimemsions,
    //          const std::string& value);

    // bool Get(uint32_t idx, const std::string& pk, uint64_t ts, uint32_t ts_idx,
    //          std::string& value);  // NOLINT

    bool Get(uint32_t idx, const std::string& pk, uint64_t ts,
             std::string& value);  // NOLINT

    bool Get(const std::string& pk, uint64_t ts, std::string& value);  // NOLINT

    bool Delete(const std::string& pk, uint32_t idx) override;

    uint64_t GetExpireTime(const TTLSt& ttl_st) override;

    uint64_t GetRecordCnt() const override {
        uint64_t count = 0;
        if (cf_hs_.size() == 1) {
            db_->GetIntProperty(cf_hs_[0], "rocksdb.estimate-num-keys", &count);
        } else {
            db_->GetIntProperty(cf_hs_[1], "rocksdb.estimate-num-keys", &count);
        }
        return count;
    }

    uint64_t GetOffset() { return offset_.load(std::memory_order_relaxed); }

    void SetOffset(uint64_t offset) {
        offset_.store(offset, std::memory_order_relaxed);
    }

    TableIterator* NewIterator(const std::string& pk, Ticket& ticket) override;

    TableIterator* NewIterator(uint32_t idx, const std::string& pk,
                               Ticket& ticket) override;

    // TableIterator* NewIterator(uint32_t idx, int32_t ts_idx,
    //                            const std::string& pk, Ticket& ticket);

    TableIterator* NewTraverseIterator(uint32_t idx) override;

    // TableIterator* NewTraverseIterator(uint32_t idx, uint32_t ts_idx);


    ::hybridse::vm::WindowIterator* NewWindowIterator(uint32_t idx) {
        return NULL;
    }

    ::hybridse::vm::WindowIterator* NewWindowIterator(uint32_t idx,
                                                   uint32_t ts_idx) {
        return NULL;
    }

    void SchedGc() override;

    void GcHead();
    void GcTTLAndHead();
    void GcTTLOrHead();

    bool IsExpire(const ::openmldb::api::LogEntry& entry) override;

    void CompactDB() {
        for (rocksdb::ColumnFamilyHandle* cf : cf_hs_) {
            db_->CompactRange(rocksdb::CompactRangeOptions(), cf, nullptr,
                              nullptr);
        }
    }

    int CreateCheckPoint(const std::string& checkpoint_dir);
    
    bool DeleteIndex(const std::string& idx_name) override;
    uint64_t GetRecordIdxCnt() override;
    bool GetRecordIdxCnt(uint32_t idx, uint64_t** stat, uint32_t* size) override;
    uint64_t GetRecordPkCnt() override;
    inline uint64_t GetRecordByteSize() const override { 
        // TODO
        return 0; 
    }
    uint64_t GetRecordIdxByteSize() override;
    uint64_t Release() override;

 private:
    rocksdb::DB* db_;
    rocksdb::WriteOptions write_opts_;
    std::vector<rocksdb::ColumnFamilyDescriptor> cf_ds_;
    std::vector<rocksdb::ColumnFamilyHandle*> cf_hs_;
    rocksdb::Options options_;
    KeyTSComparator cmp_;
    std::atomic<uint64_t> offset_;
    std::string db_root_path_;
    ::openmldb::storage::TTLType ttl_type_;
};

}  // namespace storage
}  // namespace openmldb
