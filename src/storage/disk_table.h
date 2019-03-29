//
// Created by yangjun on 12/14/18.
//

#pragma once

#include <vector>
#include <map>
#include <atomic>
#include "proto/tablet.pb.h"
#include <gflags/gflags.h>
#include <rocksdb/db.h>
#include <rocksdb/slice.h>
#include <rocksdb/options.h>
#include <rocksdb/status.h>
#include <rocksdb/table.h>
#include <rocksdb/slice_transform.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/compaction_filter.h>
#include "base/slice.h"
#include "base/endianconv.h"
#include "storage/iterator.h"
#include <boost/lexical_cast.hpp>
#include "timer.h"

typedef google::protobuf::RepeatedPtrField<::rtidb::api::Dimension> Dimensions;

namespace rtidb {
namespace storage {

const static uint32_t TS_LEN = sizeof(uint64_t);

static int ParseKeyAndTs(const rocksdb::Slice& s, std::string& key, uint64_t& ts) {
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
    memrev64ifbe(static_cast<void*>(&ts));
    char buf[TS_LEN];
    memcpy(buf, static_cast<void*>(&ts), TS_LEN);
    return key + std::string(buf, TS_LEN);
}

class KeyTSComparator : public rocksdb::Comparator {
public:
    KeyTSComparator() {}
    virtual const char* Name() const override { return "KeyTSComparator"; }

    virtual int Compare(const rocksdb::Slice& a, const rocksdb::Slice& b) const override {
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
    virtual void FindShortestSeparator(std::string* /*start*/, const rocksdb::Slice& /*limit*/) const override {}
    virtual void FindShortSuccessor(std::string* /*key*/) const override {}
};

class AbsoluteTTLCompactionFilter : public rocksdb::CompactionFilter {
public:
    AbsoluteTTLCompactionFilter(uint64_t ttl) : ttl_(ttl) {}
    virtual ~AbsoluteTTLCompactionFilter() {}

    virtual const char* Name() const override { return "AbsoluteTTLCompactionFilter"; }

    virtual bool Filter(int /*level*/, const rocksdb::Slice& key,
                      const rocksdb::Slice& /*existing_value*/,
                      std::string* /*new_value*/,
                      bool* /*value_changed*/) const override {
        if (key.size() < TS_LEN) {
            return false;
        }
        uint64_t ts = 0;
        memcpy(static_cast<void*>(&ts), key.data() + key.size() - TS_LEN, TS_LEN);
        memrev64ifbe(static_cast<void*>(&ts));
        uint64_t cur_time = ::baidu::common::timer::get_micros() / 1000;
        if (ts < cur_time - ttl_) {
            return true;
        }
        return false;
    }

private:
    uint64_t ttl_;
};

class DiskTableIterator : public TableIterator {
public:
    DiskTableIterator(rocksdb::Iterator* it, const std::string& pk);
    virtual ~DiskTableIterator();
    virtual bool Valid() override;
    virtual void Next() override;
    virtual rtidb::base::Slice GetValue() const override;
    virtual std::string GetPK() const override;
    virtual uint64_t GetKey() const override;
    virtual void SeekToFirst() override;
    virtual void Seek(uint64_t time) override;

private:
    rocksdb::Iterator* it_;
    std::string pk_;
    uint64_t ts_;
};

class DiskTableTraverseIterator : public TableIterator {
public:
    DiskTableTraverseIterator(rocksdb::Iterator* it, ::rtidb::api::TTLType ttl_type, uint64_t expire_value);
    virtual ~DiskTableTraverseIterator();
    virtual bool Valid() override;
    virtual void Next() override;
    virtual rtidb::base::Slice GetValue() const override;
    virtual std::string GetPK() const override;
    virtual uint64_t GetKey() const override;
    virtual void SeekToFirst() override;
    virtual void Seek(const std::string& pk, uint64_t time) override;

private:
    void NextPK();
    bool IsExpired();

private:
    rocksdb::Iterator* it_;
    ::rtidb::api::TTLType ttl_type_;
    uint64_t expire_value_;
    uint32_t record_idx_;
    std::string pk_;
    uint64_t ts_;
};

class DiskTable {

public:
    DiskTable(const std::string& name,
                uint32_t id,
                uint32_t pid,
                const std::map<std::string, uint32_t>& mapping,
                uint64_t ttl,
                ::rtidb::api::TTLType ttl_type,
                ::rtidb::api::StorageMode storage_mode);

    virtual ~DiskTable();

    bool InitColumnFamilyDescriptor();

    bool Init();

    bool LoadTable();

    static void initOptionTemplate();

    bool Put(const std::string& pk,
             uint64_t time,
             const char* data,
             uint32_t size);

    bool Put(uint64_t time,
             const std::string& value,
             const Dimensions& dimensions);

    bool Get(uint32_t idx, const std::string& pk, uint64_t ts, std::string& value);

    bool Get(const std::string& pk, uint64_t ts, std::string& value);

    bool Delete(const std::string& pk, uint32_t idx);

    inline void SetSchema(const std::string& schema) {
        schema_ = schema;
    }

    inline const std::string& GetSchema() {
        return schema_;
    }

    inline uint32_t GetIdxCnt() const {
        return idx_cnt_;
    }

    inline std::string GetName() const {
        return name_;
    }

    inline bool IsLeader() const {
        return is_leader_;
    }

    void SetLeader(bool is_leader) {
        is_leader_ = is_leader;
    }

    inline ::rtidb::api::TTLType& GetTTLType() {
        return ttl_type_;
    }

    uint64_t GetTTL() const {
        return ttl_.load(std::memory_order_relaxed) / (60 * 1000);
    }

    uint64_t GetExpireTime();

    inline uint64_t GetRecordCnt() const {
        uint64_t count = 0;
        if (cf_hs_.size() == 1)
            db_->GetIntProperty(cf_hs_[0], "rocksdb.estimate-num-keys", &count);
        else {
            db_->GetIntProperty(cf_hs_[1], "rocksdb.estimate-num-keys", &count);
        }
        return count;
    }

    uint64_t GetOffset() {
        return offset_.load(std::memory_order_relaxed);
    }

    inline std::map<std::string, uint32_t>& GetMapping() {
        return mapping_;
    }

    inline ::rtidb::api::StorageMode GetStorageMode() {
        return storage_mode_;
    }

    DiskTableIterator* NewIterator(const std::string& pk);

    DiskTableIterator* NewIterator(uint32_t idx, const std::string& pk);

    DiskTableTraverseIterator* NewTraverseIterator(uint32_t idx);

    void SchedGc();
    void GcHead();
    void GcTTL();

    void CompactDB() {
        for (rocksdb::ColumnFamilyHandle* cf : cf_hs_) {
            db_->Flush(rocksdb::FlushOptions(), cf);
            rocksdb::Slice begin = CombineKeyTs("test1", UINT64_MAX);
            rocksdb::Slice end = CombineKeyTs("test100", 0);
            //db_->CompactRange(rocksdb::CompactRangeOptions(), cf, nullptr, nullptr);
            db_->CompactRange(rocksdb::CompactRangeOptions(), cf, &begin, &end);
            db_->Flush(rocksdb::FlushOptions(), cf);
        }
    }

private:
    rocksdb::DB* db_;
    std::vector<rocksdb::ColumnFamilyDescriptor> cf_ds_;
    std::vector<rocksdb::ColumnFamilyHandle*> cf_hs_;
    rocksdb::Options options_;

    std::string const name_;
    uint32_t const id_;
    uint32_t const pid_;
    uint32_t const idx_cnt_;
    std::string schema_;
    std::map<std::string, uint32_t> mapping_;
    std::atomic<uint64_t> ttl_;
    ::rtidb::api::TTLType ttl_type_;
    ::rtidb::api::StorageMode storage_mode_;
    KeyTSComparator cmp_;
    bool is_leader_;
    std::atomic<uint64_t> offset_;
    rocksdb::CompactionFilter* compaction_filter_;
};

}
}
