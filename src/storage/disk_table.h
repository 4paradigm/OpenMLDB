//
// Created by yangjun on 12/14/18.
//

#pragma once

#include <vector>
#include <map>
#include <atomic>
#include "proto/common.pb.h"
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
#include <rocksdb/utilities/checkpoint.h>
#include "base/slice.h"
#include "base/endianconv.h"
#include "storage/iterator.h"
#include "storage/table.h"
#include <boost/lexical_cast.hpp>
#include "timer.h"

typedef google::protobuf::RepeatedPtrField<::rtidb::api::Dimension> Dimensions;

namespace rtidb {
namespace storage {

const static uint32_t TS_LEN = sizeof(uint64_t);
const static uint32_t TS_POS_LEN = sizeof(uint8_t);

__attribute__((unused))
static int ParseKeyAndTs(bool has_ts_idx, const rocksdb::Slice& s, std::string& key, uint64_t& ts, uint8_t& ts_idx) {
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
        memcpy(static_cast<void*>(&ts_idx), s.data() + s.size() - len, TS_POS_LEN);
    }
    memcpy(static_cast<void*>(&ts), s.data() + s.size() - TS_LEN, TS_LEN);
    memrev64ifbe(static_cast<void*>(&ts));
    return 0;
}

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

static inline std::string CombineKeyTs(const std::string& key, uint64_t ts, uint8_t ts_pos) {
  memrev64ifbe(static_cast<void*>(&ts));
  char buf[key.size() + TS_LEN + TS_POS_LEN];
  memcpy(buf, key.c_str(), key.size());
  memcpy(buf + key.size(), static_cast<void*>(&ts_pos), TS_POS_LEN);
  memcpy(buf + key.size() + TS_POS_LEN, static_cast<void*>(&ts), TS_LEN);
  return std::string(buf, key.size() + TS_LEN + TS_POS_LEN);
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

class KeyTsPrefixTransform : public rocksdb::SliceTransform {
public:
    virtual const char* Name() const override { return "KeyTsPrefixTransform"; }
    virtual rocksdb::Slice Transform(const rocksdb::Slice& src) const override {
        assert(InDomain(src));
        return rocksdb::Slice(src.data(), src.size() - TS_LEN);
    }

    virtual bool InDomain(const rocksdb::Slice& src) const override { 
        return src.size() >= TS_LEN;
    }

    virtual bool InRange(const rocksdb::Slice& dst) const override {
        return dst.size() <= TS_LEN;
    }

    virtual bool FullLengthEnabled(size_t* len) const override {
        return false;
    }

    virtual bool SameResultWhenAppended(const rocksdb::Slice& prefix) const override {
        return InDomain(prefix);
    }
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

class AbsoluteTTLFilterFactory : public rocksdb::CompactionFilterFactory {
public:
    AbsoluteTTLFilterFactory(uint64_t ttl) : ttl_(ttl) {};
    std::unique_ptr<rocksdb::CompactionFilter> CreateCompactionFilter(
        const rocksdb::CompactionFilter::Context& context) override {
        return std::unique_ptr<rocksdb::CompactionFilter>(new AbsoluteTTLCompactionFilter(ttl_));
    }
    const char* Name() const override {
        return "AbsoluteTTLFilterFactory";
    }
private:
    uint64_t ttl_;
};

class DiskTableIterator : public TableIterator {
public:
    DiskTableIterator(rocksdb::DB* db, rocksdb::Iterator* it, const rocksdb::Snapshot* snapshot, const std::string& pk);
    DiskTableIterator(rocksdb::DB* db, rocksdb::Iterator* it, const rocksdb::Snapshot* snapshot, const std::string& pk, uint8_t ts_idx);
    virtual ~DiskTableIterator();
    virtual bool Valid() override;
    virtual void Next() override;
    virtual rtidb::base::Slice GetValue() const override;
    virtual std::string GetPK() const override;
    virtual uint64_t GetKey() const override;
    virtual void SeekToFirst() override;
    virtual void Seek(uint64_t time) override;

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
    DiskTableTraverseIterator(rocksdb::DB* db, rocksdb::Iterator* it, const rocksdb::Snapshot* snapshot, 
                ::rtidb::api::TTLType ttl_type, uint64_t expire_value);
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
    rocksdb::DB* db_;
    rocksdb::Iterator* it_;
    const rocksdb::Snapshot* snapshot_;
    ::rtidb::api::TTLType ttl_type_;
    uint32_t record_idx_;
    uint64_t expire_value_;
    std::string pk_;
    uint64_t ts_;
};

class DiskTable : public Table {

public:
    DiskTable(const std::string& name,
                uint32_t id,
                uint32_t pid,
                const std::map<std::string, uint32_t>& mapping,
                uint64_t ttl,
                ::rtidb::api::TTLType ttl_type,
                ::rtidb::common::StorageMode storage_mode);

    DiskTable(const ::rtidb::api::TableMeta& table_meta);
    DiskTable(const DiskTable&) = delete;
    DiskTable& operator=(const DiskTable&) = delete;

    virtual ~DiskTable();

    bool InitColumnFamilyDescriptor();

    bool InitTableProperty();

    virtual bool Init() override;

    bool LoadTable();

    static void initOptionTemplate();

    virtual bool Put(const std::string& pk,
             uint64_t time,
             const char* data,
             uint32_t size) override;

    virtual bool Put(uint64_t time,
             const std::string& value,
             const Dimensions& dimensions) override;

    virtual bool Put(const Dimensions& dimensions, const TSDimensions& ts_dimemsions, 
            const std::string& value) override;

    virtual bool Put(const ::rtidb::api::LogEntry& entry) override;

    bool Get(uint32_t idx, const std::string& pk, uint64_t ts, std::string& value);

    bool Get(const std::string& pk, uint64_t ts, std::string& value);

    bool Delete(const std::string& pk, uint32_t idx) override;

    virtual uint64_t GetExpireTime(uint64_t ttl) override;

    uint64_t GetExpireTime();

    virtual uint64_t GetRecordCnt() const override {
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

    virtual TableIterator* NewIterator(const std::string& pk, Ticket& ticket) override;

    virtual TableIterator* NewIterator(uint32_t idx, const std::string& pk, Ticket& ticket) override;

    virtual TableIterator* NewIterator(uint32_t index, int32_t ts_idx, const std::string& pk, Ticket& ticket) override;

    virtual TableIterator* NewTraverseIterator(uint32_t idx) override;

    virtual TableIterator* NewTraverseIterator(uint32_t index, uint32_t ts_idx) override;

    virtual void SchedGc() override;
    void GcHead();
    void GcTTL();

    virtual bool IsExpire(const ::rtidb::api::LogEntry& entry) override;

    void CompactDB() {
        for (rocksdb::ColumnFamilyHandle* cf : cf_hs_) {
            db_->CompactRange(rocksdb::CompactRangeOptions(), cf, nullptr, nullptr);
        }
    }
    
    int CreateCheckPoint(const std::string& checkpoint_dir);

private:
    rocksdb::DB* db_;
    rocksdb::WriteOptions write_opts_;
    std::vector<rocksdb::ColumnFamilyDescriptor> cf_ds_;
    std::vector<rocksdb::ColumnFamilyHandle*> cf_hs_;
    rocksdb::Options options_;
    KeyTSComparator cmp_;
    std::atomic<uint64_t> offset_;
};

}
}
