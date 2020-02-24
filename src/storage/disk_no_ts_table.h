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
    AbsoluteTTLCompactionFilter(uint64_t ttl) : ttl_(ttl), ttl_vec_ptr_(NULL) {}
    AbsoluteTTLCompactionFilter(std::vector<std::shared_ptr<std::atomic<uint64_t>>>* ttl_vec_ptr) : 
        ttl_(0), ttl_vec_ptr_(ttl_vec_ptr) {}
    virtual ~AbsoluteTTLCompactionFilter() {}

    virtual const char* Name() const override { return "AbsoluteTTLCompactionFilter"; }

    virtual bool Filter(int /*level*/, const rocksdb::Slice& key,
                      const rocksdb::Slice& /*existing_value*/,
                      std::string* /*new_value*/,
                      bool* /*value_changed*/) const override {
        if (key.size() < TS_LEN) {
            return false;
        }
        uint64_t real_ttl = ttl_;
        if (ttl_vec_ptr_ != NULL) {
            if (key.size() < TS_LEN + TS_POS_LEN) {
                return false;
            }
            uint8_t ts_idx = *((uint8_t*)(key.data() + key.size() - TS_LEN - TS_POS_LEN));
            if (ts_idx >= ttl_vec_ptr_->size()) {
                return false;
            }
            real_ttl = (*ttl_vec_ptr_)[ts_idx]->load(std::memory_order_relaxed);
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
    uint64_t ttl_;
    std::vector<std::shared_ptr<std::atomic<uint64_t>>>* ttl_vec_ptr_;
};

class AbsoluteTTLFilterFactory : public rocksdb::CompactionFilterFactory {
public:
    AbsoluteTTLFilterFactory(std::atomic<uint64_t>* ttl_ptr) : ttl_ptr_(ttl_ptr), ttl_vec_ptr_(NULL) {};
    AbsoluteTTLFilterFactory(std::vector<std::shared_ptr<std::atomic<uint64_t>>>* ttl_vec_ptr) : 
        ttl_ptr_(NULL), ttl_vec_ptr_(ttl_vec_ptr) {};
    std::unique_ptr<rocksdb::CompactionFilter> CreateCompactionFilter(
        const rocksdb::CompactionFilter::Context& context) override {
        if (ttl_vec_ptr_ != NULL) {
            return std::unique_ptr<rocksdb::CompactionFilter>(new AbsoluteTTLCompactionFilter(ttl_vec_ptr_));
        } else {
            return std::unique_ptr<rocksdb::CompactionFilter>(
                    new AbsoluteTTLCompactionFilter(ttl_ptr_->load(std::memory_order_relaxed)));
        }
    }
    const char* Name() const override {
        return "AbsoluteTTLFilterFactory";
    }
private:
    std::atomic<uint64_t>* ttl_ptr_;
    std::vector<std::shared_ptr<std::atomic<uint64_t>>>* ttl_vec_ptr_;
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
                ::rtidb::api::TTLType ttl_type, const uint64_t& expire_time, const uint64_t& expire_cnt);
    DiskTableTraverseIterator(rocksdb::DB* db, rocksdb::Iterator* it, const rocksdb::Snapshot* snapshot,
                            ::rtidb::api::TTLType ttl_type, const uint64_t& expire_time, const uint64_t& expire_cnt, int32_t ts_idx);
    virtual ~DiskTableTraverseIterator();
    virtual bool Valid() override;
    virtual void Next() override;
    virtual rtidb::base::Slice GetValue() const override;
    virtual std::string GetPK() const override;
    virtual uint64_t GetKey() const override;
    virtual void SeekToFirst() override;
    virtual void Seek(const std::string& pk, uint64_t time) override;
    virtual uint64_t GetCount() const override;

private:
    void NextPK();
    bool IsExpired();

private:
    rocksdb::DB* db_;
    rocksdb::Iterator* it_;
    const rocksdb::Snapshot* snapshot_;
    ::rtidb::api::TTLType ttl_type_;
    uint32_t record_idx_;
    TTLDesc expire_value_;
    std::string pk_;
    uint64_t ts_;
    bool has_ts_idx_;
    uint8_t ts_idx_;
    uint64_t traverse_cnt_;

};

class DiskNoTsTable {

public:
    DiskNoTsTable(const std::string& name,
                uint32_t id,
                uint32_t pid,
                const std::map<std::string, uint32_t>& mapping,
                ::rtidb::common::StorageMode storage_mode,
                const std::string& db_root_path);

    DiskTable(const ::rtidb::api::TableMeta& table_meta,
              const std::string& db_root_path);
    DiskTable(const DiskTable&) = delete;
    DiskTable& operator=(const DiskTable&) = delete;

    virtual ~DiskTable();

    bool InitColumnFamilyDescriptor();

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

    bool Get(uint32_t idx, const std::string& pk, 
            uint64_t ts, uint32_t ts_idx, std::string& value);

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

    void SetOffset(uint64_t offset) {
        offset_.store(offset, std::memory_order_relaxed);
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
    void GcTTLAndHead();
    void GcTTLOrHead();

    virtual bool IsExpire(const ::rtidb::api::LogEntry& entry) override;

    void CompactDB() {
        for (rocksdb::ColumnFamilyHandle* cf : cf_hs_) {
            db_->CompactRange(rocksdb::CompactRangeOptions(), cf, nullptr, nullptr);
        }
    }
    
    int CreateCheckPoint(const std::string& checkpoint_dir);

private:
    ::rtidb::common::StorageMode storage_mode_;
    std::string name_;
    uint32_t id_;
    uint32_t pid_;
    uint32_t idx_cnt_;
    std::atomic<uint64_t> diskused_;
    bool is_leader_;
    std::atomic<uint32_t> table_status_;
    std::string schema_;
    std::map<std::string, uint32_t> mapping_;
    ::rtidb::api::CompressType compress_type_;
    ::rtidb::api::TableMeta table_meta_;
    int64_t last_make_snapshot_time_;

    rocksdb::DB* db_;
    rocksdb::WriteOptions write_opts_;
    std::vector<rocksdb::ColumnFamilyDescriptor> cf_ds_;
    std::vector<rocksdb::ColumnFamilyHandle*> cf_hs_;
    rocksdb::Options options_;
    rocksdb::Comparator cmp_;
    std::atomic<uint64_t> offset_;
    std::string db_root_path_;
};

}
}
