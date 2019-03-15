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
#include "base/slice.h"
#include "storage/iterator.h"
#include <boost/lexical_cast.hpp>

typedef google::protobuf::RepeatedPtrField<::rtidb::api::Dimension> Dimensions;

using rocksdb::DB;
using rocksdb::Options;
using rocksdb::ColumnFamilyDescriptor;
using rocksdb::ColumnFamilyHandle;

namespace rtidb {
namespace storage {

static int ParseKeyAndTs(const std::string& s, std::string& key, uint64_t& ts) {
    std::string::size_type index = s.find_last_of("|");
    if (index != std::string::npos) {
        key = s.substr(0, index);
        try {
            ts = boost::lexical_cast<uint64_t>(s.substr(index + 1));
            return 0;
        } catch (std::exception const& e) {
            return -1;
        }
    }
    return -1;
}

static inline std::string CombineKeyTs(const std::string& key, uint64_t ts) {
    return key + "|" + std::to_string(ts);
}

class KeyTSComparator : public rocksdb::Comparator {
public:
    KeyTSComparator() {}

    virtual const char* Name() const override { return "KeyTSComparator"; }

    virtual int Compare(const rocksdb::Slice& a, const rocksdb::Slice& b) const override {
        std::string key1, key2;
        uint64_t ts1 = 0, ts2 = 0;
        ParseKeyAndTs(a.ToString(), key1, ts1);
        ParseKeyAndTs(b.ToString(), key2, ts2);

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

    bool Init();

    bool ReadTableFromDisk();

    void SelfTune();

    static void initOptionTemplate();

    bool Put(const std::string& pk,
             uint64_t time,
             const char* data,
             uint32_t size);

    // Put a multi dimension record
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
        //use the size of the vector of column family handles, minus the default column family, no longer use idx_cnt_
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

private:
    DB* db_;
    std::vector<ColumnFamilyDescriptor> cf_ds_;
    std::vector<ColumnFamilyHandle*> cf_hs_;
    Options options_;

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
//  uint32_t const idx_cnt_;
//  std::atomic<uint64_t> record_cnt_;
//  std::atomic<int64_t> time_offset_;
//  std::vector<std::string> replicas_;
//  std::atomic<uint32_t> table_status_;
//  std::atomic<uint64_t> record_byte_size_;
//  ::rtidb::api::CompressType compress_type_;
};

}
}
