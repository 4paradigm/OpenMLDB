//
// table.h
// Copyright (C) 2017 4paradigm.com
// Author denglong
// Date 2019-07-24
//

#pragma once

#include <atomic>
#include <memory>
#include "proto/tablet.pb.h"
#include "storage/iterator.h"
#include "storage/ticket.h"

namespace rtidb {
namespace storage {

typedef google::protobuf::RepeatedPtrField<::rtidb::api::Dimension> Dimensions;
typedef google::protobuf::RepeatedPtrField<::rtidb::api::TSDimension> TSDimensions;

enum TableStat {
    kUndefined = 0,
    kNormal,
    kLoading,
    kMakingSnapshot,
    kSnapshotPaused
};

class Table {

public:
    Table() {}
    Table(::rtidb::common::StorageMode storage_mode, const std::string& name, uint32_t id, uint32_t pid, 
            uint64_t ttl, bool is_leader, uint64_t ttl_offset,
            const std::map<std::string, uint32_t>& mapping, 
            ::rtidb::common::TTLType ttl_type, ::rtidb::api::CompressType compress_type) :
        storage_mode_(storage_mode), name_(name), id_(id), pid_(pid), idx_cnt_(mapping.size()),
        ttl_offset_(ttl_offset), is_leader_(is_leader),
        mapping_(mapping), ttl_type_(ttl_type), compress_type_(compress_type) {
        ::rtidb::common::TTLDesc* ttl_desc = table_meta_.mutable_ttl_desc();
        ttl_desc->set_ttl_type(ttl_type);
        if (ttl_type == ::rtidb::common::TTLType::kAbsoluteTime) {
            // abs_ttl_.store(ttl);
            // lat_ttl_.store(0);
            // new_abs_ttl_.store(ttl);
            // new_lat_ttl_.store(0);
            ttl_desc->set_abs_ttl(ttl/(60*1000));
            ttl_desc->set_lat_ttl(0);
        } else {
            // abs_ttl_.store(0);
            // lat_ttl_.store(ttl/(60*1000));
            // new_abs_ttl_.store(0);
            // new_lat_ttl_.store(ttl/(60*1000));
            ttl_desc->set_abs_ttl(0);
            ttl_desc->set_lat_ttl(ttl/(60*1000));
        }
    }
    virtual ~Table() {}
    virtual bool Init() = 0;

    int InitColumnDesc();

    bool CheckTsValid(uint32_t index, int32_t ts_idx);

    virtual bool Put(const std::string& pk, uint64_t time, const char* data, uint32_t size) = 0;

    virtual bool Put(uint64_t time, const std::string& value, const Dimensions& dimensions) = 0;

    virtual bool Put(const Dimensions& dimensions, const TSDimensions& ts_dimemsions,
             const std::string& value) = 0;

    bool Put(const ::rtidb::api::LogEntry& entry) {
        if (entry.dimensions_size() > 0) {
            return entry.ts_dimensions_size() > 0 ?
                Put(entry.dimensions(), entry.ts_dimensions(), entry.value()) :
                Put(entry.ts(), entry.value(), entry.dimensions());
        } else {
            return Put(entry.pk(), entry.ts(), entry.value().c_str(), entry.value().size());
        }
    }

    virtual bool Delete(const std::string& pk, uint32_t idx) = 0;

    virtual TableIterator* NewIterator(const std::string& pk, Ticket& ticket) = 0;

    virtual TableIterator* NewIterator(uint32_t index, const std::string& pk, Ticket& ticket) = 0;

    virtual TableIterator* NewIterator(uint32_t index, int32_t ts_idx, const std::string& pk, Ticket& ticket) = 0;

    virtual TableIterator* NewTraverseIterator(uint32_t index) = 0;
    virtual TableIterator* NewTraverseIterator(uint32_t index, uint32_t ts_idx) = 0;

    virtual void SchedGc() = 0;

    virtual uint64_t GetRecordCnt() const = 0;

    virtual bool IsExpire(const ::rtidb::api::LogEntry& entry) = 0;

    virtual uint64_t GetExpireTime(uint64_t ttl) = 0;

    inline ::rtidb::common::StorageMode GetStorageMode() const {
        return storage_mode_;
    }

    inline std::string GetName() const {
        return name_;
    }

    inline uint32_t GetId() const {
        return id_;
    }

    inline uint32_t GetIdxCnt() const {
        return idx_cnt_;
    }

    inline uint32_t GetPid() const {
        return pid_;
    }

    inline bool IsLeader() const {
        return is_leader_;
    }

    void SetLeader(bool is_leader) {
        is_leader_ = is_leader;
    }
    
    inline uint32_t GetTableStat() {
        return table_status_.load(std::memory_order_relaxed);
    }

    inline void SetTableStat(uint32_t table_status) {
        table_status_.store(table_status, std::memory_order_relaxed);
    }

    inline void SetSchema(const std::string& schema) {
        schema_.assign(schema);
    }
    inline const std::string& GetSchema() {
        return schema_;
    }

    inline const ::rtidb::api::CompressType GetCompressType() {
        return compress_type_;
    }

    const ::rtidb::api::TableMeta& GetTableMeta() const {
        return table_meta_;
    }

    inline void SetTableMeta(::rtidb::api::TableMeta& table_meta) {
        table_meta_.CopyFrom(table_meta);
    }

    inline std::map<std::string, uint32_t>& GetMapping() {
        return mapping_;
    }

    inline std::map<std::string, uint8_t>& GetTSMapping() {
        return ts_mapping_;
    }

    inline std::map<uint32_t, std::vector<uint32_t>>& GetColumnMap() {
        return column_key_map_;
    }

    inline void SetTTLType(const ::rtidb::common::TTLType& type) {
        ttl_type_ = type;
    }

    inline ::rtidb::common::TTLType GetTTLType() {
        return ttl_type_;
    }

    TTLDesc GetTTL() {
        return GetTTL(0);
    }

    TTLDesc GetTTL(uint32_t index) {
        auto pos = column_key_map_.find(index);
        if (pos != column_key_map_.end() && !pos->second.empty()) {
            if (pos->second.front() < abs_ttl_vec_.size()) {
                return TTLDesc(abs_ttl_vec_[pos->second.front()]->load(std::memory_order_relaxed)/(60*1000),
                    lat_ttl_vec_[pos->second.front()]->load(std::memory_order_relaxed));
            }
        }
        return TTLDesc(abs_ttl_.load(std::memory_order_relaxed)/(60*1000),
            lat_ttl_.load(std::memory_order_relaxed));
    }

    TTLDesc GetTTL(uint32_t index, uint32_t ts_index) {
        if (ts_index < abs_ttl_vec_.size()) {
            return TTLDesc(abs_ttl_vec_[ts_index]->load(std::memory_order_relaxed)/(60*1000),
                    lat_ttl_vec_[ts_index]->load(std::memory_order_relaxed));
        }
        return TTLDesc(abs_ttl_.load(std::memory_order_relaxed)/(60*1000),
            lat_ttl_.load(std::memory_order_relaxed));
    }

    inline void SetTTL(const uint64_t& abs_ttl, const uint64_t lat_ttl) {
        new_abs_ttl_.store(abs_ttl * 60 * 1000, std::memory_order_relaxed);
        new_lat_ttl_.store(lat_ttl, std::memory_order_relaxed);
    }

    inline void SetTTL(const uint32_t ts_idx, const uint64_t& abs_ttl, const uint64_t& lat_ttl) {
        if (ts_idx < new_abs_ttl_vec_.size()) {
            new_abs_ttl_vec_[ts_idx]->store(abs_ttl * 60 * 1000, std::memory_order_relaxed);
            new_lat_ttl_vec_[ts_idx]->store(lat_ttl, std::memory_order_relaxed);
        }
    }

protected:
    void UpdateTTL();
    bool InitFromMeta();

    ::rtidb::common::StorageMode storage_mode_;
    std::string name_;
    uint32_t id_;
    uint32_t pid_;
    uint32_t idx_cnt_;
    std::atomic<uint64_t> abs_ttl_;
    std::atomic<uint64_t> new_abs_ttl_;
    std::atomic<uint64_t> lat_ttl_;
    std::atomic<uint64_t> new_lat_ttl_;
    uint64_t ttl_offset_;
    bool is_leader_;
    std::atomic<uint32_t> table_status_;
    std::string schema_;
    std::map<std::string, uint32_t> mapping_;
    std::map<std::string, uint8_t> ts_mapping_;
    std::map<uint32_t, std::vector<uint32_t>> column_key_map_;
    std::vector<std::shared_ptr<std::atomic<uint64_t>>> abs_ttl_vec_;
    std::vector<std::shared_ptr<std::atomic<uint64_t>>> new_abs_ttl_vec_;
    std::vector<std::shared_ptr<std::atomic<uint64_t>>> lat_ttl_vec_;
    std::vector<std::shared_ptr<std::atomic<uint64_t>>> new_lat_ttl_vec_;
    ::rtidb::common::TTLType ttl_type_;
    ::rtidb::api::CompressType compress_type_;
    ::rtidb::api::TableMeta table_meta_;
};

}
}
