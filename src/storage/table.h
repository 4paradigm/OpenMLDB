//
// table.h
// Copyright (C) 2017 4paradigm.com
// Author denglong
// Date 2019-07-24
//

#pragma once

#include <atomic>
#include <map>
#include <memory>
#include <string>
#include <vector>
#include <utility>

#include "proto/tablet.pb.h"
#include "storage/iterator.h"
#include "storage/schema.h"
#include "storage/ticket.h"
#include "vm/catalog.h"

namespace rtidb {
namespace storage {

typedef google::protobuf::RepeatedPtrField<::rtidb::api::Dimension> Dimensions;
typedef google::protobuf::RepeatedPtrField<::rtidb::api::TSDimension> TSDimensions;
typedef google::protobuf::RepeatedPtrField<rtidb::common::ColumnDesc> Schemas;

enum TableStat {
    kUndefined = 0,
    kNormal,
    kLoading,
    kMakingSnapshot,
    kSnapshotPaused
};

class Table {
 public:
    Table();
    Table(::rtidb::common::StorageMode storage_mode, const std::string& name,
          uint32_t id, uint32_t pid, uint64_t ttl, bool is_leader,
          uint64_t ttl_offset, const std::map<std::string, uint32_t>& mapping,
          ::rtidb::api::TTLType ttl_type,
          ::rtidb::api::CompressType compress_type);
    virtual ~Table() {}
    virtual bool Init() = 0;

    int InitColumnDesc();

    bool CheckTsValid(uint32_t index, int32_t ts_idx);

    virtual bool Put(const std::string& pk, uint64_t time, const char* data,
                     uint32_t size) = 0;

    virtual bool Put(uint64_t time, const std::string& value,
                     const Dimensions& dimensions) = 0;

    virtual bool Put(const Dimensions& dimensions,
                     const TSDimensions& ts_dimemsions,
                     const std::string& value) = 0;

    bool Put(const ::rtidb::api::LogEntry& entry) {
        if (entry.dimensions_size() > 0) {
            return entry.ts_dimensions_size() > 0
                       ? Put(entry.dimensions(), entry.ts_dimensions(),
                             entry.value())
                       : Put(entry.ts(), entry.value(), entry.dimensions());
        } else {
            return Put(entry.pk(), entry.ts(), entry.value().c_str(),
                       entry.value().size());
        }
    }

    virtual bool Delete(const std::string& pk, uint32_t idx) = 0;

    virtual TableIterator* NewIterator(const std::string& pk,
                                       Ticket& ticket) = 0;  // NOLINT

    virtual TableIterator* NewIterator(uint32_t index, const std::string& pk,
                                       Ticket& ticket) = 0;  // NOLINT

    virtual TableIterator* NewIterator(uint32_t index, int32_t ts_idx,
                                       const std::string& pk,
                                       Ticket& ticket) = 0;  // NOLINT

    virtual TableIterator* NewTraverseIterator(uint32_t index) = 0;
    virtual TableIterator* NewTraverseIterator(uint32_t index,
                                               uint32_t ts_idx) = 0;

    virtual ::fesql::vm::WindowIterator* NewWindowIterator(uint32_t index) = 0;
    virtual ::fesql::vm::WindowIterator* NewWindowIterator(uint32_t index,
                                                           uint32_t ts_idx) = 0;

    virtual void SchedGc() = 0;

    virtual uint64_t GetRecordCnt() const = 0;

    virtual bool IsExpire(const ::rtidb::api::LogEntry& entry) = 0;

    virtual uint64_t GetExpireTime(uint64_t ttl) = 0;

    inline ::rtidb::common::StorageMode GetStorageMode() const {
        return storage_mode_;
    }

    inline std::string GetName() const { return name_; }

    inline uint32_t GetId() const { return id_; }

    inline uint32_t GetIdxCnt() const { return table_index_.Size(); }

    inline uint32_t GetPid() const { return pid_; }

    inline bool IsLeader() const { return is_leader_; }

    void SetLeader(bool is_leader) { is_leader_ = is_leader; }

    inline uint32_t GetTableStat() {
        return table_status_.load(std::memory_order_relaxed);
    }

    inline void SetTableStat(uint32_t table_status) {
        table_status_.store(table_status, std::memory_order_relaxed);
    }

    inline uint64_t GetDiskused() {
        return diskused_.load(std::memory_order_relaxed);
    }

    inline void SetDiskused(uint64_t size) {
        diskused_.store(size, std::memory_order_relaxed);
    }

    inline void SetSchema(const std::string& schema) { schema_.assign(schema); }

    inline const std::string& GetSchema() { return schema_; }

    inline const ::rtidb::api::CompressType GetCompressType() {
        return compress_type_;
    }

    inline void AddVersionSchema() {
        Schemas base_schema;
        for (const auto& col : table_meta_.column_desc()) {
            rtidb::common::ColumnDesc* new_col = base_schema.Add();
            new_col->CopyFrom(col);
        }
        for (const auto ver : table_meta_.schema_versions()) {
            int remain_size = ver.field_count() - table_meta_.column_desc_size();
            if (remain_size < 0)  {
                LOG(INFO) << "do not need add ver " << ver.id() << " because remain size less than 0";
                continue;
            }
            if (remain_size > table_meta_.added_column_desc_size()) {
                LOG(INFO) << "skip add ver " << ver.id() << " because remain size great than added column deisc size";
                continue;
            }
            std::shared_ptr<Schemas> new_schema = std::make_shared<Schemas>();
            new_schema->CopyFrom(table_meta_.column_desc());
            for (int i = 0; i < remain_size; i++) {
                rtidb::common::ColumnDesc* col = new_schema->Add();
                col->CopyFrom(table_meta_.added_column_desc(i));
            }
            version_schema_.insert(std::make_pair(ver.id(), new_schema));
        }
    }

    const ::rtidb::api::TableMeta& GetTableMeta() const { return table_meta_; }

    inline void SetTableMeta(::rtidb::api::TableMeta& table_meta) {  // NOLINT
        table_meta_.CopyFrom(table_meta);
        std::lock_guard<std::mutex> lock(mu_);
        version_schema_.clear();
        AddVersionSchema();
    }

    std::shared_ptr<Schemas> GetVersionSchema(int32_t ver) {
        std::lock_guard<std::mutex> lock(mu_);
        auto it = version_schema_.find(ver);
        if (it == version_schema_.end()) {
            return nullptr;
        }
        return it->second;
    }

    std::vector<std::shared_ptr<IndexDef>> GetAllIndex() {
        return table_index_.GetAllIndex();
    }

    std::shared_ptr<IndexDef> GetIndex(const std::string& name) {
        return table_index_.GetIndex(name);
    }

    std::shared_ptr<IndexDef> GetIndex(uint32_t idx) {
        return table_index_.GetIndex(idx);
    }

    inline std::map<std::string, uint8_t>& GetTSMapping() {
        return ts_mapping_;
    }

    inline void SetTTLType(const ::rtidb::api::TTLType& type) {
        ttl_type_ = type;
    }

    inline ::rtidb::api::TTLType GetTTLType() { return ttl_type_; }

    TTLDesc GetTTL() { return GetTTL(0); }

    TTLDesc GetTTL(uint32_t index) {
        auto index_def = GetIndex(index);
        if (index_def && index_def->IsReady()) {
            auto ts_vec = index_def->GetTsColumn();
            if (!ts_vec.empty() && ts_vec.front() < abs_ttl_vec_.size()) {
                return TTLDesc(abs_ttl_vec_[ts_vec.front()]->load(
                                   std::memory_order_relaxed) /
                                   (60 * 1000),
                               lat_ttl_vec_[ts_vec.front()]->load(
                                   std::memory_order_relaxed));
            }
        }
        return TTLDesc(abs_ttl_.load(std::memory_order_relaxed) / (60 * 1000),
                       lat_ttl_.load(std::memory_order_relaxed));
    }

    TTLDesc GetTTL(uint32_t index, uint32_t ts_index) {
        if (ts_index < abs_ttl_vec_.size()) {
            return TTLDesc(
                abs_ttl_vec_[ts_index]->load(std::memory_order_relaxed) /
                    (60 * 1000),
                lat_ttl_vec_[ts_index]->load(std::memory_order_relaxed));
        }
        return TTLDesc(abs_ttl_.load(std::memory_order_relaxed) / (60 * 1000),
                       lat_ttl_.load(std::memory_order_relaxed));
    }

    inline void SetTTL(const uint64_t abs_ttl, const uint64_t lat_ttl) {
        new_abs_ttl_.store(abs_ttl * 60 * 1000, std::memory_order_relaxed);
        new_lat_ttl_.store(lat_ttl, std::memory_order_relaxed);
    }

    inline void SetTTL(const uint32_t ts_idx, const uint64_t abs_ttl,
                       const uint64_t lat_ttl) {
        if (ts_idx < new_abs_ttl_vec_.size()) {
            new_abs_ttl_vec_[ts_idx]->store(abs_ttl * 60 * 1000,
                                            std::memory_order_relaxed);
            new_lat_ttl_vec_[ts_idx]->store(lat_ttl, std::memory_order_relaxed);
        }
    }

    inline void SetMakeSnapshotTime(int64_t time) {
        last_make_snapshot_time_ = time;
    }

    inline int64_t GetMakeSnapshotTime() { return last_make_snapshot_time_; }

 protected:
    void UpdateTTL();
    bool InitFromMeta();

    ::rtidb::common::StorageMode storage_mode_;
    std::string name_;
    uint32_t id_;
    uint32_t pid_;
    std::atomic<uint64_t> diskused_;
    std::atomic<uint64_t> abs_ttl_;
    std::atomic<uint64_t> new_abs_ttl_;
    std::atomic<uint64_t> lat_ttl_;
    std::atomic<uint64_t> new_lat_ttl_;
    uint64_t ttl_offset_;
    bool is_leader_;
    std::atomic<uint32_t> table_status_;
    std::string schema_;
    std::map<std::string, uint8_t> ts_mapping_;
    TableIndex table_index_;
    std::vector<std::shared_ptr<std::atomic<uint64_t>>> abs_ttl_vec_;
    std::vector<std::shared_ptr<std::atomic<uint64_t>>> new_abs_ttl_vec_;
    std::vector<std::shared_ptr<std::atomic<uint64_t>>> lat_ttl_vec_;
    std::vector<std::shared_ptr<std::atomic<uint64_t>>> new_lat_ttl_vec_;
    ::rtidb::api::TTLType ttl_type_;
    ::rtidb::api::CompressType compress_type_;
    ::rtidb::api::TableMeta table_meta_;
    int64_t last_make_snapshot_time_;
    std::map<int32_t, std::shared_ptr<Schemas>> version_schema_;
    std::mutex mu_;
};

}  // namespace storage
}  // namespace rtidb
