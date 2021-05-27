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

#include "proto/tablet.pb.h"
#include "storage/iterator.h"
#include "storage/schema.h"
#include "storage/ticket.h"
#include "vm/catalog.h"

namespace fedb {
namespace storage {

typedef google::protobuf::RepeatedPtrField<::fedb::api::Dimension> Dimensions;
typedef google::protobuf::RepeatedPtrField<::fedb::api::TSDimension> TSDimensions;
using Schema = google::protobuf::RepeatedPtrField<fedb::common::ColumnDesc>;

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
    Table(const std::string& name,
          uint32_t id, uint32_t pid, uint64_t ttl, bool is_leader,
          uint64_t ttl_offset, const std::map<std::string, uint32_t>& mapping,
          ::fedb::type::TTLType ttl_type,
          ::fedb::type::CompressType compress_type);
    virtual ~Table() {}
    virtual bool Init() = 0;

    int InitColumnDesc();

    virtual bool Put(const std::string& pk, uint64_t time, const char* data,
                     uint32_t size) = 0;

    virtual bool Put(uint64_t time, const std::string& value,
                     const Dimensions& dimensions) = 0;

    virtual bool Put(const Dimensions& dimensions,
                     const TSDimensions& ts_dimemsions,
                     const std::string& value) = 0;

    bool Put(const ::fedb::api::LogEntry& entry) {
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

    virtual TableIterator* NewTraverseIterator(uint32_t index) = 0;

    virtual ::hybridse::vm::WindowIterator* NewWindowIterator(uint32_t index) = 0;

    virtual void SchedGc() = 0;

    virtual uint64_t GetRecordCnt() const = 0;

    virtual bool IsExpire(const ::fedb::api::LogEntry& entry) = 0;

    virtual uint64_t GetExpireTime(const TTLSt& ttl_st) = 0;

    inline std::string GetName() const { return name_; }
    inline std::string GetDB() {
        auto table_meta = GetTableMeta();
        if (table_meta->has_db()) {
            return table_meta->db();
        }
        return "";
    }

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

    inline const ::fedb::type::CompressType GetCompressType() {
        return compress_type_;
    }

    void AddVersionSchema(const ::fedb::api::TableMeta& table_meta);

    std::shared_ptr<::fedb::api::TableMeta> GetTableMeta() {
        return std::atomic_load_explicit(&table_meta_, std::memory_order_relaxed);
    }

    void SetTableMeta(::fedb::api::TableMeta& table_meta); // NOLINT

    std::shared_ptr<Schema> GetVersionSchema(int32_t ver) {
        auto versions = std::atomic_load_explicit(&version_schema_, std::memory_order_relaxed);
        auto it = versions->find(ver);
        if (it == versions->end()) {
            return nullptr;
        }
        return it->second;
    }

    std::map<int32_t, std::shared_ptr<Schema>> GetAllVersionSchema() {
        return *std::atomic_load_explicit(&version_schema_, std::memory_order_relaxed);
    }

    std::vector<std::shared_ptr<IndexDef>> GetAllIndex() {
        return table_index_.GetAllIndex();
    }

    std::shared_ptr<IndexDef> GetIndex(const std::string& name) {
        return table_index_.GetIndex(name);
    }

    std::shared_ptr<IndexDef> GetIndex(const std::string& name, uint32_t ts_idx) {
        return table_index_.GetIndex(name, ts_idx);
    }

    std::shared_ptr<IndexDef> GetIndex(uint32_t idx) {
        return table_index_.GetIndex(idx);
    }

    std::shared_ptr<IndexDef> GetIndex(uint32_t idx, uint32_t ts_idx) {
        return table_index_.GetIndex(idx, ts_idx);
    }

    std::shared_ptr<IndexDef> GetPkIndex() {
        return table_index_.GetPkIndex();
    }

    inline std::map<std::string, uint8_t>& GetTSMapping() {
        return ts_mapping_;
    }

    void SetTTL(const ::fedb::storage::UpdateTTLMeta& ttl_meta);

    inline void SetMakeSnapshotTime(int64_t time) {
        last_make_snapshot_time_ = time;
    }

    inline int64_t GetMakeSnapshotTime() { return last_make_snapshot_time_; }

    bool CheckFieldExist(const std::string& name);

 protected:
    void UpdateTTL();
    bool InitFromMeta();

    std::string name_;
    uint32_t id_;
    uint32_t pid_;
    std::atomic<uint64_t> diskused_;
    uint64_t ttl_offset_;
    bool is_leader_;
    std::atomic<uint32_t> table_status_;
    std::string schema_;
    std::map<std::string, uint8_t> ts_mapping_;
    TableIndex table_index_;
    ::fedb::type::CompressType compress_type_;
    std::shared_ptr<::fedb::api::TableMeta> table_meta_;
    int64_t last_make_snapshot_time_;
    std::shared_ptr<std::map<int32_t, std::shared_ptr<Schema>>> version_schema_;
    std::shared_ptr<std::vector<::fedb::storage::UpdateTTLMeta>> update_ttl_;
};

}  // namespace storage
}  // namespace fedb
