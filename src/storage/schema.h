//
// schema.h
// Copyright (C) 2017 4paradigm.com
// Author denglong
// Date 2020-03-11
//

#pragma once

#include <algorithm>
#include <atomic>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "proto/name_server.pb.h"
#include "proto/tablet.pb.h"
#include "proto/type.pb.h"

namespace rtidb {
namespace storage {

static constexpr uint32_t MAX_INDEX_NUM = 200;

struct TTLDesc {
    TTLDesc() = default;

    TTLDesc(uint64_t abs, uint64_t lat) : abs_ttl(abs), lat_ttl(lat) {}

    inline bool HasExpire(::rtidb::api::TTLType ttl_type) const {
        switch (ttl_type) {
            case ::rtidb::api::TTLType::kAbsoluteTime:
                return abs_ttl != 0;
            case ::rtidb::api::TTLType::kLatestTime:
                return lat_ttl != 0;
            case ::rtidb::api::TTLType::kAbsAndLat:
                return abs_ttl != 0 && lat_ttl != 0;
            case ::rtidb::api::TTLType::kAbsOrLat:
                return abs_ttl != 0 || lat_ttl != 0;
            default:
                return false;
        }
    }

    inline std::string ToString(::rtidb::api::TTLType ttl_type) const {
        switch (ttl_type) {
            case ::rtidb::api::TTLType::kAbsoluteTime:
                return std::to_string(abs_ttl) + "min";
            case ::rtidb::api::TTLType::kLatestTime:
                return std::to_string(lat_ttl);
            case ::rtidb::api::TTLType::kAbsAndLat:
                return std::to_string(abs_ttl) + "min&&" + std::to_string(lat_ttl);
            case ::rtidb::api::TTLType::kAbsOrLat:
                return std::to_string(abs_ttl) + "min||" + std::to_string(lat_ttl);
            default:
                return "";
        }
    }

    uint64_t abs_ttl;
    uint64_t lat_ttl;
};

enum class IndexStatus { kReady = 0, kWaiting, kDeleting, kDeleted };

class ColumnDef {
 public:
    ColumnDef(const std::string& name, uint32_t id, ::rtidb::type::DataType type, bool not_null);
    inline uint32_t GetId() const { return id_; }
    const std::string& GetName() const { return name_; }
    inline ::rtidb::type::DataType GetType() const { return type_; }
    bool NotNull() { return not_null_; }

 private:
    std::string name_;
    uint32_t id_;
    ::rtidb::type::DataType type_;
    bool not_null_;
};

class TableColumn {
 public:
    TableColumn() = default;
    std::shared_ptr<ColumnDef> GetColumn(uint32_t idx);
    std::shared_ptr<ColumnDef> GetColumn(const std::string& name);
    void AddColumn(std::shared_ptr<ColumnDef> column_def);
    const std::vector<std::shared_ptr<ColumnDef>>& GetAllColumn();
    const std::vector<uint32_t>& GetBlobIdxs();
    inline uint32_t Size() { return columns_.size(); }

 private:
    std::vector<std::shared_ptr<ColumnDef>> columns_;
    std::unordered_map<std::string, std::shared_ptr<ColumnDef>> column_map_;
    std::vector<uint32_t> blob_idxs_;
};

class IndexDef {
 public:
    IndexDef(const std::string& name, uint32_t id);
    IndexDef(const std::string& name, uint32_t id, IndexStatus stauts);
    IndexDef(const std::string& name, uint32_t id, const IndexStatus& stauts, ::rtidb::type::IndexType type,
             const std::vector<ColumnDef>& column_idx_map);
    const std::string& GetName() { return name_; }
    const std::vector<uint32_t>& GetTsColumn() { return ts_column_; }
    void SetTsColumn(const std::vector<uint32_t>& ts_vec) { ts_column_ = ts_vec; }
    inline bool IsReady() { return status_.load(std::memory_order_acquire) == IndexStatus::kReady; }
    inline uint32_t GetId() { return index_id_; }
    void SetStatus(IndexStatus status) { status_.store(status, std::memory_order_release); }
    IndexStatus GetStatus() { return status_.load(std::memory_order_acquire); }
    inline ::rtidb::type::IndexType GetType() { return type_; }
    inline const std::vector<ColumnDef>& GetColumns() { return columns_; }

 private:
    std::string name_;
    uint32_t index_id_;
    std::atomic<IndexStatus> status_;
    ::rtidb::type::IndexType type_;
    std::vector<ColumnDef> columns_;
    std::vector<uint32_t> ts_column_;
};

bool ColumnDefSortFunc(const ColumnDef& cd_a, const ColumnDef& cd_b);

class TableIndex {
 public:
    TableIndex();
    void ReSet();
    std::shared_ptr<IndexDef> GetIndex(uint32_t idx);
    std::shared_ptr<IndexDef> GetIndex(const std::string& name);
    int AddIndex(std::shared_ptr<IndexDef> index_def);
    std::vector<std::shared_ptr<IndexDef>> GetAllIndex();
    inline uint32_t Size() const { return std::atomic_load_explicit(&indexs_, std::memory_order_relaxed)->size(); }
    bool HasAutoGen();
    std::shared_ptr<IndexDef> GetPkIndex();
    const std::shared_ptr<IndexDef> GetIndexByCombineStr(const std::string& combine_str);
    bool IsColName(const std::string& name);
    bool IsUniqueColName(const std::string& name);

 private:
    std::shared_ptr<std::vector<std::shared_ptr<IndexDef>>> indexs_;
    std::shared_ptr<IndexDef> pk_index_;
    std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<IndexDef>>> combine_col_name_map_;
    std::shared_ptr<std::vector<std::string>> col_name_vec_;
    std::shared_ptr<std::vector<std::string>> unique_col_name_vec_;
};

class PartitionSt {
 public:
    PartitionSt() = default;
    explicit PartitionSt(const ::rtidb::nameserver::TablePartition& partitions);
    explicit PartitionSt(const ::rtidb::common::TablePartition& partitions);

    inline const std::string& GetLeader() const { return leader_; }
    inline const std::vector<std::string>& GetFollower() const { return follower_; }
    inline uint32_t GetPid() const { return pid_; }

    bool operator==(const PartitionSt& partition_st) const;

 private:
    uint32_t pid_;
    std::string leader_;
    std::vector<std::string> follower_;
};

class TableSt {
 public:
    TableSt() : name_(), db_(), tid_(0), pid_num_(0), partitions_() {}

    explicit TableSt(const ::rtidb::nameserver::TableInfo& table_info);

    explicit TableSt(const ::rtidb::api::TableMeta& meta);

    inline const std::string& GetName() const { return name_; }

    inline const std::string& GetDB() const { return db_; }

    inline uint32_t GetTid() const { return tid_; }

    std::shared_ptr<std::vector<PartitionSt>> GetPartitions() const {
        return std::atomic_load_explicit(&partitions_, std::memory_order_relaxed);
    }

    PartitionSt GetPartition(uint32_t pid) const {
        auto partitions = std::atomic_load_explicit(&partitions_, std::memory_order_relaxed);
        if (pid < partitions->size()) {
            return partitions->at(pid);
        }
        return PartitionSt();
    }

    bool SetPartition(const PartitionSt& partition_st);

    inline uint32_t GetPartitionNum() const { return pid_num_; }

    inline const ::google::protobuf::RepeatedPtrField<::rtidb::common::ColumnDesc>& GetColumns() const {
        return column_desc_;
    }

    inline const ::google::protobuf::RepeatedPtrField<::rtidb::common::ColumnKey>& GetColumnKey() const {
        return column_key_;
    }

 private:
    std::string name_;
    std::string db_;
    uint32_t tid_;
    uint32_t pid_num_;
    ::google::protobuf::RepeatedPtrField<::rtidb::common::ColumnDesc> column_desc_;
    ::google::protobuf::RepeatedPtrField<::rtidb::common::ColumnKey> column_key_;
    std::shared_ptr<std::vector<PartitionSt>> partitions_;
};

}  // namespace storage
}  // namespace rtidb
