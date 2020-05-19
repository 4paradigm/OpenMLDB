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
                return std::to_string(abs_ttl) + "min&&" +
                       std::to_string(lat_ttl);
            case ::rtidb::api::TTLType::kAbsOrLat:
                return std::to_string(abs_ttl) + "min||" +
                       std::to_string(lat_ttl);
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
    ColumnDef(const std::string& name, uint32_t id,
              ::rtidb::type::DataType type);
    inline uint32_t GetId() const { return id_; }
    const std::string& GetName() const { return name_; }
    inline ::rtidb::type::DataType GetType() const { return type_; }

 private:
    std::string name_;
    uint32_t id_;
    ::rtidb::type::DataType type_;
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
    IndexDef(const std::string& name, uint32_t id, const IndexStatus& stauts,
             ::rtidb::type::IndexType type,
             const std::vector<ColumnDef>& column_idx_map);
    const std::string& GetName() { return name_; }
    const std::vector<uint32_t>& GetTsColumn() { return ts_column_; }
    void SetTsColumn(const std::vector<uint32_t>& ts_vec) {
        ts_column_ = ts_vec;
    }
    inline bool IsReady() {
        return status_.load(std::memory_order_acquire) == IndexStatus::kReady;
    }
    inline uint32_t GetId() { return index_id_; }
    void SetStatus(IndexStatus status) {
        status_.store(status, std::memory_order_release);
    }
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
    inline uint32_t Size() const {
        return std::atomic_load_explicit(&indexs_, std::memory_order_relaxed)
            ->size();
    }
    bool HasAutoGen();
    std::shared_ptr<IndexDef> GetPkIndex();
    const std::shared_ptr<IndexDef> GetIndexByCombineStr(
        const std::string& combine_str);
    bool FindColName(const std::string& name);

 private:
    std::shared_ptr<std::vector<std::shared_ptr<IndexDef>>> indexs_;
    std::shared_ptr<IndexDef> pk_index_;
    std::shared_ptr<std::unordered_map<std::string, std::shared_ptr<IndexDef>>>
        combine_col_name_map_;
    std::shared_ptr<std::vector<std::string>> col_name_vec_;
};

}  // namespace storage
}  // namespace rtidb
