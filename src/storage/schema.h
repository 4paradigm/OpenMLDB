//
// schema.h
// Copyright (C) 2017 4paradigm.com
// Author denglong
// Date 2020-03-11
//

#pragma once

#include <vector>
#include <memory>
#include <atomic>
#include "proto/type.pb.h"
#include "proto/tablet.pb.h"

namespace rtidb {
namespace storage {

struct TTLDesc {
    TTLDesc() = default;
    
    TTLDesc(uint64_t abs, uint64_t lat) : abs_ttl(abs), lat_ttl(lat) {}

    inline bool HasExpire(::rtidb::api::TTLType ttl_type) const {
        switch(ttl_type) {
            case ::rtidb::api::TTLType::kAbsoluteTime: 
                return abs_ttl != 0;
            case ::rtidb::api::TTLType::kLatestTime: 
                return lat_ttl != 0;
            case ::rtidb::api::TTLType::kAbsAndLat: 
                return abs_ttl != 0 && lat_ttl != 0;
            case ::rtidb::api::TTLType::kAbsOrLat: 
                return abs_ttl != 0 || lat_ttl != 0;
            default: return false;
        }
    }

    inline std::string ToString(::rtidb::api::TTLType ttl_type) const {
        switch(ttl_type) {
            case ::rtidb::api::TTLType::kAbsoluteTime: 
                return std::to_string(abs_ttl) + "min";
            case ::rtidb::api::TTLType::kLatestTime: 
                return std::to_string(lat_ttl);
            case ::rtidb::api::TTLType::kAbsAndLat: 
                return std::to_string(abs_ttl) + "min&&" + std::to_string(lat_ttl);
            case ::rtidb::api::TTLType::kAbsOrLat: 
                return std::to_string(abs_ttl)+ "min||" + std::to_string(lat_ttl);
            default: return "";
        }
    }

    uint64_t abs_ttl;
    uint64_t lat_ttl;
};

enum IndexStatus {
    kReady = 0,
    kWaiting,
    kDeleting,
    kDeleted
};

class IndexDef {
public:
    IndexDef(const std::string& name, uint32_t id);
    IndexDef(const std::string& name, uint32_t id, IndexStatus stauts);
    ~IndexDef();
    const std::string& GetName() { return name_; }
    const std::vector<uint32_t>& GetTsColumn() { return ts_column_; }
    void SetTsColumn(const std::vector<uint32_t>& ts_vec) {
        ts_column_ = ts_vec;
    }
    inline bool IsReady() { 
        return status_.load(std::memory_order_relaxed) == IndexStatus::kReady;
    }
    inline uint32_t GetId() { return index_id_; }
    void SetStatus(IndexStatus status) {
        status_.store(status, std::memory_order_relaxed);
    }
    IndexStatus GetStatus() { 
        return status_.load(std::memory_order_relaxed);
    }

private:
    std::string name_;
    uint32_t index_id_;
    std::atomic<IndexStatus> status_;
    ::rtidb::type::IndexType type_;
    std::vector<::rtidb::common::ColumnDesc> column_;
    std::vector<uint32_t> ts_column_;
};

class TableIndex {
public:    
    TableIndex();
    ~TableIndex();
    void ReSet();
    std::shared_ptr<IndexDef> GetIndex(uint32_t idx);
    std::shared_ptr<IndexDef> GetIndex(const std::string& name);
    void AddIndex(std::shared_ptr<IndexDef> index_def);
    void SetAllIndex(const std::vector<std::shared_ptr<IndexDef>>& index_vec);
    std::vector<std::shared_ptr<IndexDef>> GetAllIndex();
    inline uint32_t Size() {
        return std::atomic_load_explicit(&indexs_, std::memory_order_relaxed)->size();
    }

private:
    std::shared_ptr<std::vector<std::shared_ptr<IndexDef>>> indexs_;
};

}
}
