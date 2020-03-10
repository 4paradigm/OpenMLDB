//
// Created by kongsys on 9/5/19.
//

#include "table.h"
#include "logging.h"
#include <algorithm>

using ::baidu::common::INFO;
using ::baidu::common::WARNING;
using ::baidu::common::DEBUG;

namespace rtidb {
namespace storage {
bool Table::CheckTsValid(uint32_t index, int32_t ts_idx) {
    auto column_map_iter = column_key_map_.find(index);
    if (column_map_iter == column_key_map_.end()) {
        return false;
    }
    if (std::find(column_map_iter->second->column_idx.cbegin(), column_map_iter->second->column_idx.cend(), ts_idx)
                == column_map_iter->second->column_idx.cend()) {
        PDLOG(WARNING, "ts cloumn not member of index, ts id %d index id %d, failed getting table tid %u pid %u", 
                    ts_idx, index, id_, pid_);
        return false;
    }
    return true;
}

int Table::InitColumnDesc() {
    if (table_meta_.column_desc_size() > 0) {
        uint32_t key_idx = 0;
        uint32_t ts_idx = 0;
        for (const auto &column_desc : table_meta_.column_desc()) {
            if (column_desc.add_ts_idx()) {
                mapping_.insert(std::make_pair(column_desc.name(), key_idx));
                key_idx++;
            } else if (column_desc.is_ts_col()) {
                ts_mapping_.insert(std::make_pair(column_desc.name(), ts_idx));
                uint64_t abs_ttl = abs_ttl_.load();
                uint64_t lat_ttl = lat_ttl_.load();
                if (column_desc.has_abs_ttl() || column_desc.has_lat_ttl()) {
                    abs_ttl = column_desc.abs_ttl() * 60 * 1000;
                    lat_ttl = column_desc.lat_ttl();
                } else if (column_desc.has_ttl()) {
                    if (ttl_type_ == ::rtidb::api::TTLType::kAbsoluteTime) {
                        abs_ttl = column_desc.ttl() * 60 * 1000;
                        lat_ttl = 0;
                    } else {
                        abs_ttl = 0;
                        lat_ttl = column_desc.ttl();
                    }
                }
                abs_ttl_vec_.push_back(std::make_shared<std::atomic<uint64_t>>(abs_ttl));
                new_abs_ttl_vec_.push_back(std::make_shared<std::atomic<uint64_t>>(abs_ttl));
                lat_ttl_vec_.push_back(std::make_shared<std::atomic<uint64_t>>(lat_ttl));
                new_lat_ttl_vec_.push_back(std::make_shared<std::atomic<uint64_t>>(lat_ttl));
                ts_idx++;
            }
        }
        if (ts_mapping_.size() > 1 && !mapping_.empty()) {
            mapping_.clear();
        }
        if (table_meta_.column_key_size() > 0) {
            mapping_.clear();
            key_idx = 0;
            for (const auto &column_key : table_meta_.column_key()) {
                uint32_t cur_key_idx = key_idx;
                std::string name = column_key.index_name();
                auto it = mapping_.find(name);
                if (it != mapping_.end()) {
                    return -1;
                }
                mapping_.insert(std::make_pair(name, key_idx));
                key_idx++;
                if (ts_mapping_.empty()) {
                    continue;
                }
                if (column_key_map_.find(cur_key_idx) == column_key_map_.end()) {
                    column_key_map_.insert(std::make_pair(cur_key_idx, std::make_shared<ColumnKey>()));
                }
                if (ts_mapping_.size() == 1) {
                    column_key_map_[cur_key_idx]->column_idx.push_back(ts_mapping_.begin()->second);
                    continue;
                }
                for (const auto &ts_name : column_key.ts_name()) {
                    auto ts_iter = ts_mapping_.find(ts_name);
                    if (ts_iter == ts_mapping_.end()) {
                        PDLOG(WARNING, "not found ts_name[%s]. tid %u pid %u",
                              ts_name.c_str(), id_, pid_);
                        return -1;
                    }
                    if (std::find(column_key_map_[cur_key_idx]->column_idx.begin(), column_key_map_[cur_key_idx]->column_idx.end(),
                                  ts_iter->second) == column_key_map_[cur_key_idx]->column_idx.end()) {
                        column_key_map_[cur_key_idx]->column_idx.push_back(ts_iter->second);
                    }
                }
                if (column_key.flag()) {
                    column_key_map_[cur_key_idx]->status = ::rtidb::storage::kDeleted;
                }
            }
        } else {
            if (!ts_mapping_.empty()) {
                for (const auto &kv : mapping_) {
                    uint32_t cur_key_idx = kv.second;
                    if (column_key_map_.find(cur_key_idx) == column_key_map_.end()) {
                        column_key_map_.insert(std::make_pair(cur_key_idx, std::make_shared<ColumnKey>()));
                    }
                    uint32_t cur_ts_idx = ts_mapping_.begin()->second;
                    if (std::find(column_key_map_[cur_key_idx]->column_idx.begin(), column_key_map_[cur_key_idx]->column_idx.end(),
                                  cur_ts_idx) == column_key_map_[cur_key_idx]->column_idx.end()) {
                        column_key_map_[cur_key_idx]->column_idx.push_back(cur_ts_idx);
                    }
                }
            }
        }

        if (ts_idx > UINT8_MAX) {
            PDLOG(INFO, "failed create table because ts column more than %d, tid %u pid %u", UINT8_MAX + 1, id_, pid_);
        }
    } else {
        for (int32_t i = 0; i < table_meta_.dimensions_size(); i++) {
            mapping_.insert(std::make_pair(table_meta_.dimensions(i), (uint32_t) i));
            PDLOG(INFO, "add index name %s, idx %d to table %s, tid %u, pid %u",
                  table_meta_.dimensions(i).c_str(), i, table_meta_.name().c_str(), id_, pid_);
        }
    }
    // add default dimension
    if (mapping_.empty()) {
        mapping_.insert(std::make_pair("idx0", 0));
        PDLOG(INFO, "no index specified with default");
    }
    if (column_key_map_.empty()) {
        for (const auto& iter : mapping_) {
            column_key_map_.insert(std::make_pair(iter.second, std::make_shared<ColumnKey>()));
        }
    }
    return 0;
}

void Table::UpdateTTL() {
    if (abs_ttl_.load(std::memory_order_relaxed) != new_abs_ttl_.load(std::memory_order_relaxed)) {
        uint64_t ttl_for_logger = abs_ttl_.load(std::memory_order_relaxed) / 1000 / 60;
        uint64_t new_ttl_for_logger = new_abs_ttl_.load(std::memory_order_relaxed) / 1000 / 60;
        PDLOG(INFO, "update abs_ttl form %lu to %lu, table %s tid %u pid %u",
                    ttl_for_logger, new_ttl_for_logger, name_.c_str(), id_, pid_);
        abs_ttl_.store(new_abs_ttl_.load(std::memory_order_relaxed), std::memory_order_relaxed);
    }
    if (lat_ttl_.load(std::memory_order_relaxed) != new_lat_ttl_.load(std::memory_order_relaxed)) {
        uint64_t ttl_for_logger = lat_ttl_.load(std::memory_order_relaxed);
        uint64_t new_ttl_for_logger = new_lat_ttl_.load(std::memory_order_relaxed);
        PDLOG(INFO, "update lat_ttl form %lu to %lu, table %s tid %u pid %u",
                    ttl_for_logger, new_ttl_for_logger, name_.c_str(), id_, pid_);
        lat_ttl_.store(new_lat_ttl_.load(std::memory_order_relaxed), std::memory_order_relaxed);
    }
    for (uint32_t i = 0; i < abs_ttl_vec_.size(); i++) {
        if (abs_ttl_vec_[i]->load(std::memory_order_relaxed) != new_abs_ttl_vec_[i]->load(std::memory_order_relaxed)) {
            uint64_t ttl_for_logger = abs_ttl_vec_[i]->load(std::memory_order_relaxed) / 1000 / 60;
            uint64_t new_ttl_for_logger = new_abs_ttl_vec_[i]->load(std::memory_order_relaxed) / 1000 / 60;
            PDLOG(INFO, "update abs_ttl form %lu to %lu, table %s tid %u pid %u ts_index %u",
                    ttl_for_logger, new_ttl_for_logger, name_.c_str(), id_, pid_, i);
            abs_ttl_vec_[i]->store(new_abs_ttl_vec_[i]->load(std::memory_order_relaxed), std::memory_order_relaxed);
        }
        if (lat_ttl_vec_[i]->load(std::memory_order_relaxed) != new_lat_ttl_vec_[i]->load(std::memory_order_relaxed)) {
            uint64_t ttl_for_logger = lat_ttl_vec_[i]->load(std::memory_order_relaxed);
            uint64_t new_ttl_for_logger = new_lat_ttl_vec_[i]->load(std::memory_order_relaxed);
            PDLOG(INFO, "update lat_ttl form %lu to %lu, table %s tid %u pid %u ts_index %u",
                    ttl_for_logger, new_ttl_for_logger, name_.c_str(), id_, pid_, i);
            lat_ttl_vec_[i]->store(new_lat_ttl_vec_[i]->load(std::memory_order_relaxed), std::memory_order_relaxed);
        }
    }
}

bool Table::InitFromMeta() {
    if (table_meta_.has_mode() && table_meta_.mode() != ::rtidb::api::TableMode::kTableLeader) {
        is_leader_ = false;
    }
    if (table_meta_.has_ttl_desc()) {
        abs_ttl_ = table_meta_.ttl_desc().abs_ttl() * 60 * 1000;
        lat_ttl_ = table_meta_.ttl_desc().lat_ttl();
        ttl_type_ = table_meta_.ttl_desc().ttl_type();
    } else if (table_meta_.has_ttl_type() || table_meta_.has_ttl()) {
        ttl_type_ = table_meta_.ttl_type();
        if (ttl_type_ == ::rtidb::api::TTLType::kAbsoluteTime) {
            abs_ttl_ = table_meta_.ttl() * 60 * 1000;
            lat_ttl_ = 0;
        } else {
            abs_ttl_ = 0;
            lat_ttl_ = table_meta_.ttl();
        }
    } else {
        PDLOG(WARNING, "init table with no ttl_desc");
    }
    new_abs_ttl_.store(abs_ttl_.load());
    new_lat_ttl_.store(lat_ttl_.load());
    if (InitColumnDesc() < 0) {
        PDLOG(WARNING, "init column desc failed, tid %u pid %u", id_, pid_);
        return false;
    }
    if (table_meta_.has_schema()) schema_ = table_meta_.schema();
    if (table_meta_.has_compress_type()) compress_type_ = table_meta_.compress_type();
    idx_cnt_ = mapping_.size();
    return true;
}

}
}
