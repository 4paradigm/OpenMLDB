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

Table::Table() {
    table_index_ = std::make_shared<TableIndex>();
}

Table::Table(::rtidb::common::StorageMode storage_mode, const std::string& name, uint32_t id, uint32_t pid,
            uint64_t ttl, bool is_leader, uint64_t ttl_offset,
            const std::map<std::string, uint32_t>& mapping,
            ::rtidb::api::TTLType ttl_type, ::rtidb::api::CompressType compress_type) :
        storage_mode_(storage_mode), name_(name), id_(id), pid_(pid), idx_cnt_(mapping.size()),
        ttl_offset_(ttl_offset), is_leader_(is_leader),
        ttl_type_(ttl_type), compress_type_(compress_type) {
    ::rtidb::api::TTLDesc* ttl_desc = table_meta_.mutable_ttl_desc();
    ttl_desc->set_ttl_type(ttl_type);
    if (ttl_type == ::rtidb::api::TTLType::kAbsoluteTime) {
        ttl_desc->set_abs_ttl(ttl/(60*1000));
        ttl_desc->set_lat_ttl(0);
    } else {
        ttl_desc->set_abs_ttl(0);
        ttl_desc->set_lat_ttl(ttl/(60*1000));
    }
    last_make_snapshot_time_ = 0;
    table_index_ = std::make_shared<TableIndex>();
    for (const auto& kv : mapping) {
        table_index_->AddIndex(std::make_shared<IndexDef>(kv.first, kv.second));
    }
}

bool Table::CheckTsValid(uint32_t index, int32_t ts_idx) {
    std::shared_ptr<IndexDef> index_def = GetIndex(index);
    if (!index_def || !index_def->IsReady()) {
        return false;
    }
    auto ts_vec = index_def->GetTsColumn();
    if (std::find(ts_vec.begin(), ts_vec.end(), ts_idx) == ts_vec.end()) {
        PDLOG(WARNING, "ts index %u is not member of index %u, tid %u pid %u", 
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
                table_index_->AddIndex(std::make_shared<IndexDef>(column_desc.name(), key_idx));
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
        if (ts_mapping_.size() > 1) {
            table_index_->ReSet();
        }
        if (table_meta_.column_key_size() > 0) {
            table_index_->ReSet();;
            key_idx = 0;
            for (const auto &column_key : table_meta_.column_key()) {
                std::string name = column_key.index_name();
                if (table_index_->GetIndex(name)) {
                    return -1;
                }
                if (column_key.flag()) {
                    table_index_->AddIndex(std::make_shared<IndexDef>(name, key_idx, ::rtidb::storage::kDeleted));
                } else {
                    table_index_->AddIndex(std::make_shared<IndexDef>(name, key_idx, ::rtidb::storage::kReady));
                }
                key_idx++;
                if (ts_mapping_.empty()) {
                    continue;
                }
                std::vector<uint32_t> ts_vec;
                if (ts_mapping_.size() == 1) {
                    ts_vec.push_back(ts_mapping_.begin()->second);
                } else {
                    for (const auto &ts_name : column_key.ts_name()) {
                        auto ts_iter = ts_mapping_.find(ts_name);
                        if (ts_iter == ts_mapping_.end()) {
                            PDLOG(WARNING, "not found ts_name[%s]. tid %u pid %u",
                                  ts_name.c_str(), id_, pid_);
                            return -1;
                        }
                        if (std::find(ts_vec.begin(), ts_vec.end(), ts_iter->second) == ts_vec.end()) {
                            ts_vec.push_back(ts_iter->second);
                        }
                    }
                }
                table_index_->GetIndex(name)->SetTsColumn(ts_vec);
            }
        } else {
            if (!ts_mapping_.empty()) {
                auto indexs = table_index_->GetAllIndex();
                std::vector<uint32_t> ts_vec;
                ts_vec.push_back(ts_mapping_.begin()->second);
                for (auto &index_def : indexs) {
                    index_def->SetTsColumn(ts_vec);
                }
            }
        }

        if (ts_idx > UINT8_MAX) {
            PDLOG(WARNING, "failed create table because ts column more than %d, tid %u pid %u", UINT8_MAX + 1, id_, pid_);
        }
    } else {
        for (int32_t i = 0; i < table_meta_.dimensions_size(); i++) {
            table_index_->AddIndex(std::make_shared<IndexDef>(table_meta_.dimensions(i), (uint32_t)i));
            PDLOG(INFO, "add index name %s, idx %d to table %s, tid %u, pid %u",
                  table_meta_.dimensions(i).c_str(), i, table_meta_.name().c_str(), id_, pid_);
        }
    }
    // add default dimension
    auto indexs = table_index_->GetAllIndex();
    if (indexs.empty()) {
        table_index_->AddIndex(std::make_shared<IndexDef>("idx0", 0));
        PDLOG(INFO, "no index specified with default");
    }
    if (table_meta_.column_key_size() == 0) {
        for (const auto &column_desc : table_meta_.column_desc()) {
            if (column_desc.add_ts_idx()) {
                rtidb::common::ColumnKey *column_key = table_meta_.add_column_key();
                column_key->add_col_name(column_desc.name());
                column_key->set_index_name(column_desc.name());
            }
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
    idx_cnt_ = table_index_->GetAllIndex().size();
    return true;
}

}
}
