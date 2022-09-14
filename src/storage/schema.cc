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

#include "storage/schema.h"

#include <atomic>
#include <map>
#include <set>
#include <utility>

#include "codec/schema_codec.h"
#include "glog/logging.h"

namespace openmldb {
namespace storage {

ColumnDef::ColumnDef(const std::string& name, uint32_t id, ::openmldb::type::DataType type, bool not_null)
    : name_(name), id_(id), type_(type), not_null_(not_null) {}

std::shared_ptr<ColumnDef> TableColumn::GetColumn(uint32_t idx) {
    if (idx < columns_.size()) {
        return columns_.at(idx);
    }
    return std::shared_ptr<ColumnDef>();
}

std::shared_ptr<ColumnDef> TableColumn::GetColumn(const std::string& name) {
    auto it = column_map_.find(name);
    if (it != column_map_.end()) {
        return it->second;
    } else {
        return std::shared_ptr<ColumnDef>();
    }
}

const std::vector<std::shared_ptr<ColumnDef>>& TableColumn::GetAllColumn() { return columns_; }

void TableColumn::AddColumn(std::shared_ptr<ColumnDef> column_def) {
    columns_.push_back(column_def);
    column_map_.insert(std::make_pair(column_def->GetName(), column_def));
}

::openmldb::common::ColumnKey IndexDef::GenColumnKey() {
    ::openmldb::common::ColumnKey column_key;
    column_key.set_index_name(name_);
    if (!IsReady()) {
        column_key.set_flag(1);
    }
    for (const auto& col : columns_) {
        column_key.add_col_name(col.GetName());
    }
    if (ts_column_) {
        column_key.set_ts_name(ts_column_->GetName());
    }
    auto index_ttl = GetTTL();
    auto ttl = column_key.mutable_ttl();
    ttl->set_ttl_type(index_ttl->GetProtoTTLType());
    ttl->set_abs_ttl(index_ttl->abs_ttl / (60 * 1000));
    ttl->set_lat_ttl(index_ttl->lat_ttl);
    return column_key;
}

IndexDef::IndexDef(const std::string& name, uint32_t id)
    : name_(name),
      index_id_(id),
      inner_pos_(0),
      status_(IndexStatus::kReady),
      type_(::openmldb::type::IndexType::kTimeSerise),
      columns_(),
      ttl_st_(),
      ts_column_(nullptr) {}

IndexDef::IndexDef(const std::string& name, uint32_t id, IndexStatus status)
    : name_(name),
      index_id_(id),
      inner_pos_(0),
      status_(status),
      type_(::openmldb::type::IndexType::kTimeSerise),
      columns_(),
      ttl_st_(),
      ts_column_(nullptr) {}

IndexDef::IndexDef(const std::string& name, uint32_t id, const IndexStatus& status, ::openmldb::type::IndexType type,
                   const std::vector<ColumnDef>& columns)
    : name_(name),
      index_id_(id),
      inner_pos_(0),
      status_(status),
      type_(type),
      columns_(columns),
      ttl_st_(),
      ts_column_(nullptr) {}

void IndexDef::SetTTL(const TTLSt& ttl) {
    auto cur_ttl = std::make_shared<TTLSt>(ttl);
    std::atomic_store_explicit(&ttl_st_, cur_ttl, std::memory_order_relaxed);
}

std::shared_ptr<TTLSt> IndexDef::GetTTL() const {
    auto ttl = std::atomic_load_explicit(&ttl_st_, std::memory_order_relaxed);
    return ttl;
}

TTLType IndexDef::GetTTLType() const { return GetTTL()->ttl_type; }

uint32_t InnerIndexSt::GetKeyEntryMaxHeight(uint32_t abs_max_height, uint32_t lat_max_height) const {
    uint32_t max_height = lat_max_height;
    for (const auto& cur_index : index_) {
        ::openmldb::storage::TTLType ttl_type = cur_index->GetTTLType();
        if (ttl_type == ::openmldb::storage::TTLType::kAbsoluteTime ||
            ttl_type == ::openmldb::storage::TTLType::kAbsAndLat) {
            max_height = abs_max_height;
            break;
        }
    }
    return max_height;
}

bool ColumnDefSortFunc(const ColumnDef& cd_a, const ColumnDef& cd_b) { return (cd_a.GetId() < cd_b.GetId()); }

TableIndex::TableIndex() {
    indexs_ = std::make_shared<std::vector<std::shared_ptr<IndexDef>>>();
    inner_indexs_ = std::make_shared<std::vector<std::shared_ptr<InnerIndexSt>>>();
    for (uint32_t i = 0; i < MAX_INDEX_NUM; i++) {
        column_key_2_inner_index_.emplace_back(std::make_shared<std::atomic<int32_t>>(-1));
    }
    pk_index_ = std::shared_ptr<IndexDef>();
    col_name_vec_ = std::make_shared<std::vector<std::string>>();
}

void TableIndex::ReSet() {
    auto new_indexs = std::make_shared<std::vector<std::shared_ptr<IndexDef>>>();
    std::atomic_store_explicit(&indexs_, new_indexs, std::memory_order_relaxed);

    auto inner_indexs = std::make_shared<std::vector<std::shared_ptr<InnerIndexSt>>>();
    std::atomic_store_explicit(&inner_indexs_, inner_indexs, std::memory_order_relaxed);
    column_key_2_inner_index_.clear();
    for (uint32_t i = 0; i < MAX_INDEX_NUM; i++) {
        column_key_2_inner_index_.emplace_back(std::make_shared<std::atomic<int32_t>>(-1));
    }

    pk_index_.reset();

    auto new_vec = std::make_shared<std::vector<std::string>>();
    std::atomic_store_explicit(&col_name_vec_, new_vec, std::memory_order_relaxed);
}

int TableIndex::ParseFromMeta(const ::openmldb::api::TableMeta& table_meta) {
    ReSet();
    uint32_t tid = table_meta.tid();
    uint32_t pid = table_meta.pid();
    if (table_meta.column_desc_size() > 0) {
        std::set<std::string> ts_col_set;
        for (const auto& column_key : table_meta.column_key()) {
            if (!column_key.ts_name().empty()) {
                ts_col_set.insert(column_key.ts_name());
            }
        }
        std::map<std::string, std::shared_ptr<ColumnDef>> col_map;
        for (int idx = 0; idx < table_meta.column_desc_size(); idx++) {
            const auto& column_desc = table_meta.column_desc(idx);
            ::openmldb::type::DataType type = column_desc.data_type();
            const std::string& name = column_desc.name();
            auto col = std::make_shared<ColumnDef>(name, idx, type, column_desc.not_null());
            col_map.emplace(name, col);
            if (ts_col_set.find(name) != ts_col_set.end()) {
                if (!ColumnDef::CheckTsType(type)) {
                    LOG(WARNING) << "type mismatch, col " << name << " is can not set ts col, tid " << tid;
                    return -1;
                }
            }
        }
        for (int idx = 0; idx < table_meta.added_column_desc_size(); idx++) {
            const auto& column_desc = table_meta.added_column_desc(idx);
            ::openmldb::type::DataType type = column_desc.data_type();
            const std::string& name = column_desc.name();
            auto col = std::make_shared<ColumnDef>(name, idx + table_meta.column_desc_size(),
                    type, column_desc.not_null());
            col_map.emplace(name, col);
            if (ts_col_set.find(name) != ts_col_set.end()) {
                if (!ColumnDef::CheckTsType(type)) {
                    LOG(WARNING) << "type mismatch, col " << name << " is can not set ts col, tid " << tid;
                    return -1;
                }
            }
        }
        uint32_t key_idx = 0;
        for (int pos = 0; pos < table_meta.column_key_size(); pos++) {
            const auto& column_key = table_meta.column_key(pos);
            std::string name = column_key.index_name();
            ::openmldb::storage::IndexStatus status;
            if (column_key.flag()) {
                status = ::openmldb::storage::IndexStatus::kDeleted;
            } else {
                status = ::openmldb::storage::IndexStatus::kReady;
            }
            std::vector<ColumnDef> col_vec;
            for (const auto& cur_col_name : column_key.col_name()) {
                col_vec.push_back(*(col_map[cur_col_name]));
            }
            auto index = std::make_shared<IndexDef>(column_key.index_name(), key_idx, status,
                                                    ::openmldb::type::IndexType::kTimeSerise, col_vec);
            if (!column_key.ts_name().empty()) {
                const std::string& ts_name = column_key.ts_name();
                index->SetTsColumn(col_map[ts_name]);
            } else {
                // set default ts col
                index->SetTsColumn(std::make_shared<ColumnDef>(DEFUALT_TS_COL_NAME, DEFUALT_TS_COL_ID,
                            ::openmldb::type::kTimestamp, true));
            }
            if (column_key.has_ttl()) {
                index->SetTTL(::openmldb::storage::TTLSt(column_key.ttl()));
            }
            if (AddIndex(index) < 0) {
                DLOG(WARNING) << "add index failed";
                return -1;
            }
            key_idx++;
        }
    }
    // add default dimension
    if (indexs_->empty()) {
        auto index = std::make_shared<IndexDef>("idx0", 0);
        index->SetTsColumn(std::make_shared<ColumnDef>(DEFUALT_TS_COL_NAME, DEFUALT_TS_COL_ID,
                    ::openmldb::type::kTimestamp, true));
        if (AddIndex(index) < 0) {
            DLOG(WARNING) << "add index failed";
            return -1;
        }
        index->SetTTL(TTLSt());
        LOG(INFO) << "no index specified with default. tid " << tid << ", pid " << pid;
    }
    FillIndexVal(table_meta);
    if (!indexs_->empty()) {
        pk_index_ = indexs_->front();
    } else {
        LOG(WARNING) << "no pk index. tid " << tid << ", pid " << pid;
        return -1;
    }
    return 0;
}

void TableIndex::FillIndexVal(const ::openmldb::api::TableMeta& table_meta) {
    inner_indexs_->clear();
    if (table_meta.column_key_size() > 0) {
        std::map<std::string, uint32_t> name_pos_map;
        std::vector<std::vector<std::shared_ptr<IndexDef>>> pos_index_vec;
        uint32_t inner_cnt = 0;
        for (int idx = 0; idx < table_meta.column_key_size(); idx++) {
            const auto& column_key = table_meta.column_key(idx);
            std::string combine_col_name;
            if (column_key.col_name_size() == 0) {
                combine_col_name = column_key.index_name();
            } else if (column_key.col_name_size() < 2) {
                combine_col_name = column_key.col_name(0);
            } else {
                for (const auto& cur_col_name : column_key.col_name()) {
                    combine_col_name.append(cur_col_name);
                    combine_col_name.append("|");
                }
            }
            auto iter = name_pos_map.find(combine_col_name);
            if (iter == name_pos_map.end()) {
                auto pair = name_pos_map.emplace(combine_col_name, inner_cnt);
                iter = pair.first;
                pos_index_vec.push_back(std::vector<std::shared_ptr<IndexDef>>());
                inner_cnt++;
            }
            column_key_2_inner_index_[idx]->store(name_pos_map[combine_col_name], std::memory_order_relaxed);
            pos_index_vec[iter->second].push_back(GetIndex(column_key.index_name()));
        }
        for (uint32_t idx = 0; idx < pos_index_vec.size(); idx++) {
            inner_indexs_->push_back(std::make_shared<InnerIndexSt>(idx, pos_index_vec[idx]));
            for (auto& index : pos_index_vec[idx]) {
                index->SetInnerPos(idx);
            }
        }
    } else {
        for (uint32_t idx = 0; idx < indexs_->size(); idx++) {
            auto& index = indexs_->at(idx);
            index->SetInnerPos(idx);
            std::vector<std::shared_ptr<IndexDef>> vec = {index};
            inner_indexs_->push_back(std::make_shared<InnerIndexSt>(idx, vec));
            column_key_2_inner_index_[idx]->store(idx, std::memory_order_relaxed);
        }
    }
}

void TableIndex::AddInnerIndex(const std::shared_ptr<InnerIndexSt>& inner_index) {
    std::shared_ptr<std::vector<std::shared_ptr<InnerIndexSt>>> old_inner_indexs;
    std::shared_ptr<std::vector<std::shared_ptr<InnerIndexSt>>> new_inner_indexs;
    do {
        old_inner_indexs = std::atomic_load_explicit(&inner_indexs_, std::memory_order_acquire);
        new_inner_indexs = std::make_shared<std::vector<std::shared_ptr<InnerIndexSt>>>(*old_inner_indexs);
        new_inner_indexs->push_back(inner_index);
    } while (!atomic_compare_exchange_weak(&inner_indexs_, &old_inner_indexs, new_inner_indexs));
}

std::shared_ptr<std::vector<std::shared_ptr<InnerIndexSt>>> TableIndex::GetAllInnerIndex() const {
    return std::atomic_load_explicit(&inner_indexs_, std::memory_order_relaxed);
}

std::shared_ptr<InnerIndexSt> TableIndex::GetInnerIndex(uint32_t idx) const {
    auto indexs = std::atomic_load_explicit(&inner_indexs_, std::memory_order_relaxed);
    if (idx < indexs->size()) {
        return indexs->at(idx);
    }
    return std::shared_ptr<InnerIndexSt>();
}

int32_t TableIndex::GetInnerIndexPos(uint32_t column_key_pos) const {
    if (column_key_pos >= column_key_2_inner_index_.size()) {
        return -1;
    }
    return column_key_2_inner_index_.at(column_key_pos)->load(std::memory_order_relaxed);
}

void TableIndex::SetInnerIndexPos(uint32_t column_key_pos, uint32_t inner_pos) {
    if (column_key_pos >= column_key_2_inner_index_.size()) {
        return;
    }
    return column_key_2_inner_index_.at(column_key_pos)->store(inner_pos, std::memory_order_relaxed);
}

std::shared_ptr<IndexDef> TableIndex::GetIndex(uint32_t idx) {
    auto indexs = std::atomic_load_explicit(&indexs_, std::memory_order_relaxed);
    if (idx < indexs->size()) {
        return indexs->at(idx);
    }
    return std::shared_ptr<IndexDef>();
}

std::shared_ptr<IndexDef> TableIndex::GetIndex(uint32_t idx, uint32_t ts_idx) {
    auto indexs = std::atomic_load_explicit(&indexs_, std::memory_order_relaxed);
    if (idx < indexs->size()) {
        auto index = indexs->at(idx);
        auto ts_col = index->GetTsColumn();
        if (ts_col && ts_col->GetId() == ts_idx) {
            return index;
        }
    }
    return std::shared_ptr<IndexDef>();
}

std::shared_ptr<IndexDef> TableIndex::GetIndex(const std::string& name) {
    auto indexs = std::atomic_load_explicit(&indexs_, std::memory_order_relaxed);
    for (const auto& index : *indexs) {
        if (index->GetName() == name) {
            return index;
        }
    }
    return std::shared_ptr<IndexDef>();
}

std::shared_ptr<IndexDef> TableIndex::GetIndex(const std::string& name, uint32_t ts_idx) {
    auto indexs = std::atomic_load_explicit(&indexs_, std::memory_order_relaxed);
    for (const auto& index : *indexs) {
        if (index->GetName() == name) {
            auto ts_col = index->GetTsColumn();
            if (ts_col && ts_col->GetId() == ts_idx) {
                return index;
            }
            break;
        }
    }
    return std::shared_ptr<IndexDef>();
}

std::vector<std::shared_ptr<IndexDef>> TableIndex::GetAllIndex() {
    auto indexs = std::atomic_load_explicit(&indexs_, std::memory_order_relaxed);
    return *indexs;
}

int TableIndex::AddIndex(std::shared_ptr<IndexDef> index_def) {
    auto old_indexs = std::atomic_load_explicit(&indexs_, std::memory_order_relaxed);
    if (old_indexs->size() >= MAX_INDEX_NUM) {
        return -1;
    }
    auto new_indexs = std::make_shared<std::vector<std::shared_ptr<IndexDef>>>(*old_indexs);
    for (const auto& index : *new_indexs) {
        if (index->GetName() == index_def->GetName()) {
            return -1;
        }
    }
    new_indexs->push_back(index_def);
    std::atomic_store_explicit(&indexs_, new_indexs, std::memory_order_relaxed);
    if (index_def->GetType() == ::openmldb::type::kPrimaryKey || index_def->GetType() == ::openmldb::type::kAutoGen) {
        pk_index_ = index_def;
    }

    auto old_vec = std::atomic_load_explicit(&col_name_vec_, std::memory_order_relaxed);
    auto new_vec = std::make_shared<std::vector<std::string>>(*old_vec);
    std::string combine_name = "";
    for (auto& col_def : index_def->GetColumns()) {
        if (!combine_name.empty()) {
            combine_name.append("_");
        }
        combine_name.append(col_def.GetName());
        new_vec->push_back(col_def.GetName());
    }
    std::atomic_store_explicit(&col_name_vec_, new_vec, std::memory_order_relaxed);
    return 0;
}

uint32_t TableIndex::Size() const {
    auto indexs = std::atomic_load_explicit(&indexs_, std::memory_order_relaxed);
    return indexs->size();
}

int32_t TableIndex::GetMaxIndexId() const {
    auto indexs = std::atomic_load_explicit(&indexs_, std::memory_order_relaxed);
    if (!indexs->empty()) {
        return indexs->back()->GetId();
    }
    return -1;
}

bool TableIndex::HasAutoGen() {
    if (pk_index_->GetType() == ::openmldb::type::kAutoGen) {
        return true;
    }
    return false;
}

std::shared_ptr<IndexDef> TableIndex::GetPkIndex() { return pk_index_; }

bool TableIndex::IsColName(const std::string& name) {
    auto vec = std::atomic_load_explicit(&col_name_vec_, std::memory_order_relaxed);
    auto iter = std::find(vec->begin(), vec->end(), name);
    if (iter == vec->end()) {
        return false;
    }
    return true;
}

PartitionSt::PartitionSt(const ::openmldb::nameserver::TablePartition& partitions) : pid_(partitions.pid()) {
    for (const auto& meta : partitions.partition_meta()) {
        if (!meta.is_alive()) {
            continue;
        }
        if (meta.is_leader()) {
            leader_ = meta.endpoint();
        } else {
            follower_.push_back(meta.endpoint());
        }
    }
}

PartitionSt::PartitionSt(const ::openmldb::common::TablePartition& partitions) : pid_(partitions.pid()) {
    for (const auto& meta : partitions.partition_meta()) {
        if (!meta.is_alive()) {
            continue;
        }
        if (meta.is_leader()) {
            leader_ = meta.endpoint();
        } else {
            follower_.push_back(meta.endpoint());
        }
    }
}

bool PartitionSt::operator==(const PartitionSt& partition_st) const {
    if (pid_ != partition_st.GetPid()) {
        return false;
    }
    if (leader_ != partition_st.GetLeader()) {
        return false;
    }
    const std::vector<std::string>& o_follower = partition_st.GetFollower();
    if (follower_.size() != o_follower.size()) {
        return false;
    }
    for (const auto& endpoint : follower_) {
        if (std::find(o_follower.begin(), o_follower.end(), endpoint) == o_follower.end()) {
            return false;
        }
    }
    return true;
}

TableSt::TableSt(const ::openmldb::nameserver::TableInfo& table_info)
    : name_(table_info.name()),
      db_(table_info.db()),
      tid_(table_info.tid()),
      pid_num_(table_info.table_partition_size()),
      column_desc_(table_info.column_desc()),
      column_key_(table_info.column_key()) {
    partitions_ = std::make_shared<std::vector<PartitionSt>>();
    for (const auto& table_partition : table_info.table_partition()) {
        uint32_t pid = table_partition.pid();
        if (pid > partitions_->size()) {
            continue;
        }
        partitions_->emplace_back(PartitionSt(table_partition));
    }
}

TableSt::TableSt(const ::openmldb::api::TableMeta& meta)
    : name_(meta.name()),
      db_(meta.db()),
      tid_(meta.tid()),
      pid_num_(meta.table_partition_size()),
      column_desc_(meta.column_desc()),
      column_key_(meta.column_key()) {
    partitions_ = std::make_shared<std::vector<PartitionSt>>();
    for (const auto& table_partition : meta.table_partition()) {
        uint32_t pid = table_partition.pid();
        if (pid > partitions_->size()) {
            continue;
        }
        partitions_->emplace_back(PartitionSt(table_partition));
    }
}

bool TableSt::SetPartition(const PartitionSt& partition_st) {
    uint32_t pid = partition_st.GetPid();
    if (pid >= pid_num_) {
        return false;
    }
    auto old_partitions = GetPartitions();
    auto new_partitions = std::make_shared<std::vector<PartitionSt>>(*old_partitions);
    (*new_partitions)[pid] = partition_st;
    std::atomic_store_explicit(&partitions_, new_partitions, std::memory_order_relaxed);
    return true;
}

}  // namespace storage
}  // namespace openmldb
