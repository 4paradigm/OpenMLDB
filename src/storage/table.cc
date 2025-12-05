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

#include "storage/table.h"

#include <algorithm>
#include <utility>

#include "base/glog_wrapper.h"
#include "codec/schema_codec.h"
#include "schema/index_util.h"
#include "storage/mem_table.h"
#include "storage/disk_table.h"

namespace openmldb {
namespace storage {

Table::Table() {}

Table::Table(::openmldb::common::StorageMode storage_mode, const std::string& name, uint32_t id, uint32_t pid,
             uint64_t ttl, bool is_leader, uint64_t ttl_offset, const std::map<std::string, uint32_t>& mapping,
             ::openmldb::type::TTLType ttl_type, ::openmldb::type::CompressType compress_type)
    : storage_mode_(storage_mode),
      name_(name),
      id_(id),
      pid_(pid),
      is_leader_(is_leader),
      ttl_offset_(ttl_offset),
      compress_type_(compress_type),
      version_schema_(),
      update_ttl_(std::make_shared<std::vector<::openmldb::storage::UpdateTTLMeta>>()) {
    table_meta_ = std::make_shared<::openmldb::api::TableMeta>();
    ::openmldb::common::TTLSt ttl_st;
    ttl_st.set_ttl_type(ttl_type);
    if (ttl_type == ::openmldb::type::TTLType::kAbsoluteTime) {
        ttl_st.set_abs_ttl(ttl / (60 * 1000));
        ttl_st.set_lat_ttl(0);
    } else {
        ttl_st.set_abs_ttl(0);
        ttl_st.set_lat_ttl(ttl / (60 * 1000));
    }
    last_make_snapshot_time_ = 0;
    std::map<uint32_t, std::string> idx_map;
    for (const auto& kv : mapping) {
        idx_map.emplace(kv.second, kv.first);
    }
    for (const auto& kv : idx_map) {
        ::openmldb::common::ColumnDesc* column_desc = table_meta_->add_column_desc();
        column_desc->set_name(kv.second);
        column_desc->set_data_type(::openmldb::type::kString);
        ::openmldb::common::ColumnKey* index = table_meta_->add_column_key();
        index->set_index_name(kv.second);
        index->add_col_name(kv.second);
        ::openmldb::common::TTLSt* cur_ttl = index->mutable_ttl();
        cur_ttl->CopyFrom(ttl_st);
    }
    AddVersionSchema(*table_meta_);
}

void Table::AddVersionSchema(const ::openmldb::api::TableMeta& table_meta) {
    auto new_versions = std::make_shared<std::map<int32_t, std::shared_ptr<Schema>>>();
    new_versions->insert(std::make_pair(1, std::make_shared<Schema>(table_meta.column_desc())));
    auto version_decoder = std::make_shared<std::map<int32_t, std::shared_ptr<codec::RowView>>>();
    version_decoder->emplace(1, std::make_shared<codec::RowView>(*(new_versions->begin()->second)));
    for (const auto& ver : table_meta.schema_versions()) {
        int remain_size = ver.field_count() - table_meta.column_desc_size();
        if (remain_size < 0) {
            LOG(INFO) << "do not need add ver " << ver.id() << " because remain size less than 0";
            continue;
        }
        if (remain_size > table_meta.added_column_desc_size()) {
            LOG(INFO) << "skip add ver " << ver.id() << " because remain size great than added column deisc size";
            continue;
        }
        std::shared_ptr<Schema> new_schema = std::make_shared<Schema>(table_meta.column_desc());
        for (int i = 0; i < remain_size; i++) {
            openmldb::common::ColumnDesc* col = new_schema->Add();
            col->CopyFrom(table_meta.added_column_desc(i));
        }
        new_versions->emplace(ver.id(), new_schema);
        version_decoder->emplace(ver.id(), std::make_shared<codec::RowView>(*new_schema));
    }
    std::atomic_store_explicit(&version_schema_, new_versions, std::memory_order_relaxed);
    std::atomic_store_explicit(&version_decoder_, version_decoder, std::memory_order_relaxed);
}

void Table::SetTableMeta(::openmldb::api::TableMeta& table_meta) {  // NOLINT
    auto cur_table_meta = std::make_shared<::openmldb::api::TableMeta>(table_meta);
    std::atomic_store_explicit(&table_meta_, cur_table_meta, std::memory_order_release);
    AddVersionSchema(table_meta);
}

int Table::InitColumnDesc() {
    if (table_index_.ParseFromMeta(*table_meta_) < 0) {
        DLOG(WARNING) << "parse meta failed";
        return -1;
    }
    if (table_meta_->column_key_size() == 0) {
        DLOG(WARNING) << "no index";
        return -1;
    }
    AddVersionSchema(*table_meta_);
    return 0;
}
void Table::SetTTL(const ::openmldb::storage::UpdateTTLMeta& ttl_meta) {
    std::shared_ptr<std::vector<::openmldb::storage::UpdateTTLMeta>> old_ttl;
    std::shared_ptr<std::vector<::openmldb::storage::UpdateTTLMeta>> new_ttl;
    std::vector<::openmldb::storage::UpdateTTLMeta> set_ttl_vec;
    do {
        old_ttl = std::atomic_load_explicit(&update_ttl_, std::memory_order_acquire);
        new_ttl = std::make_shared<std::vector<::openmldb::storage::UpdateTTLMeta>>(*old_ttl);
        new_ttl->push_back(ttl_meta);
    } while (!atomic_compare_exchange_weak(&update_ttl_, &old_ttl, new_ttl));
    auto table_meta = GetTableMeta();
    auto new_table_meta = std::make_shared<::openmldb::api::TableMeta>(*table_meta);
    for (int idx = 0; idx < new_table_meta->column_key_size(); idx++) {
        auto column_key = new_table_meta->mutable_column_key(idx);
        if (ttl_meta.index_name.empty() || column_key->index_name() == ttl_meta.index_name) {
            auto cur_ttl = column_key->mutable_ttl();
            cur_ttl->set_ttl_type(ttl_meta.ttl.GetProtoTTLType());
            // second to minute
            cur_ttl->set_abs_ttl(ttl_meta.ttl.abs_ttl / (60 * 1000));
            cur_ttl->set_lat_ttl(ttl_meta.ttl.lat_ttl);
        }
    }
    std::atomic_store_explicit(&table_meta_, new_table_meta, std::memory_order_release);
}

void Table::UpdateTTL() {
    std::shared_ptr<std::vector<::openmldb::storage::UpdateTTLMeta>> old_ttl;
    std::shared_ptr<std::vector<::openmldb::storage::UpdateTTLMeta>> new_ttl;
    std::vector<::openmldb::storage::UpdateTTLMeta> set_ttl_vec;
    do {
        old_ttl = std::atomic_load_explicit(&update_ttl_, std::memory_order_acquire);
        if (old_ttl->empty()) {
            return;
        }
        new_ttl = std::make_shared<std::vector<::openmldb::storage::UpdateTTLMeta>>(*old_ttl);
        set_ttl_vec.clear();
        set_ttl_vec.swap(*new_ttl);
    } while (!atomic_compare_exchange_weak(&update_ttl_, &old_ttl, new_ttl));
    auto indexs = table_index_.GetAllIndex();
    for (const auto& ttl_meta : set_ttl_vec) {
        for (auto& index : indexs) {
            if (!ttl_meta.index_name.empty()) {
                if (index->GetName() == ttl_meta.index_name) {
                    index->SetTTL(ttl_meta.ttl);
                    LOG(INFO) << "Update index " << index->GetName() << " ttl " << ttl_meta.ttl.ToString();
                }
            } else {
                index->SetTTL(ttl_meta.ttl);
                LOG(INFO) << "Update index " << index->GetName() << " ttl " << ttl_meta.ttl.ToString();
            }
        }
    }
}

bool Table::AddIndex(const ::openmldb::common::ColumnKey& column_key) {
    // TODO(denglong): support ttl type and merge index
    auto table_meta = GetTableMeta();
    auto new_table_meta = std::make_shared<::openmldb::api::TableMeta>(*table_meta);
    std::shared_ptr<IndexDef> index_def = GetIndex(column_key.index_name());
    if (index_def) {
        if (index_def->GetStatus() != IndexStatus::kDeleted) {
            PDLOG(WARNING, "index %s is exist. tid %u pid %u", column_key.index_name().c_str(), id_, pid_);
            return false;
        }
        if (column_key.has_ttl()) {
            index_def->SetTTL(::openmldb::storage::TTLSt(column_key.ttl()));
        }
    }
    int index_pos = schema::IndexUtil::GetPosition(column_key, new_table_meta->column_key());
    if (index_pos >= 0) {
        new_table_meta->mutable_column_key(index_pos)->CopyFrom(column_key);
    } else {
        new_table_meta->add_column_key()->CopyFrom(column_key);
    }
    if (!index_def) {
        auto cols = GetSchema();
        if (!cols) {
            return false;
        }
        std::map<std::string, ColumnDef> schema;
        for (int idx = 0; idx < cols->size(); idx++) {
            const auto& col = cols->Get(idx);
            schema.emplace(col.name(), ColumnDef(col.name(), idx, col.data_type(), col.not_null()));
        }
        std::vector<ColumnDef> col_vec;
        for (const auto& col_name : column_key.col_name()) {
            auto it = schema.find(col_name);
            if (it == schema.end()) {
                PDLOG(WARNING, "not found col_name[%s]. tid %u pid %u", col_name.c_str(), id_, pid_);
                return false;
            }
            col_vec.push_back(it->second);
        }
        
        common::IndexType index_type = column_key.has_type() ? column_key.type() : common::IndexType::kCovering;
        index_def = std::make_shared<IndexDef>(column_key.index_name(), table_index_.GetMaxIndexId() + 1,
                IndexStatus::kReady, ::openmldb::type::IndexType::kTimeSerise, col_vec, index_type);
        if (!column_key.ts_name().empty()) {
            if (auto ts_iter = schema.find(column_key.ts_name()); ts_iter == schema.end()) {
                PDLOG(WARNING, "not found ts_name[%s]. tid %u pid %u", column_key.ts_name().c_str(), id_, pid_);
                return false;
            } else {
                index_def->SetTsColumn(std::make_shared<ColumnDef>(ts_iter->second));
            }
        } else {
            index_def->SetTsColumn(std::make_shared<ColumnDef>(DEFAULT_TS_COL_NAME, DEFAULT_TS_COL_ID,
                        ::openmldb::type::kTimestamp, true));
        }
        if (column_key.has_ttl()) {
            index_def->SetTTL(::openmldb::storage::TTLSt(column_key.ttl()));
        } else {
            index_def->SetTTL(*(table_index_.GetIndex(0)->GetTTL()));
        }
        uint32_t inner_id = table_index_.GetAllInnerIndex()->size();
        index_def->SetInnerPos(inner_id);
        if (!AddIndexToTable(index_def)) {
            PDLOG(WARNING, "add index to table failed. tid %u pid %u", id_, pid_);
            return false;
        }
        if (table_index_.AddIndex(index_def) < 0) {
            PDLOG(WARNING, "add index failed. tid %u pid %u", id_, pid_);
            return false;
        }
        std::vector<std::shared_ptr<IndexDef>> index_vec = {index_def};
        auto inner_index_st = std::make_shared<InnerIndexSt>(inner_id, index_vec);
        table_index_.AddInnerIndex(inner_index_st);
        table_index_.SetInnerIndexPos(new_table_meta->column_key_size() - 1, inner_id);
    }
    index_def->SetStatus(IndexStatus::kReady);
    std::atomic_store_explicit(&table_meta_, new_table_meta, std::memory_order_release);
    return true;
}

bool Table::DeleteIndex(const std::string& idx_name) {
    std::shared_ptr<IndexDef> index_def = table_index_.GetIndex(idx_name);
    if (!index_def) {
        PDLOG(WARNING, "index %s does not exist. tid %u pid %u", idx_name.c_str(), id_, pid_);
        return false;
    }
    if (!index_def->IsReady()) {
        PDLOG(WARNING, "index %s can't delete. tid %u pid %u", idx_name.c_str(), id_, pid_);
        return false;
    }
    auto table_meta = GetTableMeta();
    uint32_t index_id = index_def->GetId();
    if (index_id == 0) {
        PDLOG(WARNING, "index %s is primary key, cannot delete. tid %u pid %u", idx_name.c_str(), id_, pid_);
        return false;
    } else if (index_id >= static_cast<uint32_t>(table_meta->column_key_size())) {
        PDLOG(WARNING, "invalid index id %u. tid %u pid %u", index_id, id_, pid_);
        return false;
    }
    auto new_table_meta = std::make_shared<::openmldb::api::TableMeta>(*table_meta);
    new_table_meta->mutable_column_key(index_id)->set_flag(1);
    std::atomic_store_explicit(&table_meta_, new_table_meta, std::memory_order_release);
    index_def->SetStatus(IndexStatus::kWaiting);
    return true;
}

bool Table::InitFromMeta() {
    if (table_meta_->has_mode() && table_meta_->mode() != ::openmldb::api::TableMode::kTableLeader) {
        is_leader_ = false;
    }
    if (InitColumnDesc() < 0) {
        PDLOG(WARNING, "init column desc failed, tid %u pid %u", id_, pid_);
        return false;
    }
    if (table_meta_->has_compress_type()) {
        compress_type_ = table_meta_->compress_type();
    }
    return true;
}

bool Table::CheckFieldExist(const std::string& name) {
    auto table_meta = std::atomic_load_explicit(&table_meta_, std::memory_order_acquire);
    for (const auto& column : table_meta->column_desc()) {
        if (column.name() == name) {
            LOG(WARNING) << "field name[" << name << "] repeated in tablet!";
            return true;
        }
    }
    for (const auto& col : table_meta->added_column_desc()) {
        if (col.name() == name) {
            LOG(WARNING) << "field name[" << name << "] repeated in tablet!";
            return true;
        }
    }
    return false;
}

}  // namespace storage
}  // namespace openmldb
