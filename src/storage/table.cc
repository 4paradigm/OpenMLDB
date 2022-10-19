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
