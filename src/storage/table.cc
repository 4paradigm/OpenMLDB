//
// Copyright (C) 2017 4paradigm.com
// Created by kongsys on 9/5/19.
//

#include "storage/table.h"
#include <algorithm>
#include <utility>
#include "base/glog_wapper.h"
#include "codec/schema_codec.h"

namespace rtidb {
namespace storage {

Table::Table() {}

Table::Table(::rtidb::common::StorageMode storage_mode, const std::string &name,
             uint32_t id, uint32_t pid, uint64_t ttl, bool is_leader,
             uint64_t ttl_offset,
             const std::map<std::string, uint32_t> &mapping,
             ::rtidb::api::TTLType ttl_type,
             ::rtidb::api::CompressType compress_type)
    : storage_mode_(storage_mode),
      name_(name),
      id_(id),
      pid_(pid),
      ttl_offset_(ttl_offset),
      is_leader_(is_leader),
      compress_type_(compress_type),
      version_schema_(),
      update_ttl_(std::make_shared<std::vector<::rtidb::storage::UpdateTTLMeta>>()) {
    ::rtidb::api::TTLDesc *ttl_desc = table_meta_.mutable_ttl_desc();
    ttl_desc->set_ttl_type(ttl_type);
    if (ttl_type == ::rtidb::api::TTLType::kAbsoluteTime) {
        ttl_desc->set_abs_ttl(ttl / (60 * 1000));
        ttl_desc->set_lat_ttl(0);
    } else {
        ttl_desc->set_abs_ttl(0);
        ttl_desc->set_lat_ttl(ttl / (60 * 1000));
    }
    last_make_snapshot_time_ = 0;
    ::rtidb::storage::TTLSt ttl_st(ttl_desc->abs_ttl(), ttl_desc->lat_ttl(),
            ::rtidb::storage::TTLSt::ConvertTTLType(ttl_desc->ttl_type()));
    std::map<uint32_t, std::string> idx_map;
    for (const auto &kv : mapping) {
        idx_map.emplace(kv.second, kv.first);
    }
    for (const auto& kv : idx_map) {
        ::rtidb::common::ColumnDesc* column_desc = table_meta_.add_column_desc();
        column_desc->set_name(kv.second);
        column_desc->set_type("string");
        column_desc->set_type("string");
        column_desc->set_add_ts_idx(true);
    }
}

void Table::AddVersionSchema() {
    auto new_versions = std::make_shared<std::map<int32_t, std::shared_ptr<Schema>>>();
    new_versions->insert(std::make_pair(1, std::make_shared<Schema>(table_meta_.column_desc())));
    for (const auto& ver : table_meta_.schema_versions()) {
        int remain_size = ver.field_count() - table_meta_.column_desc_size();
        if (remain_size < 0)  {
            LOG(INFO) << "do not need add ver " << ver.id() << " because remain size less than 0";
            continue;
        }
        if (remain_size > table_meta_.added_column_desc_size()) {
            LOG(INFO) << "skip add ver " << ver.id() << " because remain size great than added column deisc size";
            continue;
        }
        std::shared_ptr<Schema> new_schema = std::make_shared<Schema>(table_meta_.column_desc());
        for (int i = 0; i < remain_size; i++) {
            rtidb::common::ColumnDesc* col = new_schema->Add();
            col->CopyFrom(table_meta_.added_column_desc(i));
        }
        new_versions->insert(std::make_pair(ver.id(), new_schema));
    }
    std::atomic_store_explicit(&version_schema_, new_versions, std::memory_order_relaxed);
}

void Table::SetTableMeta(::rtidb::api::TableMeta& table_meta) { // NOLINT
    table_meta_.CopyFrom(table_meta);
    AddVersionSchema();
}

int Table::InitColumnDesc() {
    ts_mapping_.clear();
    if (table_index_.ParseFromMeta(table_meta_, &ts_mapping_) < 0) {
        return -1;
    }
    if (table_meta_.column_key_size() == 0) {
        for (const auto &column_desc : table_meta_.column_desc()) {
            if (column_desc.add_ts_idx()) {
                rtidb::common::ColumnKey *column_key =
                    table_meta_.add_column_key();
                column_key->add_col_name(column_desc.name());
                column_key->set_index_name(column_desc.name());
                if (!ts_mapping_.empty()) {
                    column_key->add_ts_name(ts_mapping_.begin()->first);
                }
            }
        }
    }
    AddVersionSchema();
    return 0;
}
void Table::SetTTL(const ::rtidb::storage::UpdateTTLMeta& ttl_meta) {
    std::shared_ptr<std::vector<::rtidb::storage::UpdateTTLMeta>> old_ttl;
    std::shared_ptr<std::vector<::rtidb::storage::UpdateTTLMeta>> new_ttl;
    std::vector<::rtidb::storage::UpdateTTLMeta> set_ttl_vec;
    do {
        old_ttl = std::atomic_load_explicit(&update_ttl_, std::memory_order_acquire);
        new_ttl = std::make_shared<std::vector<::rtidb::storage::UpdateTTLMeta>>(*old_ttl);
        new_ttl->push_back(ttl_meta);
    } while (!atomic_compare_exchange_weak(&update_ttl_, &old_ttl, new_ttl));
}

void Table::UpdateTTL() {
    std::shared_ptr<std::vector<::rtidb::storage::UpdateTTLMeta>> old_ttl;
    std::shared_ptr<std::vector<::rtidb::storage::UpdateTTLMeta>> new_ttl;
    std::vector<::rtidb::storage::UpdateTTLMeta> set_ttl_vec;
    do {
        old_ttl = std::atomic_load_explicit(&update_ttl_, std::memory_order_acquire);
        if (old_ttl->empty()) {
            return;
        }
        new_ttl = std::make_shared<std::vector<::rtidb::storage::UpdateTTLMeta>>(*old_ttl);
        set_ttl_vec.clear();
        set_ttl_vec.swap(*new_ttl);
    } while (!atomic_compare_exchange_weak(&update_ttl_, &old_ttl, new_ttl));
    auto indexs = table_index_.GetAllIndex();
    for (const auto& ttl_meta : set_ttl_vec) {
        for (auto& index : indexs) {
            if (!ttl_meta.index_name.empty()) {
                if (index->GetName() == ttl_meta.index_name) {
                    index->SetTTL(ttl_meta.ttl);
                }
            } else if (ttl_meta.ts_idx >= 0) {
                auto ts_col = index->GetTsColumn();
                if (ts_col && ts_col->GetTsIdx() == ttl_meta.ts_idx) {
                    index->SetTTL(ttl_meta.ttl);
                }
            } else {
                index->SetTTL(ttl_meta.ttl);
            }
        }
    }
}

bool Table::InitFromMeta() {
    if (table_meta_.has_mode() &&
        table_meta_.mode() != ::rtidb::api::TableMode::kTableLeader) {
        is_leader_ = false;
    }
    if (InitColumnDesc() < 0) {
        PDLOG(WARNING, "init column desc failed, tid %u pid %u", id_, pid_);
        return false;
    }
    if (table_meta_.has_schema()) schema_ = table_meta_.schema();
    if (table_meta_.has_compress_type())
        compress_type_ = table_meta_.compress_type();
    return true;
}

bool Table::CheckFieldExist(const std::string& name) {
    for (const auto& column : table_meta_.column_desc()) {
        if (column.name() == name) {
            LOG(WARNING) << "field name[" << name << "] repeated in tablet!";
            return true;
        }
    }
    for (const auto& col : table_meta_.added_column_desc()) {
        if (col.name() == name) {
            LOG(WARNING) << "field name[" << name << "] repeated in tablet!";
            return true;
        }
    }
    return false;
}

}  // namespace storage
}  // namespace rtidb
