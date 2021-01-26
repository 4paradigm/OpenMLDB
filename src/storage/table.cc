//
// Copyright (C) 2017 4paradigm.com
// Created by kongsys on 9/5/19.
//

#include "storage/table.h"
#include <algorithm>
#include <utility>
#include "base/glog_wapper.h"  // NOLINT
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
      version_schema_() {
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
    for (const auto &kv : mapping) {
        std::vector<ColumnDef> col = { ColumnDef(kv.first, kv.second, ::rtidb::type::DataType::kString, true) };
        auto index = std::make_shared<IndexDef>(kv.first, kv.second, IndexStatus::kReady, 
                ::rtidb::type::kTimeSerise, col);
        index->SetTTL(ttl_st);
        table_index_.AddIndex(index);
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
    uint64_t abs_ttl = 0;
    uint64_t lat_ttl = 0;
    if (table_meta_.ttl_type() == ::rtidb::api::TTLType::kAbsoluteTime) {
        abs_ttl = table_meta_.ttl() * 60 * 1000;
    } else {
        lat_ttl = table_meta_.ttl();
    }
    TTLSt table_ttl(abs_ttl, lat_ttl, TTLSt::ConvertTTLType(table_meta_.ttl_type()));
    if (table_meta_.column_desc_size() > 0) {
        std::set<std::string> ts_col_set;
        int set_ts_cnt = 0;
        for (const auto &column_key : table_meta_.column_key()) {
            if (column_key.ts_name_size() > 0) {
                set_ts_cnt++;
            }
            for (int pos = 0; pos < column_key.ts_name_size(); pos ++) {
                ts_col_set.insert(column_key.ts_name(pos));
            }
        }
        if (set_ts_cnt > 0 && set_ts_cnt != table_meta_.column_key_size()) {
            PDLOG(WARNING, "must set ts col in all column key, tid %u", id_);
            return -1;
        }
        uint32_t key_idx = 0;
        uint32_t ts_idx = 0;
        std::map<std::string, std::shared_ptr<ColumnDef>> col_map;
        std::map<std::string, TTLSt> ts_ttl;
        for (int idx = 0; idx < table_meta_.column_desc_size(); idx++) {
            const auto& column_desc = table_meta_.column_desc(idx);
            std::shared_ptr<ColumnDef> col;
            ::rtidb::type::DataType type = ::rtidb::codec::SchemaCodec::ConvertStrType(column_desc.type());
            const std::string& name = column_desc.name();
            col = std::make_shared<ColumnDef>(name, key_idx, type, true);
            col_map.emplace(name, col);
            if (column_desc.add_ts_idx()) {
                std::vector<ColumnDef> col_vec = { *col };
                auto index = std::make_shared<IndexDef>(name, key_idx, IndexStatus::kReady, 
                        ::rtidb::type::IndexType::kTimeSerise, col_vec);
                index->SetTTL(table_ttl);
                if (table_index_.AddIndex(index) < 0) {
                    return -1;
                }
                key_idx++;
            } else if (column_desc.is_ts_col()) {
                if (ts_col_set.find(name) == ts_col_set.end()) {
                    PDLOG(WARNING, "ts col %s has not set in columnkey, tid %u", name.c_str(), id_);
                    return -1;
                }
                if (!ColumnDef::CheckTsType(type)) {
                    PDLOG(WARNING, "type mismatch, col %s is can not set ts col, tid %u", name.c_str(), id_);
                    return -1;
                }
                col->SetTsIdx(ts_idx);
                ts_mapping_.insert(std::make_pair(name, ts_idx));
                ts_idx++;
                if (column_desc.has_abs_ttl() || column_desc.has_lat_ttl()) {
                    abs_ttl = column_desc.abs_ttl() * 60 * 1000;
                    lat_ttl = column_desc.lat_ttl();
                } else if (column_desc.has_ttl()) {
                    if (table_meta_.ttl_type() == ::rtidb::api::TTLType::kAbsoluteTime) {
                        abs_ttl = column_desc.ttl() * 60 * 1000;
                        lat_ttl = 0;
                    } else {
                        abs_ttl = 0;
                        lat_ttl = column_desc.ttl();
                    }
                }
                ts_ttl.emplace(column_desc.name(),
                        TTLSt(abs_ttl, lat_ttl, TTLSt::ConvertTTLType(table_meta_.ttl_type())));
            } else if (ts_col_set.find(name) != ts_col_set.end()) {
                if (!ColumnDef::CheckTsType(type)) {
                    PDLOG(WARNING, "type mismatch, col %s is can not set ts col, tid %u", name.c_str(), id_);
                    return -1;
                }
                col->SetTsIdx(ts_idx);
                ts_mapping_.insert(std::make_pair(column_desc.name(), ts_idx));
                ts_idx++;
            }
        }
        if (ts_idx > UINT8_MAX) {
            PDLOG(WARNING, "failed create table because ts column more than %d, tid %u pid %u",
                            UINT8_MAX + 1, id_, pid_);
        }
        if (ts_mapping_.size() > 1) {
            table_index_.ReSet();
        }
        if (table_meta_.column_key_size() > 0) {
            table_index_.ReSet();
            key_idx = 0;
            for (const auto &column_key : table_meta_.column_key()) {
                std::string name = column_key.index_name();
                ::rtidb::storage::IndexStatus status;
                if (column_key.flag()) {
                    status = ::rtidb::storage::IndexStatus::kDeleted;
                } else {
                    status = ::rtidb::storage::IndexStatus::kReady;
                }
                std::vector<ColumnDef> col_vec;
                for (const auto& cur_col_name : column_key.col_name()) {
                    col_vec.push_back(*(col_map[cur_col_name]));
                }
                int ts_name_pos = -1;
                do {
                    auto index = std::make_shared<IndexDef>(column_key.index_name(), key_idx, status, 
                            ::rtidb::type::IndexType::kTimeSerise, col_vec);
                    index->SetTTL(table_ttl);
                    if (column_key.ts_name_size() > 0) {
                        ts_name_pos++;
                        const std::string& ts_name = column_key.ts_name(ts_name_pos);
                        index->SetTsColumn(col_map[ts_name]);
                        if (ts_ttl.find(ts_name) != ts_ttl.end()) {
                            index->SetTTL(ts_ttl[ts_name]);
                        }
                    }
                    if (column_key.has_ttl()) {
                        const auto& proto_ttl = column_key.ttl();
                        index->SetTTL(::rtidb::storage::TTLSt(proto_ttl));
                    }
                    if (table_index_.AddIndex(index) < 0) {
                        return -1;
                    }
                    key_idx++;
                } while (ts_name_pos > 0 && ts_name_pos < column_key.ts_name_size());
            }
        } else {
            if (!ts_mapping_.empty()) {
                auto indexs = table_index_.GetAllIndex();
                auto ts_col = col_map[ts_mapping_.begin()->first];
                for (auto &index_def : indexs) {
                    index_def->SetTsColumn(ts_col);
                }
            }
        }

    } else {
        for (int32_t i = 0; i < table_meta_.dimensions_size(); i++) {
            auto index = std::make_shared<IndexDef>(table_meta_.dimensions(i), (uint32_t)i);
            index->SetTTL(table_ttl);
            if (table_index_.AddIndex(index) < 0) {
                return -1;
            }
            PDLOG(INFO, "add index name %s, idx %d to table %s, tid %u, pid %u",
                  table_meta_.dimensions(i).c_str(), i,
                  table_meta_.name().c_str(), id_, pid_);
        }
    }
    // add default dimension
    auto indexs = table_index_.GetAllIndex();
    if (indexs.empty()) {
        auto index = std::make_shared<IndexDef>("idx0", 0);
        index->SetTTL(table_ttl);
        if (table_index_.AddIndex(index) < 0) {
            return -1;
        }
        PDLOG(INFO, "no index specified with default. tid %u, pid %u", id_, pid_);
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

void Table::UpdateTTL() {
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
