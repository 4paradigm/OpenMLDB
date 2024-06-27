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

#include "storage/index_organized_table.h"

#include <snappy.h>

#include "absl/strings/str_join.h"  // dlog
#include "absl/strings/str_split.h"
#include "sdk/sql_router.h"
#include "storage/iot_segment.h"

DECLARE_uint32(absolute_default_skiplist_height);

namespace openmldb::storage {

IOTIterator* NewNullIterator() {
    // if TimeEntries::Iterator is null, nothing will be used
    return new IOTIterator(nullptr, type::CompressType::kNoCompress, {});
}

// TODO(hw): temp func to create iot iterator
IOTIterator* NewIOTIterator(Segment* segment, const Slice& key, Ticket& ticket, type::CompressType compress_type,
                            std::unique_ptr<hybridse::codec::WindowIterator> cidx_iter) {
    void* entry = nullptr;
    auto entries = segment->GetKeyEntries();
    if (entries == nullptr || segment->GetTsCnt() > 1 || entries->Get(key, entry) < 0 || entry == nullptr) {
        return NewNullIterator();
    }
    ticket.Push(reinterpret_cast<KeyEntry*>(entry));
    return new IOTIterator(reinterpret_cast<KeyEntry*>(entry)->entries.NewIterator(), compress_type,
                           std::move(cidx_iter));
}

IOTIterator* NewIOTIterator(Segment* segment, const Slice& key, uint32_t idx, Ticket& ticket,
                            type::CompressType compress_type,
                            std::unique_ptr<hybridse::codec::WindowIterator> cidx_iter) {
    auto ts_idx_map = segment->GetTsIdxMap();
    auto pos = ts_idx_map.find(idx);
    if (pos == ts_idx_map.end()) {
        LOG(WARNING) << "can't find idx in segment";
        return NewNullIterator();
    }
    auto entries = segment->GetKeyEntries();
    if (segment->GetTsCnt() == 1) {
        return NewIOTIterator(segment, key, ticket, compress_type, std::move(cidx_iter));
    }
    void* entry_arr = nullptr;
    if (entries->Get(key, entry_arr) < 0 || entry_arr == nullptr) {
        return NewNullIterator();
    }
    auto entry = reinterpret_cast<KeyEntry**>(entry_arr)[pos->second];
    ticket.Push(entry);
    return new IOTIterator(entry->entries.NewIterator(), compress_type, std::move(cidx_iter));
}

TableIterator* IndexOrganizedTable::NewIterator(uint32_t index, const std::string& pk, Ticket& ticket) {
    std::shared_ptr<IndexDef> index_def = table_index_.GetIndex(index);
    if (!index_def || !index_def->IsReady()) {
        LOG(WARNING) << "index is invalid";
        return nullptr;
    }
    DLOG(INFO) << "new iter for index and pk " << index << " name " << index_def->GetName();
    uint32_t seg_idx = SegIdx(pk);
    Slice spk(pk);
    uint32_t real_idx = index_def->GetInnerPos();
    Segment* segment = GetSegment(real_idx, seg_idx);
    auto ts_col = index_def->GetTsColumn();
    if (ts_col) {
        // if secondary, use iot iterator
        if (index_def->IsSecondaryIndex()) {
            // get clustered index iter for secondary index
            auto handler = catalog_->GetTable(GetDB(), GetName());
            if (!handler) {
                LOG(WARNING) << "no TableHandler for " << GetDB() << "." << GetName();
                return nullptr;
            }
            auto tablet_table_handler = std::dynamic_pointer_cast<catalog::TabletTableHandler>(handler);
            if (!tablet_table_handler) {
                LOG(WARNING) << "convert TabletTableHandler failed for " << GetDB() << "." << GetName();
                return nullptr;
            }
            LOG(INFO) << "create iot iterator for pk";
            // TODO(hw): iter may be invalid if catalog updated
            auto iter =
                NewIOTIterator(segment, spk, ts_col->GetId(), ticket, GetCompressType(),
                               std::move(tablet_table_handler->GetWindowIterator(table_index_.GetIndex(0)->GetName())));
            return iter;
        }
        // clsutered and covering still use old iterator
        return segment->NewIterator(spk, ts_col->GetId(), ticket, GetCompressType());
    }
    // cidx without ts? or invalid case
    DLOG(INFO) << "index ts col is null, reate no-ts iterator";
    // TODO(hw): sidx without ts?
    return segment->NewIterator(spk, ticket, GetCompressType());
}

TraverseIterator* IndexOrganizedTable::NewTraverseIterator(uint32_t index) {
    std::shared_ptr<IndexDef> index_def = GetIndex(index);
    if (!index_def || !index_def->IsReady()) {
        PDLOG(WARNING, "index %u not found. tid %u pid %u", index, id_, pid_);
        return nullptr;
    }
    DLOG(INFO) << "new traverse iter for index " << index << " name " << index_def->GetName();
    uint64_t expire_time = 0;
    uint64_t expire_cnt = 0;
    auto ttl = index_def->GetTTL();
    if (GetExpireStatus()) {  // gc enabled
        expire_time = GetExpireTime(*ttl);
        expire_cnt = ttl->lat_ttl;
    }
    uint32_t real_idx = index_def->GetInnerPos();
    auto ts_col = index_def->GetTsColumn();
    if (ts_col) {
        // if secondary, use iot iterator
        if (index_def->IsSecondaryIndex()) {
            // get clustered index iter for secondary index
            auto handler = catalog_->GetTable(GetDB(), GetName());
            if (!handler) {
                LOG(WARNING) << "no TableHandler for " << GetDB() << "." << GetName();
                return nullptr;
            }
            auto tablet_table_handler = std::dynamic_pointer_cast<catalog::TabletTableHandler>(handler);
            if (!tablet_table_handler) {
                LOG(WARNING) << "convert TabletTableHandler failed for " << GetDB() << "." << GetName();
                return nullptr;
            }
            LOG(INFO) << "create iot traverse iterator for traverse";
            // TODO(hw): iter may be invalid if catalog updated
            auto iter = new IOTTraverseIterator(
                GetSegments(real_idx), GetSegCnt(), ttl->ttl_type, expire_time, expire_cnt, ts_col->GetId(),
                GetCompressType(),
                std::move(tablet_table_handler->GetWindowIterator(table_index_.GetIndex(0)->GetName())));
            return iter;
        }
        DLOG(INFO) << "create memtable traverse iterator for traverse";
        return new MemTableTraverseIterator(GetSegments(real_idx), GetSegCnt(), ttl->ttl_type, expire_time, expire_cnt,
                                            ts_col->GetId(), GetCompressType());
    }
    DLOG(INFO) << "index ts col is null, reate no-ts iterator";
    return new MemTableTraverseIterator(GetSegments(real_idx), GetSegCnt(), ttl->ttl_type, expire_time, expire_cnt, 0,
                                        GetCompressType());
}

::hybridse::vm::WindowIterator* IndexOrganizedTable::NewWindowIterator(uint32_t index) {
    std::shared_ptr<IndexDef> index_def = table_index_.GetIndex(index);
    if (!index_def || !index_def->IsReady()) {
        LOG(WARNING) << "index id " << index << " not found. tid " << id_ << " pid " << pid_;
        return nullptr;
    }
    LOG(INFO) << "new window iter for index " << index << " name " << index_def->GetName();
    uint64_t expire_time = 0;
    uint64_t expire_cnt = 0;
    auto ttl = index_def->GetTTL();
    if (GetExpireStatus()) {
        expire_time = GetExpireTime(*ttl);
        expire_cnt = ttl->lat_ttl;
    }
    uint32_t real_idx = index_def->GetInnerPos();
    auto ts_col = index_def->GetTsColumn();
    uint32_t ts_idx = 0;
    if (ts_col) {
        ts_idx = ts_col->GetId();
    }
    DLOG(INFO) << "ts is null? " << ts_col << ", ts_idx " << ts_idx;
    // if secondary, use iot iterator
    if (index_def->IsSecondaryIndex()) {
        // get clustered index iter for secondary index
        auto handler = catalog_->GetTable(GetDB(), GetName());
        if (!handler) {
            LOG(WARNING) << "no TableHandler for " << GetDB() << "." << GetName();
            return nullptr;
        }
        auto tablet_table_handler = std::dynamic_pointer_cast<catalog::TabletTableHandler>(handler);
        if (!tablet_table_handler) {
            LOG(WARNING) << "convert TabletTableHandler failed for " << GetDB() << "." << GetName();
            return nullptr;
        }
        LOG(INFO) << "create iot key traverse iterator for window";
        // TODO(hw): iter may be invalid if catalog updated
        auto iter =
            new IOTKeyIterator(GetSegments(real_idx), GetSegCnt(), ttl->ttl_type, expire_time, expire_cnt, ts_idx,
                               GetCompressType(), tablet_table_handler, table_index_.GetIndex(0)->GetName());
        return iter;
    }
    return new MemTableKeyIterator(GetSegments(real_idx), GetSegCnt(), ttl->ttl_type, expire_time, expire_cnt, ts_idx,
                                   GetCompressType());
}

bool IndexOrganizedTable::Init() {
    if (!InitMeta()) {
        LOG(WARNING) << "init meta failed. tid " << id_ << " pid " << pid_;
        return false;
    }
    // IOTSegment should know which is the cidx, sidx and covering idx are both duplicate(even the values are different)
    auto inner_indexs = table_index_.GetAllInnerIndex();
    for (uint32_t i = 0; i < inner_indexs->size(); i++) {
        const std::vector<uint32_t>& ts_vec = inner_indexs->at(i)->GetTsIdx();
        uint32_t cur_key_entry_max_height = KeyEntryMaxHeight(inner_indexs->at(i));

        Segment** seg_arr = new Segment*[seg_cnt_];
        DLOG_ASSERT(!ts_vec.empty()) << "must have ts, include auto gen ts";
        if (!ts_vec.empty()) {
            for (uint32_t j = 0; j < seg_cnt_; j++) {
                // let segment know whether it is cidx
                seg_arr[j] = new IOTSegment(cur_key_entry_max_height, ts_vec, inner_indexs->at(i)->GetTsIdxType());
                PDLOG(INFO, "init %u, %u segment. height %u, ts col num %u. tid %u pid %u", i, j,
                      cur_key_entry_max_height, ts_vec.size(), id_, pid_);
            }
        } else {
            // unavaildable
            for (uint32_t j = 0; j < seg_cnt_; j++) {
                seg_arr[j] = new IOTSegment(cur_key_entry_max_height);
                PDLOG(INFO, "init %u, %u segment. height %u tid %u pid %u", i, j, cur_key_entry_max_height, id_, pid_);
            }
        }
        segments_[i] = seg_arr;
        key_entry_max_height_ = cur_key_entry_max_height;
    }
    LOG(INFO) << "init iot table name " << name_ << ", id " << id_ << ", pid " << pid_ << ", seg_cnt " << seg_cnt_;
    return true;
}

bool IndexOrganizedTable::Put(const std::string& pk, uint64_t time, const char* data, uint32_t size) {
    uint32_t seg_idx = SegIdx(pk);
    Segment* segment = GetSegment(0, seg_idx);
    if (segment == nullptr) {
        return false;
    }
    Slice spk(pk);
    segment->Put(spk, time, data, size);
    record_byte_size_.fetch_add(GetRecordSize(size));
    return true;
}

absl::Status IndexOrganizedTable::Put(uint64_t time, const std::string& value, const Dimensions& dimensions,
                                      bool put_if_absent) {
    if (dimensions.empty()) {
        return absl::InvalidArgumentError(absl::StrCat(id_, ".", pid_, ": empty dimension"));
    }
    // inner index pos: -1 means invalid, so it's positive in inner_index_key_map
    std::map<int32_t, Slice> inner_index_key_map;
    std::pair<int32_t, std::string> cidx_inner_key_pair{-1, ""};
    std::vector<int32_t> secondary_inners;
    for (auto iter = dimensions.begin(); iter != dimensions.end(); iter++) {
        int32_t inner_pos = table_index_.GetInnerIndexPos(iter->idx());
        if (inner_pos < 0) {
            return absl::InvalidArgumentError(absl::StrCat(id_, ".", pid_, ": invalid dimension idx ", iter->idx()));
        }
        if (iter->idx() == 0) {
            cidx_inner_key_pair = {inner_pos, iter->key()};
        }
        inner_index_key_map.emplace(inner_pos, iter->key());
    }

    const int8_t* data = reinterpret_cast<const int8_t*>(value.data());
    std::string uncompress_data;
    uint32_t data_length = value.length();
    if (GetCompressType() == openmldb::type::kSnappy) {
        snappy::Uncompress(value.data(), value.size(), &uncompress_data);
        data = reinterpret_cast<const int8_t*>(uncompress_data.data());
        data_length = uncompress_data.length();
    }
    if (data_length < codec::HEADER_LENGTH) {
        return absl::InvalidArgumentError(absl::StrCat(id_, ".", pid_, ": invalid value"));
    }
    uint8_t version = codec::RowView::GetSchemaVersion(data);
    auto decoder = GetVersionDecoder(version);
    if (decoder == nullptr) {
        return absl::InvalidArgumentError(absl::StrCat(id_, ".", pid_, ": invalid schema version ", version));
    }
    std::optional<uint64_t> clustered_tsv;
    std::map<uint32_t, std::map<int32_t, uint64_t>> ts_value_map;
    // we need two ref cnt
    // 1. clustered and covering: put row ->DataBlock(i)
    // 2. secondary: put pkeys+pts -> DataBlock(j)
    uint32_t real_ref_cnt = 0, secondary_ref_cnt = 0;
    // cidx_inner_key_pair can get the clustered index
    for (const auto& kv : inner_index_key_map) {
        auto inner_index = table_index_.GetInnerIndex(kv.first);
        if (!inner_index) {
            return absl::InvalidArgumentError(absl::StrCat(id_, ".", pid_, ": invalid inner index pos ", kv.first));
        }
        std::map<int32_t, uint64_t> ts_map;
        for (const auto& index_def : inner_index->GetIndex()) {
            if (!index_def->IsReady()) {
                continue;
            }
            auto ts_col = index_def->GetTsColumn();
            if (ts_col) {
                int64_t ts = 0;
                if (ts_col->IsAutoGenTs()) {
                    // clustered index still use current time to ttl and delete iter, we'll check time series size if ts
                    // is auto gen
                    ts = time;
                } else if (decoder->GetInteger(data, ts_col->GetId(), ts_col->GetType(), &ts) != 0) {
                    return absl::InvalidArgumentError(absl::StrCat(id_, ".", pid_, ": get ts failed"));
                }
                if (ts < 0) {
                    return absl::InvalidArgumentError(absl::StrCat(id_, ".", pid_, ": ts is negative ", ts));
                }
                // TODO(hw): why uint32_t to int32_t?
                ts_map.emplace(ts_col->GetId(), ts);

                if (index_def->IsSecondaryIndex()) {
                    secondary_ref_cnt++;
                } else {
                    real_ref_cnt++;
                }
                if (index_def->IsClusteredIndex()) {
                    clustered_tsv = ts;
                }
            }
        }
        if (!ts_map.empty()) {
            ts_value_map.emplace(kv.first, std::move(ts_map));
        }
    }
    if (ts_value_map.empty()) {
        return absl::InvalidArgumentError(absl::StrCat(id_, ".", pid_, ": empty ts value map"));
    }
    // it's ok to have no clustered/covering put or no secondary put, put will be applyed on other pid
    // but if no clustered/covering put and no secondary put, it's invalid, check it in put-loop
    DataBlock* cblock = nullptr;
    DataBlock* sblock = nullptr;
    if (real_ref_cnt > 0) {
        cblock = new DataBlock(real_ref_cnt, value.c_str(), value.length());  // hard copy
    }
    if (secondary_ref_cnt > 0) {
        // dimensions may not contain cidx, but we need cidx pkeys+pts for secondary index
        // if contains, just use the key; if not, extract from value
        if (cidx_inner_key_pair.first == -1) {
            DLOG(INFO) << "cidx not in dimensions, extract from value";
            auto cidx = table_index_.GetIndex(0);
            auto hint = MakePkeysHint(table_meta_->column_desc(), table_meta_->column_key(0));
            if (hint.empty()) {
                return absl::InvalidArgumentError(absl::StrCat(id_, ".", pid_, ": cidx pkeys hint empty"));
            }
            cidx_inner_key_pair.second =
                ExtractPkeys(table_meta_->column_key(0), (int8_t*)value.c_str(), *decoder, hint);
            if (cidx_inner_key_pair.second.empty()) {
                return absl::InvalidArgumentError(absl::StrCat(id_, ".", pid_, ": cidx pkeys+pts extract failed"));
            }
            DLOG_ASSERT(!clustered_tsv) << "clustered ts should not be set too";
            auto ts_col = cidx->GetTsColumn();
            if (!ts_col) {
                return absl::InvalidArgumentError(absl::StrCat(id_, ".", pid_, ":no ts column in cidx"));
            }
            int64_t ts = 0;
            if (ts_col->IsAutoGenTs()) {
                // clustered index still use current time to ttl and delete iter, we'll check time series size if ts is
                // auto gen
                ts = time;
            } else if (decoder->GetInteger(data, ts_col->GetId(), ts_col->GetType(), &ts) != 0) {
                return absl::InvalidArgumentError(absl::StrCat(id_, ".", pid_, ": get ts failed"));
            }
            if (ts < 0) {
                return absl::InvalidArgumentError(absl::StrCat(id_, ".", pid_, ": ts is negative ", ts));
            }
            clustered_tsv = ts;
        }
        auto pkeys_pts = PackPkeysAndPts(cidx_inner_key_pair.second, clustered_tsv.value());
        if (GetCompressType() == type::kSnappy) {  // sidx iterator will uncompress when getting pkeys+pts
            std::string val;
            ::snappy::Compress(pkeys_pts.c_str(), pkeys_pts.length(), &val);
            sblock = new DataBlock(secondary_ref_cnt, val.c_str(), val.length());
        } else {
            sblock = new DataBlock(secondary_ref_cnt, pkeys_pts.c_str(), pkeys_pts.length());  // hard copy
        }
    }
    DLOG(INFO) << "put iot table " << id_ << "." << pid_ << " key+ts " << cidx_inner_key_pair.second << " - "
               << (clustered_tsv ? std::to_string(clustered_tsv.value()) : "-1") << ", real ref cnt " << real_ref_cnt
               << " secondary ref cnt " << secondary_ref_cnt;

    for (const auto& kv : inner_index_key_map) {
        auto iter = ts_value_map.find(kv.first);
        if (iter == ts_value_map.end()) {
            continue;
        }
        uint32_t seg_idx = SegIdx(kv.second.ToString());
        auto iot_segment = dynamic_cast<IOTSegment*>(GetSegment(kv.first, seg_idx));
        // TODO(hw): put if absent unsupportted
        if (put_if_absent) {
            return absl::InvalidArgumentError(absl::StrCat(id_, ".", pid_, ": iot put if absent is not supported"));
        }
        // clustered segment should be dedup and update will trigger all index update(impl in cli router)
        if (!iot_segment->Put(kv.second, iter->second, cblock, sblock, false)) {
            // even no put_if_absent, return false if exists or wrong
            return absl::AlreadyExistsError("data exists or wrong");
        }
    }
    // cblock and sblock both will sub record_byte_size_ when delete, so add them all
    // TODO(hw): test for cal
    if (real_ref_cnt > 0) {
        record_byte_size_.fetch_add(GetRecordSize(cblock->size));
    }
    if (secondary_ref_cnt > 0) {
        record_byte_size_.fetch_add(GetRecordSize(sblock->size));
    }

    return absl::OkStatus();
}

absl::Status IndexOrganizedTable::CheckDataExists(uint64_t tsv, const Dimensions& dimensions) {
    // get cidx dim
    if (dimensions.empty()) {
        return absl::InvalidArgumentError(absl::StrCat(id_, ".", pid_, ": empty dimension"));
    }
    // inner index pos: -1 means invalid, so it's positive in inner_index_key_map
    std::pair<int32_t, std::string> cidx_inner_key_pair{-1, ""};
    for (auto iter = dimensions.begin(); iter != dimensions.end(); iter++) {
        int32_t inner_pos = table_index_.GetInnerIndexPos(iter->idx());
        if (inner_pos < 0) {
            return absl::InvalidArgumentError(absl::StrCat(id_, ".", pid_, ": invalid dimension idx ", iter->idx()));
        }
        if (iter->idx() == 0) {
            cidx_inner_key_pair = {inner_pos, iter->key()};
        }
    }
    if (cidx_inner_key_pair.first == -1) {
        return absl::InvalidArgumentError(absl::StrCat(id_, ".", pid_, ": cidx not found"));
    }
    auto cidx = table_index_.GetIndex(0);
    if (!cidx->IsReady()) {
        return absl::InvalidArgumentError(absl::StrCat(id_, ".", pid_, ": cidx is not ready"));
    }
    auto ts_col = cidx->GetTsColumn();
    if (!ts_col) {
        return absl::InvalidArgumentError(absl::StrCat(id_, ".", pid_, ": no ts column"));
    }
    DLOG(INFO) << "check iot table " << id_ << "." << pid_ << " key+ts " << cidx_inner_key_pair.second << " - " << tsv
               << ", on index " << cidx->GetName() << " ts col " << ts_col->GetId();

    uint32_t seg_idx = SegIdx(cidx_inner_key_pair.second);
    auto iot_segment = dynamic_cast<IOTSegment*>(GetSegment(cidx_inner_key_pair.first, seg_idx));
    // ts id -> ts value
    return iot_segment->CheckKeyExists(cidx_inner_key_pair.second, {{ts_col->GetId(), tsv}});
}

// <pkey_col_name, (idx_in_row, type)>, error if empty
std::map<std::string, std::pair<uint32_t, type::DataType>> MakePkeysHint(
    const codec::Schema& schema, const common::ColumnKey& cidx_ck) {
    if (cidx_ck.col_name().empty()) {
        LOG(WARNING) << "empty cidx column key";
        return {};
    }
    // pkey col idx in row
    std::set<std::string> pkey_set;
    for (int i = 0; i < cidx_ck.col_name().size(); i++) {
        pkey_set.insert(cidx_ck.col_name().Get(i));
    }
    if (pkey_set.empty()) {
        LOG(WARNING) << "empty pkey set";
        return {};
    }
    if (pkey_set.size() != static_cast<std::set<std::string>::size_type>(cidx_ck.col_name().size())) {
        LOG(WARNING) << "pkey set size not equal to cidx pkeys size";
        return {};
    }
    std::map<std::string, std::pair<uint32_t, type::DataType>> col_idx;
    for (int i = 0; i < schema.size(); i++) {
        if (pkey_set.find(schema.Get(i).name()) != pkey_set.end()) {
            col_idx[schema.Get(i).name()] = {i, schema.Get(i).data_type()};
        }
    }
    if (col_idx.size() != pkey_set.size()) {
        LOG(WARNING) << "col idx size not equal to cidx pkeys size";
        return {};
    }
    return col_idx;
}

// error if empty
std::string MakeDeleteSQL(
    const std::string& db, const std::string& name, const common::ColumnKey& cidx_ck, const int8_t* values, uint64_t ts,
    const codec::RowView& row_view, const std::map<std::string, std::pair<uint32_t, type::DataType>>& col_idx) {
    auto sql_prefix = absl::StrCat("delete from ", db, ".", name, " where ");
    std::string cond;
    for (int i = 0; i < cidx_ck.col_name().size(); i++) {
        // append primary keys, pkeys in dimension are encoded, so we should get them from raw value
        // split can't work if string has `|`
        auto& col_name = cidx_ck.col_name().Get(i);
        auto col = col_idx.find(col_name);
        if (col == col_idx.end()) {
            LOG(WARNING) << "col " << col_name << " not found in col idx";
            return "";
        }
        std::string val;
        row_view.GetStrValue(values, col->second.first, &val);
        if (!cond.empty()) {
            absl::StrAppend(&cond, " and ");
        }
        // TODO(hw): string should add quotes how about timestamp?
        // check existence before, so here we skip
        absl::StrAppend(&cond, col_name);
        if (auto t = col->second.second; t == type::kVarchar || t == type::kString) {
            absl::StrAppend(&cond, "=\"", val, "\"");
        } else {
            absl::StrAppend(&cond, "=", val);
        }
    }
    // ts must be integer, won't be string
    if (!cidx_ck.ts_name().empty() && cidx_ck.ts_name() != storage::DEFAULT_TS_COL_NAME) {
        if (!cond.empty()) {
            absl::StrAppend(&cond, " and ");
        }
        absl::StrAppend(&cond, cidx_ck.ts_name(), "=", std::to_string(ts));
    }
    auto sql = absl::StrCat(sql_prefix, cond, ";");
    // TODO(hw): if delete failed, we can't revert. And if sidx skeys+sts doesn't change, no need to delete and
    // then insert
    DLOG(INFO) << "delete sql " << sql;
    return sql;
}

// error if empty
std::string ExtractPkeys(
    const common::ColumnKey& cidx_ck, const int8_t* values, const codec::RowView& row_view,
    const std::map<std::string, std::pair<uint32_t, type::DataType>>& col_idx) {
    // join with |
    std::vector<std::string> pkeys;
    for (int i = 0; i < cidx_ck.col_name().size(); i++) {
        auto& col_name = cidx_ck.col_name().Get(i);
        auto col = col_idx.find(col_name);
        if (col == col_idx.end()) {
            LOG(WARNING) << "col " << col_name << " not found in col idx";
            return "";
        }
        std::string val;
        row_view.GetStrValue(values, col->second.first, &val);
        pkeys.push_back(val);
    }
    return absl::StrJoin(pkeys, "|");
}

// index gc should try to do ExecuteGc for each waiting segment, but if some segments are gc before, we should release
// them so it will be a little complex
// should run under lock
absl::Status IndexOrganizedTable::ClusteredIndexGCByDelete(const std::shared_ptr<sdk::SQLRouter>& router) {
    auto cur_index = table_index_.GetIndex(0);
    if (!cur_index) {
        return absl::FailedPreconditionError(
            absl::StrCat("cidx def is null for ", id_, ".", pid_));  // why index is null?
    }
    if (!cur_index->IsClusteredIndex()) {
        return absl::InternalError(absl::StrCat("cidx is not clustered for ", id_, ".", pid_));  // immpossible
    }
    if (!cur_index->IsReady()) {
        return absl::FailedPreconditionError(
            absl::StrCat("cidx is not ready for ", id_, ".", pid_, ", status ", cur_index->GetStatus()));
    }
    auto& ts_col = cur_index->GetTsColumn();
    // sometimes index def is valid, but ts_col is nullptr? protect it
    if (!ts_col) {
        return absl::FailedPreconditionError(
            absl::StrCat("no ts col of cidx for ", id_, ".", pid_));  // current time ts can be get too
    }
    // clustered index grep all entries or less to delete(it's simpler to run delete sql)
    // not the real gc, so don't change index status
    auto i = cur_index->GetId();
    std::map<uint32_t, TTLSt> ttl_st_map;
    // only set cidx
    ttl_st_map.emplace(ts_col->GetId(), *cur_index->GetTTL());
    GCEntryInfo info;  // not thread safe
    for (uint32_t j = 0; j < seg_cnt_; j++) {
        uint64_t seg_gc_time = ::baidu::common::timer::get_micros() / 1000;
        Segment* segment = segments_[i][j];
        auto iot_segment = dynamic_cast<IOTSegment*>(segment);
        iot_segment->GrepGCEntry(ttl_st_map, &info);
        seg_gc_time = ::baidu::common::timer::get_micros() / 1000 - seg_gc_time;
        PDLOG(INFO, "grep cidx segment[%u][%u] gc entries done consumed %lu for table %s tid %u pid %u", i, j,
              seg_gc_time, name_.c_str(), id_, pid_);
    }
    // delete entries by sql
    if (info.Size() > 0) {
        LOG(INFO) << "delete cidx " << info.Size() << " entries by sql";
        auto meta = GetTableMeta();
        auto cols = meta->column_desc();  // copy
        codec::RowView row_view(cols);
        auto hint = MakePkeysHint(cols, meta->column_key(0));
        if (hint.empty()) {
            return absl::InternalError("make pkeys hint failed");
        }
        for (size_t i = 0; i < info.Size(); i++) {
            auto& keys_ts = info.GetEntries()[i];
            auto values = keys_ts.second;  // get pkeys from values
            auto ts = keys_ts.first;
            auto sql =
                MakeDeleteSQL(GetDB(), GetName(), meta->column_key(0), (int8_t*)values->data, ts, row_view, hint);
            // TODO(hw): if delete failed, we can't revert. And if sidx skeys+sts doesn't change, no need to delete and
            // then insert
            if (sql.empty()) {
                return absl::InternalError("make delete sql failed");
            }
            // delete will move node to node cache, it's alive, so GCEntryInfo can unref it
            hybridse::sdk::Status status;
            router->ExecuteSQL(sql, &status);
            if (!status.IsOK()) {
                return absl::InternalError("execute sql failed " + status.ToString());
            }
        }
    }

    return absl::OkStatus();
}

// TODO(hw): don't refactor with MemTable, make MemTable stable
void IndexOrganizedTable::SchedGCByDelete(const std::shared_ptr<sdk::SQLRouter>& router) {
    std::lock_guard<std::mutex> lock(gc_lock_);
    uint64_t consumed = ::baidu::common::timer::get_micros();
    if (!enable_gc_.load(std::memory_order_relaxed)) {
        LOG(INFO) << "iot table " << name_ << "[" << id_ << "." << pid_ << "] gc disabled";
        return;
    }
    LOG(INFO) << "iot table " << name_ << "[" << id_ << "." << pid_ << "] start making gc";
    // gc cidx first, it'll delete on all indexes
    auto st = ClusteredIndexGCByDelete(router);
    if (!st.ok()) {
        LOG(WARNING) << "cidx gc by delete error: " << st.ToString();
    }
    // TODO how to check the record byte size?
    uint64_t gc_idx_cnt = 0;
    uint64_t gc_record_byte_size = 0;
    auto inner_indexs = table_index_.GetAllInnerIndex();
    for (uint32_t i = 0; i < inner_indexs->size(); i++) {
        const std::vector<std::shared_ptr<IndexDef>>& real_index = inner_indexs->at(i)->GetIndex();
        std::map<uint32_t, TTLSt> ttl_st_map;
        bool need_gc = true;
        size_t deleted_num = 0;
        std::vector<size_t> deleting_pos;
        for (size_t pos = 0; pos < real_index.size(); pos++) {
            auto cur_index = real_index[pos];
            auto ts_col = cur_index->GetTsColumn();
            if (ts_col) {
                ttl_st_map.emplace(ts_col->GetId(), *(cur_index->GetTTL()));
            }
            if (cur_index->GetStatus() == IndexStatus::kWaiting) {
                cur_index->SetStatus(IndexStatus::kDeleting);
                need_gc = false;
            } else if (cur_index->GetStatus() == IndexStatus::kDeleting) {
                deleting_pos.push_back(pos);
            } else if (cur_index->GetStatus() == IndexStatus::kDeleted) {
                deleted_num++;
            }
        }
        if (!deleting_pos.empty()) {
            if (segments_[i] != nullptr) {
                for (uint32_t k = 0; k < seg_cnt_; k++) {
                    if (segments_[i][k] != nullptr) {
                        StatisticsInfo statistics_info(segments_[i][k]->GetTsCnt());
                        if (real_index.size() == 1 || deleting_pos.size() + deleted_num == real_index.size()) {
                            segments_[i][k]->ReleaseAndCount(&statistics_info);
                        } else {
                            segments_[i][k]->ReleaseAndCount(deleting_pos, &statistics_info);
                        }
                        gc_idx_cnt += statistics_info.GetTotalCnt();
                        gc_record_byte_size += statistics_info.record_byte_size;
                        LOG(INFO) << "release segment[" << i << "][" << k << "] done, gc record cnt "
                                  << statistics_info.GetTotalCnt() << ", gc record byte size "
                                  << statistics_info.record_byte_size;
                    }
                }
            }
            for (auto pos : deleting_pos) {
                real_index[pos]->SetStatus(IndexStatus::kDeleted);
            }
            deleted_num += deleting_pos.size();
        }
        if (!need_gc) {
            continue;
        }
        // skip cidx gc in segment, gcfreelist shouldn't be skiped, so we don't change the condition
        if (deleted_num == real_index.size() || ttl_st_map.empty()) {
            continue;
        }
        for (uint32_t j = 0; j < seg_cnt_; j++) {
            uint64_t seg_gc_time = ::baidu::common::timer::get_micros() / 1000;
            auto segment = dynamic_cast<IOTSegment*>(segments_[i][j]);
            StatisticsInfo statistics_info(segment->GetTsCnt());
            segment->IncrGcVersion();
            segment->GcFreeList(&statistics_info);
            // don't gc in cidx, it's not a good way to impl, refactor later
            segment->ExecuteGc(ttl_st_map, &statistics_info, segment->ClusteredTs());
            gc_idx_cnt += statistics_info.GetTotalCnt();
            gc_record_byte_size += statistics_info.record_byte_size;
            seg_gc_time = ::baidu::common::timer::get_micros() / 1000 - seg_gc_time;
            VLOG(1) << "gc segment[" << i << "][" << j << "] done, consumed time " << seg_gc_time << "ms for table "
                    << name_ << "[" << id_ << "." << pid_ << "], statistics_info: [" << statistics_info.DebugString()
                    << "]";
        }
    }
    consumed = ::baidu::common::timer::get_micros() - consumed;
    LOG(INFO) << "record byte size before gc: " << record_byte_size_.load()
              << ", gc record byte size: " << gc_record_byte_size << ", gc idx cnt: " << gc_idx_cnt
              << ", gc consumed: " << consumed / 1000 << " ms";
    record_byte_size_.fetch_sub(gc_record_byte_size, std::memory_order_relaxed);
    UpdateTTL();
    LOG(INFO) << "update ttl done";
}

bool IndexOrganizedTable::AddIndexToTable(const std::shared_ptr<IndexDef>& index_def) {
    std::vector<uint32_t> ts_vec = {index_def->GetTsColumn()->GetId()};
    uint32_t inner_id = index_def->GetInnerPos();
    Segment** seg_arr = new Segment*[seg_cnt_];
    for (uint32_t j = 0; j < seg_cnt_; j++) {
        seg_arr[j] = new IOTSegment(FLAGS_absolute_default_skiplist_height, ts_vec, {index_def->GetIndexType()});
        LOG(INFO) << "init iot segment inner_ts" << inner_id << "." << j << " for table " << name_ << "[" << id_ << "."
                  << pid_ << "], height " << FLAGS_absolute_default_skiplist_height << ", ts col num " << ts_vec.size()
                  << ", type " << IndexType_Name(index_def->GetIndexType());
    }
    segments_[inner_id] = seg_arr;
    return true;
}

}  // namespace openmldb::storage
