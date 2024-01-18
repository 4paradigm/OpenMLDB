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

#include "storage/disk_table.h"
#include <snappy.h>
#include <utility>
#include "absl/cleanup/cleanup.h"
#include "base/file_util.h"
#include "base/glog_wrapper.h"
#include "gflags/gflags.h"
#include "storage/disk_table_iterator.h"

DECLARE_bool(disable_wal);

DECLARE_string(file_compression);
DECLARE_uint32(block_cache_mb);
DECLARE_uint32(write_buffer_mb);
DECLARE_uint32(block_cache_shardbits);
DECLARE_bool(verify_compression);
DECLARE_int32(disk_gc_interval);
DECLARE_uint32(max_log_file_size);
DECLARE_uint32(keep_log_file_num);

namespace openmldb {
namespace storage {

static rocksdb::Options ssd_option_template;
static rocksdb::Options hdd_option_template;
static bool options_template_initialized = false;

DiskTable::DiskTable(const std::string& name, uint32_t id, uint32_t pid, const std::map<std::string, uint32_t>& mapping,
                     uint64_t ttl, ::openmldb::type::TTLType ttl_type, ::openmldb::common::StorageMode storage_mode,
                     const std::string& table_path)
    : Table(storage_mode, name, id, pid, ttl * 60 * 1000, true, 0, mapping, ttl_type,
            ::openmldb::type::CompressType::kNoCompress),
      write_opts_(),
      offset_(0),
      table_path_(table_path) {
    if (!options_template_initialized) {
        initOptionTemplate();
    }
    write_opts_.disableWAL = FLAGS_disable_wal;
    db_ = nullptr;
}

DiskTable::DiskTable(const ::openmldb::api::TableMeta& table_meta, const std::string& table_path)
    : Table(table_meta.storage_mode(), table_meta.name(), table_meta.tid(), table_meta.pid(), 0, true, 0,
            std::map<std::string, uint32_t>(), ::openmldb::type::TTLType::kAbsoluteTime,
            ::openmldb::type::CompressType::kNoCompress),
      write_opts_(),
      offset_(0),
      table_path_(table_path) {
    if (!options_template_initialized) {
        initOptionTemplate();
    }
    diskused_ = 0;
    write_opts_.disableWAL = FLAGS_disable_wal;
    db_ = nullptr;
    table_meta_ = std::make_shared<::openmldb::api::TableMeta>(table_meta);
}

DiskTable::~DiskTable() {
    for (auto handle : cf_hs_) {
        delete handle;
    }
    if (db_ != nullptr) {
        db_->Close();
        delete db_;
    }
}

void DiskTable::initOptionTemplate() {
    std::shared_ptr<rocksdb::Cache> cache = rocksdb::NewLRUCache(FLAGS_block_cache_mb << 20,
                                                                 FLAGS_block_cache_shardbits);  // Can be set by flags
    // SSD options template
    ssd_option_template.max_open_files = -1;
    ssd_option_template.env->SetBackgroundThreads(1, rocksdb::Env::Priority::HIGH);  // flush threads
    ssd_option_template.env->SetBackgroundThreads(4, rocksdb::Env::Priority::LOW);   // compaction threads
    ssd_option_template.memtable_prefix_bloom_size_ratio = 0.02;
    ssd_option_template.compaction_style = rocksdb::kCompactionStyleLevel;
    ssd_option_template.write_buffer_size = FLAGS_write_buffer_mb << 20;  // L0 file size = write_buffer_size
    ssd_option_template.level0_file_num_compaction_trigger = 1 << 4;      // L0 total size = write_buffer_size * 16
    ssd_option_template.level0_slowdown_writes_trigger = 1 << 5;
    ssd_option_template.level0_stop_writes_trigger = 1 << 6;
    ssd_option_template.max_bytes_for_level_base =
        ssd_option_template.write_buffer_size *
        ssd_option_template.level0_file_num_compaction_trigger;  // L1 size ~ L0 total size
    ssd_option_template.target_file_size_base =
        ssd_option_template.max_bytes_for_level_base >> 4;  // number of L1 files = 16

    rocksdb::BlockBasedTableOptions table_options;
    // table_options.cache_index_and_filter_blocks = true;
    // table_options.pin_l0_filter_and_index_blocks_in_cache = true;
    table_options.block_cache = cache;
    // table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10,
    // false));
    table_options.whole_key_filtering = false;
    table_options.block_size = 256 << 10;
    table_options.use_delta_encoding = false;
#ifdef PZFPGA_ENABLE
    if (FLAGS_file_compression.compare("pz") == 0) {
        PDLOG(INFO, "initOptionTemplate PZ compression enabled");
        ssd_option_template.compression = rocksdb::kPZCompression;
    } else if (FLAGS_file_compression.compare("lz4") == 0) {
        PDLOG(INFO, "initOptionTemplate lz4 compression enabled");
        ssd_option_template.compression = rocksdb::kLZ4Compression;
    } else if (FLAGS_file_compression.compare("zlib") == 0) {
        PDLOG(INFO, "initOptionTemplate zlib compression enabled");
        ssd_option_template.compression = rocksdb::kZlibCompression;
    } else {
        PDLOG(INFO, "initOptionTemplate NO compression enabled");
        ssd_option_template.compression = rocksdb::kNoCompression;
    }
    if (FLAGS_verify_compression) table_options.verify_compression = true;
#endif
    ssd_option_template.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));
    // HDD options template
    hdd_option_template.max_open_files = -1;
    hdd_option_template.env->SetBackgroundThreads(1, rocksdb::Env::Priority::HIGH);  // flush threads
    hdd_option_template.env->SetBackgroundThreads(1, rocksdb::Env::Priority::LOW);   // compaction threads
    hdd_option_template.memtable_prefix_bloom_size_ratio = 0.02;
    hdd_option_template.optimize_filters_for_hits = true;
    hdd_option_template.level_compaction_dynamic_level_bytes = true;
    hdd_option_template.max_file_opening_threads =
        1;  // set to the number of disks on which the db root folder is mounted
    hdd_option_template.compaction_readahead_size = 16 << 20;
    hdd_option_template.new_table_reader_for_compaction_inputs = true;
    hdd_option_template.compaction_style = rocksdb::kCompactionStyleLevel;
    hdd_option_template.level0_file_num_compaction_trigger = 10;
    hdd_option_template.level0_slowdown_writes_trigger = 20;
    hdd_option_template.level0_stop_writes_trigger = 40;
    hdd_option_template.write_buffer_size = 256 << 20;
    hdd_option_template.target_file_size_base = 256 << 20;
    hdd_option_template.max_bytes_for_level_base = 1024 << 20;
    hdd_option_template.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));

    options_template_initialized = true;
}

bool DiskTable::InitColumnFamilyDescriptor() {
    cf_ds_.clear();
    cf_ds_.push_back(
        rocksdb::ColumnFamilyDescriptor(rocksdb::kDefaultColumnFamilyName, rocksdb::ColumnFamilyOptions()));
    if (storage_mode_ == ::openmldb::common::StorageMode::kSSD) {
        options_ = ssd_option_template;
    } else {
        options_ = hdd_option_template;
    }
    options_.max_log_file_size = FLAGS_max_log_file_size;
    options_.keep_log_file_num = FLAGS_keep_log_file_num;
    auto inner_indexs = table_index_.GetAllInnerIndex();
    for (const auto& inner_index : *inner_indexs) {
        rocksdb::Options cur_options = options_;
        bool use_compaction_filter = false;
        for (const auto& index_def : inner_index->GetIndex()) {
            if (index_def->GetTTLType() == ::openmldb::storage::TTLType::kAbsoluteTime) {
                cur_options.periodic_compaction_seconds = FLAGS_disk_gc_interval * 60;
                use_compaction_filter = true;
                break;
            }
        }
        rocksdb::ColumnFamilyOptions cfo(cur_options);
        cfo.comparator = &cmp_;
        cfo.prefix_extractor.reset(new KeyTsPrefixTransform());
        if (use_compaction_filter) {
            cfo.compaction_filter_factory = std::make_shared<AbsoluteTTLFilterFactory>(inner_index);
        }
        const auto& indexs = inner_index->GetIndex();
        auto index_def = indexs.front();
        cf_ds_.push_back(rocksdb::ColumnFamilyDescriptor(index_def->GetName(), cfo));
        DEBUGLOG("add cf_name %s. tid %u pid %u", index_def->GetName().c_str(), id_, pid_);
    }
    return true;
}

bool DiskTable::Init() {
    if (!InitFromMeta()) {
        return false;
    }
    InitColumnFamilyDescriptor();
    std::string path = table_path_ + "/data";
    if (!openmldb::base::IsExists(path)) {
        PDLOG(INFO, "Create new disk table with path %s", path);
    }

    if (!::openmldb::base::MkdirRecur(path)) {
        PDLOG(WARNING, "fail to create path %s", path.c_str());
        return false;
    }
    options_.create_if_missing = true;
    options_.error_if_exists = false;
    options_.create_missing_column_families = true;
    rocksdb::Status s = rocksdb::DB::Open(options_, path, cf_ds_, &cf_hs_, &db_);
    if (!s.ok()) {
        PDLOG(WARNING, "rocksdb open failed. tid %u pid %u error %s", id_, pid_, s.ToString().c_str());
        return false;
    }
    PDLOG(INFO, "Open DB. tid %u pid %u ColumnFamilyHandle size %u with data path %s",
        id_, pid_, cf_hs_.size(), path.c_str());
    cf_hs_.resize(MAX_INDEX_NUM, nullptr);
    return true;
}

bool DiskTable::Put(const std::string& pk, uint64_t time, const char* data, uint32_t size) {
    rocksdb::Status s;
    std::string combine_key = CombineKeyTs(rocksdb::Slice(pk), time);
    rocksdb::Slice spk = rocksdb::Slice(combine_key);
    s = db_->Put(write_opts_, cf_hs_[1], spk, rocksdb::Slice(data, size));
    if (s.ok()) {
        offset_.fetch_add(1, std::memory_order_relaxed);
        return true;
    } else {
        DEBUGLOG("Put failed. tid %u pid %u msg %s", id_, pid_, s.ToString().c_str());
        return false;
    }
}

bool DiskTable::Put(uint64_t time, const std::string& value, const Dimensions& dimensions) {
    const int8_t* data = reinterpret_cast<const int8_t*>(value.data());
    std::string uncompress_data;
    if (GetCompressType() == openmldb::type::kSnappy) {
        snappy::Uncompress(value.data(), value.size(), &uncompress_data);
        data = reinterpret_cast<const int8_t*>(uncompress_data.data());
    }
    uint8_t version = codec::RowView::GetSchemaVersion(data);
    auto decoder = GetVersionDecoder(version);
    if (decoder == nullptr) {
        PDLOG(WARNING, "invalid schema version %u, tid %u pid %u", version, id_, pid_);
        return false;
    }
    rocksdb::WriteBatch batch;
    for (auto it = dimensions.begin(); it != dimensions.end(); ++it) {
        auto index_def = table_index_.GetIndex(it->idx());
        if (!index_def || !index_def->IsReady()) {
            PDLOG(WARNING, "failed putting key %s to dimension %u in table tid %u pid %u", it->key().c_str(),
                  it->idx(), id_, pid_);
        }
        int32_t inner_pos = table_index_.GetInnerIndexPos(it->idx());
        auto inner_index = table_index_.GetInnerIndex(inner_pos);
        auto ts_col = index_def->GetTsColumn();
        std::string combine_key;
        if (ts_col) {
            int64_t ts = 0;
            if (ts_col->IsAutoGenTs()) {
                ts = time;
            } else if (decoder->GetInteger(data, ts_col->GetId(), ts_col->GetType(), &ts) != 0) {
                PDLOG(WARNING, "get ts failed. tid %u pid %u", id_, pid_);
                return false;
            }
            if (ts < 0) {
                PDLOG(WARNING, "ts %ld is negative. tid %u pid %u", ts, id_, pid_);
                return false;
            }
            if (inner_index->GetIndex().size() > 1) {
                combine_key = CombineKeyTs(it->key(), ts, ts_col->GetId());
            } else {
                combine_key = CombineKeyTs(it->key(), ts);
            }
            rocksdb::Slice spk = rocksdb::Slice(combine_key);
            batch.Put(cf_hs_[inner_pos + 1], spk, value);
        }
    }
    auto s = db_->Write(write_opts_, &batch);
    if (s.ok()) {
        offset_.fetch_add(1, std::memory_order_relaxed);
        return true;
    } else {
        DEBUGLOG("Put failed. tid %u pid %u msg %s", id_, pid_, s.ToString().c_str());
        return false;
    }
}

bool DiskTable::Delete(const ::openmldb::api::LogEntry& entry) {
    std::optional<uint64_t> start_ts = entry.has_ts() ? std::optional<uint64_t>(entry.ts()) : std::nullopt;
    std::optional<uint64_t> end_ts = entry.has_end_ts() ? std::optional<uint64_t>(entry.end_ts()) : std::nullopt;
    if (entry.dimensions_size() > 0) {
        for (const auto& dimension : entry.dimensions()) {
            if (!Delete(dimension.idx(), dimension.key(), start_ts, end_ts)) {
                return false;
            }
        }
        return true;
    } else {
        for (const auto& index : table_index_.GetAllIndex()) {
            if (!index || !index->IsReady()) {
                continue;
            }
            auto ts_col = index->GetTsColumn();
            if (!ts_col->IsAutoGenTs() && ts_col->GetName() != entry.ts_name()) {
                continue;
            }
            uint32_t idx = index->GetId();
            std::shared_ptr<TraverseIterator> iter(NewTraverseIterator(idx));
            iter->SeekToFirst();
            while (iter->Valid()) {
                Delete(idx, iter->GetPK(), start_ts, end_ts);
                iter->NextPK();
            }
        }
    }
    return true;
}

bool DiskTable::Delete(uint32_t idx, const std::string& pk,
        const std::optional<uint64_t>& start_ts, const std::optional<uint64_t>& end_ts) {
    auto index_def = table_index_.GetIndex(idx);
    if (!index_def || !index_def->IsReady()) {
        return false;
    }
    uint64_t real_start_ts = start_ts.has_value() ? start_ts.value() : UINT64_MAX;
    uint64_t real_end_ts = end_ts.has_value() ? end_ts.value() : 0;
    std::string combine_key1;
    std::string combine_key2;
    int32_t inner_pos = index_def->GetInnerPos();
    auto inner_index = table_index_.GetInnerIndex(inner_pos);
    if (inner_index && inner_index->GetIndex().size() > 1) {
        auto ts_col = index_def->GetTsColumn();
        if (!ts_col) {
            return false;
        }
        combine_key1 = CombineKeyTs(pk, real_start_ts, ts_col->GetId());
        combine_key2 = CombineKeyTs(pk, real_end_ts, ts_col->GetId());
    } else {
        combine_key1 = CombineKeyTs(pk, real_start_ts);
        combine_key2 = CombineKeyTs(pk, real_end_ts);
    }
    rocksdb::WriteBatch batch;
    batch.DeleteRange(cf_hs_[inner_pos + 1], rocksdb::Slice(combine_key1), rocksdb::Slice(combine_key2));
    rocksdb::Status s = db_->Write(write_opts_, &batch);
    if (!s.ok()) {
        DEBUGLOG("Delete failed. tid %u pid %u msg %s", id_, pid_, s.ToString().c_str());
        return false;
    }
    offset_.fetch_add(1, std::memory_order_relaxed);
    return true;
}

bool DiskTable::Get(uint32_t idx, const std::string& pk, uint64_t ts, std::string& value) {
    Ticket ticket;
    auto it = NewIterator(idx, pk, ticket);
    it->Seek(ts);
    if ((it->Valid()) && (it->GetKey() == ts)) {
        value = it->GetValue().ToString();
        delete it;
        return true;
    } else {
        delete it;
        return false;
    }
}

bool DiskTable::Get(const std::string& pk, uint64_t ts, std::string& value) { return Get(0, pk, ts, value); }

base::Status DiskTable::Truncate() {
    const rocksdb::Snapshot* snapshot = db_->GetSnapshot();
    absl::Cleanup release_snapshot = [this, snapshot] { this->db_->ReleaseSnapshot(snapshot); };
    rocksdb::ReadOptions ro = rocksdb::ReadOptions();
    ro.snapshot = snapshot;
    ro.prefix_same_as_start = true;
    ro.pin_data = true;
    rocksdb::WriteBatch batch;
    for (const auto& inner_index : *(table_index_.GetAllInnerIndex())) {
        uint32_t idx = inner_index->GetId();
        std::unique_ptr<rocksdb::Iterator> it(db_->NewIterator(ro, cf_hs_[idx + 1]));
        it->SeekToFirst();
        if (it->Valid()) {
            std::string start_key(it->key().data(), it->key().size());
            it->SeekToLast();
            if (it->Valid()) {
                rocksdb::Slice cur_pk;
                uint64_t ts = 0;
                uint32_t ts_idx = 0;
                const auto& indexs = inner_index->GetIndex();
                std::string end_key;
                if (indexs.size() > 1) {
                    ParseKeyAndTs(true, it->key(), &cur_pk, &ts, &ts_idx);
                    end_key = CombineKeyTs(cur_pk, 0, ts_idx);
                } else {
                    ParseKeyAndTs(false, it->key(), &cur_pk, &ts, &ts_idx);
                    end_key = CombineKeyTs(cur_pk, 0);
                }
                PDLOG(INFO, "delete range. start key %s end key %s inner idx %u tid %u pid %u",
                        start_key.c_str(), end_key.c_str(), idx, id_, pid_);
                batch.DeleteRange(cf_hs_[idx + 1], rocksdb::Slice(start_key), rocksdb::Slice(end_key));
            }
        }
    }
    rocksdb::Status s = db_->Write(write_opts_, &batch);
    if (!s.ok()) {
        PDLOG(WARNING, "delete failed, tid %u pid %u msg %s", id_, pid_, s.ToString().c_str());
        return {-1, s.ToString()};
    }
    return {};
}

void DiskTable::SchedGc() {
    HandleDeletedIndex();
    GcHead();
    UpdateTTL();
}

void DiskTable::HandleDeletedIndex() {
    auto inner_indexs = table_index_.GetAllInnerIndex();
    for (const auto& inner_index : *inner_indexs) {
        // kWaiting -> kDeleting -> kDeleted
        const auto& indexs = inner_index->GetIndex();
        for (const auto& cur_index : indexs) {
            switch (cur_index->GetStatus()) {
                case IndexStatus::kWaiting:
                    cur_index->SetStatus(IndexStatus::kDeleting);
                    break;
                case IndexStatus::kDeleting: {
                    if (indexs.size() == 1) {
                        uint32_t idx = inner_index->GetId();
                        db_->DropColumnFamily(cf_hs_[idx + 1]);
                        db_->DestroyColumnFamilyHandle(cf_hs_[idx + 1]);
                        cf_hs_[idx + 1] = nullptr;
                        PDLOG(INFO, "drop column family. tid %u pid %u index name %s idx %u",
                            id_, pid_, cur_index->GetName().c_str(), idx);
                    }
                    cur_index->SetStatus(IndexStatus::kDeleted);
                    break;
                }
                case IndexStatus::kDeleted:
                case IndexStatus::kReady:
                    break;
            }
        }
    }
}

void DiskTable::GcHead() {
    uint64_t start_time = ::baidu::common::timer::get_micros() / 1000;
    auto inner_indexs = table_index_.GetAllInnerIndex();
    const rocksdb::Snapshot* snapshot = db_->GetSnapshot();
    absl::Cleanup release_snapshot = [this, snapshot] { this->db_->ReleaseSnapshot(snapshot); };
    for (const auto& inner_index : *inner_indexs) {
        uint32_t idx = inner_index->GetId();
        rocksdb::ReadOptions ro = rocksdb::ReadOptions();
        ro.snapshot = snapshot;
        // ro.prefix_same_as_start = true;
        ro.pin_data = true;
        std::unique_ptr<rocksdb::Iterator> it(db_->NewIterator(ro, cf_hs_[idx + 1]));
        it->SeekToFirst();
        const auto& indexs = inner_index->GetIndex();
        if (indexs.size() > 1) {
            bool need_ttl = false;
            std::map<uint32_t, uint64_t> ttl_map;
            for (const auto& index : indexs) {
                if (index->GetTTLType() != ::openmldb::storage::TTLType::kLatestTime) {
                    continue;
                }
                auto ts_col = index->GetTsColumn();
                if (ts_col) {
                    auto lat_ttl = index->GetTTL()->lat_ttl;
                    if (lat_ttl > 0) {
                        ttl_map.emplace(ts_col->GetId(), lat_ttl);
                        need_ttl = true;
                    }
                }
            }
            if (!need_ttl) {
                continue;
            }
            std::map<uint32_t, uint32_t> key_cnt;
            std::map<uint32_t, uint64_t> delete_key_map;
            std::string last_pk;
            while (it->Valid()) {
                rocksdb::Slice cur_pk;
                uint64_t ts = 0;
                uint32_t ts_idx = 0;
                ParseKeyAndTs(true, it->key(), &cur_pk, &ts, &ts_idx);
                if (!last_pk.empty() && cur_pk.compare(rocksdb::Slice(last_pk)) == 0) {
                    auto ttl_iter = ttl_map.find(ts_idx);
                    if (ttl_iter != ttl_map.end() && ttl_iter->second > 0) {
                        auto key_cnt_iter = key_cnt.find(ts_idx);
                        if (key_cnt_iter == key_cnt.end()) {
                            key_cnt.insert(std::make_pair(ts_idx, 1));
                        } else {
                            key_cnt_iter->second++;
                        }
                        if (key_cnt_iter->second > ttl_iter->second &&
                            delete_key_map.find(ts_idx) == delete_key_map.end()) {
                            delete_key_map.insert(std::make_pair(ts_idx, ts));
                        }
                    }
                } else {
                    for (const auto& kv : delete_key_map) {
                        std::string combine_key1 = CombineKeyTs(rocksdb::Slice(last_pk), kv.second, kv.first);
                        std::string combine_key2 = CombineKeyTs(rocksdb::Slice(last_pk), 0, kv.first);
                        rocksdb::Status s = db_->DeleteRange(write_opts_, cf_hs_[idx + 1], rocksdb::Slice(combine_key1),
                                                             rocksdb::Slice(combine_key2));
                        if (!s.ok()) {
                            PDLOG(WARNING, "Delete failed. tid %u pid %u msg %s", id_, pid_, s.ToString().c_str());
                        }
                    }
                    delete_key_map.clear();
                    key_cnt.clear();
                    key_cnt.insert(std::make_pair(ts_idx, 1));
                    last_pk.assign(cur_pk.data(), cur_pk.size());
                }
                it->Next();
            }
            for (const auto& kv : delete_key_map) {
                std::string combine_key1 = CombineKeyTs(last_pk, kv.second, kv.first);
                std::string combine_key2 = CombineKeyTs(last_pk, 0, kv.first);
                rocksdb::Status s = db_->DeleteRange(write_opts_, cf_hs_[idx + 1], rocksdb::Slice(combine_key1),
                                                     rocksdb::Slice(combine_key2));
                if (!s.ok()) {
                    PDLOG(WARNING, "Delete failed. tid %u pid %u msg %s", id_, pid_, s.ToString().c_str());
                }
            }
        } else {
            auto index = indexs.front();
            auto ttl_num = index->GetTTL()->lat_ttl;
            if (ttl_num < 1 || index->GetTTLType() != ::openmldb::storage::TTLType::kLatestTime) {
                continue;
            }
            std::string last_pk;
            uint64_t count = 0;
            while (it->Valid()) {
                rocksdb::Slice cur_pk;
                uint64_t ts = 0;
                ParseKeyAndTs(it->key(), &cur_pk, &ts);
                if (!last_pk.empty() && cur_pk.compare(rocksdb::Slice(last_pk)) == 0) {
                    if (ts == 0 || count < ttl_num) {
                        it->Next();
                        count++;
                        continue;
                    } else {
                        std::string combine_key1 = CombineKeyTs(cur_pk, ts);
                        std::string combine_key2 = CombineKeyTs(cur_pk, 0);
                        rocksdb::Status s = db_->DeleteRange(write_opts_, cf_hs_[idx + 1], rocksdb::Slice(combine_key1),
                                                             rocksdb::Slice(combine_key2));
                        if (!s.ok()) {
                            PDLOG(WARNING, "Delete failed. tid %u pid %u msg %s", id_, pid_, s.ToString().c_str());
                        }
                        it->Seek(rocksdb::Slice(combine_key2));
                    }
                } else {
                    count = 1;
                    last_pk.assign(cur_pk.data(), cur_pk.size());
                    it->Next();
                }
            }
        }
    }
    uint64_t time_used = ::baidu::common::timer::get_micros() / 1000 - start_time;
    PDLOG(INFO, "Gc used %lu second. tid %u pid %u", time_used / 1000, id_, pid_);
}

void DiskTable::GcTTLOrHead() {}

void DiskTable::GcTTLAndHead() {}

// ttl as ms
uint64_t DiskTable::GetExpireTime(const TTLSt& ttl_st) {
    if (ttl_st.abs_ttl == 0 || ttl_st.ttl_type == ::openmldb::storage::TTLType::kLatestTime) {
        return 0;
    }
    uint64_t cur_time = ::baidu::common::timer::get_micros() / 1000;
    return cur_time - ttl_st.abs_ttl;
}

bool DiskTable::IsExpire(const ::openmldb::api::LogEntry& entry) {
    // TODO(denglong)
    return false;
}

int DiskTable::CreateCheckPoint(const std::string& checkpoint_dir) {
    rocksdb::Checkpoint* checkpoint = nullptr;
    rocksdb::Status s = rocksdb::Checkpoint::Create(db_, &checkpoint);
    if (!s.ok()) {
        PDLOG(WARNING, "Create failed. tid %u pid %u msg %s", id_, pid_, s.ToString().c_str());
        return -1;
    }
    s = checkpoint->CreateCheckpoint(checkpoint_dir);
    delete checkpoint;
    if (!s.ok()) {
        PDLOG(WARNING, "CreateCheckpoint failed. tid %u pid %u msg %s", id_, pid_, s.ToString().c_str());
        return -1;
    }
    return 0;
}

TableIterator* DiskTable::NewIterator(const std::string& pk, Ticket& ticket) {
    return DiskTable::NewIterator(0, pk, ticket);
}

TableIterator* DiskTable::NewIterator(uint32_t idx, const std::string& pk, Ticket& ticket) {
    std::shared_ptr<IndexDef> index_def = table_index_.GetIndex(idx);
    if (!index_def) {
        PDLOG(WARNING, "index %u not found in table, tid %u pid %u", idx, id_, pid_);
        return nullptr;
    }
    uint32_t inner_pos = index_def->GetInnerPos();
    auto inner_index = table_index_.GetInnerIndex(inner_pos);
    rocksdb::ReadOptions ro = rocksdb::ReadOptions();
    const rocksdb::Snapshot* snapshot = db_->GetSnapshot();
    ro.snapshot = snapshot;
    ro.prefix_same_as_start = true;
    ro.pin_data = true;
    rocksdb::Iterator* it = db_->NewIterator(ro, cf_hs_[inner_pos + 1]);
    if (inner_index && inner_index->GetIndex().size() > 1) {
        auto ts_col = index_def->GetTsColumn();
        if (ts_col) {
            return new DiskTableIterator(db_, it, snapshot, pk, ts_col->GetId(), GetCompressType());
        }
    }
    return new DiskTableIterator(db_, it, snapshot, pk, GetCompressType());
}

TraverseIterator* DiskTable::NewTraverseIterator(uint32_t index) {
    std::shared_ptr<IndexDef> index_def = table_index_.GetIndex(index);
    if (!index_def) {
        return nullptr;
    }
    uint32_t inner_pos = index_def->GetInnerPos();
    auto inner_index = table_index_.GetInnerIndex(inner_pos);
    auto ttl = index_def->GetTTL();
    uint64_t expire_time = GetExpireTime(*ttl);
    uint64_t expire_cnt = ttl->lat_ttl;
    rocksdb::ReadOptions ro = rocksdb::ReadOptions();
    const rocksdb::Snapshot* snapshot = db_->GetSnapshot();
    ro.snapshot = snapshot;
    // ro.prefix_same_as_start = true;
    ro.pin_data = true;
    rocksdb::Iterator* it = db_->NewIterator(ro, cf_hs_[inner_pos + 1]);
    if (inner_index && inner_index->GetIndex().size() > 1) {
        auto ts_col = index_def->GetTsColumn();
        if (ts_col) {
            return new DiskTableTraverseIterator(db_, it, snapshot, ttl->ttl_type, expire_time, expire_cnt,
                                                 ts_col->GetId(), GetCompressType());
        }
    }
    return new DiskTableTraverseIterator(db_, it, snapshot, ttl->ttl_type, expire_time, expire_cnt, GetCompressType());
}

::hybridse::vm::WindowIterator* DiskTable::NewWindowIterator(uint32_t idx) {
    std::shared_ptr<IndexDef> index_def = table_index_.GetIndex(idx);
    if (!index_def) {
        return nullptr;
    }
    uint32_t inner_pos = index_def->GetInnerPos();
    auto inner_index = table_index_.GetInnerIndex(inner_pos);
    auto ttl = index_def->GetTTL();
    uint64_t expire_time = GetExpireTime(*ttl);
    uint64_t expire_cnt = ttl->lat_ttl;
    rocksdb::ReadOptions ro = rocksdb::ReadOptions();
    const rocksdb::Snapshot* snapshot = db_->GetSnapshot();
    ro.snapshot = snapshot;
    // ro.prefix_same_as_start = true;
    ro.pin_data = true;
    rocksdb::Iterator* it = db_->NewIterator(ro, cf_hs_[inner_pos + 1]);
    if (inner_index && inner_index->GetIndex().size() > 1) {
        auto ts_col = index_def->GetTsColumn();
        if (ts_col) {
            return new DiskTableKeyIterator(db_, it, snapshot, ttl->ttl_type, expire_time, expire_cnt,
                    ts_col->GetId(), cf_hs_[inner_pos + 1], GetCompressType());
        }
    }
    return new DiskTableKeyIterator(db_, it, snapshot, ttl->ttl_type, expire_time, expire_cnt,
            cf_hs_[inner_pos + 1], GetCompressType());
}

bool DiskTable::AddIndexToTable(const std::shared_ptr<IndexDef>& index_def) {
    rocksdb::ColumnFamilyOptions cfo;
    if (storage_mode_ == ::openmldb::common::StorageMode::kSSD) {
        cfo = rocksdb::ColumnFamilyOptions(ssd_option_template);
    } else {
        cfo = rocksdb::ColumnFamilyOptions(hdd_option_template);
    }
    cfo.comparator = &cmp_;
    cfo.prefix_extractor.reset(new KeyTsPrefixTransform());
    if (index_def->GetTTLType() == ::openmldb::storage::TTLType::kAbsoluteTime ||
        index_def->GetTTLType() == ::openmldb::storage::TTLType::kAbsOrLat) {
        uint32_t inner_pos = index_def->GetInnerPos();
        cfo.compaction_filter_factory =
            std::make_shared<AbsoluteTTLFilterFactory>(table_index_.GetInnerIndex(inner_pos));
    }
    rocksdb::ColumnFamilyHandle* handle;
    rocksdb::Status s = db_->CreateColumnFamily(cfo, index_def->GetName(), &handle);
    if (!s.ok()) {
        PDLOG(WARNING, "failed to create ColumnFamily in rocksdb. tid %u pid %u error %s",
                id_, pid_, s.ToString().c_str());
        return false;
    }
    cf_hs_[index_def->GetInnerPos() + 1] = handle;
    PDLOG(INFO, "add cf_name %s. tid %u pid %u", index_def->GetName().c_str(), id_, pid_);
    return true;
}

uint64_t DiskTable::GetRecordIdxCnt() {
    // TODO(litongxin)
    return 0;
}

bool DiskTable::GetRecordIdxCnt(uint32_t idx, uint64_t** stat, uint32_t* size) {
    // TODO(litongxin)
    return true;
}

uint64_t DiskTable::GetRecordPkCnt() {
    // TODO(litongxin)
    return 0;
}

uint64_t DiskTable::GetRecordIdxByteSize() {
    // TODO(litongxin)
    return 0;
}

int DiskTable::GetCount(uint32_t index, const std::string& pk, uint64_t& count) {
    PDLOG(WARNING, "Count in disk table is slow");
    std::shared_ptr<IndexDef> index_def = table_index_.GetIndex(index);
    if (index_def && !index_def->IsReady()) {
        return -1;
    }
    uint32_t inner_pos = index_def->GetInnerPos();
    auto inner_index = table_index_.GetInnerIndex(inner_pos);
    rocksdb::ReadOptions ro = rocksdb::ReadOptions();
    const rocksdb::Snapshot* snapshot = db_->GetSnapshot();
    ro.snapshot = snapshot;
    // ro.prefix_same_as_start = true;
    ro.pin_data = true;
    rocksdb::Iterator* it = db_->NewIterator(ro, cf_hs_[inner_pos + 1]);

    bool has_ts_idx = false;
    uint32_t ts_idx = 0;
    if (inner_index && inner_index->GetIndex().size() > 1) {
        has_ts_idx = true;
        auto ts_col = index_def->GetTsColumn();
        ts_idx = ts_col->GetId();
    }

    std::string combine;
    uint64_t tmp_ts = UINT64_MAX;
    if (has_ts_idx) {
        combine = CombineKeyTs(rocksdb::Slice(pk), tmp_ts, ts_idx);
    } else {
        combine = CombineKeyTs(rocksdb::Slice(pk), tmp_ts);
    }
    it->Seek(rocksdb::Slice(combine));

    count = 0;
    for (; it->Valid(); it->Next()) {
        uint32_t cur_ts_idx = UINT32_MAX;
        rocksdb::Slice cur_pk;
        uint64_t cur_ts = 0;
        ParseKeyAndTs(has_ts_idx, it->key(), &cur_pk, &cur_ts, &cur_ts_idx);
        if (cur_pk.compare(rocksdb::Slice(pk)) == 0) {
            if (has_ts_idx && cur_ts_idx != ts_idx) {
                break;
            }
            count++;
        } else {
            break;
        }
    }

    return 0;
}

}  // namespace storage
}  // namespace openmldb
