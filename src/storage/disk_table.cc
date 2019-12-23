//
// Created by yangjun on 12/14/18.
//

#include "storage/disk_table.h"
#include "logging.h"
#include "base/hash.h"
#include "base/file_util.h"

using ::baidu::common::INFO;
using ::baidu::common::WARNING;
using ::baidu::common::DEBUG;

DECLARE_bool(disable_wal);
DECLARE_uint32(max_traverse_cnt);

namespace rtidb {
namespace storage {

static rocksdb::Options ssd_option_template;
static rocksdb::Options hdd_option_template;
static bool options_template_initialized = false;

DiskTable::DiskTable(const std::string &name, uint32_t id, uint32_t pid,
                     const std::map<std::string, uint32_t> &mapping, 
                     uint64_t ttl, ::rtidb::api::TTLType ttl_type,
                     ::rtidb::common::StorageMode storage_mode,
                     const std::string& db_root_path) :
        Table(storage_mode, name, id, pid, ttl * 60 * 1000, true, 0, mapping, ttl_type, 
                ::rtidb::api::CompressType::kNoCompress),
        write_opts_(), offset_(0), db_root_path_(db_root_path){
    if (!options_template_initialized) {
        initOptionTemplate();
    }
    write_opts_.disableWAL = FLAGS_disable_wal;
    db_ = nullptr;
}

DiskTable::DiskTable(const ::rtidb::api::TableMeta& table_meta,
        const std::string& db_root_path) :
        Table(table_meta.storage_mode(), table_meta.name(), table_meta.tid(), table_meta.pid(),
                0, true, 0, std::map<std::string, uint32_t>(), 
                ::rtidb::api::TTLType::kAbsoluteTime, 
                ::rtidb::api::CompressType::kNoCompress),
        write_opts_(), offset_(0), db_root_path_(db_root_path) {
    table_meta_.CopyFrom(table_meta);
    if (!options_template_initialized) {
        initOptionTemplate();
    }
    write_opts_.disableWAL = FLAGS_disable_wal;
    db_ = nullptr;
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
    std::shared_ptr<rocksdb::Cache> cache = rocksdb::NewLRUCache(512 << 20, 8); //Can be set by flags
    //SSD options template
    ssd_option_template.max_open_files = -1;
    ssd_option_template.env->SetBackgroundThreads(1, rocksdb::Env::Priority::HIGH); //flush threads
    ssd_option_template.env->SetBackgroundThreads(4, rocksdb::Env::Priority::LOW);  //compaction threads
    ssd_option_template.memtable_prefix_bloom_size_ratio = 0.02;
    ssd_option_template.compaction_style = rocksdb::kCompactionStyleLevel;
    ssd_option_template.level0_file_num_compaction_trigger = 10;
    ssd_option_template.level0_slowdown_writes_trigger = 20;
    ssd_option_template.level0_stop_writes_trigger = 40;
    ssd_option_template.write_buffer_size = 64 << 20;
    ssd_option_template.target_file_size_base = 64 << 20;
    ssd_option_template.max_bytes_for_level_base = 512 << 20;

    rocksdb::BlockBasedTableOptions table_options;
    table_options.cache_index_and_filter_blocks = true;
    table_options.pin_l0_filter_and_index_blocks_in_cache = true;
    table_options.block_cache = cache;
    // table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10, false));
    table_options.whole_key_filtering = false;
    table_options.block_size = 256 << 10;
    table_options.use_delta_encoding = false;
    ssd_option_template.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));

    //HDD options template
    hdd_option_template.max_open_files = -1;
    hdd_option_template.env->SetBackgroundThreads(1, rocksdb::Env::Priority::HIGH); //flush threads
    hdd_option_template.env->SetBackgroundThreads(1, rocksdb::Env::Priority::LOW);  //compaction threads
    hdd_option_template.memtable_prefix_bloom_size_ratio = 0.02;
    hdd_option_template.optimize_filters_for_hits = true;
    hdd_option_template.level_compaction_dynamic_level_bytes = true;
    hdd_option_template.max_file_opening_threads = 1; //set to the number of disks on which the db root folder is mounted
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
    cf_ds_.push_back(rocksdb::ColumnFamilyDescriptor(rocksdb::kDefaultColumnFamilyName, 
            rocksdb::ColumnFamilyOptions()));
    for (auto iter = mapping_.begin(); iter != mapping_.end(); ++iter) {
        rocksdb::ColumnFamilyOptions cfo;
        if (storage_mode_ == ::rtidb::common::StorageMode::kSSD) {
            cfo = rocksdb::ColumnFamilyOptions(ssd_option_template);
            options_ = ssd_option_template;
        } else {
            cfo = rocksdb::ColumnFamilyOptions(hdd_option_template);
            options_ = hdd_option_template;
        }
        cfo.comparator = &cmp_;
        cfo.prefix_extractor.reset(new KeyTsPrefixTransform());
        if (ttl_type_ == ::rtidb::api::TTLType::kAbsoluteTime || ttl_type_ == ::rtidb::api::TTLType::kAbsOrLat) {
            auto column_key_map_iter = column_key_map_.find(iter->second);
            if (column_key_map_iter == column_key_map_.end() || column_key_map_iter->second.empty()) {
                cfo.compaction_filter_factory = std::make_shared<AbsoluteTTLFilterFactory>(&abs_ttl_);
            } else if (column_key_map_iter->second.size() == 1) {
                cfo.compaction_filter_factory = std::make_shared<AbsoluteTTLFilterFactory>(abs_ttl_vec_[column_key_map_iter->second.front()].get());
            } else {
                cfo.compaction_filter_factory = std::make_shared<AbsoluteTTLFilterFactory>(&abs_ttl_vec_);
            }
        }
        cf_ds_.push_back(rocksdb::ColumnFamilyDescriptor(iter->first, cfo));
        PDLOG(DEBUG, "add cf_name %s. tid %u pid %u", iter->first.c_str(), id_, pid_);
    }
    return true;
}

bool DiskTable::Init() {
    if (!InitFromMeta()) {
        return false;
    }
    InitColumnFamilyDescriptor();
    std::string path = db_root_path_ + "/" + std::to_string(id_) + "_" + std::to_string(pid_) + "/data";
    if (!::rtidb::base::MkdirRecur(path)) {
        PDLOG(WARNING, "fail to create path %s", path.c_str());
        return false;
    }
    options_.create_if_missing = true;
    options_.error_if_exists = true;
    options_.create_missing_column_families = true;
    rocksdb::Status s = rocksdb::DB::Open(options_, path, cf_ds_, &cf_hs_, &db_);
    if (!s.ok()) {
        PDLOG(WARNING, "rocksdb open failed. tid %u pid %u error %s", id_, pid_, s.ToString().c_str());
        return false;
    } 
    PDLOG(INFO, "Open DB. tid %u pid %u ColumnFamilyHandle size %d with data path %s", id_, pid_, idx_cnt_,
            path.c_str());
    return true;
}

bool DiskTable::Put(const std::string &pk, uint64_t time, const char *data, uint32_t size) {
    rocksdb::Status s;
    rocksdb::Slice spk = rocksdb::Slice(CombineKeyTs(pk, time));
    s = db_->Put(write_opts_, cf_hs_[1], spk, rocksdb::Slice(data, size));
    if (s.ok()) {
        offset_.fetch_add(1, std::memory_order_relaxed);
        return true;
    } else {
        PDLOG(DEBUG, "Put failed. tid %u pid %u msg %s", id_, pid_, s.ToString().c_str());
        return false;
    }
}

bool DiskTable::Put(uint64_t time, const std::string &value, const Dimensions &dimensions) {
    rocksdb::WriteBatch batch;
    rocksdb::Status s;
    Dimensions::const_iterator it = dimensions.begin();
    for (; it != dimensions.end(); ++it) {
        if (it->idx() >= idx_cnt_) {
            PDLOG(WARNING, "failed putting key %s to dimension %u in table tid %u pid %u",
                            it->key().c_str(), it->idx(), id_, pid_);
            return false;
        }
        rocksdb::Slice spk = rocksdb::Slice(CombineKeyTs(it->key(), time));
        batch.Put(cf_hs_[it->idx() + 1], spk, value);
    }
    s = db_->Write(write_opts_, &batch);
    if (s.ok()) {
        offset_.fetch_add(1, std::memory_order_relaxed);
        return true;
    } else {
        PDLOG(DEBUG, "Put failed. tid %u pid %u msg %s", id_, pid_, s.ToString().c_str());
        return false;
    }
}

bool DiskTable::Put(const Dimensions& dimensions, const TSDimensions& ts_dimemsions, const std::string& value) {
    if (dimensions.size() == 0 || ts_dimemsions.size() == 0) {
         PDLOG(WARNING, "empty dimesion. tid %u pid %u", id_, pid_);
         return false;
    }
    rocksdb::WriteBatch batch;
    for (auto it = dimensions.begin(); it != dimensions.end(); it++) {
          if (it->idx() >= idx_cnt_) {
              PDLOG(WARNING, "idx greater than idx_cnt_, failed putting key %s to dimension %u in table tid %u pid %u",
                  it->key().c_str(), it->idx(), id_, pid_);
              return false;
          }
          auto& ts_vector = column_key_map_[it->idx()];
          for (const auto& cur_ts : ts_dimemsions) {
              if (std::find(ts_vector.cbegin(), ts_vector.cend(), cur_ts.idx()) == ts_vector.cend()) {
                continue;
              }
              rocksdb::Slice spk;
              if (ts_vector.size() == 1) {
                  spk = rocksdb::Slice(CombineKeyTs(it->key(), cur_ts.ts()));
              } else {
                  spk = rocksdb::Slice(CombineKeyTs(it->key(), cur_ts.ts(), (uint8_t)cur_ts.idx()));
              }
              batch.Put(cf_hs_[it->idx() + 1], spk, value);
          }
      }
      rocksdb::Status s = db_->Write(write_opts_, &batch);
      if (s.ok()) {
            offset_.fetch_add(1, std::memory_order_relaxed);
      } else {
            PDLOG(DEBUG, "Put failed. tid %u pid %u msg %s", id_, pid_, s.ToString().c_str());
      }
      return s.ok();
}

bool DiskTable::Delete(const std::string& pk, uint32_t idx) {
    rocksdb::WriteBatch batch;
    std::map<uint32_t, std::vector<uint32_t>>::iterator it = column_key_map_.find(idx);
    if (it != column_key_map_.end()) {
        if (it->second.size() == 1) {
            batch.DeleteRange(cf_hs_[idx+1],
                rocksdb::Slice(CombineKeyTs(pk, UINT64_MAX)), rocksdb::Slice(CombineKeyTs(pk, 0)));
        } else {
            for (auto ts : it->second) {
                batch.DeleteRange(cf_hs_[idx+1], rocksdb::Slice(CombineKeyTs(pk, UINT64_MAX, ts)),
                    rocksdb::Slice(CombineKeyTs(pk, 0, ts)));
            }
        }
    } else {
        batch.DeleteRange(cf_hs_[idx+1],
            rocksdb::Slice(CombineKeyTs(pk, UINT64_MAX)), rocksdb::Slice(CombineKeyTs(pk, 0)));
    }
    rocksdb::Status s = db_->Write(write_opts_, &batch);
    if (s.ok()) {
        offset_.fetch_add(1, std::memory_order_relaxed);
        return true;
    } else {
        PDLOG(DEBUG, "Delete failed. tid %u pid %u msg %s", id_, pid_, s.ToString().c_str());
        return false;
    }
}

bool DiskTable::Get(uint32_t idx, const std::string& pk, 
        uint64_t ts, uint32_t ts_idx, std::string& value) {
    if (idx >= idx_cnt_) {
        PDLOG(WARNING, "idx greater than idx_cnt_, failed getting table tid %u pid %u", id_, pid_);
        return false;
    }
    auto column_map_iter = column_key_map_.find(idx);
    if (column_map_iter == column_key_map_.end()) {
        PDLOG(WARNING, "index %d not found in column key map table tid %u pid %u", idx, id_, pid_);
        return false;
    }
    if (std::find(column_map_iter->second.begin(), column_map_iter->second.end(), ts_idx) 
                            == column_map_iter->second.end()) {
        PDLOG(WARNING, "ts cloumn not member of index, ts id %d index id %d, failed getting table tid %u pid %u", 
                        ts_idx, idx, id_, pid_);
        return false;
    }
    rocksdb::Slice spk ;
    if (column_map_iter->second.size() > 1) {
        spk = rocksdb::Slice(CombineKeyTs(pk, ts, ts_idx));
    } else {
        spk = rocksdb::Slice(CombineKeyTs(pk, ts));
    }
    rocksdb::Status s;
    s = db_->Get(rocksdb::ReadOptions(), cf_hs_[idx + 1], spk, &value);
    if (s.ok()) {
        return true;
    } else {
        return false;
    }
}

bool DiskTable::Get(uint32_t idx, const std::string& pk, uint64_t ts, std::string& value) {
    if (idx >= idx_cnt_) {
        PDLOG(WARNING, "idx greater than idx_cnt_, failed getting table tid %u pid %u", id_, pid_);
        return false;
    }
    rocksdb::Slice spk ;
    auto pos = column_key_map_.find(idx);
    if ((pos != column_key_map_.end()) && (pos->second.size() > 1)) {
        spk = rocksdb::Slice(CombineKeyTs(pk, ts, pos->second[0]));
    } else {
        spk = rocksdb::Slice(CombineKeyTs(pk, ts));
    }
    rocksdb::Status s;
    s = db_->Get(rocksdb::ReadOptions(), cf_hs_[idx + 1], spk, &value);
    if (s.ok()) {
        return true;
    } else {
        return false;
    }
}

bool DiskTable::Get(const std::string& pk, uint64_t ts, std::string& value) {
    return Get(0, pk, ts, value);
}

bool DiskTable::LoadTable() {
    if (!InitFromMeta()) {
        return false;
    }
    InitColumnFamilyDescriptor();
    std::string path = db_root_path_ + "/" + std::to_string(id_) + "_" + std::to_string(pid_) + "/data";
    if (!rtidb::base::IsExists(path)) {
        return false;
    }
    options_.create_if_missing = false;
    options_.error_if_exists = false;
    options_.create_missing_column_families = false;
    rocksdb::Status s = rocksdb::DB::Open(options_, path, cf_ds_, &cf_hs_, &db_);
    PDLOG(DEBUG, "Load DB. tid %u pid %u ColumnFamilyHandle size %d,", id_, pid_, idx_cnt_);
    if (!s.ok()) {
        PDLOG(WARNING, "Load DB failed. tid %u pid %u msg %s", id_, pid_, s.ToString().c_str());
        return false;
    }    
    return true;
}

void DiskTable::SchedGc() {
    if (ttl_type_ == ::rtidb::api::TTLType::kLatestTime) {
        GcHead();
    } /*else {
        rocksdb will delete expired key when compact
        GcTTL();
    }*/
    UpdateTTL();
}

void DiskTable::GcTTL() {
    uint64_t start_time = ::baidu::common::timer::get_micros() / 1000;
    uint64_t expire_time = GetExpireTime();
    if (expire_time < 1) {
        return;
    }
    for (auto cf_hs : cf_hs_) {
        rocksdb::ReadOptions ro = rocksdb::ReadOptions();
        const rocksdb::Snapshot* snapshot = db_->GetSnapshot();
        ro.snapshot = snapshot;
        //ro.prefix_same_as_start = true;
        ro.pin_data = true;
        rocksdb::Iterator* it = db_->NewIterator(ro, cf_hs);
        it->SeekToFirst();
        std::string last_pk;
        while (it->Valid()) {
            std::string cur_pk;
            uint64_t ts = 0;
            ParseKeyAndTs(it->key(), cur_pk, ts);
            if (cur_pk == last_pk) {
                if (ts == 0 || ts >= expire_time) {
                    it->Next();
                    continue;
                } else {
                    rocksdb::Status s = db_->DeleteRange(write_opts_, cf_hs, 
                            rocksdb::Slice(CombineKeyTs(cur_pk, ts)), rocksdb::Slice(CombineKeyTs(cur_pk, 0)));
                    if (!s.ok()) {
                        PDLOG(WARNING, "Delete failed. tid %u pid %u msg %s", id_, pid_, s.ToString().c_str());
                    }
                    it->Seek(rocksdb::Slice(CombineKeyTs(cur_pk, 0)));
                }
            } else {
                last_pk = cur_pk;
                it->Next();
            }
        }
        delete it;
        db_->ReleaseSnapshot(snapshot);
    }
    uint64_t time_used = ::baidu::common::timer::get_micros() / 1000 - start_time;
    PDLOG(INFO, "Gc used %lu second. tid %u pid %u", time_used / 1000, id_, pid_);
}

void DiskTable::GcHead() {
    uint64_t start_time = ::baidu::common::timer::get_micros() / 1000;
    for (auto iter = mapping_.begin(); iter != mapping_.end(); ++iter) {
        uint32_t idx = iter->second;
        rocksdb::ReadOptions ro = rocksdb::ReadOptions();
        const rocksdb::Snapshot* snapshot = db_->GetSnapshot();
        ro.snapshot = snapshot;
        //ro.prefix_same_as_start = true;
        ro.pin_data = true;
        rocksdb::Iterator* it = db_->NewIterator(ro, cf_hs_[idx + 1]);
        it->SeekToFirst();
        auto column_key_map_iter = column_key_map_.find(iter->second);
        if (column_key_map_iter != column_key_map_.end() && column_key_map_iter->second.size() > 1) {
            bool need_ttl = false;
            std::map<uint32_t, uint64_t> ttl_map;
            for (auto ts_idx : column_key_map_iter->second) {
                uint64_t ttl = lat_ttl_vec_[ts_idx]->load(std::memory_order_relaxed);
                ttl_map.insert(std::make_pair(ts_idx, ttl));
                if (ttl > 0) {
                    need_ttl = true;
                }
            }
            if (!need_ttl) {
                continue;
            }
            std::map<uint32_t, uint32_t> key_cnt;
            std::map<uint32_t, uint64_t> delete_key_map;
            std::string last_pk;
            while (it->Valid()) {
                std::string cur_pk;
                uint64_t ts = 0;
                uint8_t ts_idx = 0;
                ParseKeyAndTs(true, it->key(), cur_pk, ts, ts_idx);
                if (!last_pk.empty() && cur_pk == last_pk) {
                    auto ttl_iter = ttl_map.find(ts_idx);
                    if (ttl_iter != ttl_map.end() && ttl_iter->second > 0) {
                        auto key_cnt_iter = key_cnt.find(ts_idx);
                        if (key_cnt_iter == key_cnt.end()) {
                            key_cnt.insert(std::make_pair(ts_idx, 1));
                        } else {   
                            key_cnt_iter->second++;
                        }
                        if (key_cnt_iter->second > ttl_iter->second && delete_key_map.find(ts_idx) == delete_key_map.end()) {
                            delete_key_map.insert(std::make_pair(ts_idx, ts));
                        }
                    }
                } else {
                    for (const auto& kv : delete_key_map) {
                        rocksdb::Status s = db_->DeleteRange(write_opts_, cf_hs_[idx + 1], 
                                rocksdb::Slice(CombineKeyTs(last_pk, kv.second, kv.first)), rocksdb::Slice(CombineKeyTs(last_pk, 0, kv.first)));
                        if (!s.ok()) {
                            PDLOG(WARNING, "Delete failed. tid %u pid %u msg %s", id_, pid_, s.ToString().c_str());
                        }    
                    }
                    delete_key_map.clear();
                    key_cnt.clear();
                    key_cnt.insert(std::make_pair(ts_idx, 1));
                    last_pk = cur_pk;
                }
                it->Next();
            }
            for (const auto& kv : delete_key_map) {
                rocksdb::Status s = db_->DeleteRange(write_opts_, cf_hs_[idx + 1], 
                        rocksdb::Slice(CombineKeyTs(last_pk, kv.second, kv.first)), rocksdb::Slice(CombineKeyTs(last_pk, 0, kv.first)));
                if (!s.ok()) {
                    PDLOG(WARNING, "Delete failed. tid %u pid %u msg %s", id_, pid_, s.ToString().c_str());
                }
            }
        } else {
            uint64_t ttl_num = lat_ttl_;
            if (column_key_map_iter != column_key_map_.end() && column_key_map_iter->second.front() < lat_ttl_vec_.size()) {
                ttl_num = lat_ttl_vec_[column_key_map_iter->second.front()]->load(std::memory_order_relaxed);
            }
            if (ttl_num < 1) {
                continue;
            }
            std::string last_pk;
            uint64_t count = 0;
            while (it->Valid()) {
                std::string cur_pk;
                uint64_t ts = 0;
                ParseKeyAndTs(it->key(), cur_pk, ts);
                if (!last_pk.empty() && cur_pk == last_pk) {
                    if (ts == 0 || count < ttl_num) {
                        it->Next();
                        count++;
                        continue;
                    } else {
                        rocksdb::Status s = db_->DeleteRange(write_opts_, cf_hs_[idx + 1], 
                                rocksdb::Slice(CombineKeyTs(cur_pk, ts)), rocksdb::Slice(CombineKeyTs(cur_pk, 0)));
                        if (!s.ok()) {
                            PDLOG(WARNING, "Delete failed. tid %u pid %u msg %s", id_, pid_, s.ToString().c_str());
                        }    
                        it->Seek(rocksdb::Slice(CombineKeyTs(cur_pk, 0)));
                    }
                } else {
                    count = 1;
                    last_pk = cur_pk;
                    it->Next();
                }
            }
        }
        delete it;
        db_->ReleaseSnapshot(snapshot);
    }
    uint64_t time_used = ::baidu::common::timer::get_micros() / 1000 - start_time;
    PDLOG(INFO, "Gc used %lu second. tid %u pid %u", time_used / 1000, id_, pid_);
}

// ttl as ms
uint64_t DiskTable::GetExpireTime(uint64_t ttl) {
    if (ttl == 0 || ttl_type_ == ::rtidb::api::TTLType::kLatestTime) {
        return 0;
    }
    uint64_t cur_time = ::baidu::common::timer::get_micros() / 1000;
    return cur_time - ttl;
}

uint64_t DiskTable::GetExpireTime() {
    if (abs_ttl_.load(std::memory_order_relaxed) == 0
            || ttl_type_ == ::rtidb::api::TTLType::kLatestTime) {
        return 0;
    }
    uint64_t cur_time = ::baidu::common::timer::get_micros() / 1000;
    return cur_time - abs_ttl_.load(std::memory_order_relaxed);
}


bool DiskTable::IsExpire(const ::rtidb::api::LogEntry& entry) {
    // TODO
    return false;
}

int DiskTable::CreateCheckPoint(const std::string& checkpoint_dir) {
    rocksdb::Checkpoint* checkpoint = NULL;
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

TableIterator* DiskTable::NewIterator(const std::string &pk, Ticket& ticket) {
    return DiskTable::NewIterator(0, pk, ticket);
}

TableIterator* DiskTable::NewIterator(uint32_t idx, const std::string& pk, Ticket& ticket) {
    auto column_map_iter = column_key_map_.find(idx);
    if (column_map_iter != column_key_map_.end() && !column_map_iter->second.empty()) {
        return NewIterator(idx, column_map_iter->second.front(), pk, ticket);
    }
    rocksdb::ReadOptions ro = rocksdb::ReadOptions();
    const rocksdb::Snapshot* snapshot = db_->GetSnapshot();
    ro.snapshot = snapshot;
    ro.prefix_same_as_start = true;
    ro.pin_data = true;
    rocksdb::Iterator* it = db_->NewIterator(ro, cf_hs_[idx + 1]);
    return new DiskTableIterator(db_, it, snapshot, pk);
}

TableIterator* DiskTable::NewIterator(uint32_t index, int32_t ts_idx, const std::string& pk, Ticket& ticket) {
    auto column_map_iter = column_key_map_.find(index);
    if (column_map_iter == column_key_map_.end()) {
        PDLOG(WARNING, "index %d not found in column key map table tid %u pid %u", index, id_, pid_);
        return NULL;
    }
    if (std::find(column_map_iter->second.cbegin(), column_map_iter->second.cend(), ts_idx) 
                == column_map_iter->second.cend()) {
        PDLOG(WARNING, "ts cloumn not member of index, ts id %d index id %d, failed getting table tid %u pid %u", 
                    ts_idx, index, id_, pid_);
        return NULL;
    }
    rocksdb::ReadOptions ro = rocksdb::ReadOptions();
    const rocksdb::Snapshot* snapshot = db_->GetSnapshot();
    ro.snapshot = snapshot;
    ro.prefix_same_as_start = true;
    ro.pin_data = true;
    rocksdb::Iterator* it = db_->NewIterator(ro, cf_hs_[index + 1]);
    if (column_map_iter->second.size() == 1) {
        return new DiskTableIterator(db_, it, snapshot, pk);
    }
    return new DiskTableIterator(db_, it, snapshot, pk, ts_idx);
}

TableIterator* DiskTable::NewTraverseIterator(uint32_t index) {
    auto column_map_iter = column_key_map_.find(index);
    if (column_map_iter != column_key_map_.end() && !column_map_iter->second.empty()) {
        return NewTraverseIterator(index, column_map_iter->second.front());
    }
    uint64_t expire_time = GetExpireTime(GetTTL(index, 0).abs_ttl*60*1000);
    uint64_t expire_cnt = GetTTL(index, 0).lat_ttl;
    rocksdb::ReadOptions ro = rocksdb::ReadOptions();
    const rocksdb::Snapshot* snapshot = db_->GetSnapshot();
    ro.snapshot = snapshot;
    //ro.prefix_same_as_start = true;
    ro.pin_data = true;
    rocksdb::Iterator* it = db_->NewIterator(ro, cf_hs_[index + 1]);
    return new DiskTableTraverseIterator(db_, it, snapshot, ttl_type_, expire_time, expire_cnt);
}

TableIterator* DiskTable::NewTraverseIterator(uint32_t index, uint32_t ts_index) {
    if (ts_index < 0) {
        return NULL;
    }
    auto column_map_iter = column_key_map_.find(index);
    if (column_map_iter == column_key_map_.end()) {
        PDLOG(WARNING, "index %d not found in column key map table tid %u pid %u", index, id_, pid_);
        return NULL;
    }
    if (std::find(column_map_iter->second.cbegin(), column_map_iter->second.cend(), ts_index) == column_map_iter->second.cend()) {
        PDLOG(WARNING, "ts cloumn not member of index, ts id %d index id %u, failed getting table tid %u pid %u", ts_index, index, id_, pid_);
        return NULL;
    }
    uint64_t expire_time = GetExpireTime(GetTTL(index, ts_index).abs_ttl*60*1000);
    uint64_t expire_cnt = GetTTL(index, ts_index).lat_ttl;
    rocksdb::ReadOptions ro = rocksdb::ReadOptions();
    const rocksdb::Snapshot* snapshot = db_->GetSnapshot();
    ro.snapshot = snapshot;
    //ro.prefix_same_as_start = true;
    ro.pin_data = true;
    rocksdb::Iterator* it = db_->NewIterator(ro, cf_hs_[index + 1]);
    if (column_map_iter->second.size() == 1) {
        return new DiskTableTraverseIterator(db_, it, snapshot, ttl_type_, expire_time, expire_cnt);
    }
    return new DiskTableTraverseIterator(db_, it, snapshot, ttl_type_, expire_time, expire_cnt, ts_index);
}

DiskTableIterator::DiskTableIterator(rocksdb::DB* db, rocksdb::Iterator* it, 
            const rocksdb::Snapshot* snapshot, const std::string& pk) : 
            db_(db), it_(it), snapshot_(snapshot), pk_(pk), ts_(0) {
}

DiskTableIterator::DiskTableIterator(rocksdb::DB* db, rocksdb::Iterator* it,
                                     const rocksdb::Snapshot* snapshot, const std::string& pk, uint8_t ts_idx) :
    db_(db), it_(it), snapshot_(snapshot), pk_(pk), ts_(0), ts_idx_(ts_idx) {
    has_ts_idx_ = true;
}

DiskTableIterator::~DiskTableIterator() {
    delete it_;
    db_->ReleaseSnapshot(snapshot_);
}

bool DiskTableIterator::Valid() {
    if (it_ == NULL || !it_->Valid()) {
        return false;
    }
    std::string cur_pk;
    uint8_t cur_ts_idx = UINT8_MAX;
    ParseKeyAndTs(has_ts_idx_, it_->key(), cur_pk, ts_, cur_ts_idx);
    return has_ts_idx_ ? cur_pk == pk_ && cur_ts_idx == ts_idx_ : cur_pk == pk_;
}

void DiskTableIterator::Next() {
    return it_->Next();
}

rtidb::base::Slice DiskTableIterator::GetValue() const {
    rocksdb::Slice value = it_->value();
    return rtidb::base::Slice(value.data(), value.size());
}

std::string DiskTableIterator::GetPK() const {
    return pk_;
}

uint64_t DiskTableIterator::GetKey() const {
    return ts_;
}

void DiskTableIterator::SeekToFirst() {
    if (has_ts_idx_) {
        it_->Seek(rocksdb::Slice(CombineKeyTs(pk_, UINT64_MAX, ts_idx_)));
    } else {
        it_->Seek(rocksdb::Slice(CombineKeyTs(pk_, UINT64_MAX)));
    }
}

void DiskTableIterator::Seek(const uint64_t ts) {
    if (has_ts_idx_) {
        it_->Seek(rocksdb::Slice(CombineKeyTs(pk_, ts, ts_idx_)));
    } else {
        it_->Seek(rocksdb::Slice(CombineKeyTs(pk_, ts)));
    }
}

bool DiskTableIterator::Seek(const uint64_t time, ::rtidb::api::GetType type) {
    switch(type) {
        case ::rtidb::api::GetType::kSubKeyEq:
            Seek(time);
            return Valid() && ts_ == time;
        case ::rtidb::api::GetType::kSubKeyLe:
            Seek(time);
            return true;
        case ::rtidb::api::GetType::kSubKeyLt:
            Seek(time - 1);
            return true;
        case ::rtidb::api::GetType::kSubKeyGe:
            SeekToFirst();
            return Valid() && ts_ >= time;
        case ::rtidb::api::GetType::kSubKeyGt:
            SeekToFirst();
            return Valid() && ts_ > time;
        default:
            return false;
    }
    return false;
}

bool DiskTableIterator::Seek(const uint64_t time, ::rtidb::api::GetType type, uint32_t max_cnt, uint32_t& cnt) {
    SeekToFirst();
    while(Valid() && (cnt < max_cnt || max_cnt == 0)) {
        switch(type) {
            case ::rtidb::api::GetType::kSubKeyEq:
                if (ts_ <= time) {
                    return ts_ == time;
                }
                break;
            case ::rtidb::api::GetType::kSubKeyLe:
                if (ts_ <= time) {
                    return true;
                }
                break;
            case ::rtidb::api::GetType::kSubKeyLt:
                if (ts_ < time) {
                    return true;
                }
                break;
            case ::rtidb::api::GetType::kSubKeyGe:
                return ts_ >= time;
            case ::rtidb::api::GetType::kSubKeyGt:
                return ts_ > time;
            default:
                return false;
        }
        it_->Next();
        ++cnt;
    }
    return false;
}

DiskTableTraverseIterator::DiskTableTraverseIterator(rocksdb::DB* db, rocksdb::Iterator* it, 
        const rocksdb::Snapshot* snapshot, ::rtidb::api::TTLType ttl_type, const uint64_t& expire_time, const uint64_t& expire_cnt) : 
        db_(db), it_(it), snapshot_(snapshot), ttl_type_(ttl_type), record_idx_(0), expire_value_(TTLDesc(expire_time, expire_cnt)),
        has_ts_idx_(false), ts_idx_(0), traverse_cnt_(0) {
 };

DiskTableTraverseIterator::DiskTableTraverseIterator(rocksdb::DB *db, rocksdb::Iterator *it,
    const rocksdb::Snapshot *snapshot, ::rtidb::api::TTLType ttl_type, const uint64_t& expire_time, const uint64_t& expire_cnt, int32_t ts_idx) :
    db_(db), it_(it), snapshot_(snapshot), ttl_type_(ttl_type), record_idx_(0), expire_value_(TTLDesc(expire_time, expire_cnt)),
    has_ts_idx_(true), ts_idx_(ts_idx), traverse_cnt_(0) {
};

DiskTableTraverseIterator::~DiskTableTraverseIterator() {
    delete it_;
    db_->ReleaseSnapshot(snapshot_);
}

uint64_t DiskTableTraverseIterator::GetCount() const {
    return traverse_cnt_;
}

bool DiskTableTraverseIterator::Valid() {
    if (traverse_cnt_ >= FLAGS_max_traverse_cnt) {
        return false;
    }
    return it_->Valid();
}

void DiskTableTraverseIterator::Next() {
    for (it_->Next(); it_->Valid(); it_->Next()) {
        std::string last_pk = pk_;
        uint8_t cur_ts_idx = UINT8_MAX;
        traverse_cnt_++;
        if (traverse_cnt_ >= FLAGS_max_traverse_cnt) {
            if (has_ts_idx_) {
                uint64_t ts = 0;
                std::string tmp_pk;
                ParseKeyAndTs(has_ts_idx_, it_->key(), tmp_pk, ts, cur_ts_idx);
                if (tmp_pk == pk_ && cur_ts_idx < ts_idx_) {
                    ts_ = UINT64_MAX;
                }
            }
            break;
        }
        ParseKeyAndTs(has_ts_idx_, it_->key(), pk_, ts_, cur_ts_idx);
        if (last_pk == pk_) {
            if (has_ts_idx_ && (cur_ts_idx != ts_idx_)) {
                continue;
            }
            record_idx_++;
        } else {
            record_idx_ = 0;
            if (has_ts_idx_ && (cur_ts_idx != ts_idx_)) {
                continue;
            }
            record_idx_ = 1;
        }
        if (IsExpired()) {
            NextPK();
        }
        break;
    }
}

rtidb::base::Slice DiskTableTraverseIterator::GetValue() const {
    rocksdb::Slice value = it_->value();
    return rtidb::base::Slice(value.data(), value.size());
}

std::string DiskTableTraverseIterator::GetPK() const {
    return pk_;
}

uint64_t DiskTableTraverseIterator::GetKey() const {
    return ts_;
}

void DiskTableTraverseIterator::SeekToFirst() {
    it_->SeekToFirst();
    record_idx_ = 1;
    for (; it_->Valid(); it_->Next()) {
        uint8_t cur_ts_idx = UINT8_MAX;
        traverse_cnt_++;
        if (traverse_cnt_ >= FLAGS_max_traverse_cnt) {
            if (has_ts_idx_) {
                uint64_t ts = 0;
                std::string tmp_pk;
                ParseKeyAndTs(has_ts_idx_, it_->key(), tmp_pk, ts, cur_ts_idx);
                if (tmp_pk == pk_ && cur_ts_idx < ts_idx_) {
                    ts_ = UINT64_MAX;
                }
            }
            break;
        }
        ParseKeyAndTs(has_ts_idx_, it_->key(), pk_, ts_, cur_ts_idx);
        if (has_ts_idx_ && cur_ts_idx != ts_idx_) {
            continue;
        }
        if (IsExpired()) {
            NextPK();
        }
        break;
    }
}

void DiskTableTraverseIterator::Seek(const std::string& pk, uint64_t time) {
    std::string combine;
    if (has_ts_idx_) {
        combine = CombineKeyTs(pk, time, ts_idx_);
    } else {
        combine = CombineKeyTs(pk, time);
    }
    it_->Seek(rocksdb::Slice(combine));
    if (ttl_type_ == ::rtidb::api::TTLType::kLatestTime) {
        record_idx_ = 0;
        for (; it_->Valid(); it_->Next()) {
            uint8_t cur_ts_idx = UINT8_MAX;
            traverse_cnt_++;
            if (traverse_cnt_ >= FLAGS_max_traverse_cnt) {
                if (has_ts_idx_) {
                    std::string tmp_pk;
                    uint64_t ts = 0;
                    ParseKeyAndTs(has_ts_idx_, it_->key(), tmp_pk, ts, cur_ts_idx);
                    if (tmp_pk == pk_ && cur_ts_idx < ts_idx_) {
                        ts_ = UINT64_MAX;
                    }
                }
                break;
            }
            ParseKeyAndTs(has_ts_idx_, it_->key(), pk_, ts_, cur_ts_idx);
            if (pk_ == pk) {
                if (has_ts_idx_ && (cur_ts_idx != ts_idx_)) {
                    continue;
                }
                record_idx_++;
                if (IsExpired()) {
                    NextPK();
                    break;
                } 
                if (ts_ >= time) {
                    continue;
                }
            } else {
                record_idx_ = 1;
                if (IsExpired()) {
                    NextPK();
                }
            }
            break;
        }
    } else {
        for (; it_->Valid(); it_->Next()) {
            uint8_t cur_ts_idx = UINT8_MAX;
            traverse_cnt_++;
            if (traverse_cnt_ >= FLAGS_max_traverse_cnt) {
                if (has_ts_idx_) {
                    std::string tmp_pk;
                    uint64_t ts = 0;
                    ParseKeyAndTs(has_ts_idx_, it_->key(), tmp_pk, ts, cur_ts_idx);
                    if (tmp_pk == pk_ && cur_ts_idx < ts_idx_) {
                        ts_ = UINT64_MAX;
                    }
                }
                break;
            }
            ParseKeyAndTs(has_ts_idx_, it_->key(), pk_, ts_, cur_ts_idx);
            if (pk_ == pk) {
                if (has_ts_idx_ && (cur_ts_idx != ts_idx_)) {
                    continue;
                }
                if (ts_ >= time) {
                    continue;
                }
                if (IsExpired()) {
                    NextPK();
                }    
            } else {
                if (has_ts_idx_ && (cur_ts_idx != ts_idx_)) {
                    continue;
                }
                if (IsExpired()) {
                    NextPK();
                }    
            }
            break;
        }
    }
}

bool DiskTableTraverseIterator::IsExpired() {
    if (!expire_value_.HasExpire(ttl_type_)) {
        return false;
    }
    if (ttl_type_ == ::rtidb::api::TTLType::kLatestTime) {
        return record_idx_ > expire_value_.lat_ttl;
    } else if (ttl_type_ == ::rtidb::api::TTLType::kAbsoluteTime) {
        return ts_ < expire_value_.abs_ttl;
    } else if (ttl_type_ == ::rtidb::api::TTLType::kAbsAndLat) {
        return ts_ < expire_value_.abs_ttl && record_idx_ > expire_value_.lat_ttl;
    } else {
        return ts_ < expire_value_.abs_ttl || record_idx_ > expire_value_.lat_ttl;
    }
}

void DiskTableTraverseIterator::NextPK() {
    std::string last_pk = pk_;
    std::string combine;
    if (has_ts_idx_) {
        it_->Seek(rocksdb::Slice(CombineKeyTs(last_pk, 0, ts_idx_)));
    } else {
        it_->Seek(rocksdb::Slice(CombineKeyTs(last_pk, 0)));
    }
    record_idx_ = 1;
    while (it_->Valid()) {
        uint8_t cur_ts_idx = UINT8_MAX;
        traverse_cnt_++;
        if (traverse_cnt_ >= FLAGS_max_traverse_cnt) {
            if (has_ts_idx_) {
                std::string tmp_pk;
                uint64_t ts = 0;
                ParseKeyAndTs(has_ts_idx_, it_->key(), tmp_pk, ts, cur_ts_idx);
                if (tmp_pk == pk_ && cur_ts_idx < ts_idx_) {
                    ts_ = UINT64_MAX;
                }
            }
            break;
        }
        ParseKeyAndTs(has_ts_idx_, it_->key(), pk_, ts_, cur_ts_idx);
        if (pk_ != last_pk) {
            if (has_ts_idx_ && (cur_ts_idx != ts_idx_)) {
                it_->Next();
                continue;
            }
            if (!IsExpired()) {
                return;
            } else {
                last_pk = pk_;
                std::string combine;
                if (has_ts_idx_) {
                    it_->Seek(rocksdb::Slice(CombineKeyTs(last_pk, 0, ts_idx_)));
                } else {
                    it_->Seek(rocksdb::Slice(CombineKeyTs(last_pk, 0)));
                }
                record_idx_ = 1;
            }
        } else {
            it_->Next();
        }
    }
}

}
}
