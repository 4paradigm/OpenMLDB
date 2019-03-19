//
// Created by yangjun on 12/14/18.
//

#include "storage/disk_table.h"
#include "timer.h"
#include "logging.h"
#include "base/hash.h"
#include "base/file_util.h"

using ::baidu::common::INFO;
using ::baidu::common::WARNING;
using ::baidu::common::DEBUG;

DECLARE_string(ssd_root_path);
DECLARE_string(hdd_root_path);

using namespace rocksdb;

namespace rtidb {
namespace storage {

static Options ssd_option_template;
static Options hdd_option_template;
static bool options_template_initialized = false;

DiskTable::DiskTable(const std::string &name, uint32_t id, uint32_t pid,
                     const std::map<std::string, uint32_t> &mapping, 
                     uint64_t ttl, ::rtidb::api::TTLType ttl_type,
                     ::rtidb::api::StorageMode storage_mode) :
        name_(name), id_(id), pid_(pid), idx_cnt_(mapping.size()), schema_(), mapping_(mapping),
        ttl_(ttl * 60 * 1000), ttl_type_(ttl_type), storage_mode_(storage_mode), offset_(0), compaction_filter_(nullptr) {
    if (!options_template_initialized) {
        initOptionTemplate();
    }
    db_ = nullptr;
    is_leader_ = true;
}

DiskTable::~DiskTable() {
    for (auto handle : cf_hs_) {
        delete handle;
    }
    if (db_ != nullptr) {
        delete db_;
    }
    if (compaction_filter_ != nullptr) {
        delete compaction_filter_;
    }
    /*std::string root_path = storage_mode_ == ::rtidb::api::StorageMode::kSSD ? FLAGS_ssd_root_path : FLAGS_hdd_root_path;
    std::string path = root_path + "/" + std::to_string(id_) + "_" + std::to_string(pid_) + "/data";
    Status s = DestroyDB(path, options_, cf_ds_);
    if (s.ok()) {
        PDLOG(INFO, "Destroy success. tid %u pid %u", id_, pid_);
    } else {
        PDLOG(WARNING, "Destroy failed. tid %u pid %u status %s", id_, pid_, s.ToString().c_str());
    }*/
}

void DiskTable::initOptionTemplate() {
    auto cache = NewLRUCache(512 << 20, 8); //Can be set by flags
    //SSD options template
    ssd_option_template.max_open_files = -1;
    ssd_option_template.env->SetBackgroundThreads(1, Env::Priority::HIGH); //flush threads
    ssd_option_template.env->SetBackgroundThreads(4, Env::Priority::LOW);  //compaction threads
    ssd_option_template.memtable_prefix_bloom_size_ratio = 0.02;
    ssd_option_template.compaction_style = kCompactionStyleLevel;
    ssd_option_template.level0_file_num_compaction_trigger = 10;
    ssd_option_template.level0_slowdown_writes_trigger = 20;
    ssd_option_template.level0_stop_writes_trigger = 40;
    ssd_option_template.write_buffer_size = 64 << 20;
    ssd_option_template.target_file_size_base = 64 << 20;
    ssd_option_template.max_bytes_for_level_base = 512 << 20;
    rocksdb::BlockBasedTableOptions ssd_table_options;
    ssd_table_options.cache_index_and_filter_blocks = true;
    ssd_table_options.pin_l0_filter_and_index_blocks_in_cache = true;
    ssd_table_options.block_cache = cache;
    ssd_table_options.filter_policy.reset(NewBloomFilterPolicy(10, false));
    ssd_table_options.whole_key_filtering = false;
    ssd_table_options.block_size = 4 << 10;
    ssd_table_options.use_delta_encoding = false;
    ssd_option_template.table_factory.reset(NewBlockBasedTableFactory(ssd_table_options));

    //HDD options template
    hdd_option_template.max_open_files = -1;
    hdd_option_template.env->SetBackgroundThreads(1, Env::Priority::HIGH); //flush threads
    hdd_option_template.env->SetBackgroundThreads(1, Env::Priority::LOW);  //compaction threads
    hdd_option_template.memtable_prefix_bloom_size_ratio = 0.02;
    hdd_option_template.optimize_filters_for_hits = true;
    hdd_option_template.level_compaction_dynamic_level_bytes = true;
    hdd_option_template.max_file_opening_threads = 1; //set to the number of disks on which the db root folder is mounted
    hdd_option_template.compaction_readahead_size = 16 << 20;
    hdd_option_template.new_table_reader_for_compaction_inputs = true;
    hdd_option_template.compaction_style = kCompactionStyleLevel;
    hdd_option_template.level0_file_num_compaction_trigger = 10;
    hdd_option_template.level0_slowdown_writes_trigger = 20;
    hdd_option_template.level0_stop_writes_trigger = 40;
    hdd_option_template.write_buffer_size = 256 << 20;
    hdd_option_template.target_file_size_base = 256 << 20;
    hdd_option_template.max_bytes_for_level_base = 1024 << 20;
    rocksdb::BlockBasedTableOptions hdd_table_options;
    hdd_table_options.cache_index_and_filter_blocks = true;
    hdd_table_options.pin_l0_filter_and_index_blocks_in_cache = true;
    hdd_table_options.block_cache = cache;
    hdd_table_options.filter_policy.reset(NewBloomFilterPolicy(10, false));
    hdd_table_options.whole_key_filtering = false;
    hdd_table_options.block_size = 256 << 10;
    hdd_table_options.use_delta_encoding = false;
    hdd_option_template.table_factory.reset(NewBlockBasedTableFactory(hdd_table_options));

    options_template_initialized = true;
}

bool DiskTable::InitColumnFamilyDescriptor() {
    cf_ds_.clear();
    cf_ds_.push_back(ColumnFamilyDescriptor(kDefaultColumnFamilyName, ColumnFamilyOptions()));
    std::map<std::string, uint32_t> tempmapping;
    for (auto iter = mapping_.begin(); iter != mapping_.end(); ++iter) {
        ColumnFamilyOptions cfo;
        if (storage_mode_ == ::rtidb::api::StorageMode::kSSD) {
            cfo = ColumnFamilyOptions(ssd_option_template);
            options_ = ssd_option_template;
        } else {
            cfo = ColumnFamilyOptions(hdd_option_template);
            options_ = hdd_option_template;
        }
        cfo.comparator = &cmp_;
        cf_ds_.push_back(ColumnFamilyDescriptor(iter->first, cfo));
        PDLOG(DEBUG, "add cf_name %s. tid %u pid %u", iter->first.c_str(), id_, pid_);
    }
    return true;
}

bool DiskTable::Init() {
    InitColumnFamilyDescriptor();
    std::string root_path = storage_mode_ == ::rtidb::api::StorageMode::kSSD ? FLAGS_ssd_root_path : FLAGS_hdd_root_path;
    std::string path = root_path + "/" + std::to_string(id_) + "_" + std::to_string(pid_) + "/data";
    if (!::rtidb::base::MkdirRecur(path)) {
        PDLOG(WARNING, "fail to create path %s", path.c_str());
        return false;
    }
    options_.create_if_missing = true;
    options_.error_if_exists = true;
    options_.create_missing_column_families = true;
    if (ttl_type_ == ::rtidb::api::TTLType::kAbsoluteTime && ttl_ > 0) {
        compaction_filter_ = new AbsoluteTTLCompactionFilter(ttl_);
        options_.compaction_filter = compaction_filter_;
    }
    Status s = DB::Open(options_, path, cf_ds_, &cf_hs_, &db_);
    if (!s.ok()) {
        PDLOG(WARNING, "rocksdb open failed. tid %u pid %u error %s", id_, pid_, s.ToString().c_str());
        return false;
    } 
    return true;
}

bool DiskTable::Put(const std::string &pk, uint64_t time, const char *data, uint32_t size) {
    Status s;
    Slice spk = Slice(CombineKeyTs(pk, time));
    s = db_->Put(WriteOptions(), cf_hs_[1], spk, Slice(data, size));
    PDLOG(DEBUG, "Put pk %s value %s", spk.ToString().c_str(), Slice(data, size).ToString().c_str());
    if (s.ok()) {
        offset_.fetch_add(1, std::memory_order_relaxed);
        return true;
    } else {
        PDLOG(DEBUG, "Put failed. tid %u pid %u msg %s", id_, pid_, s.ToString().c_str());
        return false;
    }
}

bool DiskTable::Put(uint64_t time, const std::string &value, const Dimensions &dimensions) {
    WriteBatch batch;
    Status s;
    Dimensions::const_iterator it = dimensions.begin();
    for (; it != dimensions.end(); ++it) {
        if (it->idx() >= (cf_hs_.size() - 1)) {
            PDLOG(WARNING, "failed putting key %s to dimension %u in table tid %u pid %u",
                            it->key().c_str(), it->idx(), id_, pid_);
            return false;
        }
        Slice spk = Slice(CombineKeyTs(it->key(), time));
        batch.Put(cf_hs_[it->idx() + 1], spk, value);
        PDLOG(DEBUG, "Put multiple, pk %s value %s idx %u tid %u pid %u",
                      spk.ToString().c_str(), value.c_str(), it->idx(), id_, pid_);
    }
    s = db_->Write(WriteOptions(), &batch);
    if (s.ok()) {
        offset_.fetch_add(1, std::memory_order_relaxed);
        return true;
    } else {
        PDLOG(DEBUG, "Put failed. tid %u pid %u msg %s", id_, pid_, s.ToString().c_str());
        return false;
    }
}

bool DiskTable::Delete(const std::string& pk, uint32_t idx) {
    Status s = db_->DeleteRange(WriteOptions(), cf_hs_[idx + 1], Slice(CombineKeyTs(pk, UINT64_MAX)), Slice(CombineKeyTs(pk, 0)));
    if (s.ok()) {
        offset_.fetch_add(1, std::memory_order_relaxed);
        return true;
    } else {
        PDLOG(DEBUG, "Delete failed. tid %u pid %u msg %s", id_, pid_, s.ToString().c_str());
        return false;
    }
}

bool DiskTable::Get(uint32_t idx, const std::string& pk, uint64_t ts, std::string& value) {
    Status s;
    Slice spk = Slice(CombineKeyTs(pk, ts));
    s = db_->Get(ReadOptions(), cf_hs_[idx + 1], spk, &value);
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
    InitColumnFamilyDescriptor();
    options_.create_if_missing = false;
    options_.error_if_exists = false;
    options_.create_missing_column_families = false;
    std::string root_path = storage_mode_ == ::rtidb::api::StorageMode::kSSD ? FLAGS_ssd_root_path : FLAGS_hdd_root_path;
    std::string path = root_path + "/" + std::to_string(id_) + "_" + std::to_string(pid_) + "/data";
    if (!rtidb::base::IsExists(path)) {
        return false;
    }
    if (ttl_type_ == ::rtidb::api::TTLType::kAbsoluteTime && ttl_ > 0) {
        compaction_filter_ = new AbsoluteTTLCompactionFilter(ttl_);
        options_.compaction_filter = compaction_filter_;
    }
    Status s = DB::Open(options_, path, cf_ds_, &cf_hs_, &db_);
    PDLOG(DEBUG, "Load DB. tid %u pid %u ColumnFamilyHandle size %d,", id_, pid_, cf_hs_.size());
    if (!s.ok()) {
        PDLOG(WARNING, "Load DB failed. tid %u pid %u msg %s", id_, pid_, s.ToString().c_str());
    }    
    return true;
}

void DiskTable::SchedGc() {
    if (ttl_type_ == ::rtidb::api::TTLType::kLatestTime && ttl_ > 0) {
        GcHead();
    }
}

void DiskTable::GcHead() {
    uint64_t start_time = ::baidu::common::timer::get_micros() / 1000;
    uint64_t ttl_num = ttl_ / 60 / 1000;
    if (ttl_num < 1) {
        return;
    }
    for (auto cf_hs : cf_hs_) {
        ReadOptions ro = ReadOptions();
        ro.prefix_same_as_start = true;
        ro.pin_data = true;
        Iterator* it = db_->NewIterator(ro, cf_hs);
        it->SeekToFirst();
        std::string last_pk;
        uint64_t count = 0;
        while (it->Valid()) {
            std::string cur_pk;
            uint64_t ts = 0;
            ParseKeyAndTs(it->key(), cur_pk, ts);
            if (last_pk.empty()) {
                last_pk = cur_pk;
            }
            if (cur_pk == last_pk) {
                if (count < ttl_num) {
                    it->Next();
                    count++;
                    continue;
                } else {
                    Status s = db_->DeleteRange(WriteOptions(), cf_hs, Slice(CombineKeyTs(cur_pk, ts)), Slice(CombineKeyTs(cur_pk, 0)));
                    if (!s.ok()) {
                        PDLOG(WARNING, "Delete failed. tid %u pid %u msg %s", id_, pid_, s.ToString().c_str());
                    }    
                    it->Seek(Slice(CombineKeyTs(cur_pk, 0)));
                }
            } else {
                count = 1;
                last_pk = cur_pk;
                it->Next();
            }
        }
        delete it;
    }
    uint64_t time_used = ::baidu::common::timer::get_micros() / 1000 - start_time;
    PDLOG(INFO, "Gc used %lu second. tid %u pid %u", time_used / 1000, id_, pid_);
}

uint64_t DiskTable::GetExpireTime() {
    if (ttl_.load(std::memory_order_relaxed) == 0
            || ttl_type_ == ::rtidb::api::TTLType::kLatestTime) {
        return 0;
    }
    uint64_t cur_time = ::baidu::common::timer::get_micros() / 1000;
    return cur_time - ttl_.load(std::memory_order_relaxed);
}

DiskTableIterator* DiskTable::NewIterator(const std::string &pk) {
    return DiskTable::NewIterator(0, pk);
}

DiskTableIterator* DiskTable::NewIterator(uint32_t idx, const std::string& pk) {
    ReadOptions ro = ReadOptions();
    ro.prefix_same_as_start = true;
    ro.pin_data = true;
    Iterator* it = db_->NewIterator(ro, cf_hs_[idx + 1]);
    return new DiskTableIterator(it, pk);
}

DiskTableTraverseIterator* DiskTable::NewTraverseIterator(uint32_t idx) {
    ReadOptions ro = ReadOptions();
    ro.prefix_same_as_start = true;
    ro.pin_data = true;
    Iterator* it = db_->NewIterator(ro, cf_hs_[idx + 1]);
    uint64_t expire_value = 0;
    if (ttl_ == 0) {
        expire_value = 0;
    } else if (ttl_type_ == ::rtidb::api::TTLType::kLatestTime) {
        expire_value = ttl_ / 60 / 1000;
    } else {
        uint64_t cur_time = ::baidu::common::timer::get_micros() / 1000;
        expire_value = cur_time - ttl_.load(std::memory_order_relaxed);
    }
    return new DiskTableTraverseIterator(it, ttl_type_, expire_value);
}

DiskTableIterator::DiskTableIterator(rocksdb::Iterator* it, const std::string& pk) : it_(it), pk_(pk), ts_(0) {
}

DiskTableIterator::~DiskTableIterator() {
    delete it_;
}

bool DiskTableIterator::Valid() {
    if (it_ == NULL || !it_->Valid()) {
        return false;
    }
    std::string cur_pk;
    ParseKeyAndTs(it_->key(), cur_pk, ts_);
    return cur_pk == pk_;
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
    it_->Seek(Slice(CombineKeyTs(pk_, UINT64_MAX)));
}

void DiskTableIterator::Seek(uint64_t ts) {
    it_->Seek(Slice(CombineKeyTs(pk_, ts)));
}

DiskTableTraverseIterator::DiskTableTraverseIterator(rocksdb::Iterator* it, 
        ::rtidb::api::TTLType ttl_type, uint64_t expire_value) : 
        it_(it), ttl_type_(ttl_type), expire_value_(expire_value), record_idx_(0) {
}

DiskTableTraverseIterator::~DiskTableTraverseIterator() {
    delete it_;
}

bool DiskTableTraverseIterator::Valid() {
    return it_->Valid();
}

void DiskTableTraverseIterator::Next() {
    it_->Next();
    if (it_->Valid()) {
        std::string tmp_pk;
        ParseKeyAndTs(it_->key(), tmp_pk, ts_);
        if (tmp_pk == pk_) {
            record_idx_++;
        } else {
            pk_ = tmp_pk;
            record_idx_ = 1;
        }
        if (IsExpired()) {
            NextPK();
        }
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
    if (it_->Valid()) {
        ParseKeyAndTs(it_->key(), pk_, ts_);
        if (IsExpired()) {
            NextPK();
        }
    }
}

void DiskTableTraverseIterator::Seek(const std::string& pk, uint64_t time) {
    if (ttl_type_ == ::rtidb::api::TTLType::kLatestTime) {
        it_->Seek(Slice(CombineKeyTs(pk, UINT64_MAX)));
        record_idx_ = 0;
        while (it_->Valid()) {
            record_idx_++;
            ParseKeyAndTs(it_->key(), pk_, ts_);
            if (pk_ == pk) {
                if (IsExpired()) {
                    NextPK();
                    break;
                } 
                if (ts_ >= time) {
                    it_->Next();
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
        it_->Seek(Slice(CombineKeyTs(pk, time)));
        while (it_->Valid()) {
            ParseKeyAndTs(it_->key(), pk_, ts_);
            if (pk_ == pk) {
                if (ts_ >= time) {
                    it_->Next();
                    continue;
                }
                if (IsExpired()) {
                    NextPK();
                }    
                break;
            } else {
                if (IsExpired()) {
                    NextPK();
                }    
            }
            break;
        }
    }
}

bool DiskTableTraverseIterator::IsExpired() {
    if (expire_value_ == 0) {
        return false;
    }
    if (ttl_type_ == ::rtidb::api::TTLType::kLatestTime) {
        return record_idx_ > expire_value_;
    }
    return ts_ < expire_value_;
}

void DiskTableTraverseIterator::NextPK() {
    std::string last_pk = pk_;
    it_->Seek(Slice(CombineKeyTs(last_pk, 0)));
    record_idx_ = 1;
    while (it_->Valid()) {
        std::string tmp_pk;
        ParseKeyAndTs(it_->key(), tmp_pk, ts_);
        if (tmp_pk != last_pk) {
            if (!IsExpired()) {
                pk_ = tmp_pk;
                return;
            } else {
                last_pk = tmp_pk;
                it_->Seek(Slice(CombineKeyTs(last_pk, 0)));
                record_idx_ = 1;
            }
        } else {
            it_->Next();
        }
    }
}

}
}
