//
// Created by wangbao on 02/24/20.
//

#include "storage/disk_no_ts_table.h"
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

DiskNoTsTable::DiskNoTsTable(const std::string &name, uint32_t id, uint32_t pid,
        bool is_leader, const std::map<std::string, uint32_t> &mapping, 
        ::rtidb::common::StorageMode storage_mode,
        const std::string& db_root_path) :
    storage_mode_(storage_mode), name_(name), id_(id), pid_(pid), idx_cnt_(mapping.size()),
    is_leader_(is_leader), mapping_(mapping), compress_type_(compress_type), last_make_snapshot_time_(0).  
    write_opts_(), offset_(0), db_root_path_(db_root_path){
        if (!options_template_initialized) {
            initOptionTemplate();
        }
        write_opts_.disableWAL = FLAGS_disable_wal;
        db_ = nullptr;
}

DiskNoTsTable::DiskNoTsTable(const ::rtidb::api::TableMeta& table_meta,
        const std::string& db_root_path) :
    storage_mode_(storage_mode), name_(name), id_(id), pid_(pid), idx_cnt_(mapping.size()),
    is_leader_(is_leader), mapping_(mapping), compress_type_(compress_type), last_make_snapshot_time_(0).  
    write_opts_(), offset_(0), db_root_path_(db_root_path){
    table_meta_.CopyFrom(table_meta);
    if (!options_template_initialized) {
        initOptionTemplate();
    }
    diskused_ = 0;
    write_opts_.disableWAL = FLAGS_disable_wal;
    db_ = nullptr;
}

DiskNoTsTable::~DiskNoTsTable() {
    for (auto handle : cf_hs_) {
        delete handle;
    }
    if (db_ != nullptr) {
        db_->Close();
        delete db_;
    }
}

void DiskNoTsTable::initOptionTemplate() {
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

bool DiskNoTsTable::InitColumnFamilyDescriptor() {
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
        cf_ds_.push_back(rocksdb::ColumnFamilyDescriptor(iter->first, cfo));
        PDLOG(DEBUG, "add cf_name %s. tid %u pid %u", iter->first.c_str(), id_, pid_);
    }
    return true;
}

bool DiskNoTsTable::Init() {
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

bool DiskNoTsTable::Put(const std::string &pk, const char *data, uint32_t size) {
    rocksdb::Status s;
    rocksdb::Slice spk = rocksdb::Slice(pk);
    s = db_->Put(write_opts_, cf_hs_[1], spk, rocksdb::Slice(data, size));
    if (s.ok()) {
        offset_.fetch_add(1, std::memory_order_relaxed);
        return true;
    } else {
        PDLOG(DEBUG, "Put failed. tid %u pid %u msg %s", id_, pid_, s.ToString().c_str());
        return false;
    }
}

bool DiskNoTsTable::Put(const std::string &value, const Dimensions &dimensions) {
    rocksdb::WriteBatch batch;
    rocksdb::Status s;
    Dimensions::const_iterator it = dimensions.begin();
    for (; it != dimensions.end(); ++it) {
        if (it->idx() >= idx_cnt_) {
            PDLOG(WARNING, "failed putting key %s to dimension %u in table tid %u pid %u",
                            it->key().c_str(), it->idx(), id_, pid_);
            return false;
        }
        rocksdb::Slice spk = rocksdb::Slice(it->key());
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

bool DiskNoTsTable::Delete(const std::string& pk, uint32_t idx) {
    rocksdb::WriteBatch batch;
    batch.DeleteRange(cf_hs_[idx+1], rocksdb::Slice(pk), rocksdb::Slice(pk));
    rocksdb::Status s = db_->Write(write_opts_, &batch);
    if (s.ok()) {
        offset_.fetch_add(1, std::memory_order_relaxed);
        return true;
    } else {
        PDLOG(DEBUG, "Delete failed. tid %u pid %u msg %s", id_, pid_, s.ToString().c_str());
        return false;
    }
}

bool DiskNoTsTable::Get(uint32_t idx, const std::string& pk, 
        std::string& value) {
    if (idx >= idx_cnt_) {
        PDLOG(WARNING, "idx greater than idx_cnt_, failed getting table tid %u pid %u", id_, pid_);
        return false;
    }
    rocksdb::Slice spk ;
    spk = rocksdb::Slice(pk);
    rocksdb::Status s;
    s = db_->Get(rocksdb::ReadOptions(), cf_hs_[idx + 1], spk, &value);
    if (s.ok()) {
        return true;
    } else {
        return false;
    }
}

}
}
