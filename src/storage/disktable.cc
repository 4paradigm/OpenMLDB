//
// Created by yangjun on 12/14/18.
//

#include "storage/disktable.h"
#include "timer.h"
#include "logging.h"
#include "base/hash.h"

using ::baidu::common::INFO;
using ::baidu::common::WARNING;
using ::baidu::common::DEBUG;

DECLARE_string(ssd_root_path);
DECLARE_string(hdd_root_path);

using namespace rocksdb;

namespace rtidb {
namespace storage {

const static uint32_t SEED = 0x52806833; // 4PD!
static Options ssd_option_template;
static Options hdd_option_template;
static bool options_template_initialized = false;

DiskTable::DiskTable(const std::string &name, uint32_t id, uint32_t pid,
                         const std::map<std::string, uint32_t> &mapping, uint64_t ttl) :
        name_(name), id_(id), pid_(pid), schema_(),
        mapping_(mapping), ttl_(ttl * 60 * 1000) {
    if (!options_template_initialized) {
        initOptionTemplate();
    }
    db_ = nullptr;
    storage_mode_ = ::rtidb::api::StorageMode::kHDD;
}

DiskTable::~DiskTable() {
    for (auto handle : cf_hs_) {
        delete handle;
    }
    if (db_ != nullptr)
        delete db_;
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

bool DiskTable::Init() {
    cf_ds_.push_back(ColumnFamilyDescriptor(kDefaultColumnFamilyName, ColumnFamilyOptions()));
    std::map<std::string, uint32_t> tempmapping;
    for (auto iter = mapping_.begin(); iter != mapping_.end(); ++iter) {
        std::vector<std::string> col_info;
        PDLOG(DEBUG, "DiskTable::CreateTable col_name = %s, idx = %u",
              iter->first.c_str(), iter->second);
        ParseColInfo(iter->first, "|", col_info);
        tempmapping.insert(std::make_pair(col_info[0], iter->second));
        ColumnFamilyOptions cfo;
        if (storage_mode_ == ::rtidb::api::StorageMode::kSSD) {
            cfo = ColumnFamilyOptions(ssd_option_template);
            // cfo.cf_paths.push_back(DbPath(FLAGS_ssd_root_path + "/" + std::to_string(id_) + "_" + std::to_string(pid_), 0));
            options_ = ssd_option_template;
        } else {
            cfo = ColumnFamilyOptions(hdd_option_template);
            // cfo.cf_paths.push_back(DbPath(FLAGS_hdd_root_path + "/" + std::to_string(id_) + "_" + std::to_string(pid_), 0));
            options_ = hdd_option_template;
        }
        auto len = static_cast<size_t>(std::stoi(col_info[1], nullptr, 10));
        if (len > 0) {
            // cfo.prefix_extractor.reset(NewCappedPrefixTransform(len));
            cfo.comparator = &cmp_;
            PDLOG(DEBUG, "prefix_extractor = NewCappedPrefixTransform(%d)", len);
        }
        cf_ds_.push_back(ColumnFamilyDescriptor(col_info[0], cfo));
        PDLOG(DEBUG, "cf_ds_ push_back complete, cf_name %s", col_info[0].c_str());
    }
    mapping_.swap(tempmapping);
    // cf_ds_[0].options.cf_paths.push_back(cf_ds_[1].options.cf_paths[0]);
    std::string path = FLAGS_hdd_root_path + "/" + std::to_string(id_) + "_" + std::to_string(pid_);
    //options_.db_paths.push_back(DbPath(db_paths_[i]+name_,0));
    //PDLOG(DEBUG, "DiskTable::CreateTable db_paths of disk %d is %s", i, options_[i].db_paths[0].path.c_str());
    options_.create_if_missing = true;
    options_.error_if_exists = true;
    options_.create_missing_column_families = true;
    // PDLOG(DEBUG, "DiskTable::CreateTable DB before open, db_ = %p, cf_hs_.size = %d path %s", db_, cf_hs_.size(), cf_ds_[0].options.cf_paths[0].path.c_str());
    Status s = DB::Open(options_, path, cf_ds_, &cf_hs_, &db_);
    PDLOG(DEBUG, "DiskTable::CreateTable DB after open, db_ = %p, cf_hs_.size = %d,", db_, cf_hs_.size());
    if (!s.ok()) {
        PDLOG(WARNING, "CreateWithPath db, status = %s", s.ToString().c_str());
        //TODO: Handling the DB creation errors
        return false;
    } 
    return true;
}

bool DiskTable::Destroy() {
    for (auto handle : cf_hs_) {
        delete handle;
    }
    if (db_ != nullptr)
        delete db_;
    Status s = DestroyDB(cf_ds_[0].options.cf_paths[0].path, options_, cf_ds_);
    if (s.ok()) {
        PDLOG(DEBUG, "DiskTable::Destroy success");
        return true;
    } else {
        PDLOG(DEBUG, "DiskTable::Destroy failed, status = %s", s.ToString().c_str());
        return false;
    }
}

bool DiskTable::Put(const std::string &pk, uint64_t time, const char *data, uint32_t size) {
    Status s;
    Slice spk = Slice(CombineKeyTs(pk, time));
    s = db_->Put(WriteOptions(), cf_hs_[1], spk, Slice(data, size));
    PDLOG(DEBUG, "DiskTable::Put single db_ = %p, cf_hs_[%d] = %p, pk = %s, value = %s",
           db_, 1, cf_hs_[1], spk.ToString().c_str(),
          Slice(data, size).ToString().c_str());
//  PDLOG(DEBUG, "record key %s, value %s tid %u pid %u", spk.ToString().c_str(), value.ToString().c_str(), id_, pid_);
    PDLOG(DEBUG, "status return %s", s.ToString().c_str());
    return s.ok();
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
        PDLOG(DEBUG, "DiskTable::Put multiple, cf_hs_[%d] = %p, pk = %s, value = %s",
              it->idx() + 1, cf_hs_[it->idx() + 1],
              spk.ToString().c_str(),
              value.c_str());
    }
    s = db_->Write(WriteOptions(), &batch);
//  record_cnt_.fetch_add(1, std::memory_order_relaxed);
//  record_byte_size_.fetch_add(GetRecordSize(value.length()));
//  s = dbs_[index]->Write(WriteOptions(), &batch);
    if (s.ok()) {
        return true;
    } else {
        PDLOG(DEBUG, "DiskTable::Put multiple failed, msg = %s", s.ToString().c_str());
        return false;
    }
}

bool DiskTable::Delete(const std::string& pk, uint32_t idx) {
    Status s = db_->DeleteRange(WriteOptions(), cf_hs_[idx + 1], Slice(CombineKeyTs(pk, UINT64_MAX)), Slice(CombineKeyTs(pk, 0)));
    return s.ok();
}

bool DiskTable::Get(uint32_t idx, const std::string& pk, uint64_t ts, std::string * value) {
    Status s;
    Slice spk = Slice(CombineKeyTs(pk, ts));
    s = db_->Get(ReadOptions(), cf_hs_[idx + 1], spk, value);
    if (s.ok()) {
        return true;
    } else {
        return false;
    }
}

bool DiskTable::Get(const std::string& pk, uint64_t ts, std::string * value) {
    return Get(0, pk, ts, value);
}

bool DiskTable::ReadTableFromDisk() {
    options_.create_if_missing = false;
    options_.error_if_exists = false;
    options_.create_missing_column_families = false;
    PDLOG(DEBUG, "DiskTable::ReadTableFromDisk DB before open, db_ = %p, cf_hs_.size = %d,", db_,
          cf_hs_.size());
    Status s = DB::Open(options_, cf_ds_[0].options.cf_paths[0].path, cf_ds_, &cf_hs_, &db_);
    PDLOG(DEBUG, "DiskTable::ReadTableFromDisk DB after open, db_ = %p, cf_hs_.size = %d,", db_,
          cf_hs_.size());
    if (!s.ok())
        PDLOG(WARNING, "ReadTableFromDisk db, status = %s", s.ToString().c_str());
    return s.ok();
}

void DiskTable::SelfTune() {

    //TODO: 支持多SSD, 两种方案, 现在选1:
    //              1. 准备多个DB instance, 设置好options.db_paths
    //              2. 准备多个CF, 设置好cfo.cf_paths,
    //      对于每一个(pk+ts, data), 根据(hash(pk) mod num_ssd), 插入到对应的DB/CF里

    //TODO: 优化所需额外输入:
    //              1. 数据库大概大小,内存额度,空间/时间 trade-off 偏好
    //              2. 不同pk的访问习惯, 如: TS范围读取, 最新的TS, PK+TS单点查询
    // 14亿, 20列 - 1000列
    //
    //TODO: Tablet接入, 包括replicators
}

void DiskTable::ParseColInfo(const std::string &src, const std::string &separator,
                               std::vector<std::string> &dest) {
    std::string str = src;
    std::string substring;
    std::string::size_type start = 0, index;
    dest.clear();
    index = str.find_last_of(separator); //以最后一个"|"为分隔符
//            PDLOG(INFO, "src = %s, index = %d", src.c_str(), index);
    if (index != std::string::npos) {
        substring = str.substr(start, index - start);
//                    PDLOG(INFO, "first substring = %s", src.c_str());
        dest.push_back(substring);
        substring = str.substr(index + 1);
//                    PDLOG(INFO, "second substring = %s", src.c_str());
        dest.push_back(substring);
    } else {
        dest.push_back(str);
        dest.push_back("1");
    }
}

Iterator *DiskTable::NewIterator(const std::string &pk) {
    return DiskTable::NewIterator(0, pk);
}

Iterator *DiskTable::NewIterator(const std::string &pk, uint64_t ts) {
    return DiskTable::NewIterator(0, pk, ts);
}

Iterator* DiskTable::NewIterator(uint32_t idx, const std::string& pk) {
    PDLOG(DEBUG, "NewIterator, idx = %u, pk = %s, cf_hs_[%u]",
            idx, pk.c_str(), idx + 1);
    ReadOptions ro = ReadOptions();
    Iterator* it;

//            ro.prefix_same_as_start = true;
//            it = db_->NewIterator(ro, cf_hs_[idx * disk_cnt_ + index + 1]);
//            PDLOG(DEBUG, "NewIterator, after new prefix true, it->status = %s, it->valid = %d", it->status().ToString().c_str(), it->Valid());
//            it->Seek(Slice(pk+"|"+std::to_string(std::numeric_limits<uint64_t>::max())));
//            PDLOG(DEBUG, "DiskTable::NewIterator prefix true, seek to pk, status = %s, valid = %d", it->status().ToString().c_str(), it->Valid());
//            for (;it->Valid();it->Next()) {
//                PDLOG(DEBUG, "DiskTable::NewIterator prefix true, scan from pk, pk = %s, value = %s, status = %s",
//                        it->key().ToString().c_str(), it->value().ToString().c_str(), it->status().ToString().c_str());
//            }
//            delete it;
//            ro.prefix_same_as_start = false;
//            it = db_->NewIterator(ro, cf_hs_[idx * disk_cnt_ + index + 1]);
//            PDLOG(DEBUG, "NewIterator, after new prefix false, it->status = %s, it->valid = %d", it->status().ToString().c_str(), it->Valid());
//            it->Seek(Slice(pk));
//            PDLOG(DEBUG, "DiskTable::NewIterator prefix false, seek to pk, status = %s, valid = %d", it->status().ToString().c_str(), it->Valid());
//            for (;it->Valid();it->Next()) {
//                PDLOG(DEBUG, "DiskTable::NewIterator prefix false, scan from pk, pk = %s, value = %s, status = %s, valid = %d",
//                      it->key().ToString().c_str(), it->value().ToString().c_str(), it->status().ToString().c_str(), it->Valid());
//            }
//            delete it;
    ro.prefix_same_as_start = true;
    ro.pin_data = true;
    it = db_->NewIterator(ro, cf_hs_[idx + 1]);
    PDLOG(DEBUG, "NewIterator, after new prefix true, it->status = %s, it->valid = %d", it->status().ToString().c_str(), it->Valid());
    it->Seek(Slice(CombineKeyTs(pk, UINT64_MAX)));
//            PDLOG(DEBUG, "NewIterator, after seek pk, it->valid = %d, it->key = %s, it->value=%s",
//                  it->Valid(), it->key().ToString().c_str(), it->value().ToString().c_str());
    return it;
}

Iterator* DiskTable::NewIterator(uint32_t idx, const std::string& pk, uint64_t ts) {
    Iterator* it = DiskTable::NewIterator(idx, pk);
    // DiskTable::SeekTsWithKeyFromItr(pk, ts, it);
    it->Seek(Slice(CombineKeyTs(pk, ts)));
    return it;
}

void DiskTable::SeekTsWithKeyFromItr(const std::string& pk, const uint64_t ts, rocksdb::Iterator* it) {
    PDLOG(DEBUG, "DiskTable::SeekTsWithKeyFromItr pk = %s, ts = %lld", pk.c_str(), ts);
    for (; it->Valid(); it->Next()) {
        std::string cur_pk;
        uint64_t cur_ts = 0;
        ParseKeyAndTs(it->key().ToString(), cur_pk, cur_ts);
        PDLOG(DEBUG, "DiskTable::SeekTsWithKeyFromItr it->pk = %s, it->ts = %lu", cur_pk.c_str(), cur_ts);
        if (pk == cur_pk && cur_ts <= ts) {
            PDLOG(DEBUG, "DiskTable::SeekTsWithKeyFromItr hit");
            return;
        }
    }
}
    
}
}
