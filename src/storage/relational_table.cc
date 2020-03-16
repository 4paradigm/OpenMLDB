//
// Created by wangbao on 02/24/20.
//

#include "storage/relational_table.h"
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

RelationalTable::RelationalTable(const std::string &name, uint32_t id, uint32_t pid,
        const std::map<std::string, uint32_t> &mapping, 
        ::rtidb::common::StorageMode storage_mode,
        const std::string& db_root_path) :
    storage_mode_(storage_mode), name_(name), id_(id), pid_(pid), idx_cnt_(mapping.size()),
    mapping_(mapping), last_make_snapshot_time_(0),  
    write_opts_(), offset_(0), db_root_path_(db_root_path){
        if (!options_template_initialized) {
            initOptionTemplate();
        }
        write_opts_.disableWAL = FLAGS_disable_wal;
        db_ = nullptr;
}

RelationalTable::RelationalTable(const ::rtidb::api::TableMeta& table_meta,
        const std::string& db_root_path) :
    storage_mode_(table_meta.storage_mode()), name_(table_meta.name()), id_(table_meta.tid()), pid_(table_meta.pid()),
    is_leader_(false), mapping_(std::map<std::string, uint32_t>()), 
    compress_type_(table_meta.compress_type()), last_make_snapshot_time_(0),   
    write_opts_(), offset_(0), db_root_path_(db_root_path){
    table_meta_.CopyFrom(table_meta);
    if (!options_template_initialized) {
        initOptionTemplate();
    }
    diskused_ = 0;
    write_opts_.disableWAL = FLAGS_disable_wal;
    db_ = nullptr;
}

RelationalTable::~RelationalTable() {
    for (auto handle : cf_hs_) {
        delete handle;
    }
    if (db_ != nullptr) {
        db_->Close();
        delete db_;
    }
}

void RelationalTable::initOptionTemplate() {
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

bool RelationalTable::InitColumnFamilyDescriptor() {
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
        cf_ds_.push_back(rocksdb::ColumnFamilyDescriptor(iter->first, cfo));
        PDLOG(DEBUG, "add cf_name %s. tid %u pid %u", iter->first.c_str(), id_, pid_);
    }
    return true;
}

int RelationalTable::InitColumnDesc() {
    if (table_meta_.column_desc_size() > 0) {
        uint32_t key_idx = 0;
        for (const auto &column_desc : table_meta_.column_desc()) {
            if (column_desc.add_ts_idx()) {
                mapping_.insert(std::make_pair(column_desc.name(), key_idx));
                key_idx++;
            } 
        }
    } else {
        for (int32_t i = 0; i < table_meta_.dimensions_size(); i++) {
            mapping_.insert(std::make_pair(table_meta_.dimensions(i), (uint32_t) i));
            PDLOG(INFO, "add index name %s, idx %d to table %s, tid %u, pid %u",
                  table_meta_.dimensions(i).c_str(), i, table_meta_.name().c_str(), id_, pid_);
        }
    }
    // add default dimension
    if (mapping_.empty()) {
        mapping_.insert(std::make_pair("idx0", 0));
        PDLOG(INFO, "no index specified with default");
    }
    return 0;
}

bool RelationalTable::InitFromMeta() {
    if (table_meta_.has_mode() && table_meta_.mode() != ::rtidb::api::TableMode::kTableLeader) {
        is_leader_ = false;
    }
    if (InitColumnDesc() < 0) {
        PDLOG(WARNING, "init column desc failed, tid %u pid %u", id_, pid_);
        return false;
    }
    if (table_meta_.has_schema()) schema_ = table_meta_.schema();
    if (table_meta_.has_compress_type()) compress_type_ = table_meta_.compress_type();
    idx_cnt_ = mapping_.size();
    return true;
}

bool RelationalTable::Init() {
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

bool RelationalTable::Put(const std::string &pk, const char *data, uint32_t size) {
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

bool RelationalTable::Put(const std::string &value, const Dimensions &dimensions) {
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

bool RelationalTable::Delete(const std::string& pk, uint32_t idx) {
    rocksdb::WriteBatch batch;
    batch.Delete(cf_hs_[idx+1], rocksdb::Slice(pk));
    rocksdb::Status s = db_->Write(write_opts_, &batch);
    if (s.ok()) {
        offset_.fetch_add(1, std::memory_order_relaxed);
        return true;
    } else {
        PDLOG(DEBUG, "Delete failed. tid %u pid %u msg %s", id_, pid_, s.ToString().c_str());
        return false;
    }
}
/**
bool RelationalTable::Get(uint32_t idx, const std::string& pk, 
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
*/
bool RelationalTable::Get(uint32_t idx, const std::string& pk, std::string& value) {
    if (idx >= idx_cnt_) {
        PDLOG(WARNING, "idx greater than idx_cnt_, failed getting table tid %u pid %u", id_, pid_);
        return false;
    }
    rocksdb::ReadOptions ro = rocksdb::ReadOptions();
    const rocksdb::Snapshot* snapshot = db_->GetSnapshot();
    ro.snapshot = snapshot;
    ro.prefix_same_as_start = true;
    ro.pin_data = true;
    rocksdb::Iterator* it = db_->NewIterator(ro, cf_hs_[idx + 1]);
    if (it == NULL) {
        return false;
    }
    it->Seek(rocksdb::Slice(pk));
    if (!it->Valid()) {
        return false;
    }
    rocksdb::Slice it_value = it->value();
    value.assign(it_value.data(), it_value.size());
    return true;
}

bool RelationalTable::Update(const ::rtidb::api::Columns& cd_columns, 
        const ::rtidb::api::Columns& col_columns) {
    const std::string& cd_value = cd_columns.value();
    const std::string& col_value = col_columns.value(); 
    std::map<std::string, int> cd_idx_map;
    Schema condition_schema;
    UpdateInternel(cd_columns, cd_idx_map, condition_schema);
    std::map<std::string, int> col_idx_map;
    Schema value_schema;
    UpdateInternel(col_columns, col_idx_map, value_schema);
    bool ok = UpdateDB(cd_idx_map, col_idx_map, condition_schema, value_schema, 
            cd_value, col_value);
    return ok;
}

void RelationalTable::UpdateInternel(const ::rtidb::api::Columns& cd_columns, 
        std::map<std::string, int>& cd_idx_map, 
        Schema& condition_schema) {
    const Schema& schema = table_meta_.column_desc();
    std::map<std::string, ::rtidb::type::DataType> cd_type_map;
    for (int i = 0; i < cd_columns.name_size(); i++) {
        cd_type_map.insert(std::make_pair(cd_columns.name(i), ::rtidb::type::kBool));
        cd_idx_map.insert(std::make_pair(cd_columns.name(i), i));
    }
    for (int i = 0; i < schema.size(); i++) {
        auto idx_iter = cd_type_map.find(schema.Get(i).name());
        if (idx_iter != cd_type_map.end()) {
            idx_iter->second = schema.Get(i).data_type(); 
        }
    }
    for (int i = 0; i < cd_columns.name_size(); i++) {
        ::rtidb::common::ColumnDesc* col = condition_schema.Add();
        col->set_name(cd_columns.name(i));
        col->set_data_type(cd_type_map.find(cd_columns.name(i))->second);
    }
}

bool RelationalTable::UpdateDB(const std::map<std::string, int>& cd_idx_map, const std::map<std::string, int>& col_idx_map,  
        const Schema& condition_schema, const Schema& value_schema, 
        const std::string& cd_value, const std::string& col_value) {
    const Schema& schema = table_meta_.column_desc();
    uint32_t cd_value_size = cd_value.length();
    std::string cd_value_r = cd_value;
    ::rtidb::base::RowView cd_view(condition_schema, reinterpret_cast<int8_t*>(&(cd_value_r[0])), cd_value_size);
    ::rtidb::type::DataType pk_data_type;
    std::string pk;
    //TODO if condition columns size is more than 1
    for (int i = 0; i < condition_schema.size(); i++) {
        pk_data_type = condition_schema.Get(i).data_type();
        switch(pk_data_type) {
            case ::rtidb::type::kSmallInt:
            { 
                int16_t val1 = 0;
                cd_view.GetInt16(i, &val1);
                pk = std::to_string(val1);
                break;
            }
            case ::rtidb::type::kInt:
            { 
                int32_t val2 = 0;
                cd_view.GetInt32(i, &val2);
                pk = std::to_string(val2);
                break;
            }
            case ::rtidb::type::kBigInt:
            {  
                int64_t val3 = 0;
                cd_view.GetInt64(i, &val3);
                pk = std::to_string(val3);
                break;
            }
            case ::rtidb::type::kVarchar:
            {
                char* ch = NULL;
                uint32_t length = 0;
                cd_view.GetString(i, &ch, &length);
                pk.assign(ch, length);
                break;
            }
            default:
            {
                PDLOG(WARNING, "unsupported data type %s", 
                    rtidb::type::DataType_Name(pk_data_type).c_str());
                return false;
            }
                //TODO: other data type
        }
        break;
    }

    std::lock_guard<std::mutex> lock(mu_);
    std::string value;
    bool ok = Get(0, pk, value);
    if (!ok) {
        PDLOG(WARNING, "get failed, update table tid %u pid %u failed", id_, pid_);
        return false;
    }
    ::rtidb::base::RowView row_view(schema, reinterpret_cast<int8_t*>(&(value[0])), value.length());
    uint32_t col_value_size = col_value.length();
    std::string col_value_r = col_value;
    ::rtidb::base::RowView value_view(value_schema, reinterpret_cast<int8_t*>(&(col_value_r[0])), col_value_size);
    uint32_t string_length = 0; 
    for (int i = 0; i < schema.size(); i++) {
        if (schema.Get(i).data_type() == rtidb::type::kVarchar) {
            auto col_iter = col_idx_map.find(schema.Get(i).name());
            if (col_iter != col_idx_map.end()) {
                char* ch = NULL;
                uint32_t length = 0;
                value_view.GetString(col_iter->second, &ch, &length);
                string_length += length;
            } else {
                char* ch = NULL;
                uint32_t length = 0;
                row_view.GetString(i, &ch, &length);
                string_length += length;
            }
        }
    }
    ::rtidb::base::RowBuilder builder(schema);
    uint32_t size = builder.CalTotalLength(string_length);
    std::string row;
    row.resize(size);
    builder.SetBuffer(reinterpret_cast<int8_t*>(&(row[0])), size);
    for (int i = 0; i < schema.size(); i++) {
        auto col_iter = col_idx_map.find(schema.Get(i).name());
        if (col_iter != col_idx_map.end()) {
            if (schema.Get(i).not_null() && value_view.IsNULL(col_iter->second)) {
                return false;
                PDLOG(WARNING, "not_null is true but value is null ,update table tid %u pid %u failed", id_, pid_);
            } else if (value_view.IsNULL(col_iter->second)) {
                builder.AppendNULL(); 
                continue;
            }
        }
        if (schema.Get(i).data_type() == rtidb::type::kBool) {
            bool val = true;
            if (col_iter != col_idx_map.end()) {
                value_view.GetBool(col_iter->second, &val);
            } else {
                row_view.GetBool(i, &val);
            }
            builder.AppendBool(val);
        } else if (schema.Get(i).data_type() == rtidb::type::kSmallInt) {
            int16_t val = 0;
            if (col_iter != col_idx_map.end()) {
                value_view.GetInt16(col_iter->second, &val);
            } else {
                row_view.GetInt16(i, &val);
            }
            builder.AppendInt16(val);
        } else if (schema.Get(i).data_type() == rtidb::type::kInt) {
            int32_t val = 0;
            if (col_iter != col_idx_map.end()) {
                value_view.GetInt32(col_iter->second, &val);
            } else {
                row_view.GetInt32(i, &val);
            }
            builder.AppendInt32(val);
        } else if (schema.Get(i).data_type() == rtidb::type::kBigInt) {
            int64_t val = 0;
            if (col_iter != col_idx_map.end()) {
                value_view.GetInt64(col_iter->second, &val);
            } else {
                row_view.GetInt64(i, &val);
            }
            builder.AppendInt64(val);
        } else if (schema.Get(i).data_type() == rtidb::type::kTimestamp) {
            int64_t val = 0;
            if (col_iter != col_idx_map.end()) {
                value_view.GetTimestamp(col_iter->second, &val);
            } else {
                row_view.GetTimestamp(i, &val);
            }
            builder.AppendTimestamp(val);
        } else if (schema.Get(i).data_type() == rtidb::type::kFloat) {
            float val = 0.0;
            if (col_iter != col_idx_map.end()) {
                value_view.GetFloat(col_iter->second, &val);
            } else {
                row_view.GetFloat(i, &val);
            }
            builder.AppendFloat(val);
        } else if (schema.Get(i).data_type() == rtidb::type::kDouble) {
            double val = 0.0;
            if (col_iter != col_idx_map.end()) {
                value_view.GetDouble(col_iter->second, &val);
            } else {
                row_view.GetDouble(i, &val);
            }
            builder.AppendDouble(val);
        } else if (schema.Get(i).data_type() == rtidb::type::kVarchar) {
            char* ch = NULL;
            uint32_t length = 0;
            if (col_iter != col_idx_map.end()) {
                value_view.GetString(col_iter->second, &ch, &length);
            } else {
                row_view.GetString(i, &ch, &length);
            }
            builder.AppendString(ch, length);
        } else {
            PDLOG(WARNING, "unsupported data type %s", 
                    rtidb::type::DataType_Name(schema.Get(i).data_type()).c_str());
            return false;
        }
    }
    ok = Put(pk, row.c_str(), row.length());
    if (!ok) {
        PDLOG(WARNING, "put failed, update table tid %u pid %u failed", id_, pid_);
        return false;
    }
    return true;
}

}
}
