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
DECLARE_uint32(snapshot_ttl_time);

namespace rtidb {
namespace storage {

static rocksdb::Options ssd_option_template;
static rocksdb::Options hdd_option_template;
static bool options_template_initialized = false;

RelationalTable::RelationalTable(const std::string &name, uint32_t id, uint32_t pid,
        ::rtidb::common::StorageMode storage_mode,
        const std::string& db_root_path) :
    storage_mode_(storage_mode), name_(name), id_(id), pid_(pid), idx_cnt_(0),
    last_make_snapshot_time_(0),  
    write_opts_(), offset_(0), db_root_path_(db_root_path),
    snapshot_index_(1) {
        if (!options_template_initialized) {
            initOptionTemplate();
        }
        write_opts_.disableWAL = FLAGS_disable_wal;
        db_ = nullptr;
}

RelationalTable::RelationalTable(const ::rtidb::api::TableMeta& table_meta,
        const std::string& db_root_path) :
    storage_mode_(table_meta.storage_mode()), name_(table_meta.name()), id_(table_meta.tid()), pid_(table_meta.pid()),
    is_leader_(false), 
    compress_type_(table_meta.compress_type()), last_make_snapshot_time_(0),   
    write_opts_(), offset_(0), db_root_path_(db_root_path),
    snapshots_(), snapshot_index_(1) {
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
    const std::vector<std::shared_ptr<IndexDef>>& indexs = table_index_.GetAllIndex();
    for (const auto& index_def : indexs) {
        rocksdb::ColumnFamilyOptions cfo;
        if (storage_mode_ == ::rtidb::common::StorageMode::kSSD) {
            cfo = rocksdb::ColumnFamilyOptions(ssd_option_template);
            options_ = ssd_option_template;
        } else {
            cfo = rocksdb::ColumnFamilyOptions(hdd_option_template);
            options_ = hdd_option_template;
        }
        cf_ds_.push_back(rocksdb::ColumnFamilyDescriptor(index_def->GetName(), cfo));
        PDLOG(DEBUG, "add cf_name %s. tid %u pid %u", index_def->GetName().c_str(), id_, pid_);
    }
    return true;
}

int RelationalTable::InitColumnDesc() {
    if (table_meta_.column_key_size() <= 0) {
        PDLOG(WARNING, "column_key_size is 0, tid %u pid %u", id_, pid_);
        return -1;
    }
    const Schema& schema = table_meta_.column_desc(); 
    if (schema.size() == 0) {
        PDLOG(WARNING, "column_desc_size is 0, tid %u pid %u", id_, pid_);
        return -1;
    }
    uint32_t col_idx = 0;
    for (auto& col_desc : schema) {
        table_column_.AddColumn(std::make_shared<ColumnDef>(col_desc.name(), 
                    col_idx, col_desc.data_type())); 
        col_idx++;
    }
    uint32_t key_idx = 0;
    for (const auto &column_key : table_meta_.column_key()) {
        const std::string& index_name = column_key.index_name();
        if (table_index_.GetIndex(index_name)) {
            return -1;
        }
        const ::rtidb::type::IndexType index_type = column_key.index_type(); 
        std::map<uint32_t, ::rtidb::common::ColumnDesc> column_idx_map;
        for (int i = 0; i < column_key.col_name_size(); i++) {
            for (int j = 0; j < schema.size(); j++) {
                if (schema.Get(j).name() == column_key.col_name(i)) {
                    column_idx_map.insert(std::make_pair(j, schema.Get(j)));
                }
            }
        }
        if (column_key.flag()) {
            table_index_.AddIndex(std::make_shared<IndexDef>(index_name, key_idx, 
                        ::rtidb::storage::kDeleted, index_type, column_idx_map));
        } else {
            table_index_.AddIndex(std::make_shared<IndexDef>(index_name, key_idx, 
                        ::rtidb::storage::kReady, index_type, column_idx_map));
        }
        key_idx++;
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
    if (table_meta_.has_compress_type()) compress_type_ = table_meta_.compress_type();
    idx_cnt_ = table_index_.Size();
    
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

bool RelationalTable::Put(const std::string& value) {
    if (table_meta_.compress_type() == ::rtidb::api::kSnappy) {
        std::string uncompressed;
        ::snappy::Uncompress(value.c_str(), value.length(), &uncompressed);
        *(const_cast<std::string*>(&value)) = uncompressed;
    }
    std::string pk = "";
    const Schema& schema = table_meta_.column_desc(); 
    if (table_index_.HasAutoGen()) {
        ::rtidb::base::RowBuilder builder(schema);
        builder.SetBuffer(reinterpret_cast<int8_t*>(const_cast<char*>(&(value[0]))), value.size());
        int64_t auto_gen_pk = id_generator_.Next();
        builder.AppendInt64(auto_gen_pk);
        pk.resize(sizeof(int64_t));
        char* to = const_cast<char*>(pk.data());
        ::rtidb::base::PackInteger(&auto_gen_pk, sizeof(int64_t), false, to);
    } else {
        ::rtidb::base::RowView view(schema, reinterpret_cast<int8_t*>(const_cast<char*>(&(value[0]))), value.size());
        std::shared_ptr<IndexDef> index_def = table_index_.GetPkIndex();
        for (auto& kv : index_def->GetColumnIdxMap()) {
            uint32_t idx = kv.first;
            ::rtidb::type::DataType data_type = kv.second.data_type();
            if (!GetPackedField(view, idx, data_type, &pk)) {
                return false;
            }
        }
    }
    //PDLOG(DEBUG, "put pk: %s, pk size %u", pk.c_str(), pk.size());
    return PutDB(pk, value.c_str(), value.size());
}

bool RelationalTable::PutDB(const std::string &pk, const char *data, uint32_t size) {
    rocksdb::WriteBatch batch;
    rocksdb::Status s;
    rocksdb::Slice spk = rocksdb::Slice(pk);
    std::shared_ptr<IndexDef> index_def = table_index_.GetPkIndex();
    if (table_meta_.compress_type() == ::rtidb::api::kSnappy) {
        std::string compressed;
        ::snappy::Compress(data, size, &compressed);
        batch.Put(cf_hs_[index_def->GetId() + 1], spk, 
                rocksdb::Slice(compressed.data(), compressed.size()));
    } else {
        batch.Put(cf_hs_[index_def->GetId() + 1], spk, rocksdb::Slice(data, size));
    }
    const Schema& schema = table_meta_.column_desc(); 
    ::rtidb::base::RowView view(schema, reinterpret_cast<int8_t*>(const_cast<char*>(data)), size);
    for (uint32_t i = 0; i < table_index_.Size(); i++) {
        std::shared_ptr<IndexDef> index_def = table_index_.GetIndex(i);
        for (auto& kv : index_def->GetColumnIdxMap()) {
            uint32_t idx = kv.first;
            ::rtidb::type::DataType data_type = kv.second.data_type();
            std::string key = "";
            if (index_def->GetType() == ::rtidb::type::kUnique) {
                if (!GetPackedField(view, idx, data_type, &key)) {
                    return false;
                }
                rocksdb::Iterator* it = GetRocksdbIterator(idx);
                if (it != NULL) {
                    it->Seek(rocksdb::Slice(key));
                    if (it->Valid() && it->key() == rocksdb::Slice(key)) {
                        PDLOG(DEBUG, "Put failed because unique key repeated. tid %u pid %u", id_, pid_);
                        delete it;
                        return false;
                    }
                }
                rocksdb::Slice unique = rocksdb::Slice(key);
                batch.Put(cf_hs_[index_def->GetId() + 1], unique, spk);
            } else if (index_def->GetType() == ::rtidb::type::kNoUnique) {
                if (!GetPackedField(view, idx, data_type, &key)) {
                    return false;
                }
                std::string no_unique = "";
                CombineNoUniqueAndPk(key, pk, &no_unique);
                batch.Put(cf_hs_[index_def->GetId() + 1], rocksdb::Slice(no_unique), rocksdb::Slice());
            }
            //TODO: combine key
            break;
        }
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

bool RelationalTable::GetPackedField(::rtidb::base::RowView& view, uint32_t idx, 
        const ::rtidb::type::DataType& data_type, std::string* key) {
    int ret = 0;
    switch(data_type) {
        case ::rtidb::type::kSmallInt: {
            int16_t si_val = 0;
            view.GetInt16(idx, &si_val);
            key->resize(sizeof(int16_t));
            char* to = const_cast<char*>(key->data());
            ret = ::rtidb::base::PackInteger(&si_val, sizeof(int16_t), false, to);
            break;
        }
        case ::rtidb::type::kInt: { 
            int32_t i_val = 0;
            view.GetInt32(idx, &i_val);
            key->resize(sizeof(int32_t));
            char* to = const_cast<char*>(key->data());
            ret = ::rtidb::base::PackInteger(&i_val, sizeof(int32_t), false, to);
            break;
        }
        case ::rtidb::type::kBigInt: {
            int64_t bi_val = 0;
            view.GetInt64(idx, &bi_val);
            key->resize(sizeof(int64_t));
            char* to = const_cast<char*>(key->data());
            ret = ::rtidb::base::PackInteger(&bi_val, sizeof(int64_t), false, to);
            break;
        }
        case ::rtidb::type::kVarchar:
        case ::rtidb::type::kString: {
            char* ch = NULL;
            uint32_t length = 0;
            view.GetString(idx, &ch, &length);
            int32_t dst_len = ::rtidb::base::GetDstStrSize(length);
            key->resize(dst_len);
            char* dst = const_cast<char*>(key->data());
            ret = ::rtidb::base::PackString(ch, length, (void**)&dst);
            break;
        }
        case ::rtidb::type::kFloat: {
            float val = 0.0;
            view.GetFloat(idx, &val);
            key->resize(sizeof(float));
            char* to = const_cast<char*>(key->data());
            ret = ::rtidb::base::PackFloat(&val, to);
            break;
        }
        case ::rtidb::type::kDouble: {
            double val = 0.0;
            view.GetDouble(idx, &val);
            key->resize(sizeof(double));
            char* to = const_cast<char*>(key->data());
            ret = ::rtidb::base::PackDouble(&val, to);
            break;
        }
        default: {
            PDLOG(WARNING, "unsupported data type %s, tid %u pid %u",
                    rtidb::type::DataType_Name(data_type).c_str(), id_, pid_);
            return false;
        }
    }
    if (ret < 0) {
        PDLOG(WARNING, "pack error, tid %u pid %u", id_, pid_);
        return false;
    }
    return true;
}

bool RelationalTable::ConvertIndex(const std::string& name, const std::string& value, 
        std::string* out_val) {
    std::shared_ptr<ColumnDef> column_def = table_column_.GetColumn(name);
    if (!column_def) {
        PDLOG(WARNING, "col name %s not exist, tid %u pid %u", name.c_str(), id_, pid_);
        return false;
    }
    ::rtidb::type::DataType type = column_def->GetType();
    int ret = 0;
    if (type == ::rtidb::type::kSmallInt) {
        int16_t val = 0;
        ::rtidb::base::GetInt16(value.data(), &val); 
        out_val->resize(sizeof(int16_t));
        char* to = const_cast<char*>(out_val->data());
        ret = ::rtidb::base::PackInteger(&val, sizeof(int16_t), false, to);
    } else if (type == ::rtidb::type::kInt) {
        int32_t val = 0;
        ::rtidb::base::GetInt32(value.data(), &val); 
        out_val->resize(sizeof(int32_t));
        char* to = const_cast<char*>(out_val->data());
        ret = ::rtidb::base::PackInteger(&val, sizeof(int32_t), false, to);
    } else if (type == ::rtidb::type::kBigInt) {
        int64_t val = 0;
        ::rtidb::base::GetInt64(value.data(), &val); 
        out_val->resize(sizeof(int64_t));
        char* to = const_cast<char*>(out_val->data());
        ret = ::rtidb::base::PackInteger(&val, sizeof(int64_t), false, to);
    } else if (type == ::rtidb::type::kFloat) {
        float val = 0.0;
        ::rtidb::base::GetFloat(value.data(), &val);  
        out_val->resize(sizeof(float));
        char* to = const_cast<char*>(out_val->data());
        ret = ::rtidb::base::PackFloat(&val, to);
    } else if (type == ::rtidb::type::kDouble) {
        double val = 0.0;
        ::rtidb::base::GetDouble(value.data(), &val);  
        out_val->resize(sizeof(double));
        char* to = const_cast<char*>(out_val->data());
        ret = ::rtidb::base::PackDouble(&val, to);
    } else if (type == ::rtidb::type::kVarchar 
            || type == ::rtidb::type::kString) {
        int32_t dst_len = ::rtidb::base::GetDstStrSize(value.length());
        out_val->resize(dst_len);
        char* dst = const_cast<char*>(out_val->data());
        ret = ::rtidb::base::PackString(value.data(), value.length(), (void**)&dst);
    } else {
        PDLOG(WARNING, "unsupported data type %s, tid %u pid %u",
                rtidb::type::DataType_Name(type).c_str(), id_, pid_);
        return false;
    }
    if (ret < 0) {
        PDLOG(WARNING, "pack error, tid %u pid %u", id_, pid_);
        return false;
    }
    //PDLOG(DEBUG, "query pk: %s", out_val->c_str());
    return true;
}

bool RelationalTable::Delete(const std::string& idx_name, 
        const std::string& key) {
    std::shared_ptr<IndexDef> index_def = table_index_.GetIndex(idx_name);
    if (!index_def) return false;
    std::string comparable_key = "";
    if (!ConvertIndex(idx_name, key, &comparable_key)) return false;
    rocksdb::Iterator* it = GetIteratorAndSeek(index_def->GetId(), rocksdb::Slice(comparable_key));
    if (it == NULL) {
        return false;
    }
    ::rtidb::type::IndexType index_type = index_def->GetType();
    if (index_type == ::rtidb::type::kPrimaryKey) {
        return DeletePk(rocksdb::Slice(comparable_key));
    } else if (index_type == ::rtidb::type::kUnique) {
        if (it->key() != rocksdb::Slice(comparable_key)) {
            PDLOG(DEBUG, "unique key %s not found. tid %u pid %u", comparable_key.c_str(), id_, pid_);
            delete it;
            return false;
        }
        return DeletePk(it->value());
    } else if (index_type == ::rtidb::type::kNoUnique) {
        int count = 0;
        while (it->Valid()) {
            rocksdb::Slice pk_slice = ParsePk(it->key(), comparable_key);
             if (pk_slice == rocksdb::Slice()) {
                if (count == 0) {
                    PDLOG(DEBUG, "ParsePk failed, key %s not exist, tid %u pid %u", comparable_key.c_str(), id_, pid_);
                    delete it;
                    return false;
                } else {
                    break;
                }
            }
            if (!DeletePk(pk_slice)) {
                delete it;
                return false;
            }
            it->Next();
            count++;
        }
    } else {
        PDLOG(WARNING, "unsupported index type %s, tid %u pid %u", 
                ::rtidb::type::IndexType_Name(index_type).c_str(), id_, pid_);
        delete it;
        return false; 
    }
    delete it;
    return true;
}

bool RelationalTable::DeletePk(const rocksdb::Slice pk_slice) {
    std::shared_ptr<IndexDef> pk_index_def = table_index_.GetPkIndex();
    uint32_t pk_id = pk_index_def->GetId();
   
    rocksdb::Iterator* pk_it = GetIteratorAndSeek(pk_id, pk_slice);
    if (pk_it == NULL) {
        return false;
    }
    if (pk_it->key() != pk_slice) {
        std::string pk(pk_slice.data(), pk_slice.size());
        PDLOG(DEBUG, "pk %s not found. tid %u pid %u", pk.c_str(), id_, pid_);
        delete pk_it;
        return false;
    }
    rocksdb::WriteBatch batch;
    batch.Delete(cf_hs_[pk_id + 1], pk_slice);
    rocksdb::Slice slice = pk_it->value();

    const Schema& schema = table_meta_.column_desc();
    ::rtidb::base::RowView view(schema, 
            reinterpret_cast<int8_t*>(const_cast<char*>(slice.data())), slice.size());
    const std::vector<std::shared_ptr<IndexDef>>& indexs = table_index_.GetAllIndex();
    for (const auto& index_def : indexs) {
        if (index_def->GetType() == ::rtidb::type::kUnique ||
                index_def->GetType() == ::rtidb::type::kNoUnique) {
            std::shared_ptr<ColumnDef> col = table_column_.GetColumn(index_def->GetName());
            if (!col) {
                PDLOG(WARNING, "col name %s not exist, tid %u pid %u", 
                        index_def->GetName().c_str(), id_, pid_);
                delete pk_it;
                return false;
            }
            std::string second_key = "";
            if (!GetPackedField(view, col->GetId(), col->GetType(), &second_key)) {
                delete pk_it;
                return false;
            }
            uint32_t index_id = index_def->GetId();
            rocksdb::Iterator* it = GetIteratorAndSeek(index_id, rocksdb::Slice(second_key));
            if (it == NULL) {
                delete pk_it;
                return false;
            }
            if (index_def->GetType() == ::rtidb::type::kUnique) {
                if (it->key() != rocksdb::Slice(second_key)) {
                    PDLOG(DEBUG, "unique key %s not found. tid %u pid %u", second_key.c_str(), id_, pid_);
                    delete pk_it;
                    delete it;
                    return false;
                }
                batch.Delete(cf_hs_[index_id + 1], rocksdb::Slice(second_key));
            } else if (index_def->GetType() == ::rtidb::type::kNoUnique) {
                int count = 0;
                while (it->Valid()) {
                    rocksdb::Slice r_pk_slice = ParsePk(it->key(), second_key);
                    if (r_pk_slice == rocksdb::Slice()) {
                        if (count == 0) {
                            PDLOG(DEBUG, "ParsePk failed, key %s not exist, tid %u pid %u", second_key.c_str(), id_, pid_);
                            delete pk_it;
                            delete it;
                            return false;
                        } else {
                            break;
                        }
                    }
                    if (pk_slice != r_pk_slice) {
                        break;
                    }
                    batch.Delete(cf_hs_[index_id + 1], it->key());
                    it->Next();
                    count++;
                }
            }
            delete it;
        }
    }
    delete pk_it;
    rocksdb::Status s = db_->Write(write_opts_, &batch);
    if (s.ok()) {
        offset_.fetch_add(1, std::memory_order_relaxed);
        return true;
    } else {
        PDLOG(DEBUG, "Delete failed. tid %u pid %u msg %s", id_, pid_, s.ToString().c_str());
        return false;
    }
}

rocksdb::Iterator* RelationalTable::GetRocksdbIterator(uint32_t idx) {
    rocksdb::ReadOptions ro = rocksdb::ReadOptions();
    ro.prefix_same_as_start = true;
    ro.pin_data = true;
    rocksdb::Iterator* it = db_->NewIterator(ro, cf_hs_[idx + 1]);
    return it;
}

rocksdb::Iterator* RelationalTable::GetIteratorAndSeek(uint32_t idx, const rocksdb::Slice key_slice) {
    rocksdb::Iterator* it = GetRocksdbIterator(idx);
    if (it == NULL) {
        PDLOG(WARNING, "idx %u not exist. tid %u pid %u", idx, id_, pid_);
        return NULL;
    }
    it->Seek(key_slice);
    if (!it->Valid()) {
        std::string key(key_slice.data(), key_slice.size());
        PDLOG(WARNING, "key %s not found. tid %u pid %u", key.c_str(), id_, pid_);
        delete it;
        return NULL;
    }
    return it;
}

bool RelationalTable::Query(
        const ::google::protobuf::RepeatedPtrField<::rtidb::api::ReadOption>& ros,
        std::string* pairs, uint32_t* count) {
    std::vector<rocksdb::Slice> value_vec;
    uint32_t total_block_size = 0;
    for (auto& ro : ros) {
        //TODO combined key
        const std::string& idx_name = ro.index(0).name(0);
        const std::string& idx_value = ro.index(0).value();
        if (!Query(idx_name, idx_value, &value_vec)) {
            return false; 
        }
    }
    for (rocksdb::Slice& value : value_vec) {
        total_block_size += value.size();
    }
    uint32_t scount = value_vec.size();
    *count = scount;
    uint32_t total_size = scount * 4 + total_block_size;
    if (scount == 0) {
        pairs->resize(0);
        return true;
    } else {
        pairs->resize(total_size);
    }
    char* rbuffer = reinterpret_cast<char*>(&((*pairs)[0]));
    uint32_t offset = 0;
    for (const auto& value : value_vec) {
        rtidb::base::Encode(value.data(), value.size(), rbuffer, offset);
        offset += (4 + value.size());
    }
    return true;
}

bool RelationalTable::Query(const std::string& idx_name, const std::string& idx_val, 
        std::vector<rocksdb::Slice>* vec) {
    std::shared_ptr<IndexDef> index_def = table_index_.GetIndex(idx_name);
    if (!index_def) return false;
    std::string key = "";
    if (!ConvertIndex(idx_name, idx_val, &key)) return false;
    return Query(index_def, rocksdb::Slice(key), vec);     
}

bool RelationalTable::Query(const std::shared_ptr<IndexDef> index_def, 
        const rocksdb::Slice key_slice, 
        std::vector<rocksdb::Slice>* vec) {
    uint32_t idx = index_def->GetId();
    ::rtidb::type::IndexType index_type = index_def->GetType(); 

    if (index_type == ::rtidb::type::kPrimaryKey ||
            index_type == ::rtidb::type::kAutoGen) {
        rocksdb::Iterator* it = GetIteratorAndSeek(idx, key_slice);
        if (it == NULL) {
            return false;
        }
        if (it->key() != key_slice) {
            std::string key(key_slice.data(), key_slice.size());
            PDLOG(WARNING, "key %s not found. tid %u pid %u", key.c_str(), id_, pid_);
            delete it;
            return false;
        }
        vec->push_back(it->value());
        delete it;
    } else if (index_type == ::rtidb::type::kUnique) {
        rocksdb::Iterator* it = GetIteratorAndSeek(idx, key_slice);
        if (it == NULL) {
            return false;
        }
        if (it->key() != key_slice) {
            std::string key(key_slice.data(), key_slice.size());
            PDLOG(WARNING, "key %s not found. tid %u pid %u", key.c_str(), id_, pid_);
            delete it;
            return false;
        }
        Query(table_index_.GetPkIndex(), it->value(), vec); 
        delete it;
    } else if (index_type == ::rtidb::type::kNoUnique) {
        //TODO multi records
        rocksdb::Iterator* it = GetIteratorAndSeek(idx, key_slice);
        if (it == NULL) {
            return false;
        }
        int count = 0;
        while (it->Valid()) {
            std::string key(key_slice.data(), key_slice.size());
            rocksdb::Slice pk_slice = ParsePk(it->key(), key);
             if (pk_slice == rocksdb::Slice()) {
                if (count == 0) {
                    PDLOG(DEBUG, "ParsePk failed, key %s not exist, tid %u pid %u", key.c_str(), id_, pid_);
                    delete it;
                    return false;
                } else {
                    delete it;
                    return true;
                }
            }
            Query(table_index_.GetPkIndex(), pk_slice, vec); 
            it->Next();
            count++;
        }
        delete it;
    }
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
    ::rtidb::base::RowView cd_view(condition_schema, reinterpret_cast<int8_t*>(const_cast<char*>(&(cd_value[0]))), cd_value_size);
    ::rtidb::type::DataType pk_data_type;
    std::string pk;
    //TODO if condition columns size is more than 1
    for (int i = 0; i < condition_schema.size(); i++) {
        pk_data_type = condition_schema.Get(i).data_type();
        if (!GetPackedField(cd_view, i, pk_data_type, &pk)) {
            return false;
        }
        //TODO combined key
    }

    std::lock_guard<std::mutex> lock(mu_);
    std::vector<rocksdb::Slice> value_vec;
    std::shared_ptr<IndexDef> index_def = table_index_.GetPkIndex();
    bool ok = Query(index_def, rocksdb::Slice(pk), &value_vec);
    if (!ok) {
        PDLOG(WARNING, "get failed, update table tid %u pid %u failed", id_, pid_);
        return false;
    }
    rocksdb::Slice slice = value_vec.at(0);

    ::rtidb::base::RowView row_view(schema, reinterpret_cast<int8_t*>(const_cast<char*>(slice.data())), slice.size());
    uint32_t col_value_size = col_value.length();
    ::rtidb::base::RowView value_view(value_schema, reinterpret_cast<int8_t*>(const_cast<char*>(&(col_value[0]))), col_value_size);
    uint32_t string_length = 0; 
    for (int i = 0; i < schema.size(); i++) {
        if (schema.Get(i).data_type() == rtidb::type::kVarchar || schema.Get(i).data_type() == rtidb::type::kString) {
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
                PDLOG(WARNING, "not_null is true but value is null, update table tid %u pid %u failed", id_, pid_);
                return false;
            } else if (value_view.IsNULL(col_iter->second)) {
                builder.AppendNULL(); 
                continue;
            }
        }
        rtidb::type::DataType cur_type = schema.Get(i).data_type();
        if (cur_type == rtidb::type::kBool) {
            bool val = true;
            if (col_iter != col_idx_map.end()) {
                value_view.GetBool(col_iter->second, &val);
            } else {
                row_view.GetBool(i, &val);
            }
            builder.AppendBool(val);
        } else if (cur_type == rtidb::type::kSmallInt) {
            int16_t val = 0;
            if (col_iter != col_idx_map.end()) {
                value_view.GetInt16(col_iter->second, &val);
            } else {
                row_view.GetInt16(i, &val);
            }
            builder.AppendInt16(val);
        } else if (cur_type == rtidb::type::kInt) {
            int32_t val = 0;
            if (col_iter != col_idx_map.end()) {
                value_view.GetInt32(col_iter->second, &val);
            } else {
                row_view.GetInt32(i, &val);
            }
            builder.AppendInt32(val);
        } else if (cur_type == rtidb::type::kBigInt) {
            int64_t val = 0;
            if (col_iter != col_idx_map.end()) {
                value_view.GetInt64(col_iter->second, &val);
            } else {
                row_view.GetInt64(i, &val);
                PDLOG(DEBUG, "id: %lu", val);
            }
            builder.AppendInt64(val);
        } else if (cur_type == rtidb::type::kTimestamp) {
            int64_t val = 0;
            if (col_iter != col_idx_map.end()) {
                value_view.GetTimestamp(col_iter->second, &val);
            } else {
                row_view.GetTimestamp(i, &val);
            }
            builder.AppendTimestamp(val);
        } else if (cur_type == rtidb::type::kFloat) {
            float val = 0.0;
            if (col_iter != col_idx_map.end()) {
                value_view.GetFloat(col_iter->second, &val);
            } else {
                row_view.GetFloat(i, &val);
            }
            builder.AppendFloat(val);
        } else if (cur_type == rtidb::type::kDouble) {
            double val = 0.0;
            if (col_iter != col_idx_map.end()) {
                value_view.GetDouble(col_iter->second, &val);
            } else {
                row_view.GetDouble(i, &val);
            }
            builder.AppendDouble(val);
        } else if (cur_type == rtidb::type::kVarchar || cur_type == rtidb::type::kString) {
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
    ok = PutDB(pk, row.c_str(), row.length());
    if (!ok) {
        PDLOG(WARNING, "put failed, update table tid %u pid %u failed", id_, pid_);
        return false;
    }
    return true;
}

void RelationalTable::ReleaseSnpashot(uint64_t snapshot_id, bool finish) {
    if (finish) {
        std::lock_guard<std::mutex> lock(mu_);
        auto iter = snapshots_.find(snapshot_id);
        if (iter == snapshots_.end()) {
            return;
        }
        std::shared_ptr<SnapshotInfo> sc = iter->second;
        snapshots_.erase(iter);
        db_->ReleaseSnapshot(sc->snapshot);
    }
}

void RelationalTable::TTLSnapshot() {
    uint64_t cur_time = baidu::common::timer::get_micros() / 1000;
    std::lock_guard<std::mutex> lock(mu_);
    for (auto iter = snapshots_.begin(); iter != snapshots_.end();) {
        if (iter->second->atime + FLAGS_snapshot_ttl_time <= cur_time ) {
            std::shared_ptr<SnapshotInfo> sc = iter->second;
            iter = snapshots_.erase(iter);
            db_->ReleaseSnapshot(sc->snapshot);
            continue;
        }
        iter++;
    }
}

RelationalTableTraverseIterator* RelationalTable::NewTraverse(uint32_t idx, uint64_t snapshot_id) {
    if (idx >= idx_cnt_) {
        PDLOG(WARNING, "idx greater than idx_cnt_, failed getting table tid %u pid %u", id_, pid_);
        return NULL;
    }
    rocksdb::ReadOptions ro = rocksdb::ReadOptions();
    ro.prefix_same_as_start = true;
    ro.pin_data = true;
    std::shared_ptr<SnapshotInfo> sc;
    if (snapshot_id > 0) {
        std::lock_guard<std::mutex> lock(mu_);
        auto iter = snapshots_.find(snapshot_id);
        if (iter != snapshots_.end()) {
            sc = iter->second;
            sc->atime = baidu::common::timer::get_micros() / 1000;
        } else {
            return NULL;
        }
    } else {
        const rocksdb::Snapshot* snapshot = db_->GetSnapshot();
        sc = std::make_shared<SnapshotInfo>();
        sc->snapshot = snapshot;
        sc->atime = baidu::common::timer::get_micros() / 1000;
        std::lock_guard<std::mutex> lock(mu_);
        snapshot_id = snapshot_index_++;
        snapshots_.insert(std::make_pair(snapshot_id, sc));
    }
    ro.snapshot = sc->snapshot;
    rocksdb::Iterator* it = db_->NewIterator(ro, cf_hs_[idx + 1]);
    return new RelationalTableTraverseIterator(this, it, snapshot_id);
}

RelationalTableTraverseIterator::RelationalTableTraverseIterator(RelationalTable* table, rocksdb::Iterator* it,
        uint64_t id):table_(table), it_(it), traverse_cnt_(0),
        finish_(false), id_(id) {
}

RelationalTableTraverseIterator::~RelationalTableTraverseIterator() {
    if (!it_->Valid()) {
        finish_ = true;
    }
    delete it_;
    table_->ReleaseSnpashot(id_, finish_);
}

bool RelationalTableTraverseIterator::Valid() {
    return  it_->Valid();
}

void RelationalTableTraverseIterator::Next() {
    traverse_cnt_++;
    it_->Next();
}

void RelationalTableTraverseIterator::SeekToFirst() {
    return it_->SeekToFirst();
}

void RelationalTableTraverseIterator::Seek(const std::string& pk) {
    rocksdb::Slice spk(pk);
    it_->Seek(spk);
}

uint64_t RelationalTableTraverseIterator::GetCount() {
    return traverse_cnt_;
}

rtidb::base::Slice RelationalTableTraverseIterator::GetValue() {
    rocksdb::Slice spk = it_->value();
    return rtidb::base::Slice(spk.data(), spk.size());
}

rtidb::base::Slice RelationalTableTraverseIterator::GetKey() {
    rocksdb::Slice key = it_->key();
    return rtidb::base::Slice(key.data(), key.size());
}

uint64_t RelationalTableTraverseIterator::GetSeq() {
    return id_;
}

void RelationalTableTraverseIterator::SetFinish(bool finish) {
    finish_ = finish;
}

}
}
