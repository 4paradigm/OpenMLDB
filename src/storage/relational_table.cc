//
// Copyright (C) 2017 4paradigm.com
// Created by wangbao on 02/24/20.
//

#include "storage/relational_table.h"

#include <algorithm>
#include <set>
#include <utility>

#include "base/file_util.h"
#include "base/hash.h"
#include "logging.h"  // NOLINT

using ::baidu::common::DEBUG;
using ::baidu::common::INFO;
using ::baidu::common::WARNING;

DECLARE_bool(disable_wal);
DECLARE_uint32(max_traverse_cnt);
DECLARE_uint32(snapshot_ttl_time);

namespace rtidb {
namespace storage {

static rocksdb::Options ssd_option_template;
static rocksdb::Options hdd_option_template;
static bool options_template_initialized = false;

RelationalTable::RelationalTable(const ::rtidb::api::TableMeta& table_meta,
                                 const std::string& db_root_path)
    : storage_mode_(table_meta.storage_mode()),
      name_(table_meta.name()),
      id_(table_meta.tid()),
      pid_(table_meta.pid()),
      is_leader_(false),
      compress_type_(table_meta.compress_type()),
      table_meta_(table_meta),
      last_make_snapshot_time_(0),
      write_opts_(),
      offset_(0),
      db_root_path_(db_root_path),
      snapshots_(),
      snapshot_index_(1),
      row_view_(table_meta_.column_desc()) {
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
    std::shared_ptr<rocksdb::Cache> cache =
        rocksdb::NewLRUCache(512 << 20, 8);  // Can be set by flags
    // SSD options template
    ssd_option_template.max_open_files = -1;
    ssd_option_template.env->SetBackgroundThreads(
        1, rocksdb::Env::Priority::HIGH);  // flush threads
    ssd_option_template.env->SetBackgroundThreads(
        4, rocksdb::Env::Priority::LOW);  // compaction threads
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
    // table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10,
    // false));
    table_options.whole_key_filtering = false;
    table_options.block_size = 256 << 10;
    table_options.use_delta_encoding = false;
    ssd_option_template.table_factory.reset(
        rocksdb::NewBlockBasedTableFactory(table_options));

    // HDD options template
    hdd_option_template.max_open_files = -1;
    hdd_option_template.env->SetBackgroundThreads(
        1, rocksdb::Env::Priority::HIGH);  // flush threads
    hdd_option_template.env->SetBackgroundThreads(
        1, rocksdb::Env::Priority::LOW);  // compaction threads
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
    hdd_option_template.table_factory.reset(
        rocksdb::NewBlockBasedTableFactory(table_options));

    options_template_initialized = true;
}

bool RelationalTable::InitColumnFamilyDescriptor() {
    cf_ds_.clear();
    cf_ds_.push_back(rocksdb::ColumnFamilyDescriptor(
        rocksdb::kDefaultColumnFamilyName, rocksdb::ColumnFamilyOptions()));
    const std::vector<std::shared_ptr<IndexDef>>& indexs =
        table_index_.GetAllIndex();
    for (const auto& index_def : indexs) {
        rocksdb::ColumnFamilyOptions cfo;
        if (storage_mode_ == ::rtidb::common::StorageMode::kSSD) {
            cfo = rocksdb::ColumnFamilyOptions(ssd_option_template);
            options_ = ssd_option_template;
        } else {
            cfo = rocksdb::ColumnFamilyOptions(hdd_option_template);
            options_ = hdd_option_template;
        }
        cf_ds_.push_back(
            rocksdb::ColumnFamilyDescriptor(index_def->GetName(), cfo));
        PDLOG(DEBUG, "add cf_name %s. tid %u pid %u",
              index_def->GetName().c_str(), id_, pid_);
    }
    return true;
}

int RelationalTable::InitColumnDesc() {
    const Schema& schema = table_meta_.column_desc();
    if (schema.size() == 0) {
        PDLOG(WARNING, "column_desc_size is 0, tid %u pid %u", id_, pid_);
        return -1;
    }
    if (table_meta_.column_key_size() == 0) {
        PDLOG(WARNING, "column_key_size is 0, tid %u pid %u", id_, pid_);
        return -1;
    }
    uint32_t col_idx = 0;
    std::map<std::string, ::rtidb::type::DataType> column_map;
    for (const auto& col_desc : schema) {
        const std::string& col_name = col_desc.name();
        ::rtidb::type::DataType type = col_desc.data_type();
        if (column_map.find(col_name) != column_map.end()) {
            PDLOG(WARNING, "column name %s repeated, tid %u pid %u",
                  col_name.c_str(), id_, pid_);
            return -1;
        }
        column_map.insert(std::make_pair(col_name, type));

        table_column_.AddColumn(
            std::make_shared<ColumnDef>(col_name, col_idx, type));
        col_idx++;
    }
    uint32_t key_idx = 0;
    for (const auto& column_key : table_meta_.column_key()) {
        const std::string& index_name = column_key.index_name();
        if (table_index_.GetIndex(index_name)) {
            PDLOG(WARNING, "index_name %s is duplicated, tid %u pid %u",
                  index_name.c_str(), id_, pid_);
            return -1;
        }
        const ::rtidb::type::IndexType index_type = column_key.index_type();
        std::vector<ColumnDef> index_columns;
        for (const std::string& col_name : column_key.col_name()) {
            auto iter = column_map.find(col_name);
            if (iter == column_map.end()) {
                PDLOG(WARNING, "column name %s not fount, tid %u pid %u",
                      col_name.c_str(), id_, pid_);
                return -1;
            }
            if (iter->second == ::rtidb::type::kBlob) {
                PDLOG(WARNING, "unsupported data type %s, tid %u pid %u",
                      rtidb::type::DataType_Name(iter->second).c_str(), id_,
                      pid_);
                return -1;
            }
            index_columns.push_back(*(table_column_.GetColumn(col_name)));
        }
        std::sort(index_columns.begin(), index_columns.end(),
                  ::rtidb::storage::ColumnDefSortFunc);
        if (column_key.flag()) {
            table_index_.AddIndex(std::make_shared<IndexDef>(
                index_name, key_idx, ::rtidb::storage::IndexStatus::kDeleted,
                index_type, index_columns));
        } else {
            table_index_.AddIndex(std::make_shared<IndexDef>(
                index_name, key_idx, ::rtidb::storage::IndexStatus::kReady,
                index_type, index_columns));
        }
        key_idx++;
    }
    std::shared_ptr<IndexDef> index_def = table_index_.GetPkIndex();
    if (!index_def) {
        PDLOG(WARNING, "pk_index_ not exist, tid %u pid %u", id_, pid_);
        return -1;
    }
    if (table_index_.HasAutoGen() && index_def->GetColumns().size() > 1) {
        PDLOG(WARNING, "auto_gen_pk don`t support combined key, tid %u pid %u",
              id_, pid_);
        return -1;
    }
    return 0;
}

bool RelationalTable::InitFromMeta() {
    if (table_meta_.has_mode() &&
        table_meta_.mode() != ::rtidb::api::TableMode::kTableLeader) {
        is_leader_ = false;
    }
    if (InitColumnDesc() < 0) {
        PDLOG(WARNING, "init column desc failed, tid %u pid %u", id_, pid_);
        return false;
    }
    if (table_meta_.has_compress_type())
        compress_type_ = table_meta_.compress_type();
    idx_cnt_ = table_index_.Size();

    return true;
}

bool RelationalTable::Init() {
    if (!InitFromMeta()) {
        return false;
    }
    InitColumnFamilyDescriptor();
    std::string path = db_root_path_ + "/" + std::to_string(id_) + "_" +
                       std::to_string(pid_) + "/data";
    if (!::rtidb::base::MkdirRecur(path)) {
        PDLOG(WARNING, "fail to create path %s", path.c_str());
        return false;
    }
    options_.create_if_missing = true;
    options_.error_if_exists = true;
    options_.create_missing_column_families = true;
    rocksdb::Status s =
        rocksdb::DB::Open(options_, path, cf_ds_, &cf_hs_, &db_);
    if (!s.ok()) {
        PDLOG(WARNING, "rocksdb open failed. tid %u pid %u error %s", id_, pid_,
              s.ToString().c_str());
        return false;
    }
    PDLOG(INFO,
          "Open DB. tid %u pid %u ColumnFamilyHandle size %d with data path %s",
          id_, pid_, idx_cnt_, path.c_str());
    return true;
}

bool RelationalTable::Put(const std::string& value) {
    ::rtidb::base::Slice slice(value);
    std::string new_value;
    if (table_meta_.compress_type() == ::rtidb::api::kSnappy) {
        ::snappy::Uncompress(value.c_str(), value.length(), &new_value);
        slice.reset(new_value.data(), new_value.size());
    }
    std::shared_ptr<IndexDef> index_def = table_index_.GetPkIndex();
    std::string pk;
    const Schema& schema = table_meta_.column_desc();
    if (table_index_.HasAutoGen()) {
        if (new_value.empty()) {
            new_value.assign(value);
            slice.reset(new_value.data(), new_value.size());
        }
        const ColumnDef& column_def = index_def->GetColumns().at(0);
        ::rtidb::codec::RowBuilder builder(schema);
        builder.SetBuffer(
            reinterpret_cast<int8_t*>(const_cast<char*>(slice.data())),
            slice.size());
        int64_t auto_gen_pk = id_generator_.Next();
        if (!builder.SetInt64(column_def.GetId(), auto_gen_pk)) {
            PDLOG(WARNING, "SetInt64 failed, idx %d, tid %u pid %u",
                  column_def.GetId(), id_, pid_);
            return false;
        }
        pk.resize(sizeof(int64_t));
        char* to = const_cast<char*>(pk.data());
        ::rtidb::codec::PackInteger(&auto_gen_pk, sizeof(int64_t), false, to);
    } else {
        if (!GetCombineStr(
                index_def,
                reinterpret_cast<int8_t*>(const_cast<char*>(slice.data())),
                &pk)) {
            return false;
        }
    }
    rocksdb::WriteBatch batch;
    if (!PutDB(rocksdb::Slice(pk), slice.data(), slice.size(), true, &batch)) {
        return false;
    }
    rocksdb::Status s = db_->Write(write_opts_, &batch);
    if (s.ok()) {
        offset_.fetch_add(1, std::memory_order_relaxed);
        return true;
    } else {
        PDLOG(DEBUG, "Put failed. tid %u pid %u msg %s", id_, pid_,
              s.ToString().c_str());
        return false;
    }
}

bool RelationalTable::PutDB(const rocksdb::Slice& spk, const char* data,
                            uint32_t size, bool unique_check,
                            rocksdb::WriteBatch* batch) {
    std::shared_ptr<IndexDef> index_def = table_index_.GetPkIndex();
    if (table_meta_.compress_type() == ::rtidb::api::kSnappy) {
        std::string compressed;
        ::snappy::Compress(data, size, &compressed);
        batch->Put(cf_hs_[index_def->GetId() + 1], spk,
                   rocksdb::Slice(compressed.data(), compressed.size()));
    } else {
        batch->Put(cf_hs_[index_def->GetId() + 1], spk,
                   rocksdb::Slice(data, size));
    }
    for (uint32_t i = 0; i < table_index_.Size(); i++) {
        std::shared_ptr<IndexDef> index_def = table_index_.GetIndex(i);
        std::string key = "";
        if (!GetCombineStr(index_def,
                           reinterpret_cast<int8_t*>(const_cast<char*>(data)),
                           &key)) {
            return false;
        }
        if (index_def->GetType() == ::rtidb::type::kUnique) {
            if (unique_check) {
                std::unique_ptr<rocksdb::Iterator> it(
                    GetRocksdbIterator(index_def->GetId()));
                if (it) {
                    it->Seek(rocksdb::Slice(key));
                    if (it->Valid() && it->key() == rocksdb::Slice(key)) {
                        PDLOG(DEBUG,
                              "Put failed because"
                              "unique key %s repeated. tid %u pid %u",
                              key.c_str(), id_, pid_);
                        return false;
                    }
                }
            }
            rocksdb::Slice unique = rocksdb::Slice(key);
            batch->Put(cf_hs_[index_def->GetId() + 1], unique, spk);
        } else if (index_def->GetType() == ::rtidb::type::kNoUnique) {
            std::string no_unique = "";
            CombineNoUniqueAndPk(key, spk.ToString(), &no_unique);
            batch->Put(cf_hs_[index_def->GetId() + 1],
                       rocksdb::Slice(no_unique), rocksdb::Slice());
        }
    }
    return true;
}

bool RelationalTable::GetPackedField(const int8_t* row, uint32_t idx,
                                     const ::rtidb::type::DataType& data_type,
                                     std::string* key) {
    int get_value_ret = 0;
    int ret = 0;
    // TODO(wangbao) resolve null
    switch (data_type) {
        case ::rtidb::type::kBool: {
            int8_t val = 0;
            get_value_ret = row_view_.GetValue(row, idx, data_type, &val);
            if (get_value_ret == 0)
                ret = ::rtidb::codec::PackValue(&val, data_type, key);
            break;
        }
        case ::rtidb::type::kSmallInt: {
            int16_t val = 0;
            get_value_ret = row_view_.GetValue(row, idx, data_type, &val);
            if (get_value_ret == 0)
                ret = ::rtidb::codec::PackValue(&val, data_type, key);
            break;
        }
        case ::rtidb::type::kInt:
        case ::rtidb::type::kDate: {
            int32_t val = 0;
            get_value_ret = row_view_.GetValue(row, idx, data_type, &val);
            if (get_value_ret == 0)
                ret = ::rtidb::codec::PackValue(&val, data_type, key);
            break;
        }
        case ::rtidb::type::kBigInt:
        case ::rtidb::type::kTimestamp: {
            int64_t val = 0;
            get_value_ret = row_view_.GetValue(row, idx, data_type, &val);
            if (get_value_ret == 0)
                ret = ::rtidb::codec::PackValue(&val, data_type, key);
            break;
        }
        case ::rtidb::type::kVarchar:
        case ::rtidb::type::kString: {
            char* ch = NULL;
            uint32_t length = 0;
            get_value_ret = row_view_.GetValue(row, idx, &ch, &length);
            if (get_value_ret == 0) {
                int32_t dst_len = ::rtidb::codec::GetDstStrSize(length);
                key->resize(dst_len);
                char* dst = const_cast<char*>(key->data());
                ret =
                    ::rtidb::codec::PackString(ch, length, (void**)&dst);  // NOLINT
            }
            break;
        }
        case ::rtidb::type::kFloat: {
            float val = 0.0;
            get_value_ret = row_view_.GetValue(row, idx, data_type, &val);
            if (get_value_ret == 0)
                ret = ::rtidb::codec::PackValue(&val, data_type, key);
            break;
        }
        case ::rtidb::type::kDouble: {
            double val = 0.0;
            get_value_ret = row_view_.GetValue(row, idx, data_type, &val);
            if (get_value_ret == 0)
                ret = ::rtidb::codec::PackValue(&val, data_type, key);
            break;
        }
        default: {
            PDLOG(WARNING, "unsupported data type %s, tid %u pid %u",
                  rtidb::type::DataType_Name(data_type).c_str(), id_, pid_);
            return false;
        }
    }
    if (get_value_ret < 0) {
        PDLOG(WARNING, "getValue failed,_type %s, tid %u pid %u",
              rtidb::type::DataType_Name(data_type).c_str(), id_, pid_);
        return false;
    }
    if (ret < 0) {
        PDLOG(WARNING, "pack data_type %s error, tid %u pid %u",
              rtidb::type::DataType_Name(data_type).c_str(), id_, pid_);
        return false;
    }
    return true;
}

bool RelationalTable::ConvertIndex(const std::string& name,
                                   const std::string& value,
                                   std::string* out_val) {
    std::shared_ptr<ColumnDef> column_def = table_column_.GetColumn(name);
    if (!column_def) {
        PDLOG(WARNING, "col name %s not exist, tid %u pid %u", name.c_str(),
              id_, pid_);
        return false;
    }
    ::rtidb::type::DataType type = column_def->GetType();
    int ret = 0;
    switch (type) {
        case ::rtidb::type::kBool: {
            int8_t val = 0;
            ::rtidb::codec::GetBool(value.data(), &val);
            ret = ::rtidb::codec::PackValue(&val, type, out_val);
            break;
        }
        case ::rtidb::type::kSmallInt: {
            int16_t val = 0;
            ::rtidb::codec::GetInt16(value.data(), &val);
            ret = ::rtidb::codec::PackValue(&val, type, out_val);
            break;
        }
        case ::rtidb::type::kInt:
        case ::rtidb::type::kDate: {
            int32_t val = 0;
            ::rtidb::codec::GetInt32(value.data(), &val);
            ret = ::rtidb::codec::PackValue(&val, type, out_val);
            break;
        }
        case ::rtidb::type::kBigInt:
        case ::rtidb::type::kTimestamp: {
            int64_t val = 0;
            ::rtidb::codec::GetInt64(value.data(), &val);
            ret = ::rtidb::codec::PackValue(&val, type, out_val);
            break;
        }
        case ::rtidb::type::kFloat: {
            float val = 0.0;
            ::rtidb::codec::GetFloat(value.data(), &val);
            ret = ::rtidb::codec::PackValue(&val, type, out_val);
            break;
        }
        case ::rtidb::type::kDouble: {
            double val = 0.0;
            ::rtidb::codec::GetDouble(value.data(), &val);
            ret = ::rtidb::codec::PackValue(&val, type, out_val);
            break;
        }
        case ::rtidb::type::kVarchar:
        case ::rtidb::type::kString: {
            int32_t dst_len = ::rtidb::codec::GetDstStrSize(value.length());
            out_val->resize(dst_len);
            char* dst = const_cast<char*>(out_val->data());
            ret = ::rtidb::codec::PackString(value.data(), value.length(),
                                             (void**)&dst);  // NOLINT
            break;
        }
        default: {
            PDLOG(WARNING, "unsupported data type %s, tid %u pid %u",
                  rtidb::type::DataType_Name(type).c_str(), id_, pid_);
            return false;
        }
    }
    if (ret < 0) {
        PDLOG(WARNING, "pack %s error, tid %u pid %u",
                rtidb::type::DataType_Name(type).c_str(), id_, pid_);
        return false;
    }
    return true;
}

bool RelationalTable::Delete(
    const ::google::protobuf::RepeatedPtrField<::rtidb::api::Columns>&
        condition_columns) {
    std::string combine_name = "";
    std::string combine_value = "";
    if (!GetCombineStr(condition_columns, &combine_name, &combine_value)) {
        return false;
    }
    std::shared_ptr<IndexDef> index_def =
        table_index_.GetIndexByCombineStr(combine_name);
    if (!index_def) return false;
    rocksdb::WriteBatch batch;
    if (!Delete(index_def, combine_value, &batch)) return false;
    rocksdb::Status s = db_->Write(write_opts_, &batch);
    if (s.ok()) {
        offset_.fetch_add(1, std::memory_order_relaxed);
        return true;
    } else {
        PDLOG(DEBUG, "Delete failed. tid %u pid %u msg %s", id_, pid_,
              s.ToString().c_str());
        return false;
    }
}

bool RelationalTable::Delete(const
    RepeatedPtrField<rtidb::api::Columns>& condition_columns,
    RepeatedField<google::protobuf::int64_t>* blob_keys) {
    std::vector<std::unique_ptr<rocksdb::Iterator>> iter_vec;
    bool ok = Query(condition_columns, &iter_vec);
    const std::vector<uint32_t>& blob_suffix = table_column_.GetBlobIdxs();
    for (auto& iter : iter_vec) {
        const int8_t* data =
            reinterpret_cast<int8_t*>(const_cast<char*>(iter->value().data()));

        for (auto i : blob_suffix) {
            int64_t val = 0;
            int ret = row_view_.GetInt64(i, &val);
            if (ret != 0) {
                PDLOG(WARNING, "get string failed. errno %d tid %u pid %u",
                      ret, id_, pid_);
                blob_keys->Clear();
                return false;
            }
            int64_t* key = blob_keys->Add();
            *key = val;
        }
    }
    ok = Delete(condition_columns);
    if (!ok) {
        PDLOG(WARNING, "delete failed. clean blob_keys");
        blob_keys->Clear();
        return false;
    }
    return true;
}

bool RelationalTable::Delete(const std::shared_ptr<IndexDef> index_def,
                             const std::string& comparable_key,
                             rocksdb::WriteBatch* batch) {
    ::rtidb::type::IndexType index_type = index_def->GetType();
    if (index_type == ::rtidb::type::kPrimaryKey) {
        return DeletePk(rocksdb::Slice(comparable_key), batch);
    }
    std::unique_ptr<rocksdb::Iterator> it(
        GetIteratorAndSeek(index_def->GetId(), rocksdb::Slice(comparable_key)));
    if (!it) {
        return false;
    }
    if (index_type == ::rtidb::type::kUnique) {
        if (it->key() != rocksdb::Slice(comparable_key)) {
            PDLOG(DEBUG, "unique key %s not found. tid %u pid %u",
                  comparable_key.c_str(), id_, pid_);
            return false;
        }
        return DeletePk(it->value(), batch);
    } else if (index_type == ::rtidb::type::kNoUnique) {
        int count = 0;
        while (it->Valid()) {
            rocksdb::Slice pk_slice = ParsePk(it->key(), comparable_key);
            if (pk_slice.empty()) {
                if (count == 0) {
                    PDLOG(DEBUG,
                          "ParsePk failed, key %s not exist, tid %u pid %u",
                          comparable_key.c_str(), id_, pid_);
                    return false;
                } else {
                    break;
                }
            }
            if (!DeletePk(pk_slice, batch)) {
                return false;
            }
            it->Next();
            count++;
        }
    } else {
        PDLOG(WARNING, "unsupported index type %s, tid %u pid %u",
              ::rtidb::type::IndexType_Name(index_type).c_str(), id_, pid_);
        return false;
    }
    return true;
}

bool RelationalTable::DeletePk(const rocksdb::Slice& pk_slice,
                               rocksdb::WriteBatch* batch) {
    std::shared_ptr<IndexDef> pk_index_def = table_index_.GetPkIndex();
    uint32_t pk_id = pk_index_def->GetId();

    std::unique_ptr<rocksdb::Iterator> pk_it(
        GetIteratorAndSeek(pk_id, pk_slice));
    if (!pk_it) {
        return false;
    }
    if (pk_it->key() != pk_slice) {
        PDLOG(DEBUG, "pk %s not found. tid %u pid %u",
              pk_slice.ToString().c_str(), id_, pid_);
        return false;
    }
    batch->Delete(cf_hs_[pk_id + 1], pk_slice);
    rocksdb::Slice slice = pk_it->value();

    const std::vector<std::shared_ptr<IndexDef>>& indexs =
        table_index_.GetAllIndex();
    for (const auto& index_def : indexs) {
        if (index_def->GetType() == ::rtidb::type::kUnique ||
            index_def->GetType() == ::rtidb::type::kNoUnique) {
            std::string second_key = "";
            if (!GetCombineStr(
                    index_def,
                    reinterpret_cast<int8_t*>(const_cast<char*>(slice.data())),
                    &second_key)) {
                return false;
            }
            uint32_t index_id = index_def->GetId();
            if (index_def->GetType() == ::rtidb::type::kUnique) {
                batch->Delete(cf_hs_[index_id + 1], rocksdb::Slice(second_key));
            } else if (index_def->GetType() == ::rtidb::type::kNoUnique) {
                std::string no_unique = "";
                CombineNoUniqueAndPk(second_key, pk_slice.ToString(),
                                     &no_unique);
                batch->Delete(cf_hs_[index_id + 1], rocksdb::Slice(no_unique));
            }
        }
    }
    return true;
}

rocksdb::Iterator* RelationalTable::GetRocksdbIterator(uint32_t idx) {
    rocksdb::ReadOptions ro = rocksdb::ReadOptions();
    ro.prefix_same_as_start = true;
    ro.pin_data = true;
    rocksdb::Iterator* it = db_->NewIterator(ro, cf_hs_[idx + 1]);
    return it;
}

rocksdb::Iterator* RelationalTable::GetIteratorAndSeek(
    uint32_t idx, const rocksdb::Slice& key_slice) {
    rocksdb::Iterator* it = GetRocksdbIterator(idx);
    if (it == NULL) {
        PDLOG(WARNING, "idx %u not exist. tid %u pid %u", idx, id_, pid_);
        return NULL;
    }
    it->Seek(key_slice);
    if (!it->Valid()) {
        PDLOG(DEBUG, "key %s not found. tid %u pid %u",
              key_slice.ToString().c_str(), id_, pid_);
        delete it;
        return NULL;
    }
    return it;
}

bool RelationalTable::Query(
    const ::google::protobuf::RepeatedPtrField<::rtidb::api::ReadOption>& ros,
    std::string* pairs, uint32_t* count) {
    std::vector<std::unique_ptr<rocksdb::Iterator>> iter_vec;
    uint32_t total_block_size = 0;
    for (auto& ro : ros) {
        if (!Query(ro.index(), &iter_vec)) {
            return false;
        }
    }
    for (const auto& iter : iter_vec) {
        total_block_size += iter->value().size();
    }
    uint32_t scount = iter_vec.size();
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
    for (const auto& iter : iter_vec) {
        rtidb::codec::Encode(iter->value().data(), iter->value().size(),
                             rbuffer, offset);
        offset += (4 + iter->value().size());
    }
    return true;
}

bool RelationalTable::Query(
    const ::google::protobuf::RepeatedPtrField<::rtidb::api::Columns>& indexs,
    std::vector<std::unique_ptr<rocksdb::Iterator>>* return_vec) {
    std::string combine_name = "";
    std::string combine_value = "";
    if (!GetCombineStr(indexs, &combine_name, &combine_value)) {
        return false;
    }
    std::shared_ptr<IndexDef> index_def =
        table_index_.GetIndexByCombineStr(combine_name);
    if (!index_def) {
        PDLOG(DEBUG, "combine_name %s not found. tid %u pid %u",
              combine_name.c_str(), id_, pid_);
        return false;
    }
    return Query(index_def, rocksdb::Slice(combine_value), return_vec);
}

bool RelationalTable::Query(
    const std::shared_ptr<IndexDef> index_def, const rocksdb::Slice& key_slice,
    std::vector<std::unique_ptr<rocksdb::Iterator>>* return_vec) {
    uint32_t idx = index_def->GetId();
    ::rtidb::type::IndexType index_type = index_def->GetType();
    std::unique_ptr<rocksdb::Iterator> it(GetIteratorAndSeek(idx, key_slice));
    if (!it) {
        return false;
    }
    if (index_type == ::rtidb::type::kPrimaryKey ||
        index_type == ::rtidb::type::kAutoGen) {
        if (it->key() != key_slice) {
            PDLOG(DEBUG, "key %s not found. tid %u pid %u",
                  key_slice.ToString().c_str(), id_, pid_);
            return false;
        }
        return_vec->push_back(std::move(it));
    } else if (index_type == ::rtidb::type::kUnique) {
        if (it->key() != key_slice) {
            PDLOG(DEBUG, "key %s not found. tid %u pid %u",
                  key_slice.ToString().c_str(), id_, pid_);
            return false;
        }
        Query(table_index_.GetPkIndex(), it->value(), return_vec);
    } else if (index_type == ::rtidb::type::kNoUnique) {
        int count = 0;
        while (it->Valid()) {
            std::string key(key_slice.data(), key_slice.size());
            rocksdb::Slice pk_slice = ParsePk(it->key(), key);
            if (pk_slice.empty()) {
                if (count == 0) {
                    PDLOG(DEBUG,
                          "ParsePk failed, key %s not exist, tid %u pid %u",
                          key.c_str(), id_, pid_);
                    return false;
                } else {
                    return true;
                }
            }
            Query(table_index_.GetPkIndex(), pk_slice, return_vec);
            it->Next();
            count++;
        }
    }
    return true;
}

bool RelationalTable::Update(const ::google::protobuf::RepeatedPtrField<
                                 ::rtidb::api::Columns>& cd_columns,
                             const ::rtidb::api::Columns& col_columns) {
    std::string combine_name = "";
    std::string combine_value = "";
    if (!GetCombineStr(cd_columns, &combine_name, &combine_value)) {
        return false;
    }
    std::shared_ptr<IndexDef> index_def =
        table_index_.GetIndexByCombineStr(combine_name);
    if (!index_def) return false;

    const std::string& col_value = col_columns.value();
    std::map<std::string, int> col_idx_map;
    Schema value_schema;
    if (!CreateSchema(col_columns, &col_idx_map, &value_schema)) return false;
    return UpdateDB(index_def, combine_value, col_idx_map, value_schema,
                    col_value);
}

bool RelationalTable::CreateSchema(const ::rtidb::api::Columns& columns,
                                   std::map<std::string, int>* idx_map,
                                   Schema* new_schema) {
    for (int i = 0; i < columns.name_size(); i++) {
        const std::string& name = columns.name(i);
        idx_map->insert(std::make_pair(name, i));
        ::rtidb::common::ColumnDesc* col = new_schema->Add();
        col->set_name(name);
        std::shared_ptr<ColumnDef> column_def = table_column_.GetColumn(name);
        if (!column_def) {
            PDLOG(WARNING, "col name %s not exist, tid %u pid %u", name.c_str(),
                  id_, pid_);
            return false;
        }
        col->set_data_type(column_def->GetType());
    }
    return true;
}

bool RelationalTable::UpdateDB(const std::shared_ptr<IndexDef> index_def,
                               const std::string& comparable_key,
                               const std::map<std::string, int>& col_idx_map,
                               const Schema& value_schema,
                               const std::string& col_value) {
    ::rtidb::type::IndexType index_type = index_def->GetType();
    if (index_type == ::rtidb::type::kAutoGen ||
        index_type == ::rtidb::type::kIncrement) {
        PDLOG(WARNING, "unsupported index type %s, tid %u pid %u.",
              rtidb::type::IndexType_Name(index_type).c_str(), id_, pid_);
        return false;
    }
    ::rtidb::base::Slice slice(col_value);
    std::string uncompressed;
    if (table_meta_.compress_type() == ::rtidb::api::kSnappy) {
        ::snappy::Uncompress(col_value.c_str(), col_value.length(),
                             &uncompressed);
        slice.reset(uncompressed.data(), uncompressed.size());
    }
    std::lock_guard<std::mutex> lock(mu_);
    std::vector<std::unique_ptr<rocksdb::Iterator>> iter_vec;
    bool ok = Query(index_def, rocksdb::Slice(comparable_key), &iter_vec);
    if (!ok) {
        PDLOG(WARNING, "get failed, update table tid %u pid %u failed", id_,
              pid_);
        return false;
    }

    bool is_update_index = false;
    bool is_update_pk = false;
    const Schema& schema = table_meta_.column_desc();
    ::rtidb::base::Slice pk_slice(comparable_key);
    rocksdb::WriteBatch batch;
    for (const auto& iter : iter_vec) {
        ::rtidb::base::Slice sc(iter->value().data(), iter->value().size());
        std::string new_val;
        if (table_meta_.compress_type() == ::rtidb::api::kSnappy) {
            ::snappy::Uncompress(iter->value().data(), iter->value().size(),
                                 &new_val);
            sc.reset(new_val.data(), new_val.size());
        }
        int8_t* buf = reinterpret_cast<int8_t*>(const_cast<char*>(sc.data()));
        std::string pk;
        if (index_type == ::rtidb::type::kUnique ||
            index_type == ::rtidb::type::kNoUnique) {
            if (!GetCombineStr(table_index_.GetPkIndex(),
                               reinterpret_cast<int8_t*>(
                                   const_cast<char*>(iter->value().data())),
                               &pk)) {
                return false;
            }
            pk_slice.reset(pk.data(), pk.size());
        }
        ::rtidb::codec::RowView value_view(
            value_schema,
            reinterpret_cast<int8_t*>(const_cast<char*>(slice.data())),
            slice.size());
        uint32_t string_length = 0;
        for (int i = 0; i < schema.size(); i++) {
            if (schema.Get(i).data_type() == rtidb::type::kVarchar ||
                schema.Get(i).data_type() == rtidb::type::kString) {
                auto col_iter = col_idx_map.find(schema.Get(i).name());
                char* ch = NULL;
                uint32_t length = 0;
                if (col_iter != col_idx_map.end()) {
                    value_view.GetString(col_iter->second, &ch, &length);
                } else {
                    row_view_.GetValue(buf, i, &ch, &length);
                }
                string_length += length;
            }
        }
        ::rtidb::codec::RowBuilder builder(schema);
        uint32_t size = builder.CalTotalLength(string_length);
        std::string row;
        row.resize(size);
        builder.SetBuffer(reinterpret_cast<int8_t*>(&(row[0])), size);
        for (int i = 0; i < schema.size(); i++) {
            auto col_iter = col_idx_map.find(schema.Get(i).name());
            if (col_iter != col_idx_map.end()) {
                if ((!is_update_index)
                        && table_index_.FindColName(schema.Get(i).name())) {
                    is_update_index = true;
                }
                if (!is_update_pk) {
                    for (const auto& col_def :
                            table_index_.GetPkIndex()->GetColumns()) {
                        if (col_def.GetName() == schema.Get(i).name()) {
                            is_update_pk = true;
                        }
                    }
                }
                if (schema.Get(i).not_null() &&
                    value_view.IsNULL(col_iter->second)) {
                    PDLOG(WARNING,
                          "not_null is true but value is null, update table "
                          "tid %u pid %u failed",
                          id_, pid_);
                    return false;
                } else if (value_view.IsNULL(col_iter->second)) {
                    builder.AppendNULL();
                    continue;
                }
            }
            int get_value_ret = 0;
            rtidb::type::DataType cur_type = schema.Get(i).data_type();
            switch (cur_type) {
                case rtidb::type::kBool: {
                    bool val = true;
                    if (col_iter != col_idx_map.end()) {
                        get_value_ret =
                            value_view.GetBool(col_iter->second, &val);
                    } else {
                        get_value_ret =
                            row_view_.GetValue(buf, i, cur_type, &val);
                    }
                    builder.AppendBool(val);
                    break;
                }
                case rtidb::type::kSmallInt: {
                    int16_t val = 0;
                    if (col_iter != col_idx_map.end()) {
                        get_value_ret =
                            value_view.GetInt16(col_iter->second, &val);
                    } else {
                        get_value_ret =
                            row_view_.GetValue(buf, i, cur_type, &val);
                    }
                    builder.AppendInt16(val);
                    break;
                }
                case rtidb::type::kInt: {
                    int32_t val = 0;
                    if (col_iter != col_idx_map.end()) {
                        get_value_ret =
                            value_view.GetInt32(col_iter->second, &val);
                    } else {
                        get_value_ret =
                            row_view_.GetValue(buf, i, cur_type, &val);
                    }
                    builder.AppendInt32(val);
                    break;
                }
                case rtidb::type::kBlob:
                case rtidb::type::kBigInt: {
                    int64_t val = 0;
                    if (col_iter != col_idx_map.end()) {
                        get_value_ret =
                            value_view.GetInt64(col_iter->second, &val);
                    } else {
                        get_value_ret =
                            row_view_.GetValue(buf, i, cur_type, &val);
                    }
                    builder.AppendInt64(val);
                    break;
                }
                case rtidb::type::kTimestamp: {
                    int64_t val = 0;
                    if (col_iter != col_idx_map.end()) {
                        get_value_ret =
                            value_view.GetTimestamp(col_iter->second, &val);
                    } else {
                        get_value_ret =
                            row_view_.GetValue(buf, i, cur_type, &val);
                    }
                    builder.AppendTimestamp(val);
                    break;
                }
                case rtidb::type::kFloat: {
                    float val = 0.0;
                    if (col_iter != col_idx_map.end()) {
                        get_value_ret =
                            value_view.GetFloat(col_iter->second, &val);
                    } else {
                        get_value_ret =
                            row_view_.GetValue(buf, i, cur_type, &val);
                    }
                    builder.AppendFloat(val);
                    break;
                }
                case rtidb::type::kDouble: {
                    double val = 0.0;
                    if (col_iter != col_idx_map.end()) {
                        get_value_ret =
                            value_view.GetDouble(col_iter->second, &val);
                    } else {
                        get_value_ret =
                            row_view_.GetValue(buf, i, cur_type, &val);
                    }
                    builder.AppendDouble(val);
                    break;
                }
                case rtidb::type::kVarchar:
                case rtidb::type::kString: {
                    char* ch = NULL;
                    uint32_t length = 0;
                    if (col_iter != col_idx_map.end()) {
                        get_value_ret = value_view.GetString(
                                col_iter->second, &ch, &length);
                    } else {
                        get_value_ret =
                            row_view_.GetValue(buf, i, &ch, &length);
                    }
                    builder.AppendString(ch, length);
                    break;
                }
                case rtidb::type::kDate: {
                    uint32_t val = 0;
                    if (col_iter != col_idx_map.end()) {
                        get_value_ret =
                            value_view.GetDate(col_iter->second, &val);
                    } else {
                        get_value_ret =
                            row_view_.GetValue(buf, i, cur_type, &val);
                    }
                    builder.AppendDate(val);
                    break;
                }
                default: {
                    PDLOG(WARNING, "unsupported data type %s",
                          rtidb::type::DataType_Name(cur_type).c_str());
                    return false;
                }
            }
            if (get_value_ret < 0) {
                PDLOG(WARNING, "getValue failed,_type %s, tid %u pid %u",
                        rtidb::type::DataType_Name(cur_type).c_str(),
                        id_, pid_);
                return false;
            }
        }
        if (is_update_index) {
            ok = DeletePk(rocksdb::Slice(pk_slice.data(), pk_slice.size()),
                          &batch);
            if (!ok) {
                PDLOG(WARNING,
                      "delete failed,"
                      "update table tid %u pid %u failed",
                      id_, pid_);
                return false;
            }
        }
        std::string new_pk;
        if (is_update_pk) {
            if (!GetCombineStr(
                        table_index_.GetPkIndex(),
                        reinterpret_cast<int8_t*>(
                            const_cast<char*>(row.data())), &new_pk)) {
                return false;
            }
            pk_slice.reset(new_pk.data(), new_pk.size());
        }
        ok = PutDB(rocksdb::Slice(pk_slice.data(), pk_slice.size()),
                   row.c_str(), row.length(), false, &batch);
        if (!ok) {
            PDLOG(WARNING, "put failed, update table tid %u pid %u failed", id_,
                  pid_);
            return false;
        }
    }
    rocksdb::Status s = db_->Write(write_opts_, &batch);
    if (s.ok()) {
        offset_.fetch_add(1, std::memory_order_relaxed);
        return true;
    } else {
        PDLOG(DEBUG, "update failed. tid %u pid %u msg %s", id_, pid_,
              s.ToString().c_str());
        return false;
    }
    return true;
}

bool RelationalTable::GetCombineStr(
    const ::google::protobuf::RepeatedPtrField<::rtidb::api::Columns>& indexs,
    std::string* combine_name, std::string* combine_value) {
    combine_name->clear();
    combine_value->clear();
    if (indexs.size() == 1) {
        *combine_name = indexs.Get(0).name(0);
        const std::string& value = indexs.Get(0).value();
        if (!ConvertIndex(*combine_name, value, combine_value)) return false;
    } else {
        std::vector<ColumnDef> vec;
        std::map<std::string, int> map;
        for (int i = 0; i < indexs.size(); i++) {
            const std::string& name = indexs.Get(i).name(0);
            map.insert(std::make_pair(name, i));
            vec.push_back(*(table_column_.GetColumn(name)));
        }
        std::sort(vec.begin(), vec.end(), ::rtidb::storage::ColumnDefSortFunc);
        int count = 0;
        for (auto& col_def : vec) {
            const std::string& name = col_def.GetName();
            if (count > 0) {
                combine_name->append("_");
            }
            combine_name->append(name);
            auto it = map.find(name);
            if (it == map.end()) return false;
            const std::string& value = indexs.Get(it->second).value();
            std::string temp = "";
            if (!ConvertIndex(name, value, &temp)) return false;
            if (count > 0) {
                combine_value->append("|");
            }
            combine_value->append(temp);
            count++;
        }
    }
    return true;
}

bool RelationalTable::GetCombineStr(const std::shared_ptr<IndexDef> index_def,
                                    const int8_t* data,
                                    std::string* comparable_pk) {
    comparable_pk->clear();
    if (index_def->GetColumns().size() == 1) {
        uint32_t idx = index_def->GetColumns().at(0).GetId();
        ::rtidb::type::DataType data_type =
            index_def->GetColumns().at(0).GetType();
        if (!GetPackedField(data, idx, data_type, comparable_pk)) {
            return false;
        }
    } else {
        int count = 0;
        std::string col_val = "";
        for (const auto& col_def : index_def->GetColumns()) {
            uint32_t idx = col_def.GetId();
            ::rtidb::type::DataType data_type = col_def.GetType();
            if (!GetPackedField(data, idx, data_type, &col_val)) {
                return false;
            }
            if (count++ > 0) {
                comparable_pk->append("|");
            }
            comparable_pk->append(col_val);
        }
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
        if (iter->second->atime + FLAGS_snapshot_ttl_time <= cur_time) {
            std::shared_ptr<SnapshotInfo> sc = iter->second;
            iter = snapshots_.erase(iter);
            db_->ReleaseSnapshot(sc->snapshot);
            continue;
        }
        iter++;
    }
}

RelationalTableTraverseIterator* RelationalTable::NewTraverse(
    uint32_t idx, uint64_t snapshot_id) {
    if (idx >= idx_cnt_) {
        PDLOG(WARNING,
              "idx greater than idx_cnt_, failed getting table tid %u pid %u",
              id_, pid_);
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

RelationalTableTraverseIterator::RelationalTableTraverseIterator(
    RelationalTable* table, rocksdb::Iterator* it, uint64_t id)
    : table_(table), it_(it), traverse_cnt_(0), finish_(false), id_(id) {}

RelationalTableTraverseIterator::~RelationalTableTraverseIterator() {
    if (!it_->Valid()) {
        finish_ = true;
    }
    delete it_;
    table_->ReleaseSnpashot(id_, finish_);
}

bool RelationalTableTraverseIterator::Valid() { return it_->Valid(); }

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

uint64_t RelationalTableTraverseIterator::GetCount() { return traverse_cnt_; }

rtidb::base::Slice RelationalTableTraverseIterator::GetValue() {
    rocksdb::Slice spk = it_->value();
    return rtidb::base::Slice(spk.data(), spk.size());
}

rtidb::base::Slice RelationalTableTraverseIterator::GetKey() {
    rocksdb::Slice key = it_->key();
    return rtidb::base::Slice(key.data(), key.size());
}

uint64_t RelationalTableTraverseIterator::GetSeq() { return id_; }

void RelationalTableTraverseIterator::SetFinish(bool finish) {
    finish_ = finish;
}

}  // namespace storage
}  // namespace rtidb
