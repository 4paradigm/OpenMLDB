//
// Created by wangbao on 02/24/20. 
//

#pragma once

#include <vector>
#include <map>
#include <atomic>
#include "proto/common.pb.h"
#include "proto/tablet.pb.h"
#include <gflags/gflags.h>
#include <rocksdb/db.h>
#include <rocksdb/slice.h>
#include <rocksdb/options.h>
#include <rocksdb/status.h>
#include <rocksdb/table.h>
#include <rocksdb/slice_transform.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/compaction_filter.h>
#include <rocksdb/utilities/checkpoint.h>
#include "base/slice.h"
#include "base/endianconv.h"
#include "storage/iterator.h"
#include "storage/table.h"
#include <boost/lexical_cast.hpp>
#include "timer.h"
#include "base/codec.h"
#include <mutex>
#include "base/id_generator.h"

typedef google::protobuf::RepeatedPtrField<::rtidb::api::Dimension> Dimensions;
using Schema = ::google::protobuf::RepeatedPtrField<::rtidb::common::ColumnDesc>;

namespace rtidb {
namespace storage {
class RelationalTableTraverseIterator {
 public:
    RelationalTableTraverseIterator(rocksdb::DB* db, rocksdb::Iterator* it,
                                    const rocksdb::Snapshot* snapshot);
    ~RelationalTableTraverseIterator();
    bool Valid();
    void Next();
    void SeekToFirst();
    void Seek(const std::string& pk);
    uint64_t GetCount();
    rtidb::base::Slice GetValue();

 private:
    rocksdb::DB* db_;
    rocksdb::Iterator* it_;
    const rocksdb::Snapshot* snapshot_;
    uint64_t traverse_cnt_;
};

class RelationalTable {

public:
    RelationalTable(const std::string& name,
                uint32_t id,
                uint32_t pid,
                ::rtidb::common::StorageMode storage_mode,
                const std::string& db_root_path);

    RelationalTable(const ::rtidb::api::TableMeta& table_meta,
              const std::string& db_root_path);
    RelationalTable(const RelationalTable&) = delete;
    RelationalTable& operator=(const RelationalTable&) = delete;
    ~RelationalTable();
    bool Init();

    bool Put(const std::string& value); 

    bool Put(const std::string& value,
             const Dimensions& dimensions);

    bool Get(const std::string& col_name, const std::string& key, rtidb::base::Slice& slice); 

    bool Delete(const std::string& pk, uint32_t idx);

    rtidb::storage::RelationalTableTraverseIterator* NewTraverse(uint32_t idx);

    bool Update(const ::rtidb::api::Columns& cd_columns, 
            const ::rtidb::api::Columns& col_columns);

    inline ::rtidb::common::StorageMode GetStorageMode() const {
        return storage_mode_;
    }

    inline uint32_t GetTableStat() {
        return table_status_.load(std::memory_order_relaxed);
    }

    inline uint32_t GetIdxCnt() const {
        return idx_cnt_;
    }

    inline void SetTableStat(uint32_t table_status) {
        table_status_.store(table_status, std::memory_order_relaxed);
    }

    uint64_t GetRecordCnt() const {
        uint64_t count = 0;
        if (cf_hs_.size() == 1)
            db_->GetIntProperty(cf_hs_[0], "rocksdb.estimate-num-keys", &count);
        else {
            db_->GetIntProperty(cf_hs_[1], "rocksdb.estimate-num-keys", &count);
        }
        return count;
    }

    uint64_t GetOffset() {
        return offset_.load(std::memory_order_relaxed);
    }

    void SetOffset(uint64_t offset) {
        offset_.store(offset, std::memory_order_relaxed);
    }

    std::vector<std::shared_ptr<IndexDef>> GetAllIndex() {
        return table_index_.GetAllIndex();
    }

    std::shared_ptr<IndexDef> GetIndex(const std::string& name) {
        return table_index_.GetIndex(name);
    }

    std::shared_ptr<IndexDef> GetIndex(uint32_t idx) {
        return table_index_.GetIndex(idx);
    }

    inline ::rtidb::api::TableMeta& GetTableMeta() {
        return table_meta_;
    }

private:
    static inline rocksdb::Slice CombineNoUniqueAndPk(const std::string& no_unique, 
            const std::string& pk) {
        std::string result;
        result.resize(no_unique.size() + pk.size());
        char* buf = const_cast<char*>(&(result[0]));
        memcpy(buf, no_unique.data(), no_unique.size());
        memcpy(buf + no_unique.size(), pk.data(), pk.size());
        return rocksdb::Slice(result.data(), result.size());
    }
    static inline int ParsePk(const rocksdb::Slice& value, const std::string& key, std::string* pk) {
        if (value.size() < key.size()) {
            return -1;
        }
        char* buf = const_cast<char*>(pk->data());
        memcpy(buf, value.data() + key.size(), value.size() - key.size());
        return 0;
    }
    bool InitColumnFamilyDescriptor();
    int InitColumnDesc();
    bool InitFromMeta();
    static void initOptionTemplate();
    bool PutDB(uint32_t pk_index_idx, 
            const std::string& pk,
            std::map<std::string, uint32_t>& unique_map,
            std::map<std::string, uint32_t>& no_unique_map,
            const char* data,
            uint32_t size);
    void UpdateInternel(const ::rtidb::api::Columns& cd_columns, 
            std::map<std::string, int>& cd_idx_map, 
            Schema& condition_schema);
    bool UpdateDB(const std::map<std::string, int>& cd_idx_map, 
            const std::map<std::string, int>& col_idx_map, 
            const Schema& condition_schema, const Schema& value_schema, 
            const std::string& cd_value, const std::string& col_value); 
    bool GetStr(::rtidb::base::RowView& view, uint32_t idx, 
            const ::rtidb::type::DataType& data_type, std::string* key); 
    bool GetMap(::rtidb::base::RowView& view, 
            std::map<std::string, uint32_t> *unique_map, 
            std::map<std::string, uint32_t> *no_unique_map); 

    std::mutex mu_;
    ::rtidb::common::StorageMode storage_mode_;
    std::string name_;
    uint32_t id_;
    uint32_t pid_;
    uint32_t idx_cnt_;
    std::atomic<uint64_t> diskused_;
    bool is_leader_;
    std::atomic<uint32_t> table_status_;
    TableIndex table_index_;
    ::rtidb::api::CompressType compress_type_;
    ::rtidb::api::TableMeta table_meta_;
    int64_t last_make_snapshot_time_;

    rocksdb::DB* db_;
    rocksdb::WriteOptions write_opts_;
    std::vector<rocksdb::ColumnFamilyDescriptor> cf_ds_;
    std::vector<rocksdb::ColumnFamilyHandle*> cf_hs_;
    rocksdb::Options options_;
    std::atomic<uint64_t> offset_;
    std::string db_root_path_;

    ::rtidb::base::IdGenerator id_generator_;
    bool has_auto_gen_;
    std::string pk_col_name_;
    int pk_idx_;
    ::rtidb::type::DataType pk_data_type_;
    std::map<std::string, std::map<uint32_t, ::rtidb::type::DataType>> unique_val_map_;
    std::map<std::string, std::map<uint32_t, ::rtidb::type::DataType>> no_unique_val_map_;
    std::map<std::string, std::map<uint32_t, ::rtidb::type::IndexType>> index_name_map_;

};

}
}
