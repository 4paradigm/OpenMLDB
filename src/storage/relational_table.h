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

    bool Get(const std::string& idx_name, const std::string& key, rtidb::base::Slice& slice); 
    bool Get(uint32_t idx, const std::string& key, rtidb::base::Slice& slice); 

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

    bool HasAutoGen() {
        return table_index_.HasAutoGen(); 
    }
    inline uint32_t GetPkId() {
        return table_index_.GetPkId();   
    }
    inline ::rtidb::api::TableMeta& GetTableMeta() {
        return table_meta_;
    }

private:
    static inline std::string CombineNoUniqueAndPk(const std::string& no_unique, 
            const std::string& pk) {
        std::string result;
        result.resize(no_unique.size() + pk.size());
        char* buf = const_cast<char*>(&(result[0]));
        memcpy(buf, no_unique.c_str(), no_unique.size());
        memcpy(buf + no_unique.size(), pk.c_str(), pk.size());
        return result;
    }
    static inline int ParsePk(const rocksdb::Slice& value, const std::string& key, std::string* pk) {
        if (value.size() < key.size()) {
            return -1;
        }
        std::string real_key;
        real_key.resize(key.size());
        char* rk = const_cast<char*>(real_key.c_str());
        memcpy(rk, value.data(), key.size());
        if (real_key != key) {
            return -2;
        }

        pk->resize(value.size() - key.size());
        char* buf = const_cast<char*>(pk->c_str());
        memcpy(buf, value.data() + key.size(), value.size() - key.size());
        return 0;
    }
    bool InitColumnFamilyDescriptor();
    int InitColumnDesc();
    bool InitFromMeta();
    static void initOptionTemplate();
    rocksdb::Iterator* Seek(uint32_t idx, const std::string& key); 
    bool PutDB(const std::string& pk, const char* data, uint32_t size);
    void UpdateInternel(const ::rtidb::api::Columns& cd_columns, 
            std::map<std::string, int>& cd_idx_map, 
            Schema& condition_schema);
    bool UpdateDB(const std::map<std::string, int>& cd_idx_map, 
            const std::map<std::string, int>& col_idx_map, 
            const Schema& condition_schema, const Schema& value_schema, 
            const std::string& cd_value, const std::string& col_value); 
    bool GetStr(::rtidb::base::RowView& view, uint32_t idx, 
            const ::rtidb::type::DataType& data_type, std::string* key); 

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

};

}
}
