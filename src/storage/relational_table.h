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
#include <boost/lexical_cast.hpp>
#include "timer.h"
#include "base/codec.h"
#include <mutex>
#include "base/id_generator.h"
#include "base/memcomparable_format.h"
#include <snappy.h>
#include "base/field_codec.h"

typedef google::protobuf::RepeatedPtrField<::rtidb::api::Dimension> Dimensions;
using Schema = ::google::protobuf::RepeatedPtrField<::rtidb::common::ColumnDesc>;

namespace rtidb {
namespace storage {

class RelationalTable;

class RelationalTableTraverseIterator {
 public:
    RelationalTableTraverseIterator(RelationalTable* table, rocksdb::Iterator* it,
                                    uint64_t id);
    ~RelationalTableTraverseIterator();
    bool Valid();
    void Next();
    void SeekToFirst();
    void Seek(const std::string& pk);
    uint64_t GetCount();
    rtidb::base::Slice GetValue();
    uint64_t GetSeq();
    rtidb::base::Slice GetKey();
    void SetFinish(bool finish);

 private:
    RelationalTable* table_;
    rocksdb::Iterator* it_;
    uint64_t traverse_cnt_;
    bool finish_;
    uint64_t id_;
};

struct SnapshotInfo {
    const rocksdb::Snapshot* snapshot;
    uint64_t atime;
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

    bool Query(const ::google::protobuf::RepeatedPtrField< ::rtidb::api::ReadOption >& ros,
            std::string* pairs, uint32_t* count);
    bool Query(const std::string& idx_name, const std::string& idx_val, std::vector<rocksdb::Slice>* vec); 
    bool Query(const std::shared_ptr<IndexDef> index_def, const rocksdb::Slice key_slice, std::vector<rocksdb::Slice>* vec); 

    bool Delete(const std::string& idx_name, const std::string& key);
    bool DeletePk(const rocksdb::Slice pk_slice); 

    rtidb::storage::RelationalTableTraverseIterator* NewTraverse(uint32_t idx, uint64_t snapshot_id);

    void ReleaseSnpashot(uint64_t seq, bool finish);

    void TTLSnapshot();

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

    std::shared_ptr<IndexDef> GetPkIndex() {
        return table_index_.GetPkIndex();
    }

private:
    inline void CombineNoUniqueAndPk(const std::string& no_unique, 
            const std::string& pk, std::string* result) {
        result->resize(no_unique.size() + pk.size());
        char* buf = const_cast<char*>(result->data());
        memcpy(buf, no_unique.c_str(), no_unique.size());
        memcpy(buf + no_unique.size(), pk.c_str(), pk.size());
    }
    inline rocksdb::Slice ParsePk(const rocksdb::Slice& value, const std::string& key) {
        if (value.size() < key.size()) {
            return rocksdb::Slice();
        }
        if (memcmp(reinterpret_cast<void*>(const_cast<char*>(key.c_str())), 
                    reinterpret_cast<void*>(const_cast<char*>(value.data())), key.size()) != 0) {
            return rocksdb::Slice();
        }
        return rocksdb::Slice(value.data() + key.size(), value.size() - key.size());
    }
    bool InitColumnFamilyDescriptor();
    int InitColumnDesc();
    bool InitFromMeta();
    static void initOptionTemplate();
    rocksdb::Iterator* GetIteratorAndSeek(uint32_t idx, const rocksdb::Slice key_slice); 
    rocksdb::Iterator* GetRocksdbIterator(uint32_t idx); 
    bool PutDB(const std::string& pk, const char* data, uint32_t size);
    void UpdateInternel(const ::rtidb::api::Columns& cd_columns, 
            std::map<std::string, int>& cd_idx_map, 
            Schema& condition_schema);
    bool UpdateDB(const std::map<std::string, int>& cd_idx_map, 
            const std::map<std::string, int>& col_idx_map, 
            const Schema& condition_schema, const Schema& value_schema, 
            const std::string& cd_value, const std::string& col_value); 
    bool GetPackedField(::rtidb::base::RowView& view, uint32_t idx, 
            const ::rtidb::type::DataType& data_type, std::string* key); 
    bool ConvertIndex(const std::string& name, const std::string& value, 
            std::string* out_val); 

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
    TableColumn table_column_;
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

    std::map<uint64_t, std::shared_ptr<SnapshotInfo>> snapshots_;
    uint64_t snapshot_index_; // 0 is invalid snapshot_index
};

}
}
