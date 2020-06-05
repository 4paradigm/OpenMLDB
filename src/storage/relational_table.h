//
// Copyright (C) 2017 4paradigm.com
// Created by wangbao on 02/24/20.
//

#pragma once

#include <gflags/gflags.h>
#include <rocksdb/compaction_filter.h>
#include <rocksdb/db.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include <rocksdb/slice_transform.h>
#include <rocksdb/status.h>
#include <rocksdb/table.h>
#include <rocksdb/utilities/checkpoint.h>
#ifdef DISALLOW_COPY_AND_ASSIGN
#undef DISALLOW_COPY_AND_ASSIGN
#endif
#include <snappy.h>

#include <atomic>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>
#include <boost/lexical_cast.hpp>
#include "base/endianconv.h"
#include "base/id_generator.h"
#include "base/slice.h"
#include "codec/row_codec.h"
#include "codec/field_codec.h"
#include "codec/memcomparable_format.h"
#include "proto/common.pb.h"
#include "proto/tablet.pb.h"
#include "storage/iterator.h"
#include "timer.h"  // NOLINT

typedef google::protobuf::RepeatedPtrField<::rtidb::api::Dimension> Dimensions;
using Schema =
    ::google::protobuf::RepeatedPtrField<::rtidb::common::ColumnDesc>;
using google::protobuf::RepeatedPtrField;
using google::protobuf::RepeatedField;

namespace rtidb {
namespace storage {

class RelationalTable;

class RelationalTableTraverseIterator {
 public:
    RelationalTableTraverseIterator(RelationalTable* table,
                                    rocksdb::Iterator* it, uint64_t id);
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
    RelationalTable(const ::rtidb::api::TableMeta& table_meta,
                    const std::string& db_root_path);
    RelationalTable(const RelationalTable&) = delete;
    RelationalTable& operator=(const RelationalTable&) = delete;
    ~RelationalTable();
    bool Init();
    bool LoadTable();

    bool Put(const std::string& value, int64_t* auto_gen_pk,
            const ::rtidb::api::WriteOption& wo);

    bool Query(const ::google::protobuf::RepeatedPtrField<
            ::rtidb::api::ReadOption>& ros,
            std::string* pairs, uint32_t* count,
            int32_t* code, std::string* msg);
    bool Query(const ::google::protobuf::RepeatedPtrField<
            ::rtidb::api::Columns>& indexs,
            std::vector<std::unique_ptr<rocksdb::Iterator>>* return_vec,
            int32_t* code, std::string* msg);
    bool Query(const std::shared_ptr<IndexDef> index_def,
               const rocksdb::Slice& key_slice,
               std::vector<std::unique_ptr<rocksdb::Iterator>>* vec);

    bool Delete(
            const RepeatedPtrField<::rtidb::api::Columns>& condition_columns,
            int32_t* code, std::string* msg, uint32_t* count);

    bool Delete(const RepeatedPtrField<rtidb::api::Columns>& condition_columns,
                RepeatedField<google::protobuf::int64_t>* blob_keys,
                int32_t* code, std::string* msg, uint32_t* count);

    bool Delete(const std::shared_ptr<IndexDef> index_def,
                const std::string& comparable_key, rocksdb::WriteBatch* batch,
                uint32_t* count);
    bool DeletePk(const rocksdb::Slice& pk_slice, rocksdb::WriteBatch* batch,
            uint32_t* count);

    rtidb::storage::RelationalTableTraverseIterator* NewTraverse(
        uint32_t idx, uint64_t snapshot_id);

    void ReleaseSnpashot(uint64_t seq, bool finish);

    void TTLSnapshot();

    bool Update(const ::google::protobuf::RepeatedPtrField<
                    ::rtidb::api::Columns>& cd_columns,
                const ::rtidb::api::Columns& col_columns,
                int32_t* code, std::string* msg, uint32_t* count);

    inline ::rtidb::common::StorageMode GetStorageMode() const {
        return storage_mode_;
    }

    inline uint32_t GetTableStat() {
        return table_status_.load(std::memory_order_relaxed);
    }

    inline uint32_t GetIdxCnt() const { return idx_cnt_; }

    inline void SetTableStat(uint32_t table_status) {
        table_status_.store(table_status, std::memory_order_relaxed);
    }

    uint64_t GetRecordCnt() const {
        uint64_t count = 0;
        if (cf_hs_.size() == 1) {
            db_->GetIntProperty(cf_hs_[0], "rocksdb.estimate-num-keys", &count);
        } else {
            db_->GetIntProperty(cf_hs_[1], "rocksdb.estimate-num-keys", &count);
        }
        return count;
    }

    uint64_t GetOffset() { return offset_.load(std::memory_order_relaxed); }

    void SetOffset(uint64_t offset) {
        offset_.store(offset, std::memory_order_relaxed);
    }

    std::shared_ptr<IndexDef> GetPkIndex() { return table_index_.GetPkIndex(); }
    inline bool HasAutoGen() { return table_index_.HasAutoGen();}

    bool GetCombinePk(const ::google::protobuf::RepeatedPtrField<
            ::rtidb::api::Columns>& indexs,
            std::string* combine_value);
    const std::shared_ptr<IndexDef> GetIndexByCombineStr(
        const std::string& combine_str) {
        return table_index_.GetIndexByCombineStr(combine_str);
    }

 private:
    inline void CombineNoUniqueAndPk(const std::string& no_unique,
                                     const std::string& pk,
                                     std::string* result) {
        result->reserve(no_unique.size() + pk.size());
        result->assign(no_unique);
        result->append(pk);
    }
    inline rocksdb::Slice ParsePk(const rocksdb::Slice& value,
                                  const std::string& key) {
        if (value.size() < key.size()) {
            return rocksdb::Slice();
        }
        if (memcmp(reinterpret_cast<void*>(const_cast<char*>(key.c_str())),
                   reinterpret_cast<void*>(const_cast<char*>(value.data())),
                   key.size()) != 0) {
            return rocksdb::Slice();
        }
        return rocksdb::Slice(value.data() + key.size(),
                              value.size() - key.size());
    }
    bool InitColumnFamilyDescriptor();
    int InitColumnDesc();
    bool InitFromMeta();
    static void initOptionTemplate();
    rocksdb::Iterator* GetIteratorAndSeek(uint32_t idx,
                                          const rocksdb::Slice& key_slice);
    rocksdb::Iterator* GetRocksdbIterator(uint32_t idx);
    bool PutDB(const rocksdb::Slice& spk, const char* data, uint32_t size,
               bool pk_check, bool unique_check, rocksdb::WriteBatch* batch);
    bool CreateSchema(const ::rtidb::api::Columns& columns,
                      std::map<std::string, int>* idx_map, Schema* new_schema);
    bool UpdateDB(const std::shared_ptr<IndexDef> index_def,
                  const std::string& comparable_key,
                  const std::map<std::string, int>& col_idx_map,
                  const Schema& value_schema, const std::string& col_value,
                  uint32_t* count);
    bool GetPackedField(const int8_t* row, uint32_t idx,
                        ::rtidb::type::DataType data_type,
                        ::rtidb::type::IndexType idx_type,
                        std::string* key);
    bool PackValue(const void *from, ::rtidb::type::DataType data_type,
            std::string* key);
    bool ConvertIndex(const std::string& name, const std::string& value,
            bool has_null, ::rtidb::type::IndexType idx_type,
            std::string* out_val);
    bool GetCombineStr(const ::google::protobuf::RepeatedPtrField<
            ::rtidb::api::Columns>& indexs,
            std::string* combine_name, std::string* combine_value);
    bool GetCombineStr(const std::shared_ptr<IndexDef> index_def,
                       const int8_t* data, std::string* comparable_pk);

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
    uint64_t snapshot_index_;  // 0 is invalid snapshot_index
    ::rtidb::codec::RowView row_view_;
};

}  // namespace storage
}  // namespace rtidb
