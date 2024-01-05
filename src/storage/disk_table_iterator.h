/*
 * Copyright 2021 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef SRC_STORAGE_DISK_TABLE_ITERATOR_H_
#define SRC_STORAGE_DISK_TABLE_ITERATOR_H_

#include <memory>
#include <string>
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "storage/iterator.h"
#include "storage/schema.h"
#include "vm/catalog.h"

namespace openmldb {
namespace storage {

class DiskTableIterator : public TableIterator {
 public:
    DiskTableIterator(rocksdb::DB* db, rocksdb::Iterator* it, const rocksdb::Snapshot* snapshot,
            const std::string& pk, type::CompressType compress_type);
    DiskTableIterator(rocksdb::DB* db, rocksdb::Iterator* it, const rocksdb::Snapshot* snapshot,
            const std::string& pk, uint32_t ts_idx, type::CompressType compress_type);
    virtual ~DiskTableIterator();
    bool Valid() override;
    void Next() override;
    openmldb::base::Slice GetValue() const override;
    std::string GetPK() const override;
    uint64_t GetKey() const override;
    void SeekToFirst() override;
    void Seek(uint64_t time) override;

 private:
    rocksdb::DB* db_;
    rocksdb::Iterator* it_;
    const rocksdb::Snapshot* snapshot_;
    std::string pk_;
    uint64_t ts_;
    uint32_t ts_idx_;
    bool has_ts_idx_ = false;
    type::CompressType compress_type_;
    mutable std::string tmp_buf_;
};

class DiskTableTraverseIterator : public TraverseIterator {
 public:
    DiskTableTraverseIterator(rocksdb::DB* db, rocksdb::Iterator* it, const rocksdb::Snapshot* snapshot,
                              ::openmldb::storage::TTLType ttl_type, const uint64_t& expire_time,
                              const uint64_t& expire_cnt, type::CompressType compress_type);
    DiskTableTraverseIterator(rocksdb::DB* db, rocksdb::Iterator* it, const rocksdb::Snapshot* snapshot,
                              ::openmldb::storage::TTLType ttl_type, const uint64_t& expire_time,
                              const uint64_t& expire_cnt, int32_t ts_idx, type::CompressType compress_type);
    virtual ~DiskTableTraverseIterator();
    bool Valid() override;
    void Next() override;
    void NextPK() override;
    openmldb::base::Slice GetValue() const override;
    std::string GetPK() const override;
    uint64_t GetKey() const override;
    void SeekToFirst() override;
    void Seek(const std::string& pk, uint64_t time) override;
    uint64_t GetCount() const override;

 private:
    bool IsExpired();

 private:
    rocksdb::DB* db_;
    rocksdb::Iterator* it_;
    const rocksdb::Snapshot* snapshot_;
    uint32_t record_idx_;
    ::openmldb::storage::TTLSt expire_value_;
    std::string pk_;
    uint64_t ts_;
    bool has_ts_idx_;
    uint32_t ts_idx_;
    uint64_t traverse_cnt_;
    type::CompressType compress_type_;
    mutable std::string tmp_buf_;
};

class DiskTableRowIterator : public ::hybridse::vm::RowIterator {
 public:
    DiskTableRowIterator(rocksdb::DB* db, rocksdb::Iterator* it, const rocksdb::Snapshot* snapshot,
                         ::openmldb::storage::TTLType ttl_type, uint64_t expire_time, uint64_t expire_cnt,
                         std::string pk, uint64_t ts, bool has_ts_idx, uint32_t ts_idx,
                         type::CompressType compress_type);

    ~DiskTableRowIterator();

    bool Valid() const override;

    void Next() override;

    inline const uint64_t& GetKey() const override;

    const ::hybridse::codec::Row& GetValue() override;

    void Seek(const uint64_t& key) override;
    void SeekToFirst() override;
    inline bool IsSeekable() const override;

 private:
    inline void ResetValue() {
        valid_value_ = false;
    }

    inline bool ValidValue() const {
        return valid_value_;
    }

 private:
    rocksdb::DB* db_;
    rocksdb::Iterator* it_;
    const rocksdb::Snapshot* snapshot_;
    uint32_t record_idx_;
    TTLSt expire_value_;
    std::string pk_;
    std::string row_pk_;
    uint64_t ts_;
    bool has_ts_idx_;
    uint32_t ts_idx_;
    ::hybridse::codec::Row row_;
    bool pk_valid_;
    bool valid_value_ = false;
    type::CompressType compress_type_;
    std::string tmp_buf_;
};

class DiskTableKeyIterator : public ::hybridse::vm::WindowIterator {
 public:
    DiskTableKeyIterator(rocksdb::DB* db, rocksdb::Iterator* it, const rocksdb::Snapshot* snapshot,
                         ::openmldb::storage::TTLType ttl_type, const uint64_t& expire_time, const uint64_t& expire_cnt,
                         int32_t ts_idx, rocksdb::ColumnFamilyHandle* column_handle,
                         type::CompressType compress_type);

    DiskTableKeyIterator(rocksdb::DB* db, rocksdb::Iterator* it, const rocksdb::Snapshot* snapshot,
                         ::openmldb::storage::TTLType ttl_type, const uint64_t& expire_time, const uint64_t& expire_cnt,
                         rocksdb::ColumnFamilyHandle* column_handle,
                         type::CompressType compress_type);

    ~DiskTableKeyIterator() override;

    void Seek(const std::string& pk) override;

    void SeekToFirst() override;

    void Next() override;

    bool Valid() override;

    std::unique_ptr<::hybridse::vm::RowIterator> GetValue() override;
    ::hybridse::vm::RowIterator* GetRawValue() override;

    const hybridse::codec::Row GetKey() override;

 private:
    void NextPK();

 private:
    rocksdb::DB* db_;
    rocksdb::Iterator* it_;
    const rocksdb::Snapshot* snapshot_;
    ::openmldb::storage::TTLType ttl_type_;
    uint64_t expire_time_;
    uint64_t expire_cnt_;
    std::string pk_;
    bool has_ts_idx_;
    uint64_t ts_;
    uint32_t ts_idx_;
    rocksdb::ColumnFamilyHandle* column_handle_;
    type::CompressType compress_type_;
};

}  // namespace storage
}  // namespace openmldb
#endif  // SRC_STORAGE_DISK_TABLE_ITERATOR_H_
