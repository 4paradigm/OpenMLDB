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

#ifndef SRC_CATALOG_DISTRIBUTE_ITERATOR_H_
#define SRC_CATALOG_DISTRIBUTE_ITERATOR_H_

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "base/hash.h"
#include "base/kv_iterator.h"
#include "client/tablet_client.h"
#include "storage/table.h"
#include "vm/catalog.h"

namespace openmldb {
namespace catalog {

using Tables = std::map<uint32_t, std::shared_ptr<::openmldb::storage::Table>>;

class FullTableIterator : public ::hybridse::codec::ConstIterator<uint64_t, ::hybridse::codec::Row> {
 public:
    FullTableIterator(uint32_t tid, std::shared_ptr<Tables> tables,
            const std::map<uint32_t, std::shared_ptr<::openmldb::client::TabletClient>>& tablet_clients);
    void Seek(const uint64_t& ts) override {}
    void SeekToFirst() override;
    bool Valid() const override;
    void Next() override;
    const ::hybridse::codec::Row& GetValue() override;
    bool IsSeekable() const override { return true; }
    // the key maybe the row num
    const uint64_t& GetKey() const override { return key_; }

 private:
    bool NextFromLocal();
    bool NextFromRemote();
    void Reset();
    void EndLocal();

 private:
    uint32_t tid_;
    std::shared_ptr<Tables> tables_;
    std::map<uint32_t, std::shared_ptr<openmldb::client::TabletClient>> tablet_clients_;
    bool in_local_;
    uint32_t cur_pid_;
    std::unique_ptr<::openmldb::storage::TableIterator> it_;
    std::unique_ptr<::openmldb::base::KvIterator> kv_it_;
    uint64_t key_;
    uint64_t last_ts_;
    std::string last_pk_;
    ::hybridse::codec::Row value_;
    std::vector<std::shared_ptr<::google::protobuf::Message>> response_vec_;
};

class RemoteWindowIterator : public ::hybridse::vm::RowIterator {
 public:
    RemoteWindowIterator(std::shared_ptr<::openmldb::base::KvIterator> kv_it,
            std::shared_ptr<::google::protobuf::Message> response)
        : kv_it_(kv_it), response_(response) {
        if (kv_it_->Valid()) {
            pk_ = kv_it_->GetPK();
        }
    }
    bool Valid() const override {
        if (kv_it_->Valid() && pk_ == kv_it_->GetPK()) {
            DLOG(INFO) << "RemoteWindowIterator Valid pk " << pk_ << " ts " << kv_it_->GetKey();
            return true;
        }
        return false;
    }
    void Next() override {
        kv_it_->Next();
    }
    const uint64_t& GetKey() const override {
        ts_ = kv_it_->GetKey();
        return ts_;
    }
    const ::hybridse::codec::Row& GetValue() override {
        auto slice_row = kv_it_->GetValue();
        size_t sz = slice_row.size();
        // for distributed environment, slice_row probably become invalid sometime because `kv_it_`'s data will be
        // changed outside from `DistributeWindowIterator`, so copy action occured here
        int8_t* copyed_row_data = new int8_t[sz];
        memcpy(copyed_row_data, slice_row.data(), sz);
        auto shared_slice = ::hybridse::base::RefCountedSlice::CreateManaged(copyed_row_data, sz);
        row_.Reset(shared_slice);
        return row_;
    }

    // seek to the first element whose key is less or equal to `key`
    // or to the end if not found
    void Seek(const uint64_t& key) override {
        // key is ordered descending
        while (kv_it_->Valid() && kv_it_->GetKey() > key && kv_it_->GetPK() == pk_) {
            kv_it_->Next();
        }
    }
    void SeekToFirst() override {}
    bool IsSeekable() const override { return true; }

 private:
    std::shared_ptr<::openmldb::base::KvIterator> kv_it_;
    std::shared_ptr<::google::protobuf::Message> response_;
    ::hybridse::codec::Row row_;
    std::string pk_;
    mutable uint64_t ts_;
};

class DistributeWindowIterator : public ::hybridse::codec::WindowIterator {
 public:
    DistributeWindowIterator(uint32_t tid, uint32_t pid_num, std::shared_ptr<Tables> tables,
            uint32_t index, const std::string& index_name,
            const std::map<uint32_t, std::shared_ptr<::openmldb::client::TabletClient>>& tablet_clients);
    void Seek(const std::string& key) override;
    void SeekToFirst() override;
    void Next() override;
    bool Valid() override;
    std::unique_ptr<::hybridse::codec::RowIterator> GetValue() override;
    ::hybridse::codec::RowIterator* GetRawValue() override;
    const ::hybridse::codec::Row GetKey() override;

 private:
    void Reset();

 private:
    uint32_t tid_;
    uint32_t pid_num_;
    std::shared_ptr<Tables> tables_;
    std::map<uint32_t, std::shared_ptr<openmldb::client::TabletClient>> tablet_clients_;
    uint32_t index_;
    std::string index_name_;
    uint32_t cur_pid_;
    std::unique_ptr<::hybridse::codec::WindowIterator> it_;
    std::shared_ptr<::openmldb::base::KvIterator> kv_it_;
    std::vector<std::shared_ptr<::google::protobuf::Message>> response_vec_;
};

}  // namespace catalog
}  // namespace openmldb
#endif  // SRC_CATALOG_DISTRIBUTE_ITERATOR_H_
