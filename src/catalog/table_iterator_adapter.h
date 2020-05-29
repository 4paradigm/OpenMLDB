/*
 * table_iterator_adapter.h
 * Copyright (C) 4paradigm.com 2020 wangtaize <wangtaize@4paradigm.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef SRC_CATALOG_TABLE_ITERATOR_ADAPTER_H_
#define SRC_CATALOG_TABLE_ITERATOR_ADAPTER_H_

#include <memory>
#include <string>

#include "base/fe_slice.h"
#include "base/iterator.h"
#include "codec/list_iterator_codec.h"
#include "glog/logging.h"
#include "storage/table.h"
#include "vm/catalog.h"

namespace rtidb {
namespace catalog {

class FullTableIterator;
class EmptyWindowIterator;

class EmptyWindowIterator
    : public ::fesql::codec::ConstIterator<uint64_t, ::fesql::codec::Row> {
 public:
    EmptyWindowIterator() : value_(), key_(0) {}

    ~EmptyWindowIterator() {}

    inline void Seek(const uint64_t& ts) {}

    inline void SeekToFirst() {}

    inline bool Valid() const { return false; }

    inline void Next() {}

    inline const ::fesql::codec::Row& GetValue() { return value_; }

    inline const uint64_t& GetKey() const { return key_; }

    bool IsSeekable() const override { return true; }

 private:
    ::fesql::codec::Row value_;
    uint64_t key_;
};

// the full table iterator
class FullTableIterator
    : public ::fesql::codec::ConstIterator<uint64_t, ::fesql::codec::Row> {
 public:
    FullTableIterator() : it_(), table_() {}

    explicit FullTableIterator(::rtidb::storage::TableIterator* it,
                               std::shared_ptr<::rtidb::storage::Table> table)
        : it_(it), table_(table), value_(), key_(0) {}
    ~FullTableIterator() { delete it_; }

    inline void Seek(const uint64_t& ts) {}

    inline void SeekToFirst() { it_->SeekToFirst(); }

    inline bool Valid() const { return it_->Valid(); }

    inline void Next() { it_->Next(); }

    inline const ::fesql::codec::Row& GetValue() {
        value_ =
            ::fesql::codec::Row(::fesql::base::RefCountedSlice::Create(it_->GetValue().data(),
                        it_->GetValue().size()));
        return value_;
    }

    inline bool IsSeekable() const override { return true; }

    // the key maybe the row num
    inline const uint64_t& GetKey() const { return key_; }

 private:
    ::rtidb::storage::TableIterator* it_;
    std::shared_ptr<::rtidb::storage::Table> table_;
    ::fesql::codec::Row value_;
    uint64_t key_;
};

}  // namespace catalog
}  // namespace rtidb

#endif  // SRC_CATALOG_TABLE_ITERATOR_ADAPTER_H_
