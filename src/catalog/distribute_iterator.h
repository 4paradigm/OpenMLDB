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

#include "base/hash.h"
#include "storage/table.h"
#include "vm/catalog.h"

namespace rtidb {
namespace catalog {

using Tables = std::map<uint32_t, std::shared_ptr<::rtidb::storage::Table>>;

class FullTableIterator
    : public ::fesql::codec::ConstIterator<uint64_t, ::fesql::codec::Row> {
 public:
    explicit FullTableIterator(std::shared_ptr<Tables> tables);
    void Seek(const uint64_t& ts) override {}
    void SeekToFirst() override;
    bool Valid() const override;
    void Next() override;
    const ::fesql::codec::Row& GetValue() override;
    bool IsSeekable() const override { return true; }
    // the key maybe the row num
    const uint64_t& GetKey() const override { return key_; }

 private:
    std::shared_ptr<Tables> tables_;
    uint32_t cur_pid_;
    std::unique_ptr<::rtidb::storage::TableIterator> it_;
    uint64_t key_;
    ::fesql::codec::Row value_;
};

class DistributeWindowIterator : public ::fesql::codec::WindowIterator {
 public:
    DistributeWindowIterator(std::shared_ptr<Tables> tables, uint32_t index);
    void Seek(const std::string& key) override;
    void SeekToFirst() override;
    void Next() override;
    bool Valid() override;
    std::unique_ptr<::fesql::codec::RowIterator> GetValue() override;
    ::fesql::codec::RowIterator* GetRawValue() override;
    const ::fesql::codec::Row GetKey() override;

 private:
    std::shared_ptr<Tables> tables_;
    uint32_t index_;
    uint32_t cur_pid_;
    uint32_t pid_num_;
    std::unique_ptr<::fesql::codec::WindowIterator> it_;
};

}  // namespace catalog
}  // namespace rtidb
#endif  // SRC_CATALOG_DISTRIBUTE_ITERATOR_H_
