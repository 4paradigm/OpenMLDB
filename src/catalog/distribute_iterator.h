/*
 * distribute_iterator.h
 * Copyright (C) 4paradigm.com 2020
 * Author denglong
 * Date 2020-09-21
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

#ifndef SRC_CATALOG_DISTRIBUTE_ITERATOR_H_
#define SRC_CATALOG_DISTRIBUTE_ITERATOR_H_

#include <memory>
#include <string>
#include <unordered_map>

#include "base/hash.h"
#include "catalog/client_manager.h"
#include "storage/table.h"
#include "vm/catalog.h"

namespace rtidb {
namespace catalog {

using Tables = std::unordered_map<uint32_t, std::shared_ptr<::rtidb::storage::Table>>;

class DistributeWindowIterator : public ::fesql::codec::WindowIterator {
 public:
    DistributeWindowIterator(std::shared_ptr<Tables> tables, uint32_t index);
    void Seek(const std::string& key) override;
    void SeekToFirst() override;
    void Next() override;
    bool Valid() override;
    std::unique_ptr<::fesql::codec::RowIterator> GetValue() override;
    ::fesql::codec::RowIterator* GetValue(int8_t* addr) override;
    const ::fesql::codec::Row GetKey() override;

 private:
    std::shared_ptr<Tables> tables_;
    uint32_t index_;
    std::unique_ptr<::fesql::codec::WindowIterator> it_;
};

class DistributeRowIterator : public ::fesql::codec::RowIterator {
 public:
    DistributeRowIterator();
    bool Valid() const override;
    void Next() override;
    const uint64_t& GetKey() const override;
    const ::fesql::codec::Row& GetValue() override;
    void Seek(const uint64_t& k) override;
    void SeekToFirst() override;
    bool IsSeekable() const override;

 private:
    bool request_all_;
    uint64_t cur_ts_;
    ::fesql::codec::Row value_;
    std::shared_ptr<TableClientManager> table_client_manager_;
};

}  // namespace catalog
}  // namespace rtidb
#endif  // SRC_CATALOG_DISTRIBUTE_ITERATOR_H_
