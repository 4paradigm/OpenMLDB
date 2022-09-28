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

#pragma once

#include <string>

#include "base/slice.h"

namespace openmldb {
namespace storage {

class TableIterator {
 public:
    TableIterator() {}
    virtual ~TableIterator() {}
    TableIterator(const TableIterator&) = delete;
    TableIterator& operator=(const TableIterator&) = delete;
    virtual bool Valid(){ };
    virtual void Next(){ };
    virtual openmldb::base::Slice GetValue() const {}; //
    virtual std::string GetPK() const { return std::string(); }
    virtual uint64_t GetKey() const {};     //
    virtual void SeekToFirst(){ };
    virtual void SeekToLast() {}
    virtual void Seek(const std::string& pk, uint64_t time) {}
    virtual void Seek(uint64_t time) {}
    virtual uint64_t GetCount() const { return 0; }
};

class TraverseIterator : public TableIterator {
 public:
    TraverseIterator() {}
    virtual ~TraverseIterator() {}
    virtual void NextPK() = 0;
};

}  // namespace storage
}  // namespace openmldb
