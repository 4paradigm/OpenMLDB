/*
 * Copyright (C) 4Paradigm
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

#ifndef SRC_STORAGE_FE_TABLE_ITERATOR_H_
#define SRC_STORAGE_FE_TABLE_ITERATOR_H_

#include <memory>
#include <string>
#include "base/fe_slice.h"
#include "base/iterator.h"
#include "codec/list_iterator_codec.h"
#include "glog/logging.h"
#include "storage/fe_segment.h"
#include "storage/fe_table.h"
#include "vm/catalog.h"

namespace fesql {
namespace storage {
using fesql::codec::Row;
using fesql::codec::WindowIterator;
class WindowTableIterator;
class FullTableIterator;
class WindowInternalIterator;
class EmptyWindowIterator;

class EmptyWindowIterator : public ConstIterator<uint64_t, Row> {
 public:
    EmptyWindowIterator() : value_(), key_(0) {}

    ~EmptyWindowIterator() {}

    inline void Seek(const uint64_t& ts) {}

    inline void SeekToFirst() {}

    inline bool Valid() const { return false; }

    inline void Next() {}

    inline const Row& GetValue() { return value_; }

    inline const uint64_t& GetKey() const { return key_; }

    bool IsSeekable() const override { return true; }

 private:
    Row value_;
    uint64_t key_;
};

class WindowInternalIterator : public ConstIterator<uint64_t, Row> {
 public:
    explicit WindowInternalIterator(
        std::unique_ptr<base::Iterator<uint64_t, DataBlock*>> ts_it);
    ~WindowInternalIterator();

    inline void Seek(const uint64_t& ts);

    inline void SeekToFirst();

    bool Valid() const;

    void Next();

    const Row& GetValue();

    const uint64_t& GetKey() const;
    bool IsSeekable() const override;

 private:
    std::unique_ptr<base::Iterator<uint64_t, DataBlock*>> ts_it_;
    Row value_;
};

class WindowTableIterator : public WindowIterator {
 public:
    WindowTableIterator(Segment*** segments, uint32_t seg_cnt, uint32_t index,
                        std::shared_ptr<Table> table);
    ~WindowTableIterator();

    void Seek(const std::string& key);
    void SeekToFirst();
    void Next();
    bool Valid();
    std::unique_ptr<vm::RowIterator> GetValue();
    vm::RowIterator* GetRawValue();
    const Row GetKey();

 private:
    void GoToStart();
    void GoToNext();

 private:
    Segment*** segments_;
    uint32_t seg_cnt_;
    uint32_t index_;
    uint32_t seg_idx_;
    std::unique_ptr<base::Iterator<base::Slice, void*>> pk_it_;
    // hold the reference
    std::shared_ptr<Table> table_;
};

// the full table iterator
class FullTableIterator : public ConstIterator<uint64_t, Row> {
 public:
    FullTableIterator()
        : seg_cnt_(0), seg_idx_(0), segments_(NULL), value_(), key_(0) {}

    explicit FullTableIterator(Segment*** segments, uint32_t seg_cnt,
                               std::shared_ptr<Table> table);

    ~FullTableIterator() {}

    inline void Seek(const uint64_t& ts) {}

    inline void SeekToFirst() {}

    bool Valid() const;

    void Next();

    const Row& GetValue();
    bool IsSeekable() const override;
    // the key maybe the row num
    const uint64_t& GetKey() const { return key_; }

 private:
    void GoToStart();
    void GoToNext();

 private:
    uint32_t seg_cnt_;
    uint32_t seg_idx_;
    Segment*** segments_;
    std::unique_ptr<base::Iterator<uint64_t, DataBlock*>> ts_it_;
    std::unique_ptr<base::Iterator<base::Slice, void*>> pk_it_;
    std::shared_ptr<Table> table_;
    Row value_;
    uint64_t key_;
};

}  // namespace storage
}  // namespace fesql

#endif  // SRC_STORAGE_FE_TABLE_ITERATOR_H_
