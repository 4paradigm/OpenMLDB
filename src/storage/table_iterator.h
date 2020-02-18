/*
 * table_iterator.h
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

#ifndef SRC_STORAGE_TABLE_ITERATOR_H_
#define SRC_STORAGE_TABLE_ITERATOR_H_

#include "vm/catalog.h"
#include "base/iterator.h"
#include "storage/segment.h"

namespace fesql {
namespace storage {

class WindowTableIterator;
class FullTableIterator;
class WindowInternalIterator;
class EmptyWindowIterator;

class EmptyWindowIterator : public vm::Iterator {

 public:
    EmptyWindowIterator(): value_() {}

    ~EmptyWindowIterator() {}

    inline void Seek(uint64_t ts) {}

    inline void SeekToFirst() {}

    inline bool Valid() { return false}

    inline void Next() {}

    inline const base::Slice GetValue() { 
        return value_;
    }

    inline const uint64_t GetKey() {
        return 0;
    }

 private:
    base::Slice value_;
};

class WindowInternalIterator : public vm::Iterator {
 public:
     explicit WindowInternalIterator(std::unique_ptr<base::Iterator<uint64_t, DataBlock*>> ts_it);
     ~WindowInternalIterator();

    inline void Seek(uint64_t ts);

    inline void SeekToFirst();

    bool Valid();

    void Next();

    const base::Slice GetValue();

    const uint64_t GetKey();
 private:
    std::unique_ptr<base::Iterator<uint64_t, DataBlock*>> ts_it_;
};

class WindowTableIterator : public vm::WindowIterator {

 public:
    WindowTableIterator(Segment*** segments, uint32_t seg_cnt, uint32_t index);
    ~WindowTableIterator();

    void Seek(const std::string& key);
    void SeekToFirst();
    void Next();
    bool Valid();
    std::unique_ptr<vm:Iterator> GetValue();
    const base::Slice GetKey();
 private:
    void GoToStart();
    void GoToNext();

 private:
    Segment*** segments_;
    uint32_t seg_cnt_;
    uint32_t index_;
    uint32_t seg_idx_;
    std::unique_ptr<base::Iterator<base::Slice, void*>> pk_it_;
    std::unique_ptr<vm::Iterator> w_it_;
    base::Slice key_;
};

// the full table iterator
class FullTableIterator : public vm::Iterator {
 public:

    FullTableIterator():seg_cnt_(0), seg_idx_(0),segments_(NULL){}

    explicit FullTableIterator(Segment*** segments, uint32_t seg_cnt);

    ~FullTableIterator(){}

    inline void Seek(uint64_t ts){}

    inline void SeekToFirst(){}

    bool Valid();

    void Next();

    const base::Slice GetValue();

    // the key maybe the row num
    const uint64_t GetKey() {}
 private:
    void GoToStart();
    void GoToNext();
 private:
    uint32_t seg_cnt_;
    uint32_t seg_idx_;
    // TODO 
    Segment*** segments_;
    std::unique_ptr<base::Iterator<uint64_t, DataBlock*>> ts_it_;
    std::unique_ptr<base::Iterator<base::Slice, void*>> pk_it_;
};

}  // namespace storage
}  // namespace fesql

#endif  // SRC_STORAGE_TABLE_ITERATOR_H_
