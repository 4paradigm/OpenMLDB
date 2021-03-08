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
#include <stdint.h>

namespace fesql {
namespace base {

struct DefaultComparator {
    int operator()(const uint64_t a, const uint64_t b) const {
        if (a > b) {
            return -1;
        } else if (a == b) {
            return 0;
        }
        return 1;
    }
};

template <class K, class V, class Ref>
class AbstractIterator {
 public:
    AbstractIterator() {}
    AbstractIterator(const AbstractIterator&) = delete;
    AbstractIterator& operator=(const AbstractIterator&) = delete;
    virtual ~AbstractIterator() {}
    virtual bool Valid() const = 0;
    virtual void Next() = 0;
    virtual const K& GetKey() const = 0;
    virtual Ref GetValue() = 0;
    virtual void Seek(const K& k) = 0;
    virtual void SeekToFirst() = 0;
    virtual bool IsSeekable() const = 0;
};

template <class K, class V>
class Iterator : public AbstractIterator<K, V, V&> {};

template <class K, class V>
class ConstIterator : public fesql::base::AbstractIterator<K, V, const V&> {};
}  // namespace base
}  // namespace fesql
