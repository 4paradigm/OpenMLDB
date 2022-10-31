/*
 * Copyright 2022 4Paradigm
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

namespace openmldb {
namespace base {

template <class K, class V>
class BaseIterator {
 public:
    BaseIterator() {}  // NOLINT
    virtual ~BaseIterator() {}

    virtual bool Valid() const = 0;

    virtual void Next() = 0;

    virtual const K& GetKey() const = 0;

    virtual V& GetValue() = 0;

    virtual void Seek(const K& k) = 0;

    virtual void SeekToFirst() = 0;

    virtual void SeekToLast() = 0;

    virtual uint32_t GetSize() = 0;
};

}  // namespace base
}  // namespace openmldb
