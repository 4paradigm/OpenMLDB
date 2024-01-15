/*
 * row_iterator.h
 * Copyright (C) 4paradigm 2021 chenjing <chenjing@4paradigm.com>
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
#ifndef HYBRIDSE_INCLUDE_CODEC_ROW_ITERATOR_H_
#define HYBRIDSE_INCLUDE_CODEC_ROW_ITERATOR_H_

#include <memory>
#include <string>

#include "base/iterator.h"
#include "codec/row.h"

namespace hybridse {
namespace codec {

using hybridse::base::ConstIterator;

/// \typedef const iterator of key-value pairs.
/// key type is `uint64_t` and value type is Row
typedef ConstIterator<uint64_t, Row> RowIterator;

/// \brief A iterator over a Row-Iterator<codec::Row> pairs dataset.
///
/// \b Example
///
/// Assuming the dataset is logically organized by segments,
/// we can use Valid, Next and GetValue methods to iterate these
/// "segments" one by one. Then when it comes to segment, it's easily to
/// access the rows with RowIterator 's interfaces.
/// ```
/// // assume we have got and initialized an window iterator already
/// // here is an example of counting segments and rows in the dataset
/// size_t segment_num = 0, row_num = 0;
/// while (window_iterator->Valid()) {
///     segment_num += 1;
///     auto &row_iterator = window_iterator->GetValue();
///     auto &key = window_iterator->GetKey();
///     while (row_iterator->Valid()) {
///         row_num += 1;
///         row_iterator->Next();
///     }
///}
/// ```
class WindowIterator {
 public:
    WindowIterator() {}
    virtual ~WindowIterator() {}

    /// Set the dataset's current position at the
    /// segment with key equals to `key`
    virtual void Seek(const std::string &key) = 0;
    /// Move to the beginning of the dataset.
    virtual void SeekToFirst() = 0;
    /// Move to the next segment in the iteration
    /// if Valid() return `true`.
    virtual void Next() = 0;
    /// Return `true` if the iteration has
    /// elements.
    virtual bool Valid() = 0;
    /// Return the RowIterator of current segment
    /// of dataset if Valid() return `true`.
    virtual std::unique_ptr<RowIterator> GetValue() {
        auto p = GetRawValue();
        if (!p) {
            return nullptr;
        }

        return std::unique_ptr<RowIterator>(p);
    }
    /// Return the RowIterator of current segment
    /// of dataset if Valid() return `true`.
    virtual RowIterator *GetRawValue() = 0;
    /// Return the key of current segment of
    /// dataset if Valid() is `true`
    virtual const Row GetKey() = 0;
};
}  // namespace codec
}  // namespace hybridse

#endif  // HYBRIDSE_INCLUDE_CODEC_ROW_ITERATOR_H_
