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

#ifndef INCLUDE_BASE_ITERATOR_H_
#define INCLUDE_BASE_ITERATOR_H_
#include <stdint.h>

namespace hybridse {
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

/// \brief An iterator over a key-value pairs dataset
/// \tparam K key type of elements
/// \tparam V value type of elements
/// \tparam Ref decorate the value returned by GetValue method, e.g, it can be
/// const V& or V&
///
/// Example:
///
/// We use the Valid and  Next() functions to manually iterate through
/// all the items of an iterator. When we reach the end and Valid() will
/// return `false`.
///
/// ```
/// // assume we have got and initialized an row iterator already
/// while (iterator->Valid()) {
///     auto &value = iterator->GetValue();
///     auto &key = iterator->GetKey();
///     iterator->Next();
/// }
/// ```
template <class K, class V, class Ref>
class AbstractIterator {
 public:
    AbstractIterator() {}
    AbstractIterator(const AbstractIterator&) = delete;
    AbstractIterator& operator=(const AbstractIterator&) = delete;
    virtual ~AbstractIterator() {}
    /// Return whether the iteration has elements
    /// or not.
    virtual bool Valid() const = 0;
    /// Implemented by subclasses to move to the next element in the iteration
    /// when Valid() return `true`.
    virtual void Next() = 0;
    /// Return the key of current element pair.
    virtual const K& GetKey() const = 0;
    /// Return the value of current element pari
    /// when Valid() return `true`.
    virtual Ref GetValue() = 0;
    /// Return whether the dataset is seekable or
    /// not. A dataset is seekable if it allows access to data with Seek()
    /// method
    virtual bool IsSeekable() const = 0;

    /// Set the dataset's current position move to
    /// the first element whose key equals to `k` offset.
    virtual void Seek(const K& k) = 0;

    /// Move to the beginning of the dataset.
    virtual void SeekToFirst() = 0;
};
/// \brief An iterator over a key-value pairs dataset
/// \tparam K key type of elements
/// \tparam V value type of elements
template <class K, class V>
class Iterator : public AbstractIterator<K, V, V&> {};

/// \brief An const iterator over a key-value pairs dataset
/// \tparam K key type of elements
/// \tparam V value type of elements
template <class K, class V>
class ConstIterator : public hybridse::base::AbstractIterator<K, V, const V&> {
};
}  // namespace base
}  // namespace hybridse
#endif  // INCLUDE_BASE_ITERATOR_H_
