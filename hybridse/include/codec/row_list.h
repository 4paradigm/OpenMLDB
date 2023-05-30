/*
 * row_window_iterator.h
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
#ifndef HYBRIDSE_INCLUDE_CODEC_ROW_LIST_H_
#define HYBRIDSE_INCLUDE_CODEC_ROW_LIST_H_

#include <memory>
#include <optional>
#include <type_traits>

#include "codec/row_iterator.h"

namespace hybridse {
namespace codec {

template <typename V>
struct AtOut {
    using T = std::conditional_t<std::is_same_v<V, codec::Row>, V, std::optional<V>>;

    static T Null() {
        if constexpr (std::is_same_v<V, codec::Row>) {
            return codec::Row();
        } else {
            return std::nullopt;
        }
    }

    static bool IsNull(T data) {
        if constexpr (std::is_same_v<V, codec::Row>) {
            return data.empty();
        } else {
            return !data.has_value();
        }
    }

    static V Value(T data) {
        if constexpr (std::is_same_v<V, codec::Row>) {
            return data;
        } else {
            return data.value_or(V{});
        }
    }
};
/// \brief Basic key-value list of HybridSe.
/// \tparam V the type of elements in this list
///
/// The user can access a element by its position in the list.
/// Also, can just use the iterator returned by GetIterator() to traverse the
/// list.
template <class V>
class ListV {
 public:
    ListV() {}
    virtual ~ListV() {}
    /// \brief Return the const iterator
    virtual std::unique_ptr<ConstIterator<uint64_t, V>> GetIterator() = 0;

    /// \brief Return the const iterator raw pointer
    virtual ConstIterator<uint64_t, V> *GetRawIterator() = 0;

    /// \brief Returns the number of elements in this list.
    ///
    /// It count element by traverse the list
    virtual const uint64_t GetCount() {
        auto iter = GetIterator();
        uint64_t cnt = 0;
        while (iter->Valid()) {
            iter->Next();
            cnt++;
        }
        return cnt;
    }

    /// \brief Return a the value of element by its position in the list
    /// \param pos is element position in the list
    virtual typename AtOut<V>::T At(uint64_t pos) {
        auto iter = GetIterator();
        if (!iter) {
            return AtOut<V>::Null();
        }
        while (pos-- > 0 && iter->Valid()) {
            iter->Next();
        }
        return iter->Valid() ? iter->GetValue() : AtOut<V>::Null();
    }
};
}  // namespace codec
}  // namespace hybridse
#endif  // HYBRIDSE_INCLUDE_CODEC_ROW_LIST_H_
