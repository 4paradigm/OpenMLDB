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

#ifndef HYBRIDSE_SRC_UDF_CONTAINERS_H_
#define HYBRIDSE_SRC_UDF_CONTAINERS_H_

#include <algorithm>
#include <functional>
#include <map>
#include <string>
#include <vector>

#include "base/type.h"
#include "codec/type_codec.h"
#include "udf/literal_traits.h"
#include "udf/udf.h"

namespace hybridse {
namespace udf {
namespace container {

/**
 * Specify actual stored type, store itself for most of the primitive types.
 */
template <typename T>
struct ContainerStorageTypeTrait {
    using type = T;
    static T to_stored_value(const T& t) { return t; }
};

template <>
struct ContainerStorageTypeTrait<openmldb::base::StringRef> {
    using type = codec::StringRef;
    static codec::StringRef to_stored_value(codec::StringRef* t) {
        return t == nullptr ? codec::StringRef() : *t;
    }
};

template <>
struct ContainerStorageTypeTrait<openmldb::base::Date> {
    using type = openmldb::base::Date;
    static openmldb::base::Date to_stored_value(openmldb::base::Date* t) {
        return t == nullptr ? openmldb::base::Date(0) : *t;
    }
};

template <>
struct ContainerStorageTypeTrait<openmldb::base::Timestamp> {
    using type = openmldb::base::Timestamp;
    static openmldb::base::Timestamp to_stored_value(openmldb::base::Timestamp* t) {
        return t == nullptr ? openmldb::base::Timestamp(0) : *t;
    }
};

template <typename T, typename BoundT>
class TopKContainer {
 public:
    // actual input argument type
    using InputT = typename DataTypeTrait<T>::CCallArgType;

    // actual stored type
    using StorageT = typename ContainerStorageTypeTrait<T>::type;

    // self type
    using ContainerT = TopKContainer<T, BoundT>;

    static void Init(ContainerT* addr) { new (addr) ContainerT(); }

    static void Output(ContainerT* ptr, codec::StringRef* output) {
        OutputString(ptr, output);
        Destroy(ptr);
    }

    static void Destroy(ContainerT* ptr) { ptr->~ContainerT(); }

    static ContainerT* Push(ContainerT* ptr, InputT t, bool is_null,
                            BoundT bound) {
        if (ptr->bound_ <= 0) {
            ptr->bound_ = bound;
        }
        if (!is_null) {
            ptr->Push(t);
        }
        return ptr;
    }

    static void OutputString(ContainerT* ptr, codec::StringRef* output) {
        auto& map = ptr->map_;
        if (map.empty()) {
            output->size_ = 0;
            output->data_ = "";
            return;
        }

        // estimate output length
        uint32_t str_len = 0;
        for (auto iter = map.rbegin(); iter != map.rend(); ++iter) {
            uint32_t key_len = v1::to_string_len(iter->first);
            str_len += (key_len + 1) * iter->second;  // "x,x,x,"
        }
        // allocate string buffer
        char* buffer = udf::v1::AllocManagedStringBuf(str_len);
        if (buffer == nullptr) {
            output->size_ = 0;
            output->data_ = "";
            return;
        }
        // fill string buffer
        char* cur = buffer;
        uint32_t remain_space = str_len;
        for (auto iter = map.rbegin(); iter != map.rend(); ++iter) {
            for (size_t k = 0; k < iter->second; ++k) {
                uint32_t key_len =
                    v1::format_string(iter->first, cur, remain_space);
                cur += key_len;
                remain_space -= key_len;
                if (remain_space-- > 0) {
                    *(cur++) = ',';
                }
            }
        }
        *(buffer + str_len - 1) = '\0';
        output->data_ = buffer;
        output->size_ = str_len - 1;
    }

    void Push(InputT t) {
        auto key = ContainerStorageTypeTrait<T>::to_stored_value(t);
        auto iter = map_.find(key);
        if (iter == map_.end()) {
            map_.insert(iter, {key, 1});
        } else {
            iter->second += 1;
        }
        elem_cnt_ += 1;
        if (elem_cnt_ > bound_) {
            auto iter_min = map_.begin();
            iter_min->second -= 1;
            if (iter_min->second == 0) {
                map_.erase(iter_min);
            }
            elem_cnt_ -= 1;
        }
    }

 private:
    std::map<StorageT, size_t, std::less<StorageT>> map_;
    BoundT elem_cnt_ = 0;
    BoundT bound_ = -1;  // delayed to be set by first push
};

template <typename K, typename V,
          typename StorageV = typename ContainerStorageTypeTrait<V>::type>
class BoundedGroupByDict {
 public:
    // actual input type
    using InputK = typename DataTypeTrait<K>::CCallArgType;
    using InputV = typename DataTypeTrait<V>::CCallArgType;

    // actual stored type
    using StorageK = typename ContainerStorageTypeTrait<K>::type;

    // self type
    using ContainerT = BoundedGroupByDict<K, V, StorageV>;

    using FormatValueF =
        std::function<uint32_t(const StorageV&, char*, size_t)>;

    // convert to internal key and value
    static inline StorageK to_stored_key(const InputK& key) {
        return ContainerStorageTypeTrait<K>::to_stored_value(key);
    }
    static inline auto to_stored_value(const InputV& value) {
        return ContainerStorageTypeTrait<V>::to_stored_value(value);
    }

    static void Init(ContainerT* addr) { new (addr) ContainerT(); }

    static void Output(ContainerT* ptr, codec::StringRef* output) {
        OutputString(ptr, output);
        Destroy(ptr);
    }

    static void Destroy(ContainerT* ptr) {
        ptr->map().clear();
        ptr->~ContainerT();
    }

    static void OutputString(ContainerT* ptr, bool is_desc,
                             codec::StringRef* output) {
        OutputString(ptr, is_desc, output,
                     [](const StorageV& value, char* buf, size_t size) {
                         return v1::format_string(value, buf, size);
                     });
    }

    static void OutputString(ContainerT* ptr, bool is_desc,
                             codec::StringRef* output,
                             const FormatValueF& format_value) {
        auto& map = ptr->map_;
        if (map.empty()) {
            output->size_ = 0;
            output->data_ = "";
            return;
        }

        // estimate output length
        uint32_t str_len = 0;
        auto stop_pos = map.end();
        auto stop_rpos = map.rend();
        if (is_desc) {
            for (auto iter = map.rbegin(); iter != map.rend(); ++iter) {
                uint32_t key_len = v1::to_string_len(iter->first);
                uint32_t value_len = format_value(iter->second, nullptr, 0);
                uint32_t new_len = str_len + key_len + value_len + 2;  // "k:v,"
                if (new_len > MAX_OUTPUT_STR_SIZE) {
                    stop_rpos = iter;
                    break;
                } else {
                    str_len = new_len;
                }
            }
        } else {
            for (auto iter = map.begin(); iter != map.end(); ++iter) {
                uint32_t key_len = v1::to_string_len(iter->first);
                uint32_t value_len = format_value(iter->second, nullptr, 0);
                uint32_t new_len = str_len + key_len + value_len + 2;  // "k:v,"
                if (new_len > MAX_OUTPUT_STR_SIZE) {
                    stop_pos = iter;
                    break;
                } else {
                    str_len = new_len;
                }
            }
        }

        // allocate string buffer
        char* buffer = udf::v1::AllocManagedStringBuf(str_len);
        if (buffer == nullptr) {
            output->size_ = 0;
            output->data_ = "";
            return;
        }

        // fill string buffer
        char* cur = buffer;
        uint32_t remain_space = str_len;
        if (is_desc) {
            for (auto iter = map.rbegin(); iter != map.rend(); ++iter) {
                if (iter == stop_rpos) {
                    break;
                }
                uint32_t key_len =
                    v1::format_string(iter->first, cur, remain_space);
                cur += key_len;
                *(cur++) = ':';
                remain_space -= key_len + 1;

                uint32_t value_len =
                    format_value(iter->second, cur, remain_space);
                cur += value_len;
                remain_space -= value_len;
                if (remain_space-- > 0) {
                    *(cur++) = ',';
                }
            }
        } else {
            for (auto iter = map.begin(); iter != map.end(); ++iter) {
                if (iter == stop_pos) {
                    break;
                }
                uint32_t key_len =
                    v1::format_string(iter->first, cur, remain_space);
                cur += key_len;
                *(cur++) = ':';
                remain_space -= key_len + 1;

                uint32_t value_len =
                    format_value(iter->second, cur, remain_space);
                cur += value_len;
                remain_space -= value_len;
                if (remain_space-- > 0) {
                    *(cur++) = ',';
                }
            }
        }

        *(buffer + str_len - 1) = '\0';
        output->data_ = buffer;
        output->size_ =
            str_len - 1;  // must leave one '\0' for string format impl
    }

    std::map<StorageK, StorageV>& map() { return map_; }

 private:
    std::map<StorageK, StorageV> map_;

    static const size_t MAX_OUTPUT_STR_SIZE = 4096;
};

}  // namespace container
}  // namespace udf
}  // namespace hybridse

#endif  // HYBRIDSE_SRC_UDF_CONTAINERS_H_
