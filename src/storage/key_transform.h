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
#ifndef SRC_STORAGE_KEY_TRANSFORM_H_
#define SRC_STORAGE_KEY_TRANSFORM_H_

#include <string>
#include "base/endianconv.h"
#include "rocksdb/slice.h"

namespace openmldb {
namespace storage {

static constexpr uint32_t TS_LEN = sizeof(uint64_t);
static constexpr uint32_t TS_POS_LEN = sizeof(uint32_t);

static inline int ParseKeyAndTs(bool has_ts_idx, const rocksdb::Slice& s,
        rocksdb::Slice* key, uint64_t* ts, uint32_t* ts_idx) {
    auto len = TS_LEN;
    if (has_ts_idx) {
        len += TS_POS_LEN;
    }
    if (s.size() < len) {
        return -1;
    } else if (s.size() > len) {
        *key = {s.data(), s.size() - len};
    }
    if (has_ts_idx) {
        memcpy(reinterpret_cast<void*>(ts_idx), s.data() + s.size() - len, TS_POS_LEN);
    }
    memcpy(reinterpret_cast<void*>(ts), s.data() + s.size() - TS_LEN, TS_LEN);
    memrev64ifbe(reinterpret_cast<void*>(ts));
    return 0;
}

static inline int ParseKeyAndTs(bool has_ts_idx, const rocksdb::Slice& s,
        std::string* key, uint64_t* ts, uint32_t* ts_idx) {
    rocksdb::Slice tmp_key;
    int ret = ParseKeyAndTs(has_ts_idx, s, &tmp_key, ts, ts_idx);
    key->assign(tmp_key.data(), tmp_key.size());
    return ret;
}

static inline int ParseKeyAndTs(const rocksdb::Slice& s, rocksdb::Slice* key, uint64_t* ts) {
    uint32_t ts_idx = 0;
    return ParseKeyAndTs(false, s, key, ts, &ts_idx);
}

static inline std::string CombineKeyTs(const rocksdb::Slice& key, uint64_t ts) {
    std::string result;
    result.resize(key.size() + TS_LEN);
    char* buf = reinterpret_cast<char*>(&(result[0]));
    memrev64ifbe(static_cast<void*>(&ts));
    memcpy(buf, key.data(), key.size());
    memcpy(buf + key.size(), static_cast<void*>(&ts), TS_LEN);
    return result;
}

static inline std::string CombineKeyTs(const rocksdb::Slice& key, uint64_t ts, uint32_t ts_pos) {
    std::string result;
    result.resize(key.size() + TS_LEN + TS_POS_LEN);
    char* buf = reinterpret_cast<char*>(&(result[0]));
    memrev64ifbe(static_cast<void*>(&ts));
    memcpy(buf, key.data(), key.size());
    memcpy(buf + key.size(), static_cast<void*>(&ts_pos), TS_POS_LEN);
    memcpy(buf + key.size() + TS_POS_LEN, static_cast<void*>(&ts), TS_LEN);
    return result;
}

}  // namespace storage
}  // namespace openmldb
#endif  // SRC_STORAGE_KEY_TRANSFORM_H_
