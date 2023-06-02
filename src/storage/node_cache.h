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

#ifndef SRC_STORAGE_NODE_CACHE_H_
#define SRC_STORAGE_NODE_CACHE_H_

#include <forward_list>
#include <memory>
#include <mutex>  // NOLINT
#include "base/slice.h"
#include "base/skiplist.h"
#include "storage/key_entry.h"
#include "storage/record.h"

namespace openmldb {
namespace storage {

class NodeCache {
 public:
    explicit NodeCache(uint32_t ts_cnt);
    ~NodeCache();
    void AddKeyEntryNode(uint64_t version, base::Node<base::Slice, void*>* node);
    void AddKeyEntry(uint64_t version, KeyEntry* key_entry);
    void AddValueNode(uint64_t version, base::Node<uint64_t, DataBlock*>* node);
    void Free(uint64_t version, StatisticsInfo* gc_info);

 private:
    uint32_t ts_cnt_;
    std::mutex mutex_;
    base::Skiplist<uint64_t, base::Node<base::Slice, void*>*, TimeComparator> key_entry_node_list_;
    base::Skiplist<uint64_t, std::forward_list<KeyEntry*>*, TimeComparator> key_entry_list_;
    base::Skiplist<uint64_t, std::forward_list<base::Node<uint64_t, DataBlock*>*>*, TimeComparator> value_node_list_;
};


}  // namespace storage
}  // namespace openmldb

#endif  // SRC_STORAGE_NODE_CACHE_H_
