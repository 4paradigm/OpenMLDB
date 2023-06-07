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
    explicit NodeCache(uint32_t ts_cnt, uint32_t height);
    ~NodeCache();
    void AddKeyEntryNode(uint64_t version, base::Node<base::Slice, void*>* node);
    void AddKeyEntry(uint64_t version, KeyEntry* key_entry);
    void AddSingleValueNode(uint64_t version, base::Node<uint64_t, DataBlock*>* node);
    void AddValueNodeList(uint64_t version, base::Node<uint64_t, DataBlock*>* node);

    void Free(uint64_t version, StatisticsInfo* gc_info);
    void Clear();

    using KeyEntryNodeList =
      base::Skiplist<uint64_t, std::forward_list<base::Node<base::Slice, void*>*>*, TimeComparator>;
    using ValueNodeList =
      base::Skiplist<uint64_t, std::forward_list<base::Node<uint64_t, DataBlock*>*>*, TimeComparator>;

 private:
    template <typename T>
    void AddNode(uint64_t version, T node, base::Skiplist<uint64_t, std::forward_list<T>*, TimeComparator>* list) {
         std::forward_list<T>* value_list = nullptr;
         std::lock_guard<std::mutex> lock(mutex_);
         if (auto ret = list->Get(version, value_list); ret < 0 || value_list == nullptr) {
            value_list = new std::forward_list<T>();
            list->Insert(version, value_list);
         }
         value_list->push_front(node);
    }

    void FreeKeyEntryNode(base::Node<base::Slice, void*>* entry_node, StatisticsInfo* gc_info);
    void FreeNodeList(base::Node<uint64_t, DataBlock*>* node, StatisticsInfo* gc_info);
    void FreeKeyEntry(KeyEntry* entry, StatisticsInfo* gc_info);
    void FreeNode(base::Node<uint64_t, DataBlock*>* node, StatisticsInfo* gc_info);

 private:
    uint32_t ts_cnt_;
    uint32_t key_entry_max_height_;
    std::mutex mutex_;
    KeyEntryNodeList key_entry_node_list_;
    ValueNodeList value_node_list_;
    ValueNodeList value_nodes_list_;  // the value in froward_list is a list of nodes
};

}  // namespace storage
}  // namespace openmldb

#endif  // SRC_STORAGE_NODE_CACHE_H_
