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

#include "storage/node_cache.h"

namespace openmldb {
namespace storage {

NodeCache::NodeCache(uint32_t ts_cnt) : ts_cnt_(ts_cnt), mutex_(), key_entry_node_list_(4, 4, tcmp),
    key_entry_list_(4, 4, tcmp), value_node_list_(4, 4, tcmp) {}

NodeCache::~NodeCache() {
}

void NodeCache::AddKeyEntryNode(uint64_t version, base::Node<base::Slice, void*>* node) {
    std::lock_guard<std::mutex> lock(mutex_);
    key_entry_node_list_.Insert(version, node);
}

void NodeCache::AddKeyEntry(uint64_t version, KeyEntry* key_entry) {
    std::lock_guard<std::mutex> lock(mutex_);
    std::forward_list<KeyEntry*>* list;
    if (auto ret = key_entry_list_.Get(version, list); ret < 0 || list == nullptr) {
        list = new std::forward_list<KeyEntry*>();
        key_entry_list_.Insert(version, list);
    }
    list->push_front(key_entry);
}

void NodeCache::AddValueNode(uint64_t version, base::Node<uint64_t, DataBlock*>* node) {
    std::lock_guard<std::mutex> lock(mutex_);
    std::forward_list<base::Node<uint64_t, DataBlock*>*>* list;
    if (auto ret = value_node_list_.Get(version, list); ret < 0 || list == nullptr) {
        list = new std::forward_list<base::Node<uint64_t, DataBlock*>*>();
        value_node_list_.Insert(version, list);
    }
    list->push_front(node);
}

void NodeCache::Free(uint64_t version, StatisticsInfo* gc_info) {
    base::Node<uint64_t, base::Node<base::Slice, void*>*>* node1 = nullptr;
    base::Node<uint64_t, std::forward_list<KeyEntry*>*>* node2 = nullptr;
    base::Node<uint64_t, std::forward_list<base::Node<uint64_t, DataBlock*>*>*>* node3 = nullptr;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        node1 = key_entry_node_list_.Split(version);
        node2 = key_entry_list_.Split(version);
        node3 = value_node_list_.Split(version);
    }
    while (node1) {
        auto entry_node = node1->GetValue();
        //FreeEntry(entry_node, gc_idx_cnt, gc_record_cnt, gc_record_byte_size);
        delete entry_node;
        auto tmp = node1;
        node1 = node1->GetNextNoBarrier(0);
        delete tmp;
    }
}


}  // namespace storage
}  // namespace openmldb
