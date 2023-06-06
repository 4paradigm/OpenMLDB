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
#include "base/glog_wrapper.h"

namespace openmldb {
namespace storage {

NodeCache::NodeCache(uint32_t ts_cnt) : ts_cnt_(ts_cnt), mutex_(), key_entry_node_list_(4, 4, tcmp),
    value_node_list_(4, 4, tcmp), value_nodes_list_(4, 4, tcmp) {}

NodeCache::~NodeCache() {
    Clear();
}

void NodeCache::Clear() {
    std::unique_ptr<KeyEntryNodeList::Iterator> it(key_entry_node_list_.NewIterator());
    it->SeekToFirst();
    while (it->Valid()) {
        auto entry_node_list = it->GetValue();
        for (auto& entry_node : *entry_node_list) {
            FreeKeyEntryNode(entry_node);
        }
        delete entry_node_list;
        it->Next();
    }
    key_entry_node_list_.Clear();
    std::unique_ptr<ValueNodeList::Iterator> node_it(value_node_list_.NewIterator());
    node_it->SeekToFirst();
    while (node_it->Valid()) {
        auto node_list = node_it->GetValue();
        for (auto& node : *node_list) {
            FreeNode(node);
        }
        delete node_list;
        node_it->Next();
    }
    value_node_list_.Clear();
    std::unique_ptr<ValueNodeList::Iterator> nodes_it(value_nodes_list_.NewIterator());
    nodes_it->SeekToFirst();
    while (nodes_it->Valid()) {
        auto node_list = nodes_it->GetValue();
        for (auto& node : *node_list) {
            FreeNodeList(node);
        }
        delete node_list;
        nodes_it->Next();
    }
    value_nodes_list_.Clear();
}


void NodeCache::AddKeyEntryNode(uint64_t version, base::Node<base::Slice, void*>* node) {
    AddNode(version, node, &key_entry_node_list_);
}

void NodeCache::AddSingleValueNode(uint64_t version, base::Node<uint64_t, DataBlock*>* node) {
    AddNode(version, node, &value_node_list_);
}

void NodeCache::AddValueNodeList(uint64_t version, base::Node<uint64_t, DataBlock*>* node) {
    AddNode(version, node, &value_nodes_list_);
}

void NodeCache::Free(uint64_t version/*, StatisticsInfo* gc_info*/) {
    base::Node<uint64_t, std::forward_list<base::Node<base::Slice, void*>*>*>* node1 = nullptr;
    base::Node<uint64_t, std::forward_list<base::Node<uint64_t, DataBlock*>*>*>* node2 = nullptr;
    base::Node<uint64_t, std::forward_list<base::Node<uint64_t, DataBlock*>*>*>* node3 = nullptr;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        node1 = key_entry_node_list_.Split(version);
        node2 = value_node_list_.Split(version);
        node3 = value_nodes_list_.Split(version);
    }
    while (node1) {
        auto entry_node_list = node1->GetValue();
        for (auto& entry_node : *entry_node_list) {
            FreeKeyEntryNode(entry_node);
        }
        delete entry_node_list;
        auto tmp = node1;
        node1 = node1->GetNextNoBarrier(0);
        delete tmp;
    }
    while (node2) {
        auto node_list = node2->GetValue();
        for (auto& node : *node_list) {
            FreeNode(node);
        }
        delete node_list;
        auto tmp = node2;
        node2 = node2->GetNextNoBarrier(0);
        delete tmp;
    }
    while (node3) {
        auto node_list = node3->GetValue();
        for (auto& node : *node_list) {
            FreeNodeList(node);
        }
        delete node_list;
        auto tmp = node3;
        node3 = node3->GetNextNoBarrier(0);
        delete tmp;
    }
}

void NodeCache::FreeNode(base::Node<uint64_t, DataBlock*>* node) {
    if (node == nullptr) {
        return;
    }
    // idx_byte_size_.fetch_sub(GetRecordTsIdxSize(node->Height()));
    DEBUGLOG("delete key %lu with height %u", node->GetKey(), node->Height());
    if (node->GetValue()->dim_cnt_down > 1) {
        node->GetValue()->dim_cnt_down--;
    } else {
        DEBUGLOG("delele data block for key %lu", node->GetKey());
        // gc_record_byte_size += GetRecordSize(node->GetValue()->size);
        delete node->GetValue();
        // gc_record_cnt++;
    }
    delete node;
}

void NodeCache::FreeNodeList(base::Node<uint64_t, DataBlock*>* node) {
    while (node) {
        auto tmp = node;
        node = node->GetNextNoBarrier(0);
        FreeNode(tmp);
    }
}

void NodeCache::FreeKeyEntry(KeyEntry* entry) {
    if (entry == nullptr) {
        return;
    }
    std::unique_ptr<TimeEntries::Iterator> it(entry->entries.NewIterator());
    it->SeekToFirst();
    if (it->Valid()) {
        uint64_t ts = it->GetKey();
        base::Node<uint64_t, DataBlock*>* data_node = entry->entries.Split(ts);
        FreeNodeList(data_node);
    }
    delete entry;
}

void NodeCache::FreeKeyEntryNode(base::Node<base::Slice, void*>* entry_node) {
    if (entry_node == nullptr) {
        return;
    }
    delete[] entry_node->GetKey().data();
    if (ts_cnt_ > 1) {
        auto entry_arr = reinterpret_cast<KeyEntry**>(entry_node->GetValue());
        for (uint32_t i = 0; i < ts_cnt_; i++) {
            // uint64_t old = gc_idx_cnt;
            KeyEntry* entry = entry_arr[i];
            FreeKeyEntry(entry);
            // idx_cnt_vec_[i]->fetch_sub(gc_idx_cnt - old, std::memory_order_relaxed);
        }
        delete[] entry_arr;
        // uint64_t byte_size =
        //    GetRecordPkMultiIdxSize(entry_node->Height(), entry_node->GetKey().size(), key_entry_max_height_, ts_cnt_);
        // idx_byte_size_.fetch_sub(byte_size, std::memory_order_relaxed);
    } else {
        // uint64_t old = gc_idx_cnt;
        KeyEntry* entry = reinterpret_cast<KeyEntry*>(entry_node->GetValue());
        FreeKeyEntry(entry);
        // uint64_t byte_size =
        //    GetRecordPkIdxSize(entry_node->Height(), entry_node->GetKey().size(), key_entry_max_height_);
        // idx_byte_size_.fetch_sub(byte_size, std::memory_order_relaxed);
        // idx_cnt_.fetch_sub(gc_idx_cnt - old, std::memory_order_relaxed);
    }
    delete entry_node;
}

}  // namespace storage
}  // namespace openmldb
