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
#include <assert.h>
#include <stdint.h>

#include <memory>
#include <atomic>
#include <iostream>

#include "base/base_iterator.h"

namespace openmldb {
namespace base {

constexpr uint16_t MAX_LIST_LEN = 1000;

template <class K, class V>
class ListNode {
 public:
    ListNode(const K& key, V& value)  // NOLINT
        : key_(key), value_(value), next_(NULL) {}
    ListNode() : key_(), value_(), next_(NULL) {}
    ~ListNode() = default;

    // Set the next node with memory barrier
    void SetNext(ListNode<K, V>* node) {
        next_.store(node, std::memory_order_release);
    }

    // Set the next node without memory barrier
    void SetNextNoBarrier(ListNode<K, V>* node) {
        next_.store(node, std::memory_order_relaxed);
    }

    ListNode<K, V>* GetNext() {
        return next_.load(std::memory_order_acquire);
    }

    ListNode<K, V>* GetNextNoBarrier() {
        return next_.load(std::memory_order_relaxed);
    }

    V& GetValue() { return value_; }

    const K& GetKey() const { return key_; }

 public:
    K const key_;
    V value_;
    std::atomic<ListNode<K, V>*> next_;
};

template <class K, class V, class Comparator>
class ConcurrentList {
 public:
    explicit ConcurrentList(Comparator cmp) : compare_(cmp) {
        head_ = new ListNode<K, V>();
    }
    ~ConcurrentList() { delete head_; }

    virtual void Insert(const K& key, V value) {  // NOLINT
        ListNode<K, V>* node = new ListNode<K, V>(key, value);
        ListNode<K, V>* pre;
        ListNode<K, V>* temp;
        do {
            pre = FindLessOrEqual(key);
            node->SetNext(pre->GetNext());
            temp = node->GetNext();
        }while (!pre->next_.compare_exchange_weak(temp, node));
    }

    ListNode<K, V>* FindLessThan(const K& key) {
        ListNode<K, V>* node = head_;
        while (true) {
            ListNode<K, V>* next = node->GetNext();
            if (next == NULL || compare_(next->GetKey(), key) > 0) {
                return node;
            } else {
                node = next;
            }
        }
    }

    ListNode<K, V>* FindLessOrEqual(const K& key) {
        ListNode<K, V>* node = head_;
        while (node != NULL) {
            ListNode<K, V>* next = node->GetNext();
            if (next == NULL) {
                return node;
            } else if (IsAfterNode(key, next)) {
                node = next;
            } else {
                return node;
            }
        }
        return NULL;
    }

    ListNode<K, V>* Split(const K& key) {
        ListNode<K, V>* target = FindLessOrEqual(key);
        if (target == NULL) {
            return NULL;
        }
        ListNode<K, V>* result = target->GetNext();
        target->SetNext(NULL);
        return result;
    }

    ListNode<K, V>* SplitByPos(uint64_t pos) {
        ListNode<K, V>* pos_node = head_;
        for (uint64_t idx = 0; idx < pos; idx++) {
            if (pos_node == NULL) {
                break;
            }
            pos_node = pos_node->GetNext();
        }
        if (pos_node == NULL) {
            return NULL;
        }
        ListNode<K, V>* result = pos_node->GetNext();
        pos_node->SetNext(NULL);
        return result;
    }

    ListNode<K, V>* SplitByKeyAndPos(const K& key, uint64_t pos) {
        ListNode<K, V>* pos_node = head_->GetNext();
        bool find_key = false;
        for (uint64_t idx = 0; idx < pos; idx++) {
            if (pos_node == NULL) {  // does not find pos, just return
                return NULL;
            }
            if (compare_(pos_node->GetKey(), key) >= 0) {  // find key before pos, mark it
                find_key = true;
            }
            pos_node = pos_node->GetNext();
        }
        if (pos_node == NULL) {
            return NULL;
        }
        if (find_key) {  // find key before pos, split by pos
            return SplitOnPosNode(pos, pos_node);
        } else {  // find pos without key, split by key
            return Split(key);
        }
    }

    ListNode<K, V>* SplitByKeyOrPos(const K& key, uint64_t pos) {
        ListNode<K, V>* pos_node = head_->GetNext();
        for (uint64_t idx = 0; idx < pos; idx++) {
            if (pos_node == NULL) {
                return NULL;
            }
            if (compare_(pos_node->GetKey(), key) >= 0) {  // find key before pos, use key
                return Split(pos_node->GetKey());
            }
            pos_node = pos_node->GetNext();
        }
        if (pos_node == NULL) {
            return NULL;
        }
        return SplitOnPosNode(pos, pos_node);
    }

    ListNode<K, V>* SplitOnPosNode(uint64_t pos, ListNode<K, V>* pos_node) {
        ListNode<K, V>* node = head_;
        ListNode<K, V>* pre = head_;
        pos++;
        uint64_t cnt = 0;
        while (node != NULL) {
            if (cnt == pos) {
                pre->SetNext(NULL);
                return node;
            }
            ListNode<K, V>* next = node->GetNext();
            if (next != NULL && compare_(pos_node->GetKey(), next->GetKey()) <= 0) {
                node->SetNext(NULL);
            }
            pre = node;
            node = node->GetNext();
            cnt++;
        }
        return NULL;
    }

    ListNode<K, V>* GetLast() {
        ListNode<K, V>* tmp = head_;
        while (tmp != NULL && tmp->GetNext() != NULL) {
            tmp = tmp->GetNext();
        }
        return tmp;
    }

    virtual bool IsEmpty() {
        if (head_->GetNextNoBarrier() == NULL) {
            return true;
        }
        return false;
    }
    class ListIterator : public BaseIterator<K, V> {
     public:
        explicit ListIterator(ConcurrentList<K, V, Comparator>*  list) : node_(NULL), list_(list) {}
        ListIterator(const ListIterator&) = default;
        ListIterator& operator=(const ListIterator&) = delete;
        ~ListIterator() {}

        ListIterator& operator=(ListIterator&& __iter) noexcept {
            node_ = __iter.node_;
            __iter.node_ = nullptr;
            return *this;
        }

        ListIterator()
            : node_(nullptr), list_(nullptr)
        {}

        explicit ListIterator(std::nullptr_t)
            : node_(nullptr), list_(nullptr) {}

        explicit ListIterator(ListNode<K, V>* __node_ptr)
            : node_(__node_ptr), list_(nullptr) {}

        ListIterator operator++(int) {
            ListIterator _copy(*this);
            node_ = node_->GetNext();
            return _copy;
        }

        ListIterator& operator++() {
            node_ = node_->GetNext();
            return *this;
        }

        ListIterator& operator+(int __n) {
            int node_count = 0;
            while (node_count < __n) {
                node_ = node_->GetNext();
                ++node_count;
            }
            return *this;
        }

        bool operator==(const ListIterator& __iter) {
            return node_ == __iter.node_;
        }

        bool operator!=(const ListIterator& __iter) {
            return node_ != __iter.node_;
        }
        bool Valid() const { return node_ != NULL; }

        void Next() {
            assert(Valid());
            node_ = node_->GetNext();
        }

        const K& GetKey() const {
            assert(Valid());
            return node_->GetKey();
        }

        V& GetValue() {
            assert(Valid());
            return node_->GetValue();
        }

        void Seek(const K& k) {
            node_ = list_->FindLessThan(k);
            Next();
        }

        void SeekToFirst() {
            node_ = list_->head_;
            Next();
        }

        void SeekToLast() {
            node_ = list_->GetLast();
        }

        uint32_t GetSize() { return list_->GetSize(); }

     private:
        ListNode<K, V>* node_;
        ConcurrentList<K, V, Comparator>* const list_;
    };

    ListIterator begin() {
        return ListIterator(head_);
    }

    ListIterator end() {
        return ListIterator(nullptr);
    }

    void Clear() {
        ListNode<K, V>* node = head_->GetNext();
        head_->SetNext(NULL);
        while (node != NULL) {
            ListNode<K, V>* tmp = node;
            node = node->GetNext();
            delete tmp;
        }
    }

    uint32_t GetSize() {
        uint32_t cnt = 0;
        ListNode<K, V>* node = head_;
        while (node != NULL) {
            cnt++;
            ListNode<K, V>* tmp = node->GetNext();
            // find the end
            if (tmp == NULL) {
                break;
            }
            node = tmp;
        }
        return cnt;
    }

    void GC() {
        if (GetSize() > MAX_LIST_LEN) {
            uint32_t cnt = 0;
            ListNode<K, V> *node = head_;
            while (node != NULL) {
                if (cnt >= MAX_LIST_LEN) {
                    ListNode<K, V> *delete_node = node->GetNext();
                    node->SetNext(nullptr);
                    while (delete_node != NULL) {
                        ListNode<K, V> *tmp1 = delete_node;
                        delete_node = delete_node->GetNext();
                        delete tmp1;
                    }
                }
                cnt++;
                ListNode<K, V> *tmp = node->GetNext();
                node = tmp;
            }
        }
    }

    ListIterator* NewIterator() { return new ListIterator(this); }

 private:
    bool IsAfterNode(const K& key, const ListNode<K, V>* node) const {
        return compare_(key, node->GetKey()) > 0;
    }

 private:
    Comparator const compare_;
    ListNode<K, V> *head_;
};

}  // namespace base
}  // namespace openmldb
