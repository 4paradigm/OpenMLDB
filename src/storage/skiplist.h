//
// skiplist.h 
// Copyright 2017 4paradigm.com

#pragma once

#include <stdint.h>
#include <atomic>
#include "base/random.h"
#include "base/iterator.h"
#include <iostream>
#include <assert.h>

namespace fesql {
namespace storage {

using ::fesql::base::Iterator;

// SkipList node , a thread safe structure 
template<class K, class V>
class Node {

public:
    // Set data reference and Node height
    Node(const K& key, V& value, uint8_t height):height_(height),
    key_(key), 
    value_(value) {
        nexts_ = new std::atomic< Node<K,V>* >[height];
    }

    Node(uint8_t height):height_(height), key_(), value_() {
        nexts_ = new std::atomic< Node<K,V>* >[height];
    }
   
    // Set the next node with memory barrier
    void SetNext(uint8_t level, Node<K,V>* node) {
        assert(level < height_ && level >= 0);
        nexts_[level].store(node, std::memory_order_release);
    }

    // Set the next node without memory barrier
    void SetNextNoBarrier(uint8_t level, Node<K,V>* node) {
        assert(level < height_ && level >= 0);
        nexts_[level].store(node, std::memory_order_relaxed);
    }

    uint8_t Height() {
        return height_;
    }

    Node<K,V>* GetNext(uint8_t level) {
        assert(level < height_ && level >= 0);
        return nexts_[level].load(std::memory_order_acquire);
    }

    Node<K,V>* GetNextNoBarrier(uint8_t level) {
        assert(level < height_ && level >= 0);
        return nexts_[level].load(std::memory_order_relaxed);
    }

    V& GetValue() {
        return value_;
    }

    const K& GetKey() const {
        return key_;
    }

    ~Node() {
        delete[] nexts_;
    }

private:
    uint8_t const height_;
    K  const key_;
    V  value_;
    std::atomic<Node<K,V>* >* nexts_;
};


template<class K, class V, class Comparator>
class SkipList {

public:
    SkipList(uint8_t max_height, uint8_t branch, const Comparator& compare):MaxHeight(max_height),
    Branch(branch),
    max_height_(0),
    compare_(compare),
    rand_(0xdeadbeef),
    head_(NULL),
    tail_(NULL) {
        head_ = new Node<K,V>(MaxHeight);
        for (uint8_t i = 0; i < head_->Height(); i++) {
            head_->SetNext(i, NULL);
        }
        max_height_.store(1, std::memory_order_relaxed);
    }
    ~SkipList() {
        delete head_;
    }

    // Insert need external synchronized
    uint8_t Insert(const K& key, V& value) {
        uint8_t height = RandomHeight();
        Node<K,V>* pre[MaxHeight];
        FindLessOrEqual(key, pre);
        if (height > GetMaxHeight()) {
            for (uint8_t i = GetMaxHeight(); i < height; i++ ) {
                pre[i] = head_;
            }
            max_height_.store(height, std::memory_order_relaxed);
        }
        Node<K,V>* node = NewNode(key, value, height);
        if (pre[0]->GetNext(0) == NULL) {
            tail_.store(node, std::memory_order_release);
        }
        for (uint8_t i = 0; i < height; i ++) {
            node->SetNextNoBarrier(i, pre[i]->GetNextNoBarrier(i));
            pre[i]->SetNext(i, node);
        }
        return height;
    }

    bool IsEmpty() {
        if (head_->GetNextNoBarrier(0) == NULL) {
            return true;
        }
        return false;
    }


    // Remove need external synchronized
    Node<K,V>* Remove(const K& key) {
        Node<K, V>* pre[MaxHeight];
        for (uint8_t i = 0; i < MaxHeight; i++) {
            pre[i] = head_;
        }
        Node<K, V>* target = FindLessOrEqual(key, pre);
        if (target == NULL) {
            return NULL;
        }
        Node<K, V>* result = target->GetNextNoBarrier(0);
        if (result == NULL || compare_(result->GetKey(), key) != 0) {
            return NULL;
        }
        for (uint8_t i = 0; i < result->Height(); i++) {
            pre[i]->SetNextNoBarrier(i, result->GetNextNoBarrier(i));
            result->SetNextNoBarrier(i, NULL);
        }
        if (result == tail_) {
            pre[0] == head_ ? tail_.store(NULL, std::memory_order_relaxed) : tail_.store(pre[0], std::memory_order_relaxed);
        }
        return result;
    }

    // Split list two parts, the return part is just a linkedlist
    Node<K,V>* Split(const K& key) {
        Node<K, V>* pre[MaxHeight];
        for (uint8_t i = 0; i < MaxHeight; i++) {
            pre[i] = NULL;
        }
        Node<K, V>* target = FindLessOrEqual(key, pre);
        if (target == NULL) {
            return NULL;
        }
        tail_.store(target, std::memory_order_release);
        Node<K, V>* result = target->GetNextNoBarrier(0);
        for (uint8_t i = 0; i < MaxHeight; i++) {
            if (pre[i] == NULL) {
                continue;
            }
            pre[i]->SetNext(i, NULL);
        }
        return result;
    }

    Node<K,V>* SplitByPos(uint64_t pos) {
        Node<K, V>* pos_node = head_->GetNext(0);
        for (uint64_t idx = 0; idx < pos; idx++) {
            if (pos_node == NULL) {
                return NULL;
            }
            pos_node = pos_node->GetNext(0);
        }
        if (pos_node == NULL) {
            return NULL;
        }
        Node<K, V>* node = head_;
        Node<K, V>* pre = head_;
        // read form head node, so let pos plus one
        pos++;
        uint64_t cnt = 0;
        while (node != NULL) {
            if (cnt == pos) {
                tail_.store(pre, std::memory_order_release);
                for (uint8_t i = 0; i < pre->Height(); i++) {
                    pre->SetNext(i, NULL);
                }
                return node;
            }
            for (uint8_t i = 1; i < node->Height(); i++) {
                Node<K, V>* next = node->GetNext(i);
                if (next != NULL && compare_(pos_node->GetKey(), next->GetKey()) <= 0) {
                    node->SetNext(i, NULL);
                }
            }
            pre = node;
            node = node->GetNext(0);
            cnt++;
        }
        return NULL;
    }
    
    const V& Get(const K& key) {
        Node<K,V>* node = FindEqual(key);
        return node->GetValue();
    }

    int Get(const K& key, V& v) {
        Node<K,V>* node = FindEqual(key);
        if (node != NULL && compare_(node->GetKey(), key) == 0) {
            v = node->GetValue();
            return 0;
        }
        return -1;
    }

    Node<K, V>* GetLast() {
        return tail_.load(std::memory_order_acquire);
    }

    uint32_t GetSize() {
        uint32_t cnt = 0;
        Node<K, V>* node = head_->GetNext(0);
        while (node != NULL) {
            cnt++;
            Node<K, V>* tmp = node->GetNext(0);
            // find the end
            if (tmp == NULL) {
                break;
            }
            node = tmp;
        }
        return cnt;
    }

    // Need external synchronized
    uint64_t Clear() {
        uint64_t cnt = 0;
        Node<K,V>* node = head_->GetNext(0);
        // Unlink all next node
        for (uint8_t i = 0; i < head_->Height(); i++) {
            head_->SetNextNoBarrier(i, NULL);
        }
        tail_.store(NULL, std::memory_order_relaxed);

        while (node != NULL) {
            cnt++;
            Node<K,V>* tmp = node;
            node = node->GetNext(0);
            // Unlink all next node
            for (uint8_t i = 0; i < tmp->Height(); i++) {
                tmp->SetNextNoBarrier(i, NULL);
            }
            delete tmp;
        }
        return cnt;
    }

    // Need external synchronized
    bool AddToFirst(const K& key, V& value) {
        {
            Node<K,V>* node = head_->GetNext(0);
            if (node != NULL && compare_(key, node->GetKey()) > 0) {
                return false;
            }
        }
        uint8_t height = RandomHeight();
        Node<K,V>* pre[MaxHeight];
        for (uint8_t i = 0; i < height; i++ ) {
            pre[i] = head_;
        }
        if (height > GetMaxHeight()) { 
            max_height_.store(height, std::memory_order_relaxed);
        }
        Node<K,V>* node = NewNode(key, value, height);
        if (pre[0]->GetNext(0) == NULL) {
            tail_.store(node, std::memory_order_release);
        }
        for (uint8_t i = 0; i < height; i ++) {
            node->SetNextNoBarrier(i, pre[i]->GetNextNoBarrier(i));
            pre[i]->SetNext(i, node);
        }
        return true;
    }


    class SkipListIterator : public Iterator<K, V> {
    public:
        SkipListIterator(SkipList<K, V, Comparator>* list):node_(NULL),
        list_(list) {}
        ~SkipListIterator() {}

        virtual bool Valid() const override {
            return node_ != NULL;
        }

        virtual void Next() override{
            assert(Valid());
            node_ = node_->GetNext(0);
        }

        virtual bool IsSeekable() const override {
            return true;
        }

        virtual const K& GetKey() const override {
            assert(Valid());
            return node_->GetKey();
        }

        virtual V& GetValue() override {
            assert(Valid());
            return node_->GetValue();
        }

        virtual void Seek(const K& k) override {
            node_ = list_->FindLessThan(k);
            Next();
        }

        virtual void SeekToFirst() override {
            node_ = list_->head_;
            Next();
        }

        void SeekToLast() {
            node_ = list_->GetLast();
        }

        uint32_t GetSize() {
            return list_->GetSize();    
        }
    private:
        Node<K, V>* node_;
        SkipList<K, V, Comparator>* const list_;
    };

    // delete the iterator after it's used
    Iterator<K, V>* NewIterator() {
        return new SkipListIterator(this);
    }

private:

    Node<K,V>* NewNode(const K& key, V& value, uint8_t height) {
        Node<K,V>* node = new Node<K,V>(key, value, height); 
        return node;
    }

    uint8_t RandomHeight() {
        uint8_t height = 1;
        while (height < MaxHeight && (rand_.Next() % Branch) == 0) {
            height ++;
        }
        return height;
    }

    Node<K, V>* FindLessOrEqual(const K& key, Node<K, V>** nodes) {
        assert(nodes != NULL);
        Node<K, V>* node = head_;
        uint8_t level = GetMaxHeight() - 1;
        while (true) {
            Node<K, V>* next = node->GetNext(level);
            if (IsAfterNode(key, next)) {
                node = next;
            }else {
                nodes[level] = node;
                if (level <= 0) {
                    return node;
                }
                level--;
            }
        }
    }

    Node<K, V>* FindEqual(const K& key) {
        Node<K, V>* node = head_; 
        uint8_t level = GetMaxHeight() - 1;
        while (true) {
            Node<K, V>* next = node->GetNext(level);
            if (next == NULL || compare_(next->GetKey(), key) > 0) {
                if (level <= 0) {
                    return node;
                }
                level --;
            }else {
                node = next;
            }
        }
    }

    Node<K, V>* FindLessThan(const K& key) {
        Node<K, V>* node = head_;
        uint8_t level = GetMaxHeight() - 1;
        while (true) {
            assert(node == head_ || compare_(node->GetKey(), key) < 0);
            Node<K, V>* next = node->GetNext(level);
            if (next == NULL || compare_(next->GetKey() , key) >=0) {
                if (level <= 0) {
                    return node;
                }
                level --;
            } else {
                node = next;
            }
        }
    }

    bool IsAfterNode(const K& key, const Node<K, V>* node) const {
        return (node != NULL) && (compare_(key, node->GetKey()) > 0);
    }

    uint8_t GetMaxHeight() const {
        return max_height_.load(std::memory_order_relaxed);
    }
    
private:
    uint8_t const MaxHeight;
    uint8_t const Branch;
    std::atomic<uint8_t> max_height_; 
    Comparator const compare_;
    ::fesql::base::Random rand_;
    Node<K, V>* head_;
    std::atomic<Node<K, V>*> tail_;
};

}
}
