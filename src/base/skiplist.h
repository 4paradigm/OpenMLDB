//
// skiplist.h 
// Copyright 2017 4paradigm.com


#ifndef RTIDB_BASE_SKIPLIST_H
#define RTIDB_BASE_SKIPLIST_H

#include <stdint.h>
#include <boost/atomic.hpp>
#include "base/random.h"
#include <iostream>

namespace rtidb {
namespace base {

// Skiplist node , a thread safe structure 
template<class K, class V>
class Node {

public:
    // Set data reference and Node height
    Node(const K& key, V& value, uint32_t height):key_(key), 
    value_(value),
    height_(height){
        nexts_ = new boost::atomic< Node<K,V>* >[height];
    }

    Node(uint32_t height):key_(), value_(),height_(height) {
        nexts_ = new boost::atomic< Node<K,V>* >[height];
    }
   
    // Set the next node with memory barrier
    void SetNext(uint32_t level, Node<K,V>* node) {
        assert(level < height_ && level >= 0);
        nexts_[level].store(node, boost::memory_order_release);
    }

    // Set the next node without memory barrier
    void SetNextNoBarrier(uint32_t level, Node<K,V>* node) {
        assert(level < height_ && level >= 0);
        nexts_[level].store(node, boost::memory_order_relaxed);
    }

    uint32_t Height() {
        return height_;
    }

    Node<K,V>* GetNext(uint32_t level) {
        assert(level < height_ && level >= 0);
        return nexts_[level].load(boost::memory_order_acquire);
    }

    Node<K,V>* GetNextNoBarrier(uint32_t level) {
        assert(level < height_ && level >= 0);
        return nexts_[level].load(boost::memory_order_relaxed);
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
    K  const key_;
    V  value_;
    boost::atomic< Node<K,V>* >* nexts_;
    uint32_t const height_;
};


template<class K, class V, class Comparator>
class Skiplist {

public:
    Skiplist(uint32_t max_height, uint32_t branch, const Comparator& compare):MaxHeight(max_height),
    Branch(branch),
    head_(NULL),
    compare_(compare),
    rand_(0xdeadbeef){
        head_ = new Node<K,V>(MaxHeight);
        for (uint32_t i = 0; i < head_->Height(); i++) {
            head_->SetNext(i, NULL);
        }
        max_height_.store(1, boost::memory_order_relaxed);
    }
    ~Skiplist() {
        delete head_;
    }

    // Insert need external synchronized
    void Insert(const K& key, V& value) {
        uint32_t height = RandomHeight();
        Node<K,V>* pre[MaxHeight];
        FindLessOrEqual(key, pre);
        if (height > GetMaxHeight()) {
            for (uint32_t i = GetMaxHeight(); i < height; i++ ) {
                pre[i] = head_;
            }
            max_height_.store(height, boost::memory_order_relaxed);
        }
        Node<K,V>* node = NewNode(key, value, height);
        for (uint32_t i = 0; i < height; i ++) {
            node->SetNextNoBarrier(i, pre[i]->GetNextNoBarrier(i));
            pre[i]->SetNext(i, node);
        }
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
        for (uint32_t i = 0; i < MaxHeight; i++) {
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
        for (uint32_t i = 0; i < result->Height(); i++) {
            pre[i]->SetNextNoBarrier(i, result->GetNextNoBarrier(i));
            result->SetNextNoBarrier(i, NULL);
        }
        return result;
    }

    // Split list two parts, the return part is just a linkedlist
    Node<K,V>* Split(const K& key) {
        Node<K, V>* pre[MaxHeight];
        for (uint32_t i = 0; i < MaxHeight; i++) {
            pre[i] = NULL;
        }
        Node<K, V>* target = FindLessOrEqual(key, pre);
        if (target == NULL) {
            return NULL;
        }
        Node<K, V>* result = target->GetNextNoBarrier(0);
        for (uint32_t i = 0; i < MaxHeight; i++) {
            if (pre[i] == NULL) {
                continue;
            }
            pre[i]->SetNext(i, NULL);
        }
        return result;
    }

    const V& Get(const K& key) {
        Node<K,V>* node = FindEqual(key);
        return node->GetValue();
    }

    Node<K, V>* GetLast() {
        Node<K, V>* node = head_->GetNext(0);
        while (node != NULL) {
            Node<K, V>* tmp = node->GetNext(0);
            // find the end
            if (tmp == NULL) {
                return node;
            }
            node = tmp;
        }
        return NULL;
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
        for (uint32_t i = 0; i < head_->Height(); i++) {
            head_->SetNextNoBarrier(i, NULL);
        }

        while (node != NULL) {
            cnt++;
            Node<K,V>* tmp = node;
            node = node->GetNext(0);
            // Unlink all next node
            for (uint32_t i = 0; i < tmp->Height(); i++) {
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
        uint32_t height = RandomHeight();
        Node<K,V>* pre[MaxHeight];
        for (uint32_t i = 0; i < height; i++ ) {
            pre[i] = head_;
        }
        if (height > GetMaxHeight()) { 
            max_height_.store(height, boost::memory_order_relaxed);
        }
        Node<K,V>* node = NewNode(key, value, height);
        for (uint32_t i = 0; i < height; i ++) {
            node->SetNextNoBarrier(i, pre[i]->GetNextNoBarrier(i));
            pre[i]->SetNext(i, node);
        }
        return true;
    }


    class Iterator {
    public:
        Iterator(Skiplist<K, V, Comparator>* list):node_(NULL),
        list_(list) {}
        ~Iterator() {}

        bool Valid() const {
            return node_ != NULL;
        }

        void Next() {
            assert(Valid());
            node_ = node_->GetNext(0);
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

    private:
        Node<K, V>* node_;
        Skiplist<K, V, Comparator>* const list_;
    };

    // delete the iterator after it's used
    Iterator* NewIterator() {
        return new Iterator(this);
    }

private:

    Node<K,V>* NewNode(const K& key, V& value, uint32_t height) {
        Node<K,V>* node = new Node<K,V>(key, value, height); 
        return node;
    }

    uint32_t RandomHeight() {
        uint32_t height = 1;
        while (height < MaxHeight && (rand_.Next() % Branch) == 0) {
            height ++;
        }
        return height;
    }

    Node<K, V>* FindLessOrEqual(const K& key, Node<K, V>** nodes) {
        assert(nodes != NULL);
        Node<K, V>* node = head_;
        uint32_t level = GetMaxHeight() - 1;
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
        uint32_t level = GetMaxHeight() - 1;
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
        uint32_t level = GetMaxHeight() - 1;
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

    uint32_t GetMaxHeight() const {
        return max_height_.load(boost::memory_order_relaxed);
    }
    

private:
    uint32_t const MaxHeight;
    uint32_t const Branch;
    Node<K, V>* head_;
    Comparator const compare_;
    boost::atomic<uint32_t> max_height_; 
    Random rand_;
    friend Iterator;
};

}// base
}// rtidb

#endif /* !SKIPLIST_H */
