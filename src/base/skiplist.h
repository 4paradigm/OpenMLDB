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
template<class T>
class Node {

public:
    // Set data reference and Node height
    Node(const T& data, uint32_t height):data_(data), 
    height_(height){
        nexts_ = new boost::atomic< Node<T>* >[height];
    }
   
    // Set the next node with memory barrier
    void SetNext(uint32_t level, Node<T>* node) {
        assert(level < height_ && level >= 0);
        nexts_[level].store(node, boost::memory_order_release);
    }

    // Set the next node without memory barrier
    void SetNextNoBarrier(uint32_t level, Node<T>* node) {
        assert(level < height_ && level >= 0);
        nexts_[level].store(node, boost::memory_order_relaxed);
    }

    uint32_t Height() {
        return height_;
    }

    Node<T>* GetNext(uint32_t level) {
        assert(level < height_ && level >= 0);
        return nexts_[level].load(boost::memory_order_acquire);
    }

    Node<T>* GetNextNoBarrier(uint32_t level) {
        assert(level < height_ && level >= 0);
        return nexts_[level].load(boost::memory_order_relaxed);
    }

    const T& GetData() const{
        return data_;
    }

    ~Node() {
        //TODO(wangtaize) free memory 
        delete[] nexts_;
    }
private:
    T const data_;
    boost::atomic< Node<T>* >* nexts_;
    uint32_t const height_;
};

template<class T, class Comparator>
class Skiplist {

public:
    Skiplist(uint32_t max_height, uint32_t branch, const Comparator& compare):MaxHeight(max_height),
    Branch(branch),
    head_(NULL),
    compare_(compare),
    rand_(0xdeadbeef){
        head_ = new Node<T>(0, MaxHeight);
        for (uint32_t i = 0; i < head_->Height(); i++) {
            head_->SetNext(i, NULL);
        }
        max_height_.store(1, boost::memory_order_relaxed);
    }
    ~Skiplist() {}

    // Insert need external synchronized
    void Insert(const T& data) {
        uint32_t height = RandomHeight();
        Node<T>* pre[MaxHeight];
        FindGreaterOrEqual(data, pre);
        if (height > GetMaxHeight()) {
            for (uint32_t i = GetMaxHeight(); i < height; i++ ) {
                pre[i] = head_;
            }
            max_height_.store(height, boost::memory_order_relaxed);
        }
        Node<T>* node = NewNode(data, height);
        for (uint32_t i = 0; i < height; i ++) {
            node->SetNextNoBarrier(i, pre[i]->GetNextNoBarrier(i));
            pre[i]->SetNext(i, node);
        }
    }

    // Split list two parts, the return part is just a linkedlist
    Node<T>* Split(const T& data) {
        Node<T>* pre[MaxHeight];
        for (uint32_t i = 0; i < MaxHeight; i++) {
            pre[i] = NULL;
        }
        Node<T>* target = FindGreaterOrEqual(data, pre);
        Node<T>* result = target->GetNextNoBarrier(0);
        for (uint32_t i = 0; i < MaxHeight; i++) {
            if (pre[i] == NULL) {
                continue;
            }
            pre[i]->SetNext(i, NULL);
        }
        return result;
    }

    class Iterator {
    public:
        Iterator(Skiplist<T, Comparator>* list):node_(NULL),
        list_(list) {}
        ~Iterator() {}

        bool Valid() const {
            return node_ != NULL;
        }

        void Next() {
            assert(Valid());
            node_ = node_->GetNext(0);
        }

        const T& GetData() const {
            assert(Valid());
            return node_->GetData();
        }

        void Seek(const T& data) {
            node_ = list_->FindLessThan(data);
            Next();
        }

        void SeekToFirst() {
            node_ = list_->head_;
            Next();
        }

    private:
        Node<T>* node_;
        Skiplist<T, Comparator>* const list_;
    };

    // delete the iterator after it's used
    Iterator* NewIterator() {
        return new Iterator(this);
    }

private:

    Node<T>* NewNode(const T& data, uint32_t height) {
        Node<T>* node = new Node<T>(data, height); 
        return node;
    }

    uint32_t RandomHeight() {
        uint32_t height = 1;
        while (height < MaxHeight && (rand_.Next() % Branch) == 0) {
            height ++;
        }
        return height;
    }

    Node<T>* FindGreaterOrEqual(const T& data, Node<T>** nodes) const {
        assert(nodes != NULL);
        Node<T>* node = head_;
        uint32_t level = GetMaxHeight() - 1;
        while (true) {
            Node<T>* next = node->GetNext(level);
            if (IsAfterNode(data, next)) {
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

    Node<T>* FindLessThan(const T& data) const {
        Node<T>* node = head_;
        uint32_t level = GetMaxHeight() - 1;
        while (true) {
            assert(node == head_ || compare_(node->GetData(), data) < 0);
            Node<T>* next = node->GetNext(level);
            if (next == NULL || compare_(next->GetData() , data) >=0) {
                if (level <= 0) {
                    return node;
                }
                level --;
            } else {
                node = next;
            }
        }
    }

    bool IsAfterNode(const T& data, const Node<T>* node) const {
        return (node != NULL) && (compare_(data, node->GetData()) > 0);
    } 

    uint32_t GetMaxHeight() const {
        return max_height_.load(boost::memory_order_relaxed);
    }

private:
    uint32_t const MaxHeight;
    uint32_t const Branch;
    Node<T>* head_;
    Comparator const compare_;
    boost::atomic<uint32_t> max_height_; 
    Random rand_;
    friend Iterator;
};

}// base
}// rtidb
#endif /* !SKIPLIST_H */
