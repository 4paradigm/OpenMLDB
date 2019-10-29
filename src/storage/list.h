
// list.h
// Copyright (C) 2017 4paradigm.com
// Author denglong
// Date 2019-10-24
//

#pragma once
#include <cstddef>
#include <assert.h>
#include <string.h>
#include <stdio.h>
#include <atomic>

namespace fesql {
namespace storage {

const uint16_t MAX_ARRAY_LIST_LEN = 500;    

struct DefaultComparator {
    int operator() (const uint64_t a, const uint64_t b) const {
        if (a > b) {
            return -1;
        } else if (a == b) {
            return 0;
        }
        return 1;
    }
};

enum class ListType {
    kArrayList = 1,
    kLinkList = 2,
};

template<class K, class V>
class Iterator {
public:    
     Iterator() {}
     virtual ~Iterator() {}
     Iterator(const Iterator&) = delete;
     Iterator& operator= (const Iterator&) = delete;
     virtual bool Valid() const = 0;
     virtual void Next() = 0;
     virtual const K& GetKey() const = 0;
     virtual V& GetValue() = 0;
     virtual void Seek(const K& k) = 0;
     virtual void SeekToFirst() = 0;
};

template<class K, class V>
class BaseList {
public:
    BaseList() {}
    virtual ~BaseList() {}
    virtual void Insert(const K& key, V& value) = 0;
    virtual uint32_t GetSize() = 0;
    virtual bool IsEmpty() = 0;
    virtual ListType GetType() const = 0;
    virtual Iterator<K, V>* NewIterator() = 0;
};

template<class K, class V>
class Node {
public:
    Node(const K& key, V& value): key_(key), value_(value), next_(NULL) {}
    Node(): key_(), value_(), next_(NULL) {}
    ~Node() {}

    // Set the next node with memory barrier
    void SetNext(Node<K,V>* node) {
        next_.store(node, std::memory_order_release);
    }

    // Set the next node without memory barrier
    void SetNextNoBarrier(Node<K,V>* node) {
        next_.store(node, std::memory_order_relaxed);
    }

    Node<K,V>* GetNext() {
        return next_.load(std::memory_order_acquire);
    }

    Node<K,V>* GetNextNoBarrier() {
        return next_.load(std::memory_order_relaxed);
    }

    V& GetValue() {
        return value_;
    }

    const K& GetKey() const {
        return key_;
    }

private:
    K  const key_;
    V  value_;
    std::atomic<Node<K,V>*> next_;
};

template<class K, class V, class Comparator>
class LinkList : public BaseList<K, V> {
public:
    LinkList(Comparator cmp) : compare_(cmp) {
        head_ = new Node<K, V>();
    }
    ~LinkList() {
        delete head_;
    }

    void Insert(const K& key, V& value) override {
        Node<K, V>* node = new Node<K, V>(key, value);
        Node<K, V>* pre = FindLessOrEqual(key);
        node->SetNextNoBarrier(pre->GetNextNoBarrier());
        pre->SetNext(node);
    }

    ListType GetType() const override { return ListType::kLinkList; }

    bool IsEmpty() override {
        if (head_->GetNextNoBarrier() == NULL) {
            return true;
        }
        return false;
    }

    void Clear() {
        Node<K,V>* node = head_->GetNext(0);
        head_->SetNext(NULL);
        while (node != NULL) {
            Node<K,V>* tmp = node;
            node = node->GetNext(0);
            delete tmp;
        }
    }

    uint32_t GetSize() override {
        uint32_t cnt = 0;
        Node<K, V>* node = head_->GetNext();
        while (node != NULL) {
            cnt++;
            Node<K, V>* tmp = node->GetNext();
            // find the end
            if (tmp == NULL) {
                break;
            }
            node = tmp;
        }
        return cnt;
    }

    Node<K, V>* FindLessThan(const K& key) {
        Node<K, V>* node = head_;
        while (true) {
            Node<K, V>* next = node->GetNext();
            if (next == NULL || compare_(next->GetKey() , key) > 0) {
                return node;
            } else {
                node = next;
            }
        }
    }

    Node<K, V>* FindLessOrEqual(const K& key) {
        Node<K, V>* node = head_;
        while (node != NULL) {
            Node<K, V>* next = node->GetNext();
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

    class LinkListIterator : public Iterator<K, V> {
    public:
         LinkListIterator(LinkList<K, V, Comparator>* list) : node_(NULL) , list_(list) {}
         virtual ~LinkListIterator() {}
         virtual bool Valid() const override {
             return node_ != NULL;
         }
         virtual void Next() override {
             assert(Valid());
             node_ = node_->GetNext();
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
         LinkListIterator(const LinkListIterator&) = delete;
         LinkListIterator& operator= (const LinkListIterator&) = delete;
    private:
         Node<K, V>* node_;
         LinkList<K, V, Comparator>* const list_;
    };

    virtual Iterator<K, V>* NewIterator() override {
        return new LinkListIterator(this);
    }

private:
    bool IsAfterNode(const K& key, const Node<K, V>* node) const {
        return compare_(key, node->GetKey()) > 0;
    }

private:
    Comparator const compare_;
    Node<K, V>* head_;
};

template<class K, class V>
struct ArrayListNode {
    K  key_;
    V  value_;
};

template<class K, class V>
struct __attribute__ ((__packed__)) ArraySt {
    uint16_t length_;
    ArrayListNode<K, V> buf_[];
};

const uint16_t  ARRAY_HDR_LEN = sizeof(uint16_t);
#define ARRAY_HDR(s) ((struct ArraySt<K, V> *)((char*)(s)-(sizeof(uint16_t))))

template<class K, class V, class Comparator>
class ArrayList : public BaseList<K, V> {
public:
    ArrayList(Comparator cmp) : compare_(cmp), array_(NULL) {}
    ~ArrayList() {
        if (array_.load(std::memory_order_relaxed) != NULL) {
            ArraySt<K, V>* st = ARRAY_HDR(array_.load(std::memory_order_relaxed));
            delete[] (char*)st;
        }
    }

    void Insert(const K& key, V& value) override {
        uint32_t length = GetSize();
        uint32_t new_length = length + 1;
        ArraySt<K, V>* st = (ArraySt<K, V>*)new char[ARRAY_HDR_LEN + new_length * sizeof(ArrayListNode<K, V>)];
        st->length_ = (uint16_t)new_length;
        ArrayListNode<K, V>* new_array = st->buf_;
        uint32_t pos = FindLessOrEqual(key);
        new_array[pos].key_ = key;
        new_array[pos].value_ = value;
        ArrayListNode<K, V>* array = array_.load(std::memory_order_relaxed);
        if (array != NULL) {
            if (length > 0) {
                if (pos > 0) {
                    memcpy((void*)new_array, (void*)array, pos * sizeof(ArrayListNode<K, V>));
                }
                if (pos < length) {
                    memcpy((void*)(new_array + pos + 1), (void*)(array + pos), (length - pos) * sizeof(ArrayListNode<K, V>));
                }
            }
        }
        array_.store(new_array, std::memory_order_release);
        if (array != NULL) {
            ArraySt<K, V>* st = ARRAY_HDR(array);
            delete[] (char*)st;
        }
    }

    ListType GetType() const override { return ListType::kArrayList; }
    
    uint32_t GetSize() override {
        ArrayListNode<K,V>* array = array_.load(std::memory_order_relaxed);
        if (array == NULL) {
            return 0;
        }
        ArraySt<K, V>* ast = ARRAY_HDR(array);
        return ast->length_;
    }

    bool IsEmpty() override {
        ArrayListNode<K,V>* array = array_.load(std::memory_order_relaxed);
        if (array == NULL) {
            return true;
        }
        return false;
    }

    // TODO : use binary search
    uint32_t FindLessOrEqual(const K& key) {
        ArrayListNode<K, V>* array = array_.load(std::memory_order_relaxed);
        uint32_t length = GetSize();
        for (uint32_t idx = 0; idx < length; idx++) {
            if (compare_(array[idx].key_, key) >= 0) {
                return idx;
            }
        }
        return length;
    }

    uint32_t FindLessThan(const K& key) {
        ArrayListNode<K, V>* array = array_.load(std::memory_order_relaxed);
        uint32_t length = GetSize();
        for (uint32_t idx = 0; idx < length; idx++) {
            if (compare_(array[idx].key_, key) > 0) {
                return idx;
            }
        }
        return length;
    }

    class ArrayListIterator : public Iterator<K, V> {
    public:
        ArrayListIterator(ArrayList<K, V, Comparator>* list) : pos_(-1) {
            if (list != NULL) {
                array_ = list->array_;
                length_ = list->GetSize();
            } else {
                array_ = NULL;
                length_ = 0;
            }
            list_ = list;
        }
        virtual ~ArrayListIterator() {}
        virtual bool Valid() const override {
            return array_ != NULL && pos_ >= 0 && pos_ < (int32_t)length_;
        }
        virtual void Next() override {
            assert(Valid());
            pos_++;
        }
        virtual const K& GetKey() const override {
            assert(Valid());
            return array_[pos_].key_;
        }
        virtual V& GetValue() override {
            assert(Valid());
            return array_[pos_].value_;
        }
        virtual void Seek(const K& key) override {
            if (array_ != NULL) {
                pos_ = list_->FindLessThan(key);
            }
        }
        virtual void SeekToFirst() override {
            pos_ = 0;
        }
        ArrayListIterator(const ArrayListIterator&) = delete;
        ArrayListIterator& operator= (const ArrayListIterator&) = delete;
    private:
        int32_t pos_;
        uint32_t length_;
        ArrayListNode<K, V>* array_;
        ArrayList<K, V, Comparator>* list_;
    };

    virtual Iterator<K, V>* NewIterator() override {
        return new ArrayListIterator(this);
    }

private:
    Comparator const compare_;
    std::atomic<ArrayListNode<K, V>*> array_;
};

template<class K, class V, class Comparator>
class List {
public:
    List(Comparator cmp) : compare_(cmp) { list_ = new ArrayList<K, V, Comparator>(cmp); }
    ~List() { delete list_.load(std::memory_order_relaxed); }
    void Insert(const K& key, V& value) {
        BaseList<K, V>* list = list_.load(std::memory_order_acquire);
        /*if (list->GetType() == ListType::kArrayList && list->GetSize() >= MAX_ARRAY_LIST_LEN) {
            list = ConvertList();
        }*/
        list->Insert(key, value);
    }

    /*BaseList<K, V>* ConvertList() {
        BaseList<K, V>* list = new LinkList(list_load(std::memory_order_acquire));
        list_.store(list, std::memory_order_relaxed);
        return list;
    }*/

private:
    Comparator const compare_;
    std::atomic<BaseList<K, V>*> list_;
}; 

}
}
