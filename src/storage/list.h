
// list.h
// Copyright (C) 2017 4paradigm.com
// Author denglong
// Date 2019-10-24
//

#pragma once
#include <cstddef>
#include <type_traits>
#include <assert.h>
#include <string.h>
#include <stdio.h>
#include <atomic>
#include "iterator.h"

namespace fesql {
namespace storage {

const uint16_t MAX_ARRAY_LIST_LEN = 400;

enum class ListType {
    kArrayList = 1,
    kLinkList = 2,
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
class LinkListNode {
public:
    LinkListNode(const K& key, V& value): key_(key), value_(value), next_(NULL) {}
    LinkListNode(): key_(), value_(), next_(NULL) {}
    ~LinkListNode() = default;

    // Set the next node with memory barrier
    void SetNext(LinkListNode<K,V>* node) {
        next_.store(node, std::memory_order_release);
    }

    // Set the next node without memory barrier
    void SetNextNoBarrier(LinkListNode<K,V>* node) {
        next_.store(node, std::memory_order_relaxed);
    }

    LinkListNode<K,V>* GetNext() {
        return next_.load(std::memory_order_acquire);
    }

    LinkListNode<K,V>* GetNextNoBarrier() {
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
    std::atomic<LinkListNode<K,V>*> next_;
};

template<class K, class V, class Comparator>
class LinkList : public BaseList<K, V> {
public:
    LinkList(Comparator cmp) : compare_(cmp) {
        head_ = new LinkListNode<K, V>();
    }
    ~LinkList() {
        delete head_;
    }

    void Insert(const K& key, V& value) override {
        LinkListNode<K, V>* node = new LinkListNode<K, V>(key, value);
        LinkListNode<K, V>* pre = FindLessOrEqual(key);
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
        LinkListNode<K,V>* node = head_->GetNext(0);
        head_->SetNext(NULL);
        while (node != NULL) {
            LinkListNode<K,V>* tmp = node;
            node = node->GetNext(0);
            delete tmp;
        }
    }

    uint32_t GetSize() override {
        uint32_t cnt = 0;
        LinkListNode<K, V>* node = head_->GetNext();
        while (node != NULL) {
            cnt++;
            LinkListNode<K, V>* tmp = node->GetNext();
            // find the end
            if (tmp == NULL) {
                break;
            }
            node = tmp;
        }
        return cnt;
    }

    LinkListNode<K, V>* FindLessThan(const K& key) {
        LinkListNode<K, V>* node = head_;
        while (true) {
            LinkListNode<K, V>* next = node->GetNext();
            if (next == NULL || compare_(next->GetKey() , key) > 0) {
                return node;
            } else {
                node = next;
            }
        }
    }

    LinkListNode<K, V>* FindLessOrEqual(const K& key) {
        LinkListNode<K, V>* node = head_;
        while (node != NULL) {
            LinkListNode<K, V>* next = node->GetNext();
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
         LinkListIterator(const LinkListIterator&) = delete;
         LinkListIterator& operator= (const LinkListIterator&) = delete;
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
    private:
         LinkListNode<K, V>* node_;
         LinkList<K, V, Comparator>* const list_;
    };

    virtual Iterator<K, V>* NewIterator() override {
        return new LinkListIterator(this);
    }

private:
    bool IsAfterNode(const K& key, const LinkListNode<K, V>* node) const {
        return compare_(key, node->GetKey()) > 0;
    }

private:
    Comparator const compare_;
    LinkListNode<K, V>* head_;
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

template<class K, class V, class Comparator, 
        class = typename std::enable_if<std::is_pod<K>::value && std::is_pod<V>::value>::type>
        //typename std::enable_if<std::is_pod<K>::value && std::is_pod<V>::value, int>::type = 0>
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

    LinkList<K, V, Comparator>* ConvertToLinkList() {
        uint32_t length = GetSize();
        if (length == 0) {
            return NULL;
        }
        LinkList<K, V, Comparator>* list = new LinkList<K, V, Comparator>(compare_);
        ArrayListNode<K, V>* array = array_.load(std::memory_order_relaxed);
        for (uint32_t idx = 0; idx < length; idx++) {
            uint32_t cur_idx = length - idx - 1;
            list.Insert(array[cur_idx].key_, array[cur_idx].value_);
        }
        return list;
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
        ArrayListIterator(const ArrayListIterator&) = delete;
        ArrayListIterator& operator= (const ArrayListIterator&) = delete;
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
        if (list->GetType() == ListType::kArrayList && list->GetSize() >= MAX_ARRAY_LIST_LEN) {
            ArrayList<K, V, Comparator>* array_list = dynamic_cast<ArrayList<K, V, Comparator>>(list);
            LinkList<K, V, Comparator>* new_list = array_list->ConvertToLinkList();
            if (new_list != NULL) {
                list_.store(new_list, std::memory_order_release);
                delete array_list;
            }
        }
        list->Insert(key, value);
    }

private:
    Comparator const compare_;
    std::atomic<BaseList<K, V>*> list_;
}; 

}
}
