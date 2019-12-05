
// list.h
// Copyright (C) 2017 4paradigm.com
// Author denglong
// Date 2019-10-24
//

#pragma once
#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <atomic>
#include <cstddef>
#include <memory>
#include <type_traits>
#include "base/iterator.h"

namespace fesql {
namespace storage {

using ::fesql::base::Iterator;

constexpr uint16_t MAX_ARRAY_LIST_LEN = 400;

enum class ListType {
    kArrayList = 1,
    kLinkList = 2,
};

template <class K, class V>
class BaseList {
 public:
    BaseList() {}
    virtual ~BaseList() {}
    virtual void Insert(const K& key, V& value) = 0;  // NOLINT
    virtual uint32_t GetSize() = 0;
    virtual bool IsEmpty() = 0;
    virtual ListType GetType() const = 0;
    virtual Iterator<K, V>* NewIterator() = 0;
};

template <class K, class V>
class LinkListNode {
 public:
    LinkListNode(const K& key, V& value)  // NOLINT
        : key_(key), value_(value), next_(NULL) {}
    LinkListNode() : key_(), value_(), next_(NULL) {}
    ~LinkListNode() = default;

    // Set the next node with memory barrier
    void SetNext(LinkListNode<K, V>* node) {
        next_.store(node, std::memory_order_release);
    }

    // Set the next node without memory barrier
    void SetNextNoBarrier(LinkListNode<K, V>* node) {
        next_.store(node, std::memory_order_relaxed);
    }

    LinkListNode<K, V>* GetNext() {
        return next_.load(std::memory_order_acquire);
    }

    LinkListNode<K, V>* GetNextNoBarrier() {
        return next_.load(std::memory_order_relaxed);
    }

    V& GetValue() { return value_; }

    const K& GetKey() const { return key_; }

 private:
    K const key_;
    V value_;
    std::atomic<LinkListNode<K, V>*> next_;
};

template <class K, class V, class Comparator>
class LinkList : public BaseList<K, V> {
 public:
    explicit LinkList(Comparator cmp) : compare_(cmp) {
        head_ = new LinkListNode<K, V>();
    }
    ~LinkList() { delete head_; }

    virtual void Insert(const K& key, V& value) {  // NOLINT
        LinkListNode<K, V>* node = new LinkListNode<K, V>(key, value);
        LinkListNode<K, V>* pre = FindLessOrEqual(key);
        node->SetNextNoBarrier(pre->GetNextNoBarrier());
        pre->SetNext(node);
    }

    ListType GetType() const override { return ListType::kLinkList; }

    virtual bool IsEmpty() {
        if (head_->GetNextNoBarrier() == NULL) {
            return true;
        }
        return false;
    }

    void Clear() {
        LinkListNode<K, V>* node = head_->GetNext(0);
        head_->SetNext(NULL);
        while (node != NULL) {
            LinkListNode<K, V>* tmp = node;
            node = node->GetNext(0);
            delete tmp;
        }
    }

    virtual uint32_t GetSize() {
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
            if (next == NULL || compare_(next->GetKey(), key) > 0) {
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

    LinkListNode<K, V>* Split(const K& key) {
        LinkListNode<K, V>* target = FindLessOrEqual(key);
        if (target == NULL) {
            return NULL;
        }
        LinkListNode<K, V>* result = target->GetNext();
        target->SetNext(NULL);
        return result;
    }

    LinkListNode<K, V>* SplitByPos(uint64_t pos) {
        LinkListNode<K, V>* pos_node = head_;
        for (uint64_t idx = 0; idx < pos; idx++) {
            if (pos_node == NULL) {
                break;
            }
            pos_node = pos_node->GetNext();
        }
        if (pos_node == NULL) {
            return NULL;
        }
        LinkListNode<K, V>* result = pos_node->GetNext();
        pos_node->SetNext(NULL);
        return result;
    }

    class LinkListIterator : public Iterator<K, V> {
     public:
        explicit LinkListIterator(LinkList<K, V, Comparator>* list)
            : node_(NULL), list_(list) {}
        LinkListIterator(const LinkListIterator&) = delete;
        LinkListIterator& operator=(const LinkListIterator&) = delete;
        virtual ~LinkListIterator() {}
        virtual bool Valid() const { return node_ != NULL; }
        virtual void Next() {
            assert(Valid());
            node_ = node_->GetNext();
        }
        virtual const K& GetKey() const {
            assert(Valid());
            return node_->GetKey();
        }
        virtual V& GetValue() {
            assert(Valid());
            return node_->GetValue();
        }
        virtual void Seek(const K& k) {
            node_ = list_->FindLessThan(k);
            Next();
        }
        virtual void SeekToFirst() {
            node_ = list_->head_;
            Next();
        }
        virtual bool IsSeekable() const { return true; }

     private:
        LinkListNode<K, V>* node_;
        LinkList<K, V, Comparator>* const list_;
    };

    virtual Iterator<K, V>* NewIterator() { return new LinkListIterator(this); }

 private:
    bool IsAfterNode(const K& key, const LinkListNode<K, V>* node) const {
        return compare_(key, node->GetKey()) > 0;
    }

 private:
    Comparator const compare_;
    LinkListNode<K, V>* head_;
};

template <class K, class V>
struct ArrayListNode {
    K key_;
    V value_;
};

template <class K, class V>
struct __attribute__((__packed__)) ArraySt {
    uint16_t length_;
    ArrayListNode<K, V> buf_[];
};

const uint16_t ARRAY_HDR_LEN = sizeof(uint16_t);
#define ARRAY_HDR(s) \
    ((struct ArraySt<K, V>*)((char*)(s) - (sizeof(uint16_t))))  // NOLINT

template <class K, class V>
void ArrayListNodeDeleter(ArrayListNode<K, V>* array) {
    if (array != NULL) {
        ArraySt<K, V>* st = ARRAY_HDR(array);
        delete[] reinterpret_cast<char*>(st);
        array = NULL;
    }
}

template <class K, class V, class Comparator,
          class = typename std::enable_if<std::is_pod<K>::value &&
                                          std::is_pod<V>::value>::type>
// typename std::enable_if<std::is_pod<K>::value && std::is_pod<V>::value,
// int>::type = 0>
class ArrayList : public BaseList<K, V> {
    static_assert(sizeof(ArraySt<K, V>) == sizeof(uint16_t));

 public:
    explicit ArrayList(Comparator cmp) : compare_(cmp), array_(NULL) {}
    virtual ~ArrayList() = default;

    virtual void Insert(const K& key, V& value) {  // NOLINT
        uint32_t length = GetSize();
        uint32_t new_length = length + 1;
        ArraySt<K, V>* st =
            (ArraySt<K, V>*)new char[ARRAY_HDR_LEN +
                                     new_length * sizeof(ArrayListNode<K, V>)];
        st->length_ = (uint16_t)new_length;
        ArrayListNode<K, V>* new_array_ptr = st->buf_;
        uint32_t pos = FindLessOrEqual(key);
        new_array_ptr[pos].key_ = key;
        new_array_ptr[pos].value_ = value;
        std::shared_ptr<ArrayListNode<K, V>> array =
            std::atomic_load_explicit(&array_, std::memory_order_relaxed);
        ArrayListNode<K, V>* array_ptr = array.get();
        if (array_ptr != NULL) {
            if (length > 0) {
                if (pos > 0) {
                    memcpy(reinterpret_cast<void*>(new_array_ptr),
                           reinterpret_cast<void*>(array_ptr),
                           pos * sizeof(ArrayListNode<K, V>));
                }
                if (pos < length) {
                    memcpy(reinterpret_cast<void*>(new_array_ptr + pos + 1),
                           reinterpret_cast<void*>(array_ptr + pos),
                           (length - pos) * sizeof(ArrayListNode<K, V>));
                }
            }
        }
        std::shared_ptr<ArrayListNode<K, V>> new_array(
            new_array_ptr, ArrayListNodeDeleter<K, V>);
        std::atomic_store_explicit(&array_, new_array,
                                   std::memory_order_release);
    }

    ListType GetType() const override { return ListType::kArrayList; }

    virtual uint32_t GetSize() {
        std::shared_ptr<ArrayListNode<K, V>> array =
            std::atomic_load_explicit(&array_, std::memory_order_acquire);
        if (!array) {
            return 0;
        }
        ArraySt<K, V>* ast = ARRAY_HDR(array.get());
        return ast->length_;
    }

    virtual bool IsEmpty() {
        std::shared_ptr<ArrayListNode<K, V>> array =
            std::atomic_load_explicit(&array_, std::memory_order_acquire);
        if (array == NULL) {
            return true;
        }
        return false;
    }

    void Clear() {
        std::atomic_store_explicit(&array_,
                                   std::shared_ptr<ArrayListNode<K, V>>(),
                                   std::memory_order_release);
    }

    void Split(const K& key) {
        uint32_t length = GetSize();
        if (length == 0) {
            return;
        }
        uint32_t pos = FindLessOrEqual(key);
        if (pos == 0) {
            Clear();
            return;
        }
        if (pos < length - 1) {
            uint32_t new_length = pos;
            ArraySt<K, V>* st =
                (ArraySt<K, V>*)new char[ARRAY_HDR_LEN +
                                         new_length *
                                             sizeof(ArrayListNode<K, V>)];
            st->length_ = (uint16_t)new_length;
            ArrayListNode<K, V>* new_array_ptr = st->buf_;
            std::shared_ptr<ArrayListNode<K, V>> array =
                std::atomic_load_explicit(&array_, std::memory_order_relaxed);
            if (array) {
                ArrayListNode<K, V>* array_ptr = array.get();
                memcpy(reinterpret_cast<void*>(new_array_ptr),
                       reinterpret_cast<void*>(array_ptr),
                       pos * sizeof(ArrayListNode<K, V>));
            }
            std::shared_ptr<ArrayListNode<K, V>> new_array(
                new_array_ptr, ArrayListNodeDeleter<K, V>);
            std::atomic_store_explicit(&array_, new_array,
                                       std::memory_order_release);
        }
    }

    void SplitByPos(uint64_t pos) {
        if (pos < 1) {
            Clear();
            return;
        }
        uint32_t length = GetSize();
        if (length == 0 || pos >= length) {
            return;
        }

        uint32_t new_length = pos;
        ArraySt<K, V>* st = reinterpret_cast<ArraySt<K, V>*>(
            new char[ARRAY_HDR_LEN + new_length * sizeof(ArrayListNode<K, V>)]);
        st->length_ = (uint16_t)new_length;
        ArrayListNode<K, V>* new_array_ptr = st->buf_;
        std::shared_ptr<ArrayListNode<K, V>> array =
            std::atomic_load_explicit(&array_, std::memory_order_relaxed);
        if (array) {
            ArrayListNode<K, V>* array_ptr = array.get();
            memcpy(reinterpret_cast<void*>(new_array_ptr),
                   reinterpret_cast<void*>(array_ptr),
                   pos * sizeof(ArrayListNode<K, V>));
        }
        std::shared_ptr<ArrayListNode<K, V>> new_array(
            new_array_ptr, ArrayListNodeDeleter<K, V>);
        std::atomic_store_explicit(&array_, new_array,
                                   std::memory_order_release);
    }

    // TODO(denglong) : use binary search
    uint32_t FindLessOrEqual(const K& key) {
        std::shared_ptr<ArrayListNode<K, V>> array =
            std::atomic_load_explicit(&array_, std::memory_order_acquire);
        ArrayListNode<K, V>* array_ptr = array.get();
        uint32_t length = GetSize();
        for (uint32_t idx = 0; idx < length; idx++) {
            if (compare_(array_ptr[idx].key_, key) >= 0) {
                return idx;
            }
        }
        return length;
    }

    uint32_t FindLessThan(const K& key) {
        std::shared_ptr<ArrayListNode<K, V>> array =
            std::atomic_load_explicit(&array_, std::memory_order_acquire);
        ArrayListNode<K, V>* array_ptr = array.get();
        uint32_t length = GetSize();
        for (uint32_t idx = 0; idx < length; idx++) {
            if (compare_(array_ptr[idx].key_, key) > 0) {
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
        LinkList<K, V, Comparator>* list =
            new LinkList<K, V, Comparator>(compare_);
        std::shared_ptr<ArrayListNode<K, V>> array =
            std::atomic_load_explicit(&array_, std::memory_order_relaxed);
        ArrayListNode<K, V>* array_ptr = array.get();
        for (uint32_t idx = 0; idx < length; idx++) {
            uint32_t cur_idx = length - idx - 1;
            list->Insert(array_ptr[cur_idx].key_, array_ptr[cur_idx].value_);
        }
        return list;
    }

    class ArrayListIterator : public Iterator<K, V> {
     public:
        explicit ArrayListIterator(ArrayList<K, V, Comparator>* list)
            : pos_(-1), length_(0), array_(), compare_() {
            if (list != NULL) {
                array_ = std::atomic_load_explicit(&(list->array_),
                                                   std::memory_order_acquire);
                if (array_) {
                    ArraySt<K, V>* ast = ARRAY_HDR(array_.get());
                    length_ = ast->length_;
                }
                compare_ = list->compare_;
            }
        }
        ArrayListIterator(const ArrayListIterator&) = delete;
        ArrayListIterator& operator=(const ArrayListIterator&) = delete;
        virtual ~ArrayListIterator() {}
        virtual bool Valid() const {
            return array_ && pos_ >= 0 && pos_ < (int32_t)length_;
        }
        virtual void Next() {
            assert(Valid());
            pos_++;
        }
        virtual const K& GetKey() const {
            assert(Valid());
            ArrayListNode<K, V>* array_ptr = array_.get();
            return array_ptr[pos_].key_;
        }
        virtual V& GetValue() {
            assert(Valid());
            ArrayListNode<K, V>* array_ptr = array_.get();
            return array_ptr[pos_].value_;
        }
        virtual void Seek(const K& key) {
            if (array_) {
                ArrayListNode<K, V>* array_ptr = array_.get();
                for (uint32_t idx = 0; idx < length_; idx++) {
                    if (compare_(array_ptr[idx].key_, key) > 0) {
                        pos_ = idx;
                        break;
                    }
                }
            }
        }
        virtual bool IsSeekable() const { return true; }
        virtual void SeekToFirst() { pos_ = 0; }

     private:
        int32_t pos_;
        uint32_t length_;
        std::shared_ptr<ArrayListNode<K, V>> array_;
        Comparator compare_;
    };

    virtual Iterator<K, V>* NewIterator() {
        return new ArrayListIterator(this);
    }

 private:
    Comparator compare_;
    std::shared_ptr<ArrayListNode<K, V>> array_;
};

template <class K, class V, class Comparator>
class List {
 public:
    explicit List(Comparator cmp) : compare_(cmp) {
        list_ = new ArrayList<K, V, Comparator>(cmp);
    }
    ~List() { delete list_.load(std::memory_order_relaxed); }
    List(const List&) = delete;
    List& operator=(const List&) = delete;
    void Insert(const K& key, V& value) {  // NOLINT
        BaseList<K, V>* list = list_.load(std::memory_order_acquire);
        if (list->GetType() == ListType::kArrayList &&
            list->GetSize() >= MAX_ARRAY_LIST_LEN) {
            ArrayList<K, V, Comparator>* array_list =
                dynamic_cast<ArrayList<K, V, Comparator>*>(list);
            LinkList<K, V, Comparator>* new_list =
                array_list->ConvertToLinkList();
            if (new_list != NULL) {
                list_.store(new_list, std::memory_order_release);
                delete array_list;
            }
            new_list->Insert(key, value);
        } else {
            list->Insert(key, value);
        }
    }

    Iterator<K, V>* NewIterator() {
        return list_.load(std::memory_order_relaxed)->NewIterator();
    }

 private:
    Comparator const compare_;
    std::atomic<BaseList<K, V>*> list_;
};

}  // namespace storage
}  // namespace fesql
