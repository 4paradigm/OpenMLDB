
#pragma once
#include <assert.h>
#include <memory>
#include <atomic>

namespace hybridse {
    namespace storage {

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

            class ListIterator {
            public:
                ListIterator(ConcurrentList<K, V, Comparator>*  list) : node_(NULL), list_(list) {}
                ListIterator(const ListIterator&) = default;
                ListIterator& operator=(const ListIterator&) = delete;
                ~ListIterator() = default;

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

                ListIterator& operator++(int) {
                    ListIterator copy(*this);
                    node_ = node_->GetNext();
                    return copy;
                }

                ListIterator& operator++() {
                    node_ = node_->GetNext();
                    return *this;
                }

                ListIterator& operator+(int __n) {
                    int node_count = 0;
                    while(node_count < __n) {
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

            uint32_t GetSize() {
                uint32_t cnt = 0;
                ListNode<K, V>* node = head_.load();
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
                    ListNode<K, V> *node = head_.load();
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
            std::atomic<ListNode<K, V> *> head_;
        };
    }
}
