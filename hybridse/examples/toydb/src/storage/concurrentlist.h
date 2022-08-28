//
// Created by xldream on 2022/8/19.
//
#pragma once
#include <memory>
#include <atomic>

constexpr uint16_t MAX_ARRAY_LIST_LEN = 1000;
namespace ConcurrentList {
template <typename ValueType>
class ConcurrentList {
 public:
    struct ListNode {
        ValueType data;
        std::atomic<ListNode*> next;
        ListNode(const ValueType& __data) {
            this->data = __data;
            this->next = nullptr;
        }
    };

    struct iterator {
        ~iterator() = default;

        iterator(const iterator& __iter) noexcept {
            node_ptr = __iter.get_node_ptr();
        }

        iterator(iterator&& __iter) noexcept {
            node_ptr = __iter.node_ptr;
            __iter.node_ptr = nullptr;
        }

        iterator& operator=(const iterator& __iter) noexcept {
            node_ptr = __iter.node_ptr;
            return *this;
        }


        iterator& operator=(iterator&& __iter) noexcept {
            node_ptr = __iter.node_ptr;
            __iter.node_ptr = nullptr;
            return *this;
        }

        iterator()
            : node_ptr(nullptr)
        {}

        explicit iterator(std::nullptr_t)
            : node_ptr(nullptr)
        {}

        explicit iterator(ListNode* __node_ptr)
            : node_ptr(__node_ptr)
        {}

        ValueType& operator*() {
            return (node_ptr->data);
        }

        iterator& operator++(int) {
            iterator copy(*this);
            node_ptr = node_ptr->next.load();
            return copy;
        }

        iterator& operator++() {
            node_ptr = node_ptr->next.load();
            return *this;
        }

        iterator& operator+(int __n) {
            int node_count = 0;
            while(node_count < __n) {
                node_ptr = node_ptr->next.load();
                ++node_count;
            }
            return *this;
        }

        bool operator==(const iterator& __iter) {
            return node_ptr == __iter.node_ptr;
        }

        bool operator!=(const iterator& __iter) {
            return node_ptr != __iter.node_ptr;
        }

        ListNode* node_ptr;
    };

    ConcurrentList(): _m_head(nullptr)
    {}

    void push_back(ValueType& __data) {
        _push(__data);
    }

    iterator begin() {
        return iterator(_m_head);
    }

    iterator end() {
        return iterator(nullptr);
    }

    uint32_t GetSize() {
        uint32_t cnt = 0;
        ListNode* node = _m_head.load();
        while (node != NULL) {
            cnt++;
            ListNode* tmp = node->next;
            if (tmp == NULL) {
                break;
            }
            node = tmp;
        }
        return cnt;
    }

    void GC() {
        if (GetSize() > MAX_ARRAY_LIST_LEN) {
            uint32_t cnt = 0;
            ListNode* node = _m_head.load();
            while(node != NULL) {
                if (cnt > MAX_ARRAY_LIST_LEN) {
                    ListNode* delete_node = node->next;
                    node->next = nullptr;
                    while (delete_node != NULL) {
                        ListNode* tmp1 = delete_node;
                        delete_node = delete_node->next;
                        delete tmp1;
                    }
                }
                cnt++;
                ListNode* tmp = node->next;
                node = tmp;
            }
        }
    }


 public:
    ~ConcurrentList() = default;
    ConcurrentList(const ConcurrentList&) noexcept = delete;
    ConcurrentList& operator=(const ConcurrentList&) noexcept = delete;
    ConcurrentList(ConcurrentList&&) noexcept = delete;
    ConcurrentList& operator=(ConcurrentList &&) noexcept = delete;

 private:
    template<typename T>
    void _push(T v) {
        auto new_node = new ListNode(v);

        auto curr_head = _m_head.load();
        new_node->next = curr_head;
        while(!_m_head.compare_exchange_weak(curr_head, new_node,
                                              std::memory_order_acq_rel)) {
            curr_head = _m_head.load();
            new_node->next = curr_head;
        }
    }
    std::atomic<ListNode*> _m_head;
};
}