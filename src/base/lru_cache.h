//---------------------------------------------------------------------------//
// Copyright (c) 2013 Kyle Lutz <kyle.r.lutz@gmail.com>
//
// Distributed under the Boost Software License, Version 1.0
// See accompanying file LICENSE_1_0.txt or copy at
// http://www.boost.org/LICENSE_1_0.txt
//
// See http://boostorg.github.com/compute for more information.
//---------------------------------------------------------------------------//

#ifndef SRC_BASE_LRU_CACHE_H_
#define SRC_BASE_LRU_CACHE_H_

#include <list>
#include <map>
#include <utility>
#include <boost/optional.hpp>
#include <boost/optional/optional_io.hpp>

namespace openmldb::base {

// a cache which evicts the least recently used item when it is full
// we need a new lru_cache, cuz we want a method `upsert` that can `update` the value of existed key and the
// recently used list
template <class Key, class Value>
class lru_cache {
 public:
    typedef Key key_type;
    typedef Value value_type;
    typedef std::list<key_type> list_type;
    typedef std::map<key_type, std::pair<value_type, typename list_type::iterator> > map_type;

    explicit lru_cache(size_t capacity) : m_capacity(capacity) {}

    ~lru_cache() = default;

    size_t size() const { return m_map.size(); }

    size_t capacity() const { return m_capacity; }

    bool empty() const { return m_map.empty(); }

    bool contains(const key_type &key) { return m_map.find(key) != m_map.end(); }

    void upsert(const key_type &key, const value_type &value) {
        typename map_type::iterator i = m_map.find(key);
        if (i == m_map.end()) {
            // insert item into the cache, but first check if it is full
            if (size() >= m_capacity) {
                // cache is full, evict the least recently used item
                evict();
            }

            // insert the new item
            m_list.push_front(key);
            m_map[key] = std::make_pair(value, m_list.begin());
        } else {
            typename list_type::iterator j = i->second.second;
            if (j != m_list.begin()) {
                // move to the front
                m_list.erase(j);
                m_list.push_front(key);
                j = m_list.begin();
            }
            // update the value too
            m_map[key] = std::make_pair(value, j);
        }
    }

    boost::optional<value_type> get(const key_type &key) {
        // lookup value in the cache
        typename map_type::iterator i = m_map.find(key);
        if (i == m_map.end()) {
            // value not in cache
            return boost::none;
        }

        // return the value, but first update its place in the most
        // recently used list
        typename list_type::iterator j = i->second.second;
        if (j != m_list.begin()) {
            // move item to the front of the most recently used list
            m_list.erase(j);
            m_list.push_front(key);

            // update iterator in map
            j = m_list.begin();
            const value_type &value = i->second.first;
            m_map[key] = std::make_pair(value, j);

            // return the value
            return value;
        } else {
            // the item is already at the front of the most recently
            // used list so just return it
            return i->second.first;
        }
    }

    void clear() {
        m_map.clear();
        m_list.clear();
    }

 private:
    void evict() {
        // evict item from the end of most recently used list
        typename list_type::iterator i = --m_list.end();
        m_map.erase(*i);
        m_list.erase(i);
    }

 private:
    map_type m_map;
    list_type m_list;
    size_t m_capacity;
};

}  // namespace openmldb::base

#endif  // SRC_BASE_LRU_CACHE_H_
