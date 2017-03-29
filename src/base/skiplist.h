//
// skiplist.h 
// Copyright 2017 elasticlog <elasticlog01@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef RTIDB_SKIPLIST_H
#define RTIDB_SKIPLIST_H

#include <stdint.h>
#include <boost/atomic.hpp>

namespace rtidb {
namespace base {

// Skiplist node , a thread safe structure 
template<typename T>
class Node {

public:
    Node(const T& data, int32_t height):data_(data), 
    nexts_(NULL),
    height_(height){
        nexts_ = new boost::atomic<T>[height];
    }
    ~Node() {}

    // Set the next reference
    void SetNext(int32_t level, const Node* node) {
        assert(level < height_);
        
    }

private:
    T const data_;
    boost::atomic<T>* nexts_[0];
    int32_t height_;
};

}// base
}// rtidb
#endif /* !SKIPLIST_H */
