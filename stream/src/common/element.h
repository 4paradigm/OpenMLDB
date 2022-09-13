/*
 * Copyright 2021 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef STREAM_SRC_COMMON_ELEMENT_H_
#define STREAM_SRC_COMMON_ELEMENT_H_

#include <absl/strings/str_cat.h>
#include <atomic>
#include <iostream>
#include <limits>
#include <memory>
#include <string>
#include <utility>
#include "common/function.h"

namespace streaming {
namespace interval_join {

enum class ElementType { kBase, kProbe, kUnknown };

class Element {
 public:
    Element(const std::string& key, const std::string& val, int64_t ts, ElementType type = ElementType::kUnknown)
        : key_(key), val_(val), ts_(ts), type_(type) {}
    Element(const Element& element)
        : key_(element.key_),
          val_(element.val_),
          ts_(element.ts_),
          type_(element.type_),
          pid_(element.pid_),
          sid_(element.sid_),
          id_(element.id_.load()),
          hash_(element.hash_),
          func_(std::move(element.func_)) {}
    Element(Element&& element)
        : key_(std::move(element.key_)),
          val_(std::move(element.val_)),
          ts_(element.ts_),
          type_(element.type_),
          pid_(element.pid_),
          sid_(element.sid_),
          id_(element.id_.load()),
          hash_(element.hash_),
          func_(std::move(element.func_)) {}
    Element() {}

    virtual Element& operator=(const Element& element) {
        key_ = element.key_;
        val_ = element.val_;
        ts_ = element.ts_;
        type_ = element.type_;
        pid_ = element.pid_;
        sid_ = element.sid_;
        id_ = element.id_.load();
        hash_ = element.hash_;
        func_ = element.func_;
        return *this;
    }

    virtual ~Element() {}

    std::string ToString() const {
        return absl::StrCat(key_, "|", ts_, ":", val_, " (type:", type(), ",pid:", pid(), ",id:", id(), ")");
    }

    friend std::ostream& operator<<(std::ostream& os, const Element& ele) {
        return os << ele.ToString() << std::endl;
    }

    const std::string& key() const { return key_; }

    int64_t ts() const { return ts_; }

    const std::string& value() const { return val_; }

    void set_type(ElementType type) { type_ = type; }

    ElementType type() const { return type_; }

    virtual std::shared_ptr<Element> Clone() const {
        return std::make_shared<Element>(*this);
    }

    virtual std::shared_ptr<Element> Clone() {
        return std::make_shared<Element>(std::move(*this));
    }

    inline void set_pid(int pid) {
        pid_ = pid;
    }

    inline int pid() const {
        return pid_;
    }

    inline void set_sid(int64_t sid) {
        sid_ = sid;
    }

    inline int sid() const {
        return sid_;
    }

    inline void set_id(int64_t id) {
        id_.store(id, std::memory_order_relaxed);
    }

    inline int64_t id() const {
        return id_.load(std::memory_order_relaxed);
    }

    inline int hash() const {
        return hash_;
    }

    inline void set_hash(int hash) {
        hash_ = hash;
    }

    inline int64_t read_time() const {
        return read_time_;
    }

    inline void set_read_time(int64_t read_time) {
        read_time_ = read_time;
    }

    inline int64_t output_time() const {
        return output_time_;
    }

    inline void set_output_time(int64_t output_time) {
        output_time_ = output_time;
    }

    inline int64_t join_count() const {
        return join_count_;
    }

    inline void increment_join_count() {
        join_count_++;
    }

    inline double effectiveness() const {
        return effectiveness_;
    }

    inline void set_effectiveness(double effectiveness) {
        effectiveness_ = effectiveness;
    }

    virtual size_t size() const {
        return val_.size();
    }

    inline const char* data() const {
        return val_.data();
    }

    inline void set_func(const std::shared_ptr<Function>& func) {
        func_ = func;
    }

    inline Function* func() const {
        return func_.get();
    }

    // for back-compatability of openmldb skip-list API
    uint8_t dim_cnt_down = 1;

 private:
    std::string key_;
    std::string val_;
    int64_t ts_;
    ElementType type_ = ElementType::kUnknown;
    int pid_ = 0;  // the target joiner id
    int sid_ = 0;  // the source partitioner id
    std::atomic<int64_t> id_ = std::numeric_limits<int64_t>::max();  // the id of element
    int hash_ = 0;
    int64_t read_time_ = -1;  // the time when element is read
    int64_t output_time_ = -1;  // the time when element is processed and outputted
    int64_t join_count_ = 0;  // the number of times being joined in time window
    double effectiveness_ = -1;  // the ratio of elements in time window and elements visited
    std::shared_ptr<Function> func_;
};

}  // namespace interval_join
}  // namespace streaming

#endif  // STREAM_SRC_COMMON_ELEMENT_H_
