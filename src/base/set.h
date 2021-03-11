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


#ifndef SRC_BASE_SET_H_
#define SRC_BASE_SET_H_

#include <mutex> // NOLINT
#include <set>
namespace rtidb {
namespace base {
// thread_safe set
template <class T>
class set {
 public:
    set() : set_(), mu_() {}
    set(const set&) = delete;
    set& operator=(const set&) = delete;

    void insert(const T& value) {
        std::lock_guard<std::mutex> lock(mu_);
        set_.insert(value);
    }

    void erase(const T& value) {
        std::lock_guard<std::mutex> lock(mu_);
        set_.erase(value);
    }

    bool contain(const T& value) {
        std::lock_guard<std::mutex> lock(mu_);
        return set_.find(value) != set_.end();
    }

 private:
    std::set<T> set_;
    std::mutex mu_;
};

}  // namespace base
}  // namespace rtidb
#endif  // SRC_BASE_SET_H_
