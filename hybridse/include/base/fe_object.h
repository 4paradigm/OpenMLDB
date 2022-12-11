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

#ifndef HYBRIDSE_INCLUDE_BASE_FE_OBJECT_H_
#define HYBRIDSE_INCLUDE_BASE_FE_OBJECT_H_

#include <type_traits>
#include <vector>

namespace hybridse {
namespace base {

class FeBaseObject {
 public:
    virtual ~FeBaseObject() {}
};

// NoeList
// list of base objects whose lifecycles (includes list itself) are managed elsewhere
template <typename T, std::enable_if_t<std::is_base_of_v<base::FeBaseObject, T>, int> = 0>
class BaseList : public base::FeBaseObject {
 public:
    using size_t = decltype(sizeof(int));

    void SetNodeId(size_t id) { node_id_ = id; }

    std::vector<T*> data_;
    size_t node_id_ = 0;
};

}  // namespace base
}  // namespace hybridse
#endif  // HYBRIDSE_INCLUDE_BASE_FE_OBJECT_H_
