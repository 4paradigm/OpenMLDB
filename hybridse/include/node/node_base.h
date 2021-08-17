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

#ifndef INCLUDE_NODE_NODE_BASE_H_
#define INCLUDE_NODE_NODE_BASE_H_

#include <glog/logging.h>
#include <sstream>
#include <string>

#include "base/fe_object.h"
#include "node/node_enum.h"

namespace hybridse {
namespace node {

class NodeManager;

template <typename T>
class NodeBase : public base::FeBaseObject {
 public:
    virtual ~NodeBase() {}

    virtual const std::string GetTypeName() const = 0;

    virtual uint32_t GetLineNum() const { return 0; }

    virtual uint32_t GetLocation() const { return 0; }

    virtual bool Equals(const T* other) const = 0;

    friend std::ostream& operator<<(std::ostream& output,
                                    const NodeBase<T>& thiz) {
        thiz.Print(output, "");
        return output;
    }

    virtual void Print(std::ostream& output, const std::string& tab) const {
        output << tab << SPACE_ST << "node[" << GetTypeName() << "]";
    }

    virtual std::string GetTreeString() const {
        std::stringstream ss;
        this->Print(ss, "");
        return ss.str();
    }

    virtual std::string GetFlatString() const {
        std::stringstream ss;
        ss << "node[" << GetTypeName() << "]";
        return ss.str();
    }

    static bool Equals(const T* lhs, const T* rhs) {
        if (lhs == rhs) {
            return true;
        } else if (lhs == nullptr) {
            return rhs == nullptr;
        } else if (rhs == nullptr) {
            return false;
        } else {
            return lhs->Equals(rhs);
        }
    }

    virtual T* ShadowCopy(NodeManager*) const { return nullptr; }

    virtual T* DeepCopy(NodeManager*) const { return nullptr; }

    virtual bool UpdateChild(size_t idx, T* new_child) { return false; }

    size_t node_id() const { return node_id_; }

 protected:
    NodeBase<T>() = default;

 private:
    friend class NodeManager;
    NodeBase<T>& operator=(const NodeBase<T>&) = default;
    NodeBase(const NodeBase<T>&) = default;

    // unique index
    size_t node_id_ = 0;
    void SetNodeId(size_t id) { node_id_ = id; }
};

}  // namespace node
}  // namespace hybridse
#endif  // INCLUDE_NODE_NODE_BASE_H_
