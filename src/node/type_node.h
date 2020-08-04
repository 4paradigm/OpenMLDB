/*
 * node/type_node.h
 * Copyright (C) 2019 chenjing <chenjing@4paradigm.com>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef SRC_NODE_TYPE_NODE_H_
#define SRC_NODE_TYPE_NODE_H_

#include <string>
#include <vector>
#include "node/sql_node.h"
#include "vm/catalog.h"

namespace fesql {
namespace node {

class TypeNode : public SQLNode {
 public:
    TypeNode() : SQLNode(node::kType, 0, 0), base_(fesql::node::kNull) {}
    explicit TypeNode(fesql::node::DataType base)
        : SQLNode(node::kType, 0, 0), base_(base), generics_({}) {}
    explicit TypeNode(fesql::node::DataType base, const TypeNode *v1)
        : SQLNode(node::kType, 0, 0), base_(base), generics_({v1}) {}
    explicit TypeNode(fesql::node::DataType base,
                      const fesql::node::TypeNode *v1,
                      const fesql::node::TypeNode *v2)
        : SQLNode(node::kType, 0, 0), base_(base), generics_({v1, v2}) {}
    ~TypeNode() {}
    virtual const std::string GetName() const {
        std::string type_name = DataTypeName(base_);
        if (!generics_.empty()) {
            for (auto type : generics_) {
                type_name.append("_");
                type_name.append(type->GetName());
            }
        }
        return type_name;
    }

    const fesql::node::TypeNode *GetGenericType(size_t idx) const {
        return generics_[idx];
    }

    bool IsGenericNullable(size_t idx) const { return generics_nullable_[idx]; }

    size_t GetGenericSize() const { return generics_.size(); }

    fesql::node::DataType base() const { return base_; }
    const std::vector<const fesql::node::TypeNode *> &generics() const {
        return generics_;
    }

    void AddGeneric(const node::TypeNode *dtype, bool nullable) {
        generics_.push_back(dtype);
        generics_nullable_.push_back(nullable);
    }

    fesql::node::DataType base_;
    std::vector<const fesql::node::TypeNode *> generics_;
    std::vector<int> generics_nullable_;
    void Print(std::ostream &output, const std::string &org_tab) const override;
    virtual bool Equals(const SQLNode *node) const;

    bool IsArithmetic() const;
    bool IsInteger() const;
    bool IsFloating() const;
};

class OpaqueTypeNode : public TypeNode {
 public:
    explicit OpaqueTypeNode(size_t bytes)
        : TypeNode(node::kOpaque), bytes_(bytes) {}

    size_t bytes() const { return bytes_; }

    const std::string GetName() const override {
        return "opaque<" + std::to_string(bytes_) + ">";
    }

 private:
    size_t bytes_;
};

class RowTypeNode : public TypeNode {
 public:
    explicit RowTypeNode(const vm::SchemaSourceList &schema_source)
        : TypeNode(node::kRow), schema_source_(schema_source) {}

    const vm::SchemaSourceList &schema_source() const { return schema_source_; }

 private:
    const vm::SchemaSourceList schema_source_;
};

}  // namespace node
}  // namespace fesql
#endif  // SRC_NODE_TYPE_NODE_H_
