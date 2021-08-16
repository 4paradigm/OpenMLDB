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

#ifndef INCLUDE_NODE_TYPE_NODE_H_
#define INCLUDE_NODE_TYPE_NODE_H_

#include <string>
#include <vector>
#include "codec/fe_row_codec.h"
#include "node/sql_node.h"
#include "vm/schemas_context.h"

namespace hybridse {
namespace node {

class NodeManager;

class TypeNode : public SqlNode {
 public:
    TypeNode() : SqlNode(node::kType, 0, 0), base_(hybridse::node::kNull) {}
    explicit TypeNode(hybridse::node::DataType base)
        : SqlNode(node::kType, 0, 0), base_(base), generics_({}) {}
    explicit TypeNode(hybridse::node::DataType base, const TypeNode *v1)
        : SqlNode(node::kType, 0, 0),
          base_(base),
          generics_({v1}),
          generics_nullable_({false}) {}
    explicit TypeNode(hybridse::node::DataType base,
                      const hybridse::node::TypeNode *v1,
                      const hybridse::node::TypeNode *v2)
        : SqlNode(node::kType, 0, 0),
          base_(base),
          generics_({v1, v2}),
          generics_nullable_({false, false}) {}
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

    const hybridse::node::TypeNode *GetGenericType(size_t idx) const {
        return generics_[idx];
    }

    bool IsGenericNullable(size_t idx) const { return generics_nullable_[idx]; }

    size_t GetGenericSize() const { return generics_.size(); }

    hybridse::node::DataType base() const { return base_; }
    const std::vector<const hybridse::node::TypeNode *> &generics() const {
        return generics_;
    }

    void AddGeneric(const node::TypeNode *dtype, bool nullable) {
        generics_.push_back(dtype);
        generics_nullable_.push_back(nullable);
    }

    hybridse::node::DataType base_;
    std::vector<const hybridse::node::TypeNode *> generics_;
    std::vector<int> generics_nullable_;
    void Print(std::ostream &output, const std::string &org_tab) const override;
    virtual bool Equals(const SqlNode *node) const;
    TypeNode *ShadowCopy(NodeManager *) const override;
    TypeNode *DeepCopy(NodeManager *) const override;

    bool IsBaseType() const;
    bool IsTuple() const;
    bool IsTupleNumbers() const;
    bool IsString() const;
    bool IsTimestamp() const;
    bool IsDate() const;
    bool IsArithmetic() const;
    bool IsNumber() const;
    bool IsIntegral() const;
    bool IsInteger() const;
    bool IsNull() const;
    bool IsBool() const;
    bool IsFloating() const;
    static Status CheckTypeNodeNotNull(const TypeNode *left_type);
    bool IsGeneric() const;
};

class OpaqueTypeNode : public TypeNode {
 public:
    explicit OpaqueTypeNode(size_t bytes)
        : TypeNode(node::kOpaque), bytes_(bytes) {}

    size_t bytes() const { return bytes_; }

    const std::string GetName() const override {
        return "opaque<" + std::to_string(bytes_) + ">";
    }

    OpaqueTypeNode *ShadowCopy(NodeManager *) const override;

 private:
    size_t bytes_;
};

class RowTypeNode : public TypeNode {
 public:
    // Initialize with external schemas context
    explicit RowTypeNode(const vm::SchemasContext *schemas_ctx);

    // Initialize with schema
    explicit RowTypeNode(const std::vector<const codec::Schema *> &schemas);

    ~RowTypeNode();

    const vm::SchemasContext *schemas_ctx() const { return schemas_ctx_; }

    RowTypeNode *ShadowCopy(NodeManager *) const override;

 private:
    bool IsOwnedSchema() const { return is_own_schema_ctx_; }

    // if initialized without a physical node context
    // hold a self-owned schemas context
    const vm::SchemasContext *schemas_ctx_;
    bool is_own_schema_ctx_;
};

}  // namespace node
}  // namespace hybridse
#endif  // INCLUDE_NODE_TYPE_NODE_H_
