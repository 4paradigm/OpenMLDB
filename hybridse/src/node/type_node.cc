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

#include "node/type_node.h"
#include "node/node_manager.h"
#include "vm/physical_op.h"

namespace hybridse {
namespace node {

bool TypeNode::IsBaseType() const {
    return IsNumber() || IsString() || IsTimestamp() || IsDate();
}
bool TypeNode::IsDate() const { return base_ == node::kDate; }
bool TypeNode::IsTuple() const { return base_ == node::kTuple; }
bool TypeNode::IsTupleNumbers() const {
    if (!IsTuple()) {
        return false;
    }
    for (auto type : generics_) {
        if (!type->IsNumber()) {
            return false;
        }
    }
    return true;
}
bool TypeNode::IsTimestamp() const { return base_ == node::kTimestamp; }
bool TypeNode::IsString() const { return base_ == node::kVarchar; }
bool TypeNode::IsArithmetic() const { return IsInteger() || IsFloating(); }
bool TypeNode::IsNumber() const { return IsInteger() || IsFloating(); }
bool TypeNode::IsNull() const { return base_ == node::kNull; }
bool TypeNode::IsBool() const { return base_ == node::kBool; }

bool TypeNode::IsIntegral() const {
    return base_ == node::kInt16 || base_ == node::kInt32 || base_ == node::kInt64;
}

bool TypeNode::IsInteger() const {
    return base_ == node::kBool || base_ == node::kInt16 ||
           base_ == node::kInt32 || base_ == node::kInt64;
}

bool TypeNode::IsFloating() const {
    return base_ == node::kFloat || base_ == node::kDouble;
}

bool TypeNode::IsGeneric() const { return !generics_.empty(); }

Status TypeNode::CheckTypeNodeNotNull(const TypeNode *left_type) {
    CHECK_TRUE(nullptr != left_type, common::kTypeError, "null type node");
    return Status::OK();
}

TypeNode *TypeNode::ShadowCopy(NodeManager *nm) const {
    auto type_node = nm->MakeTypeNode(base_);
    type_node->generics_ = this->generics_;
    return type_node;
}

TypeNode *TypeNode::DeepCopy(NodeManager *nm) const {
    // For type node, it is always immutable thus
    // deep copy can be just implemented as shadow copy.
    return ShadowCopy(nm);
}

RowTypeNode::RowTypeNode(const vm::SchemasContext *schemas_ctx)
    : TypeNode(node::kRow),
      schemas_ctx_(schemas_ctx),
      is_own_schema_ctx_(false) {}

RowTypeNode::RowTypeNode(const std::vector<const codec::Schema *> &schemas)
    : TypeNode(node::kRow),
      schemas_ctx_(new vm::SchemasContext()),
      is_own_schema_ctx_(true) {
    auto schemas_ctx = const_cast<vm::SchemasContext *>(schemas_ctx_);
    schemas_ctx->BuildTrivial(schemas);
}

RowTypeNode::~RowTypeNode() {
    if (IsOwnedSchema()) {
        delete const_cast<vm::SchemasContext *>(schemas_ctx_);
    }
}

RowTypeNode *RowTypeNode::ShadowCopy(NodeManager *nm) const {
    return nm->MakeRowType(this->schemas_ctx());
}

OpaqueTypeNode *OpaqueTypeNode::ShadowCopy(NodeManager *nm) const {
    return nm->MakeOpaqueType(bytes_);
}

}  // namespace node
}  // namespace hybridse
