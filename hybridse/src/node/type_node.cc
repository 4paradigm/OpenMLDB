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

#include "absl/strings/ascii.h"
#include "absl/strings/str_join.h"
#include "absl/strings/str_cat.h"
#include "node/node_manager.h"

namespace hybridse {
namespace node {

bool operator==(const TypeNode& lhs, const TypeNode& rhs) {
    return lhs.Equals(&rhs);
}
bool TypeNode::IsBaseOrNullType() const {
    return IsNull() || IsBaseType();
}

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

// Better function name ? Note the difference of VOID and NULL, VOID is a data type
// while NULL is a placeholder for missing or unknown information, not a real data type.
bool TypeNode::IsNull() const { return base_ == node::kNull || base_ == node::kVoid; }

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

std::string TypeNode::DebugString() const {
    return absl::StrCat(absl::AsciiStrToUpper(DataTypeName(base_)),
                        generics_.empty() ? ""
                                          : absl::StrCat("<",
                                                         absl::StrJoin(generics_, ",",
                                                                       [](std::string *out, const TypeNode *type) {
                                                                           absl::StrAppend(out, type->DebugString());
                                                                       }),
                                                         ">"));
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

// {base_name}<element_type>(num_elements)
std::string FixedArrayType::DebugString() const {
    return absl::StrCat(TypeNode::DebugString(), "(", num_elements_, ")");
}

// {base_name}_{generics}_...{num_elements}
const std::string FixedArrayType::GetName() const { return absl::StrCat(TypeNode::GetName(), "_", num_elements_); }

FixedArrayType *FixedArrayType::ShadowCopy(NodeManager *nm) const {
    return nm->MakeArrayType(element_type(), num_elements_);
}

void TypeNode::AddGeneric(const node::TypeNode *dtype, bool nullable) {
    generics_.push_back(dtype);
    generics_nullable_.push_back(nullable);
}
const hybridse::node::TypeNode *TypeNode::GetGenericType(size_t idx) const { return generics_[idx]; }
const std::string TypeNode::GetName() const {
    std::string type_name = DataTypeName(base_);
    if (!generics_.empty()) {
        for (auto type : generics_) {
            type_name.append("_");
            type_name.append(type->GetName());
        }
    }
    return type_name;
}

void TypeNode::Print(std::ostream &output, const std::string &org_tab) const {
    SqlNode::Print(output, org_tab);
    const std::string tab = org_tab + INDENT + SPACE_ED;

    output << "\n";
    PrintValue(output, tab, GetName(), "type", true);
}
bool TypeNode::Equals(const SqlNode *node) const {
    if (!SqlNode::Equals(node)) {
        return false;
    }

    const TypeNode *that = dynamic_cast<const TypeNode *>(node);
    return this->base_ == that->base_ && this->generics_.size() == that->generics_.size() &&
           std::equal(
               this->generics_.cbegin(), this->generics_.cend(), that->generics_.cbegin(),
               [&](const hybridse::node::TypeNode *a, const hybridse::node::TypeNode *b) { return TypeEquals(a, b); });
}

const std::string OpaqueTypeNode::GetName() const { return "opaque<" + std::to_string(bytes_) + ">"; }

MapType::MapType(const TypeNode *key_ty, const TypeNode *value_ty, bool value_not_null) : TypeNode(node::kMap) {
    // map key does not accept null, value is nullable unless extra attributes specified
    AddGeneric(key_ty, false);
    AddGeneric(value_ty, !value_not_null);
}
MapType::~MapType() {}
const TypeNode *MapType::key_type() const { return GetGenericType(0); }
const TypeNode *MapType::value_type() const { return GetGenericType(1); }
bool MapType::value_nullable() const { return IsGenericNullable(1); }

// MAP<KEY, VALUE>
// 1. ALL KEYs or VALUEs must share a least common type.
// 2. KEY is simple type only: void/bool/numeric/data/timestamp/string
// 3. Resolve to MAP<VOID, VOID> if arguments is empty
absl::StatusOr<MapType *> MapType::InferMapType(NodeManager* nm, absl::Span<const ExprAttrNode> types) {
    if (types.size() % 2 != 0) {
        return absl::InvalidArgumentError("map expects a positive even number of arguments");
    }

    const node::TypeNode* key = nm->MakeNode<TypeNode>();  // void type
    const node::TypeNode* value = nm->MakeNode<TypeNode>();  // void type
    for (size_t i = 0; i < types.size(); i += 2) {
        if (!types[i].type()->IsBaseOrNullType()) {
            return absl::FailedPreconditionError(
                absl::StrCat("key type for map should be void/bool/numeric/data/timestamp/string only, got ",
                             types[i].type()->DebugString()));
        }
        auto key_res = node::ExprNode::CompatibleType(nm, key, types[i].type());
        if (!key_res.ok()) {
            return key_res.status();
        }
        key = key_res.value();
        auto value_res = node::ExprNode::CompatibleType(nm, value, types[i + 1].type());
        if (!value_res.ok()) {
            return value_res.status();
        }
        value = value_res.value();
    }

    if (!types.empty() && (key->base() == kVoid || value->base() == kVoid)) {
        // only empty map resolved to MAP<VOID,VOID>
        return absl::FailedPreconditionError("KEY/VALUE type of non-empty map can't be VOID");
    }

    return nm->MakeNode<MapType>(key, value);
}

}  // namespace node
}  // namespace hybridse
