/*
 * Copyright 2021 4Paradigm
 *
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

#include "codegen/type_ir_builder.h"
#include "codegen/ir_base_builder.h"
#include "glog/logging.h"
#include "node/node_manager.h"

namespace hybridse {
namespace codegen {

bool TypeIRBuilder::IsTimestampPtr(::llvm::Type* type) {
    ::hybridse::node::DataType data_type;
    if (!IsStructPtr(type)) {
        return false;
    }

    if (!GetBaseType(type, &data_type)) {
        return false;
    }
    return data_type == node::kTimestamp;
}

bool TypeIRBuilder::IsInt64(::llvm::Type* type) {
    ::hybridse::node::DataType data_type;
    if (!GetBaseType(type, &data_type)) {
        return false;
    }
    return data_type == node::kInt64;
}

bool TypeIRBuilder::IsBool(::llvm::Type* type) {
    ::hybridse::node::DataType data_type;
    if (!GetBaseType(type, &data_type)) {
        return false;
    }
    return data_type == node::kBool;
}

bool TypeIRBuilder::IsNull(::llvm::Type* type) { return type->isTokenTy(); }

bool TypeIRBuilder::IsInterger(::llvm::Type* type) {
    return type->isIntegerTy();
}

bool TypeIRBuilder::IsNumber(::llvm::Type* type) {
    return type->isIntegerTy() || type->isFloatingPointTy();
}

bool TypeIRBuilder::isFloatPoint(::llvm::Type* type) {
    return type->isFloatingPointTy();
}

const std::string TypeIRBuilder::TypeName(::llvm::Type* type) {
    node::NodeManager tmp_node_manager;
    const node::TypeNode* type_node = nullptr;
    if (!GetFullType(&tmp_node_manager, type, &type_node)) {
        return "unknow";
    }
    return type_node->GetName();
}

bool TypeIRBuilder::IsDatePtr(::llvm::Type* type) {
    ::hybridse::node::DataType data_type;
    if (!IsStructPtr(type)) {
        return false;
    }

    if (!GetBaseType(type, &data_type)) {
        return false;
    }
    return data_type == node::kDate;
}


bool TypeIRBuilder::IsStringPtr(::llvm::Type* type) {
    ::hybridse::node::DataType data_type;
    if (!type->isPointerTy()) {
        return false;
    }

    if (!GetBaseType(type, &data_type)) {
        return false;
    }
    return data_type == node::kVarchar;
}

bool TypeIRBuilder::IsStructPtr(::llvm::Type* type) {
    if (type->getTypeID() == ::llvm::Type::PointerTyID) {
        type = reinterpret_cast<::llvm::PointerType*>(type)->getElementType();
        if (type->isStructTy()) {
            DLOG(INFO) << "Struct Name " << type->getStructName().str();
            return true;
        } else {
            DLOG(INFO) << "Isn't Struct Type";
            return false;
        }
    }
    return false;
}

base::Status TypeIRBuilder::UnaryOpTypeInfer(
    const std::function<base::Status(node::NodeManager*, const node::TypeNode*, const node::TypeNode**)> func,
    ::llvm::Type* lhs) {
    node::NodeManager tmp_node_manager;
    const node::TypeNode* left_type = nullptr;
    CHECK_TRUE(GetFullType(&tmp_node_manager, lhs, &left_type), common::kTypeError, "invalid op type")
    const node::TypeNode* output_type;
    CHECK_STATUS(func(&tmp_node_manager, left_type, &output_type))
    return base::Status::OK();
}

base::Status TypeIRBuilder::BinaryOpTypeInfer(
    const std::function<base::Status(node::NodeManager*, const node::TypeNode*, const node::TypeNode*,
                                     const node::TypeNode**)>
        func,
    ::llvm::Type* lhs, ::llvm::Type* rhs) {
    const node::TypeNode* left_type = nullptr;
    const node::TypeNode* right_type = nullptr;
    node::NodeManager tmp_node_manager;
    CHECK_TRUE(GetFullType(&tmp_node_manager, lhs, &left_type), common::kTypeError, "invalid op type")
    CHECK_TRUE(GetFullType(&tmp_node_manager, rhs, &right_type), common::kTypeError, "invalid op type")
    const node::TypeNode* output_type = nullptr;
    CHECK_STATUS(func(&tmp_node_manager, left_type, right_type, &output_type))
    return base::Status::OK();
}

}  // namespace codegen
}  // namespace hybridse
